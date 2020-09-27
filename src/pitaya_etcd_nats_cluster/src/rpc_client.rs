use crate::settings;
use async_trait::async_trait;
use pitaya_core::{
    cluster::{Error, RpcClient, ServerId, ServerInfo, ServerKind},
    context, message, protos, utils,
};
use prost::Message;
use slog::{error, info, trace};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct NatsRpcClient {
    settings: Arc<settings::Nats>,
    connection: Arc<RwLock<Option<nats::Connection>>>,
    logger: slog::Logger,
    server_info: Arc<ServerInfo>,
}

impl NatsRpcClient {
    pub fn new(
        logger: slog::Logger,
        settings: Arc<settings::Nats>,
        server_info: Arc<ServerInfo>,
    ) -> Self {
        Self {
            settings,
            connection: Arc::new(RwLock::new(None)),
            logger,
            server_info,
        }
    }
}

#[async_trait]
impl RpcClient for NatsRpcClient {
    async fn start(&self) -> Result<(), Error> {
        if self.connection.read().await.is_some() {
            return Err(Error::AlreadyConnected);
        }

        info!(self.logger, "client connecting to nats"; "url" => &self.settings.url);
        let nc = nats::ConnectionOptions::new()
            .max_reconnects(Some(self.settings.max_reconnection_attempts as usize))
            .connect(&self.settings.url)
            .map_err(Error::Nats)?;
        self.connection.write().await.replace(nc);
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Error> {
        if let Some(conn) = self.connection.write().await.take() {
            conn.close();
        }
        Ok(())
    }

    async fn call(
        &self,
        ctx: context::Context,
        rpc_type: protos::RpcType,
        msg: message::Message,
        target: Arc<ServerInfo>,
    ) -> Result<protos::Response, Error> {
        trace!(self.logger, "NatsRpcClient::call");
        let connection = self
            .connection
            .read()
            .await
            .as_ref()
            .cloned()
            .ok_or(Error::NatsConnectionNotOpen)?;

        let req = utils::build_request(ctx, rpc_type, msg, self.server_info.clone())
            .map_err(|e| Error::Internal(e.to_string()))?;
        let topic = utils::topic_for_server(&target);
        let buffer = utils::encode_proto(&req);

        trace!(
            self.logger,
            "sending nats request"; "topic" => &topic, "timeout" => self.settings.request_timeout.as_secs()
        );

        let request_timeout = self.settings.request_timeout;

        // We do a spawn_blocking here, since it otherwise will block the executor thread.
        let res = tokio::task::spawn_blocking(move || -> Result<protos::Response, Error> {
            let message = connection
                .request_timeout(&topic, buffer, request_timeout)
                .map_err(Error::Nats)?;
            let msg: protos::Response =
                Message::decode(message.data.as_ref()).map_err(Error::InvalidServerResponse)?;
            Ok(msg)
        })
        .await;

        match res {
            Err(join_error) => {
                error!(self.logger, "failed to join rpc task"; "error" => %join_error);
                Err(Error::Internal("failed to join rpc task".to_owned()))
            }
            Ok(Err(err)) => Err(err),
            Ok(Ok(r)) => Ok(r),
        }
    }

    async fn kick_user(
        &self,
        // NOTE: Ignore server_id, since it is not necessary to create the topic.
        _server_id: ServerId,
        server_kind: ServerKind,
        kick_msg: protos::KickMsg,
    ) -> Result<protos::KickAnswer, Error> {
        trace!(self.logger, "NatsRpcClient::kick_user");
        let connection = self
            .connection
            .read()
            .await
            .as_ref()
            .cloned()
            .ok_or(Error::NatsConnectionNotOpen)?;

        if kick_msg.user_id.is_empty() {
            return Err(Error::EmptyUserId);
        }

        if server_kind.0.is_empty() {
            return Err(Error::EmptyServerKind);
        }

        let request_timeout = self.settings.request_timeout;
        let res = tokio::task::spawn_blocking(move || -> Result<protos::KickAnswer, Error> {
            let topic = utils::user_kick_topic(&kick_msg.user_id, &server_kind);
            let kick_buffer = utils::encode_proto(&kick_msg);

            let message = connection
                .request_timeout(&topic, kick_buffer, request_timeout)
                .map_err(Error::Nats)?;

            let k: protos::KickAnswer =
                Message::decode(&message.data[..]).map_err(Error::InvalidServerResponse)?;
            Ok(k)
        })
        .await;

        match res {
            Err(join_error) => {
                error!(self.logger, "failed to join kick task"; "error" => %join_error);
                Err(Error::Internal("failed to join kick task".to_owned()))
            }
            Ok(Err(err)) => Err(err),
            Ok(Ok(r)) => Ok(r),
        }
    }

    async fn push_to_user(
        &self,
        // NOTE: we ignore the server id, since it is not necessary to create the topic.
        _server_id: ServerId,
        server_kind: ServerKind,
        push_msg: protos::Push,
    ) -> Result<(), Error> {
        trace!(self.logger, "NatsRpcClient::push_to_user");
        let connection = self
            .connection
            .read()
            .await
            .as_ref()
            .cloned()
            .ok_or(Error::NatsConnectionNotOpen)?;
        if push_msg.uid.is_empty() {
            return Err(Error::EmptyUserId);
        }

        if server_kind.0.is_empty() {
            return Err(Error::EmptyServerKind);
        }

        let request_timeout = self.settings.request_timeout;
        let res = tokio::task::spawn_blocking(move || -> Result<(), Error> {
            let topic = utils::user_messages_topic(&push_msg.uid, &server_kind);
            let push_buffer = utils::encode_proto(&push_msg);

            // TODO(lhahn): should we handle the returned message here somehow?
            let _message = connection
                .request_timeout(&topic, push_buffer, request_timeout)
                .map_err(Error::Nats)?;

            Ok(())
        })
        .await;

        match res {
            Err(join_error) => {
                error!(self.logger, "failed to join push task"; "error" => %join_error);
                Err(Error::Internal("failed to join push task".to_owned()))
            }
            Ok(Err(err)) => Err(err),
            Ok(Ok(_)) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{constants, discovery::EtcdLazy};
    use pitaya_core::cluster::Discovery;
    use std::collections::HashMap;
    use std::error::Error as StdError;
    use std::time::Duration;

    fn new_server() -> Arc<ServerInfo> {
        Arc::new(ServerInfo {
            frontend: true,
            hostname: "".to_owned(),
            id: ServerId::new(),
            kind: ServerKind::new(),
            metadata: HashMap::new(),
        })
    }

    #[test]
    fn nats_rpc_client_can_be_created() {
        let _client = NatsRpcClient::new(
            test_helpers::get_root_logger(),
            Arc::new(settings::Nats {
                url: "https://sfdjsdoifj".to_owned(),
                ..Default::default()
            }),
            new_server(),
        );
    }

    #[tokio::test]
    #[should_panic]
    async fn nats_fails_connection() {
        let client = NatsRpcClient::new(
            test_helpers::get_root_logger(),
            Arc::new(settings::Nats {
                url: "https://nats-io.server:3241".to_owned(),
                ..Default::default()
            }),
            new_server(),
        );
        client.start().await.unwrap();
        client.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn nats_request_timeout() -> Result<(), Error> {
        let client = NatsRpcClient::new(
            test_helpers::get_root_logger(),
            Arc::new(settings::Nats {
                request_timeout: Duration::from_millis(300),
                ..Default::default()
            }),
            new_server(),
        );
        client.start().await?;

        let target_server = Arc::new(ServerInfo {
            id: ServerId::from("my_id"),
            kind: ServerKind::from("metagame"),
            metadata: HashMap::new(),
            hostname: "hostname".to_owned(),
            frontend: false,
        });

        let response = client
            .call(
                context::Context::empty(),
                protos::RpcType::User,
                message::Message {
                    kind: message::Kind::Request,
                    id: 20,
                    data: vec![],
                    compressed: false,
                    err: false,
                    route: "room.room.join".to_string(),
                },
                target_server,
            )
            .await;

        assert!(response.is_err());
        let err = response.unwrap_err();

        match err {
            Error::Nats(nats_err) => {
                assert_eq!(nats_err.kind(), std::io::ErrorKind::TimedOut);
            }
            _ => panic!("unexpected error"),
        };

        client.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn nats_request_works() -> Result<(), Box<dyn StdError>> {
        async fn start_service_disovery(sv: Arc<ServerInfo>) -> Result<EtcdLazy, Error> {
            EtcdLazy::new(
                test_helpers::get_root_logger(),
                sv,
                Arc::new(settings::Etcd {
                    prefix: "pitaya".to_owned(),
                    url: constants::LOCAL_ETCD_URL.to_owned(),
                    lease_ttl: Duration::from_secs(50),
                }),
            )
            .await
        }

        let sv = Arc::new(ServerInfo {
            id: ServerId::from("1234567"),
            kind: ServerKind::from("room"),
            frontend: false,
            hostname: "owiejfoiwejf".to_owned(),
            metadata: HashMap::new(),
        });

        let mut service_discovery = start_service_disovery(sv.clone()).await?;

        let client = NatsRpcClient::new(
            test_helpers::get_root_logger(),
            Arc::new(settings::Nats {
                request_timeout: Duration::from_millis(300),
                ..Default::default()
            }),
            sv,
        );
        client.start().await?;

        let servers_by_kind = service_discovery
            .servers_by_kind(&ServerKind::from("room"))
            .await
            .unwrap();

        assert_eq!(servers_by_kind.len(), 1);

        let rpc_data = r#"{
            "name": "superMessage",
            "content": "how are you?"
        }"#;

        let response = client
            .call(
                context::Context::empty(),
                protos::RpcType::User,
                message::Message {
                    kind: message::Kind::Request,
                    id: 20,
                    data: rpc_data.as_bytes().to_owned(),
                    compressed: false,
                    err: false,
                    route: "room.room.join".to_string(),
                },
                servers_by_kind[0].clone(),
            )
            .await?;

        assert!(response.error.is_none());

        let data_str = String::from_utf8_lossy(&response.data);
        assert!(data_str.contains("success"));

        client.shutdown().await?;
        Ok(())
    }
}
