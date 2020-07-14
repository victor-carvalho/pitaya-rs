use crate::{error::Error, protos, settings, utils, Server, ServerId, ServerKind};
use async_trait::async_trait;
use prost::Message;
use slog::trace;
use std::sync::Arc;

#[async_trait]
pub trait RpcClient {
    async fn call(
        &self,
        target: Arc<Server>,
        req: protos::Request,
    ) -> Result<protos::Response, Error>;
    async fn kick_user(
        &self,
        server_id: ServerId,
        server_kind: ServerKind,
        kick_msg: protos::KickMsg,
    ) -> Result<protos::KickAnswer, Error>;
    async fn push_to_user(
        &self,
        server_id: ServerId,
        server_kind: ServerKind,
        push_msg: protos::Push,
    ) -> Result<(), Error>;
}

#[derive(Clone)]
pub struct NatsClient {
    settings: Arc<settings::Nats>,
    connection: Option<nats::Connection>,
    logger: slog::Logger,
}

impl NatsClient {
    pub fn new(logger: slog::Logger, settings: Arc<settings::Nats>) -> Self {
        Self {
            settings,
            connection: None,
            logger,
        }
    }

    pub fn connect(&mut self) -> Result<(), Error> {
        assert!(self.connection.is_none());
        let nc = nats::ConnectionOptions::new()
            .max_reconnects(Some(self.settings.max_reconnection_attempts as usize))
            .connect(&self.settings.url)
            .map_err(|e| Error::Nats(e))?;
        self.connection = Some(nc);
        Ok(())
    }

    pub fn close(&mut self) {
        if let Some(conn) = self.connection.take() {
            conn.close();
        }
    }
}

#[async_trait]
impl RpcClient for NatsClient {
    async fn call(
        &self,
        target: Arc<Server>,
        req: protos::Request,
    ) -> Result<protos::Response, Error> {
        trace!(self.logger, "NatsClient::call");
        let connection = self
            .connection
            .as_ref()
            .map(|conn| conn.clone())
            .ok_or(Error::NatsConnectionNotOpen)?;
        let topic = utils::topic_for_server(&target);
        let buffer = utils::encode_proto(&req);

        trace!(
            self.logger,
            "sending nats request"; "topic" => &topic, "timeout" => self.settings.request_timeout.as_secs()
        );

        let request_timeout = self.settings.request_timeout.clone();

        // We do a spawn_blocking here, since it otherwise will block the executor thread.
        let response = tokio::task::spawn_blocking(move || -> Result<protos::Response, Error> {
            let message = connection
                .request_timeout(&topic, buffer, request_timeout)
                .map_err(|e| Error::Nats(e))?;
            let msg: protos::Response = Message::decode(message.data.as_ref())?;
            Ok(msg)
        })
        .await??;

        Ok(response)
    }

    async fn kick_user(
        &self,
        _server_id: ServerId,
        server_kind: ServerKind,
        kick_msg: protos::KickMsg,
    ) -> Result<protos::KickAnswer, Error> {
        trace!(self.logger, "NatsClient::kick_user");
        let connection = self
            .connection
            .as_ref()
            .cloned()
            .ok_or(Error::NatsConnectionNotOpen)?;
        // NOTE: Ignore server_id, since it is not necessary to create the topic.
        if kick_msg.user_id.is_empty() {
            return Err(Error::InvalidUserId);
        }

        if server_kind.0.is_empty() {
            return Err(Error::InvalidServerKind);
        }

        let request_timeout = self.settings.request_timeout.clone();
        let kick_answer =
            tokio::task::spawn_blocking(move || -> Result<protos::KickAnswer, Error> {
                let topic = utils::user_kick_topic(&kick_msg.user_id, &server_kind);
                let kick_buffer = utils::encode_proto(&kick_msg);

                let message = connection
                    .request_timeout(&topic, kick_buffer, request_timeout)
                    .map_err(|e| Error::Nats(e))?;

                let k: protos::KickAnswer =
                    Message::decode(&message.data[..]).map_err(|e| Error::InvalidProto(e))?;
                Ok(k)
            })
            .await??;

        Ok(kick_answer)
    }

    async fn push_to_user(
        &self,
        // NOTE: we ignore the server id, since it is not necessary to create the topic.
        _server_id: ServerId,
        server_kind: ServerKind,
        push_msg: protos::Push,
    ) -> Result<(), Error> {
        trace!(self.logger, "NatsClient::push_to_user");
        let connection = self
            .connection
            .as_ref()
            .cloned()
            .ok_or(Error::NatsConnectionNotOpen)?;
        if push_msg.uid.is_empty() {
            return Err(Error::InvalidUserId);
        }

        if server_kind.0.is_empty() {
            return Err(Error::InvalidServerKind);
        }

        let request_timeout = self.settings.request_timeout.clone();
        tokio::task::spawn_blocking(move || -> Result<(), Error> {
            let topic = utils::user_messages_topic(&push_msg.uid, &server_kind);
            let push_buffer = utils::encode_proto(&push_msg);

            // TODO(lhahn): should we handle the returned message here somehow?
            let _message = connection
                .request_timeout(&topic, push_buffer, request_timeout)
                .map_err(|e| Error::Nats(e))?;

            Ok(())
        })
        .await??;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cluster::discovery::{EtcdLazy, ServiceDiscovery},
        constants, test_helpers, ServerId, ServerKind,
    };
    use std::collections::HashMap;
    use std::error::Error as StdError;
    use std::time::Duration;

    #[test]
    fn nats_rpc_client_can_be_created() {
        let _client = NatsClient::new(
            test_helpers::get_root_logger(),
            Arc::new(settings::Nats {
                url: "https://sfdjsdoifj".to_owned(),
                ..Default::default()
            }),
        );
    }

    #[test]
    #[should_panic]
    fn nats_fails_connection() {
        let mut client = NatsClient::new(
            test_helpers::get_root_logger(),
            Arc::new(settings::Nats {
                url: "https://nats-io.server:3241".to_owned(),
                ..Default::default()
            }),
        );
        client.connect().unwrap();
        client.close();
    }

    #[tokio::test]
    async fn nats_request_timeout() -> Result<(), Error> {
        let mut client = NatsClient::new(
            test_helpers::get_root_logger(),
            Arc::new(settings::Nats {
                request_timeout: Duration::from_millis(300),
                ..Default::default()
            }),
        );
        client.connect()?;

        let target_server = Arc::new(Server {
            id: ServerId::from("my_id"),
            kind: ServerKind::from("metagame"),
            metadata: HashMap::new(),
            hostname: "hostname".to_owned(),
            frontend: false,
        });

        let response = client
            .call(
                target_server,
                protos::Request {
                    r#type: protos::RpcType::User as i32,
                    msg: Some(protos::Msg {
                        r#type: protos::MsgType::MsgRequest as i32,
                        data: vec![],
                        route: "".to_owned(),
                        ..protos::Msg::default()
                    }),
                    frontend_id: "".to_owned(),
                    metadata: Vec::new(),
                    ..protos::Request::default()
                },
            )
            .await;

        assert!(response.is_err());
        let err = response.unwrap_err();

        match err {
            Error::Nats(nats_err) => {
                assert!(nats_err.kind() == std::io::ErrorKind::TimedOut);
            }
            _ => panic!("unexpected error"),
        };

        client.close();
        Ok(())
    }

    #[tokio::test]
    async fn nats_request_works() -> Result<(), Box<dyn StdError>> {
        async fn start_service_disovery() -> Result<EtcdLazy, etcd_client::Error> {
            let sv = Arc::new(Server {
                id: ServerId::from("1234567"),
                kind: ServerKind::from("room"),
                frontend: false,
                hostname: "owiejfoiwejf".to_owned(),
                metadata: HashMap::new(),
            });
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

        let mut service_discovery = start_service_disovery().await?;

        let mut client = NatsClient::new(
            test_helpers::get_root_logger(),
            Arc::new(settings::Nats {
                request_timeout: Duration::from_millis(300),
                ..Default::default()
            }),
        );
        client.connect()?;

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
                servers_by_kind[0].clone(),
                protos::Request {
                    r#type: protos::RpcType::User as i32,
                    msg: Some(protos::Msg {
                        r#type: protos::MsgType::MsgRequest as i32,
                        data: rpc_data.as_bytes().to_owned(),
                        route: "room.join".to_owned(),
                        ..protos::Msg::default()
                    }),
                    frontend_id: "".to_owned(),
                    metadata: "{}".as_bytes().to_owned(),
                    ..protos::Request::default()
                },
            )
            .await?;

        assert!(response.error.is_none());

        let data_str = String::from_utf8_lossy(&response.data);
        assert!(data_str.contains("success"));

        client.close();
        Ok(())
    }
}
