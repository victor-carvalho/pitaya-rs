use crate::{error::Error, protos, utils, Server};
use prost::Message;
use std::sync::Arc;
use std::time::Duration;

pub trait RpcClient {
    fn call(&self, target: Arc<Server>, req: protos::Request) -> Result<protos::Response, Error>;
}

pub struct Config {
    pub address: String,
    pub connection_timeout: Duration,
    pub request_timeout: Duration,
    pub max_reconnection_attempts: u32,
    pub max_pending_messages: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            address: String::from("http://localhost:4222"),
            connection_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(10),
            max_reconnection_attempts: 5,
            max_pending_messages: 100,
        }
    }
}

pub struct NatsClient {
    config: Config,
    connection: Option<nats::Connection>,
}

impl NatsClient {
    pub fn new(config: Config) -> Self {
        Self {
            config: config,
            connection: None,
        }
    }

    pub fn connect(&mut self) -> Result<(), Error> {
        assert!(self.connection.is_none());
        let nc = nats::connect(&self.config.address).map_err(|e| Error::Nats(e))?;
        self.connection = Some(nc);
        Ok(())
    }

    pub fn close(&mut self) {
        if let Some(conn) = self.connection.take() {
            conn.close();
        }
    }
}

impl RpcClient for NatsClient {
    fn call(&self, target: Arc<Server>, req: protos::Request) -> Result<protos::Response, Error> {
        let connection = self
            .connection
            .as_ref()
            .ok_or(Error::NatsConnectionNotOpen)?;
        let topic = utils::topic_for_server(&target);
        let buffer = {
            let mut b = Vec::with_capacity(req.encoded_len());
            req.encode(&mut b).map(|_| b)
        }?;

        let message = connection
            .request_timeout(&topic, &buffer, self.config.request_timeout)
            .map_err(|e| Error::Nats(e))?;

        let response = Message::decode(message.data.as_ref())?;

        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cluster::discovery::{EtcdConfig, EtcdLazy, ServiceDiscovery},
        ServerId, ServerKind,
    };
    use std::collections::HashMap;
    use std::error::Error as StdError;

    const ETCD_URL: &str = "localhost:2379";
    const NATS_URL: &str = "http://localhost:4222";

    #[test]
    fn nats_rpc_client_can_be_created() {
        let _client = NatsClient::new(Config {
            address: "https://sfdjsdoifj".to_owned(),
            ..Config::default()
        });
    }

    #[test]
    #[should_panic]
    fn nats_fails_connection() {
        let mut client = NatsClient::new(Config {
            address: "https://nats-io.server:3241".to_owned(),
            ..Config::default()
        });
        client.connect().unwrap();
        client.close();
    }

    #[test]
    fn nats_request_timeout() -> Result<(), Error> {
        let mut client = NatsClient::new(Config {
            request_timeout: Duration::from_millis(300),
            ..Config::default()
        });
        client.connect()?;

        let target_server = Arc::new(Server {
            id: ServerId::from("my_id"),
            kind: ServerKind::from("metagame"),
            metadata: HashMap::new(),
            hostname: "hostname".to_owned(),
            frontend: false,
        });

        let response = client.call(
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
        );

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

    #[test]
    fn nats_request_works() -> Result<(), Box<dyn StdError>> {
        async fn start_service_disovery() -> Result<EtcdLazy, etcd_client::Error> {
            let sv = Arc::new(Server {
                id: ServerId::from("1234567"),
                kind: ServerKind::from("room"),
                frontend: false,
                hostname: "owiejfoiwejf".to_owned(),
                metadata: HashMap::new(),
            });
            EtcdLazy::new(
                sv,
                EtcdConfig {
                    prefix: "pitaya".to_owned(),
                    url: ETCD_URL.to_owned(),
                    lease_ttl: Duration::from_secs(50),
                },
            )
            .await
        }

        let mut rt = tokio::runtime::Runtime::new()?;
        let mut service_discovery = rt.block_on(start_service_disovery())?;

        let mut client = NatsClient::new(Config {
            request_timeout: Duration::from_millis(300),
            ..Config::default()
        });
        client.connect()?;

        let servers_by_kind = rt.block_on(async {
            service_discovery
                .servers_by_kind(&ServerKind::from("room"))
                .await
                .unwrap()
        });

        assert_eq!(servers_by_kind.len(), 1);

        let rpc_data = r#"{
            "name": "superMessage",
            "content": "how are you?"
        }"#;

        let response = client.call(
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
        )?;

        assert!(response.error.is_none());

        let data_str = String::from_utf8_lossy(&response.data);
        assert!(data_str.contains("success"));

        client.close();
        Ok(())
    }
}
