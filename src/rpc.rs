use super::{protos, utils, Error, Server, ServerId, ServerKind};
use prost::Message;
use std::time::Duration;

trait Client {
    fn call(&self, target: &Server, req: protos::Request) -> Result<protos::Response, Error>;
}

struct NatsClientBuilder {
    pub address: String,
    pub connection_timeout: Duration,
    pub request_timeout: Duration,
    pub max_reconnection_attempts: u32,
    pub max_pending_messages: u32,
}

impl NatsClientBuilder {
    pub fn new(address: String) -> Self {
        Self {
            address: address,
            connection_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(4),
            max_reconnection_attempts: 10,
            max_pending_messages: 100,
        }
    }

    pub fn with_connection_timeout(mut self, t: Duration) -> Self {
        self.connection_timeout = t;
        self
    }

    pub fn with_request_timeout(mut self, t: Duration) -> Self {
        self.request_timeout = t;
        self
    }

    pub fn with_max_reconnection_attempts(mut self, a: u32) -> Self {
        self.max_reconnection_attempts = a;
        self
    }

    pub fn with_max_pending_messages(mut self, m: u32) -> Self {
        self.max_pending_messages = m;
        self
    }

    pub fn build(self) -> NatsClient {
        NatsClient {
            address: self.address,
            connection_timeout: self.connection_timeout,
            request_timeout: self.request_timeout,
            max_pending_messages: self.max_pending_messages,
            max_reconnection_attempts: self.max_reconnection_attempts,
            connection: None,
        }
    }
}

struct NatsClient {
    address: String,
    connection_timeout: Duration,
    request_timeout: Duration,
    max_reconnection_attempts: u32,
    max_pending_messages: u32,
    connection: Option<nats::Connection>,
}

impl NatsClient {
    pub fn connect(&mut self) -> Result<(), Error> {
        assert!(self.connection.is_none());
        let nc = nats::connect(&self.address).map_err(|e| Error::Nats(e))?;
        self.connection = Some(nc);
        Ok(())
    }

    pub fn close(&mut self) -> Result<(), Error> {
        if let Some(conn) = self.connection.take() {
            conn.close().map_err(|e| Error::Nats(e))
        } else {
            Ok(())
        }
    }
}

impl Client for NatsClient {
    fn call(&self, target: &Server, req: protos::Request) -> Result<protos::Response, Error> {
        let connection = self
            .connection
            .as_ref()
            .ok_or(Error::NatsConnectionNotOpen)?;
        let topic = utils::topic_for_server(target);
        let buffer = {
            let mut b = Vec::with_capacity(req.encoded_len());
            req.encode(&mut b).map(|_| b)
        }?;

        let message = connection
            .request_timeout(&topic, &buffer, self.request_timeout)
            .map_err(|e| Error::Nats(e))?;

        let response = Message::decode(message.data.as_ref())?;

        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const NATS_URL: &str = "http://localhost:4222";

    #[test]
    fn nats_rpc_client_can_be_created() {
        let _client = NatsClientBuilder::new("https://sfdjsdoifj".to_owned())
            .with_connection_timeout(Duration::from_millis(500))
            .build();
    }

    #[test]
    #[should_panic]
    fn nats_fails_connection() {
        let mut client = NatsClientBuilder::new("https://nats-io.server:3241".to_owned()).build();
        client.connect().unwrap();
        client.close().unwrap();
    }

    #[test]
    fn nats_request_timeout() -> Result<(), Error> {
        let mut client = NatsClientBuilder::new(NATS_URL.to_owned())
            .with_request_timeout(Duration::from_millis(300))
            .build();
        client.connect()?;

        let target_server = Server {
            id: ServerId::from("my_id"),
            kind: ServerKind::from("metagame"),
            metadata: HashMap::new(),
            hostname: "hostname".to_owned(),
            frontend: false,
        };

        let response = client.call(
            &target_server,
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

        client.close()?;
        Ok(())
    }

    #[test]
    fn nats_request_works() -> Result<(), Error> {
        let mut client = NatsClientBuilder::new(NATS_URL.to_owned())
            .with_request_timeout(Duration::from_millis(300))
            .build();
        client.connect()?;

        // TODO: fix return value here.
        return Ok(());

        let id = "455436e0-e4b7-4072-ad68-e1c9a0f1a57d";

        let target_server = Server {
            id: ServerId::from(id),
            kind: ServerKind::from("room"),
            metadata: HashMap::new(),
            hostname: "hostname".to_owned(),
            frontend: false,
        };

        let rpc_data = r#"{
            "name": "superMessage",
            "content": "how are you?"
        }"#;

        let response: protos::Response = client.call(
            &target_server,
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

        if let Some(error) = response.error {
            panic!("received error: code={} msg={}", error.code, error.msg);
        } else {
            let data_str = String::from_utf8_lossy(&response.data);
            panic!("response string: {}", data_str);
        }

        client.close()?;
        Ok(())
    }
}
