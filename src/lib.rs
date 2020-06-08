extern crate etcd_client;
extern crate nats;
extern crate prost;
extern crate tokio;
extern crate futures;
extern crate async_trait;
extern crate log;
extern crate serde;
extern crate serde_json;

mod discovery;
mod rpc;
mod utils;

mod protos {
    include!(concat!(env!("OUT_DIR"), "/protos.rs"));
}

use serde::{Serialize, Deserialize};
use std::collections::HashMap;

#[derive(Debug)]
enum Error {
    // TODO: add timeout error here as well.
    NatsConnectionNotOpen,
    MessageEncode(prost::EncodeError),
    MessageDecode(prost::DecodeError),
    Nats(std::io::Error),
    Etcd(etcd_client::Error),
    Json(serde_json::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Error::NatsConnectionNotOpen => write!(f, "nats connection not open"),
            Error::MessageEncode(ref e) => write!(f, "encode message: {}", e),
            Error::MessageDecode(ref e) => write!(f, "decode message: {}", e),
            Error::Nats(ref e) => write!(f, "nats: {}", e),
            Error::Etcd(ref e) => write!(f, "etcd: {}", e),
            Error::Json(ref e) => write!(f, "json: {}", e),
        }
    }
}

impl std::error::Error for Error {
    fn cause(&self) -> Option<&dyn std::error::Error> {
        match *self {
            Error::NatsConnectionNotOpen => None,
            Error::MessageEncode(ref e) => Some(e),
            Error::MessageDecode(ref e) => Some(e),
            Error::Nats(ref e) => Some(e),
            Error::Etcd(ref e) => Some(e),
            Error::Json(ref e) => Some(e),
        }
    }
}

impl From<prost::EncodeError> for Error {
    fn from(e: prost::EncodeError) -> Self {
        Self::MessageEncode(e)
    }
}

impl From<prost::DecodeError> for Error {
    fn from(e: prost::DecodeError) -> Self {
        Self::MessageDecode(e)
    }
}

impl From<etcd_client::Error> for Error {
    fn from(e: etcd_client::Error) -> Self {
        Self::Etcd(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize, Clone)]
struct ServerKind(String);

impl ServerKind {
    fn new() -> Self {
        Self(String::new())
    }
}

impl From<&str> for ServerKind {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize, Clone)]
struct ServerId(String);

impl ServerId {
    fn new() -> Self {
        Self(String::new())
    }
}

impl From<&str> for ServerId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Server {
    pub id: ServerId,
    #[serde(rename = "type")] 
    pub kind: ServerKind,
    pub metadata: HashMap<String, String>,
    pub hostname: String,
    pub frontend: bool,
}

#[derive(Debug)]
struct Route {
    pub server_type: String,
    pub handler: String,
    pub method: String,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn server_serialize() -> Result<(), serde_json::Error> {
        let sv = Server {
            id: ServerId::from("randomId"),
            kind: ServerKind::from("metagame"),
            metadata: vec![
                ("my_key1".to_owned(), "my_val1".to_owned()),
            ].into_iter().collect(),
            hostname: "my_hostname".to_owned(),
            frontend: true
        };
        let json = serde_json::to_string(&sv)?;
        assert_eq!(json, r#"{"id":"randomId","type":"metagame","metadata":{"my_key1":"my_val1"},"hostname":"my_hostname","frontend":true}"#);
        Ok(())
    }
}