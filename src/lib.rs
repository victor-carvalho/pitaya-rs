extern crate async_trait;
extern crate etcd_client;
extern crate futures;
extern crate log;
extern crate nats;
extern crate pretty_env_logger;
extern crate prost;
extern crate serde;
extern crate serde_json;
extern crate tokio;

mod cluster;
mod error;
mod utils;

mod protos {
    include!(concat!(env!("OUT_DIR"), "/protos.rs"));
}

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct ServerKind(String);

impl ServerKind {
    pub fn new() -> Self {
        Self(String::new())
    }
}

impl From<&str> for ServerKind {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct ServerId(String);

impl ServerId {
    pub fn new() -> Self {
        Self(String::new())
    }
}

impl From<&str> for ServerId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Server {
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
            metadata: vec![("my_key1".to_owned(), "my_val1".to_owned())]
                .into_iter()
                .collect(),
            hostname: "my_hostname".to_owned(),
            frontend: true,
        };
        let json = serde_json::to_string(&sv)?;
        assert_eq!(
            json,
            r#"{"id":"randomId","type":"metagame","metadata":{"my_key1":"my_val1"},"hostname":"my_hostname","frontend":true}"#
        );
        Ok(())
    }
}
