use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct ServerKind(pub String);

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
pub struct ServerId(pub String);

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
