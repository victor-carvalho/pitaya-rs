use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct ServerKind(pub String);

impl ServerKind {
    pub fn new() -> Self {
        Self(String::new())
    }
}

impl Default for ServerKind {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> From<S> for ServerKind
where
    S: std::string::ToString,
{
    fn from(s: S) -> Self {
        Self(s.to_string())
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct ServerId(pub String);

impl ServerId {
    pub fn new() -> Self {
        Self(String::new())
    }
}

impl Default for ServerId {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> From<S> for ServerId
where
    S: std::string::ToString,
{
    fn from(s: S) -> Self {
        Self(s.to_string())
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ServerInfo {
    pub id: ServerId,
    #[serde(rename = "type")]
    pub kind: ServerKind,
    pub metadata: HashMap<String, String>,
    pub hostname: String,
    pub frontend: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_serialize() -> Result<(), serde_json::Error> {
        let sv = ServerInfo {
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
