use crate::protos;
use serde::Serialize;
use std::collections::HashMap;
use std::convert::TryFrom;

// Context represents the context that is associated with an RPC. This context will be propagated
// through RPCs in different pitaya servers.
#[derive(Debug)]
pub struct Context {
    map: HashMap<String, serde_json::Value>,
}

impl Context {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    // Adds a new key value pair into the context.
    // Returns true if the key collided with an existing key. Note that the newer key
    // will always replace the older one.
    pub fn add<T: ToString, S: Serialize>(
        &mut self,
        key: T,
        val: S,
    ) -> Result<bool, serde_json::Error> {
        let json_val = serde_json::to_value(val)?;
        Ok(self.map.insert(key.to_string(), json_val).is_some())
    }
}

impl TryFrom<&protos::Request> for Context {
    type Error = serde_json::Error;

    fn try_from(req: &protos::Request) -> Result<Self, serde_json::Error> {
        let map: HashMap<String, serde_json::Value> = serde_json::from_slice(&req.metadata[..])?;
        Ok(Self { map })
    }
}

impl Into<Vec<u8>> for Context {
    fn into(self) -> Vec<u8> {
        // We expect here to be valid, since a context can be only contructed from a TryFrom.
        serde_json::to_vec(&self.map).expect("context map should be a valid json")
    }
}
