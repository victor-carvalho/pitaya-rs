use crate::protos;
use std::collections::HashMap;

// Context represents the context that is associated with an RPC.
struct Context {
    map: HashMap<String, serde_json::Value>,
}

impl std::convert::TryFrom<&protos::Request> for Context {
    type Error = crate::Error;

    fn try_from(req: &protos::Request) -> Result<Self, crate::Error> {
        let map: HashMap<String, serde_json::Value> = serde_json::from_slice(&req.metadata[..])?;
        Ok(Self { map })
    }
}
