use crate::protos;
use serde::Serialize;
use std::{collections::HashMap, sync::Arc};

pub struct NotFound(pub String);

pub trait FromContext<'c>: Sized {
    fn from_context(ctx: &'c Context) -> Result<Self, NotFound>;
}

// Context represents the context that is associated with an RPC. This context will be propagated
// through RPCs in different pitaya servers.
pub struct Context {
    map: HashMap<String, serde_json::Value>,
    container: Arc<state::Container>,
}

impl Context {
    pub fn new(
        req: &protos::Request,
        container: Arc<state::Container>,
    ) -> Result<Self, serde_json::Error> {
        let map: HashMap<String, serde_json::Value> = serde_json::from_slice(&req.metadata[..])?;
        Ok(Self { map, container })
    }

    pub fn empty() -> Self {
        Self {
            map: HashMap::new(),
            container: Arc::new(state::Container::new()),
        }
    }

    pub(crate) fn try_get_state<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.container.try_get::<T>()
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

impl<'s> Into<Vec<u8>> for Context {
    fn into(self) -> Vec<u8> {
        // We expect here to be valid, since a context can be only contructed from a TryFrom.
        serde_json::to_vec(&self.map).expect("context map should be a valid json")
    }
}
