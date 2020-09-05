use crate::cluster;
use std::sync::Arc;
use tokio::sync::RwLock;

// Remote is a service that is capable of handling RPC messages from other pitaya servers
// and also sending RPCs.
pub struct Remote {
    rpc_client: Box<dyn cluster::RpcClient>,
    rpc_server: Box<dyn cluster::RpcServer>,
}

impl Remote {
    pub fn new(service_discovery: Box<dyn cluster::Discovery>) -> Self {
        todo!()
    }
}
