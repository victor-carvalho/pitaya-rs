use crate::cluster;

// Remote is a service that is capable of handling RPC messages from other pitaya servers
// and also sending RPCs.
pub struct Remote {
    _rpc_client: Box<dyn cluster::RpcClient>,
    _rpc_server: Box<dyn cluster::RpcServer>,
}

impl Remote {
    pub fn new(_service_discovery: Box<dyn cluster::Discovery>) -> Self {
        todo!()
    }
}
