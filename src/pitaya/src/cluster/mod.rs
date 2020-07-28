use crate::protos;
use async_trait::async_trait;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{broadcast, oneshot};

pub(crate) mod discovery;
pub(crate) mod rpc_client;
pub(crate) mod rpc_server;
pub mod server;

#[derive(Debug, Error)]
pub enum Error {
    #[error("etcd: {0}")]
    Etcd(#[from] etcd_client::Error),

    #[error("nats: {0}")]
    Nats(std::io::Error),

    #[error("nats connection not open")]
    NatsConnectionNotOpen,

    #[error("no servers of kind {0:?} found")]
    NoServersFound(server::ServerKind),

    #[error("corrupt server")]
    CorruptServer(serde_json::Error),

    #[error("empty server kind")]
    EmptyServerKind,

    #[error("empty user id")]
    EmptyUserId,

    #[error("internal: {0}")]
    Internal(String),

    #[error("invalid server response")]
    InvalidServerResponse(prost::DecodeError),
}

// The trait Cluster represents the clustering of Pitaya servers.
// A cluster implementation knows how to find specific servers and to
// expose the current server to the cluster as well.
#[async_trait]
pub trait Cluster<D, S, C>
where
    D: Discovery,
    S: Server,
    C: Client,
{
    // This function will register the current server in the cluster.
    // This will allow the current server to discover other servers, as well as to
    // comunicate with them via RPCs.
    async fn register(&mut self) -> Result<(D, S, C), Error>;
}

// The Discovery trait allows the program to discover other pitaya servers in the cluster.
#[async_trait]
pub trait Discovery {
    // Discover a server based on its id.
    async fn server_by_id(
        &mut self,
        id: &server::ServerId,
        kind: &server::ServerKind,
    ) -> Result<Option<Arc<server::Server>>, Error>;

    // Discover servers by a specified kind.
    async fn servers_by_kind(
        &mut self,
        kind: &server::ServerKind,
    ) -> Result<Vec<Arc<server::Server>>, Error>;

    // Allows the current server to subscribe for notifications of added and removed servers.
    fn subscribe(&mut self) -> broadcast::Receiver<Notification>;
}

// Server represents a trait for handling RPCs comming from the cluster.
#[async_trait]
pub trait Server {
    async fn next_rpc(&mut self) -> Option<Rpc>;
}

// Client represents an RPC client for the other servers in the cluster.
#[async_trait]
pub trait Client {
    // This function sends an RPC to a given server in the cluster.
    async fn call(
        &self,
        target: Arc<server::Server>,
        req: protos::Request,
    ) -> Result<protos::Response, Error>;

    // Kicks a user connected to a specific frontend server.
    async fn kick_user(
        &self,
        server_id: server::ServerId,
        server_kind: server::ServerKind,
        kick_msg: protos::KickMsg,
    ) -> Result<protos::KickAnswer, Error>;

    // Sends a push to a user connected to a specific frontend server.
    async fn push_to_user(
        &self,
        server_id: server::ServerId,
        server_kind: server::ServerKind,
        push_msg: protos::Push,
    ) -> Result<(), Error>;
}

// A notification occurs whenever a cluster enters or exists the cluster.
#[derive(Debug, Clone)]
pub enum Notification {
    // Represents a server that was added on the cluster.
    ServerAdded(Arc<server::Server>),
    // Represents a server that was removed from the cluster.
    ServerRemoved(Arc<server::Server>),
}

// Represents an RPC that comes from another server in the cluster.
#[derive(Debug)]
pub struct Rpc {
    req: protos::Request,
    responder: oneshot::Sender<protos::Response>,
}

impl Rpc {
    pub fn request(&mut self) -> &protos::Request {
        &self.req
    }

    // Responds to the RPC with the given response. Returns true
    // on success and false if it was not able to answer.
    pub fn respond(self, res: protos::Response) -> bool {
        self.responder.send(res).map(|_| true).unwrap_or(false)
    }

    pub fn responder(self) -> oneshot::Sender<protos::Response> {
        self.responder
    }
}
