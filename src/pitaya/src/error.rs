use crate::{cluster, metrics, ServerKind};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("nats connection not open")]
    NatsConnectionNotOpen,

    #[error("tokio: {0}")]
    Tokio(#[from] std::io::Error),

    #[error("no servers of kind {0:?} found")]
    NoServersFound(ServerKind),

    #[error("server id not found: {0}")]
    ServerIdNotFound(String),

    #[error("no server kind on route {0}")]
    NoServerKindOnRoute(String),

    #[error("encode message: {0}")]
    MessageEncode(#[from] prost::EncodeError),

    #[error("decode message: {0}")]
    MessageDecode(#[from] prost::DecodeError),

    #[error("nats: {0}")]
    Nats(std::io::Error),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("failed to join on task: {0}")]
    TaskJoin(#[from] tokio::task::JoinError),

    #[error("channel receiver was closed")]
    ChannelReceiverClosed,

    #[error("rpc server already started")]
    RpcServerAlreadyStarted,

    #[error("invalid route")]
    InvalidRoute,

    #[error("invalid user id")]
    InvalidUserId,

    #[error("invalid server kind")]
    InvalidServerKind,

    #[error("invalid address for {module}: {address}")]
    InvalidAddress { module: String, address: String },

    #[error("config error: {0}")]
    Config(#[from] config::ConfigError),

    #[error("invalid context")]
    InvalidContext,

    #[error("cluster: {0}")]
    Cluster(#[from] cluster::Error),

    #[error("metrics: {0}")]
    Metrics(#[from] metrics::Error),

    #[error("core: {0}")]
    Core(#[from] pitaya_core::Error),
}
