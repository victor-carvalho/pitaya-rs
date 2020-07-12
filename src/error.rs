use crate::ServerKind;

#[derive(Debug)]
pub enum Error {
    // TODO: add timeout error here as well.
    NatsConnectionNotOpen,
    Tokio(std::io::Error),
    NoServersFound(ServerKind),
    MessageEncode(prost::EncodeError),
    MessageDecode(prost::DecodeError),
    Nats(std::io::Error),
    Etcd(etcd_client::Error),
    Json(serde_json::Error),
    TaskJoin(tokio::task::JoinError),
    ChannelReceiverClosed,
    RpcServerAlreadyStarted,
    InvalidRoute,
    InvalidUserId,
    InvalidServerKind,
    InvalidProto(prost::DecodeError),
    Config(config::ConfigError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Error::NatsConnectionNotOpen => write!(f, "nats connection not open"),
            Error::MessageEncode(ref e) => write!(f, "encode message: {}", e),
            Error::MessageDecode(ref e) => write!(f, "decode message: {}", e),
            Error::Nats(ref e) => write!(f, "nats: {}", e),
            Error::Etcd(ref e) => write!(f, "etcd: {}", e),
            Error::Json(ref e) => write!(f, "json: {}", e),
            Error::TaskJoin(ref e) => write!(f, "task join: {}", e),
            Error::NoServersFound(ref k) => write!(f, "no servers for kind: {}", k.0),
            Error::InvalidRoute => write!(f, "invalid route"),
            Error::RpcServerAlreadyStarted => write!(f, "rpc server has already started"),
            Error::ChannelReceiverClosed => write!(f, "channel receiver closed"),
            Error::Tokio(ref e) => write!(f, "tokio: {}", e),
            Error::InvalidUserId => write!(f, "invalid user id"),
            Error::InvalidServerKind => write!(f, "invalid server kind"),
            Error::InvalidProto(ref e) => write!(f, "invalid proto: {}", e),
            Error::Config(ref e) => write!(f, "invalid config: {}", e),
        }
    }
}

impl std::error::Error for Error {
    fn cause(&self) -> Option<&dyn std::error::Error> {
        match *self {
            Error::MessageEncode(ref e) => Some(e),
            Error::MessageDecode(ref e) => Some(e),
            Error::Nats(ref e) => Some(e),
            Error::Etcd(ref e) => Some(e),
            Error::Json(ref e) => Some(e),
            Error::TaskJoin(ref e) => Some(e),
            Error::Tokio(ref e) => Some(e),
            Error::InvalidProto(ref e) => Some(e),
            Error::Config(ref e) => Some(e),
            _ => None,
        }
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(e: tokio::task::JoinError) -> Self {
        Self::TaskJoin(e)
    }
}

impl From<prost::EncodeError> for Error {
    fn from(e: prost::EncodeError) -> Self {
        Self::MessageEncode(e)
    }
}

impl From<prost::DecodeError> for Error {
    fn from(e: prost::DecodeError) -> Self {
        Self::MessageDecode(e)
    }
}

impl From<etcd_client::Error> for Error {
    fn from(e: etcd_client::Error) -> Self {
        Self::Etcd(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

impl From<config::ConfigError> for Error {
    fn from(e: config::ConfigError) -> Self {
        Self::Config(e)
    }
}
