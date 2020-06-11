#[derive(Debug)]
pub enum Error {
    // TODO: add timeout error here as well.
    NatsConnectionNotOpen,
    MessageEncode(prost::EncodeError),
    MessageDecode(prost::DecodeError),
    Nats(std::io::Error),
    Etcd(etcd_client::Error),
    Json(serde_json::Error),
    TaskJoin(tokio::task::JoinError),
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
        }
    }
}

impl std::error::Error for Error {
    fn cause(&self) -> Option<&dyn std::error::Error> {
        match *self {
            Error::NatsConnectionNotOpen => None,
            Error::MessageEncode(ref e) => Some(e),
            Error::MessageDecode(ref e) => Some(e),
            Error::Nats(ref e) => Some(e),
            Error::Etcd(ref e) => Some(e),
            Error::Json(ref e) => Some(e),
            Error::TaskJoin(ref e) => Some(e),
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
