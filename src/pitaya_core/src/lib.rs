pub mod cluster;
pub mod constants;
pub mod context;
pub mod handler;
pub mod message;
pub mod metrics;
pub mod service;
pub mod session;
pub mod state;
pub mod utils;
pub mod protos {
    include!(concat!(env!("OUT_DIR"), "/protos.rs"));
}

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid route")]
    InvalidRoute,

    #[error("collistion on context key {0}")]
    ContextKeyCollistion(String),

    #[error("route provided was not found")]
    RouteNotFound,

    #[error("invalid json argument to handler")]
    InvalidJsonArgument,

    #[error("invalid protobuf argument to handler")]
    InvalidProtobufArgument,

    #[error("cluster: {0}")]
    Cluster(#[from] cluster::Error),
}

impl ToError for Error {
    fn to_error(self) -> protos::Error {
        match self {
            Error::RouteNotFound => protos::Error {
                code: constants::CODE_NOT_FOUND.into(),
                msg: "route cannot be found".into(),
                ..Default::default()
            },
            Error::InvalidJsonArgument => protos::Error {
                code: constants::CODE_BAD_FORMAT.into(),
                msg: "received invalid json argument at handler".into(),
                ..Default::default()
            },
            Error::InvalidProtobufArgument => protos::Error {
                code: constants::CODE_BAD_FORMAT.into(),
                msg: "received invalid protobuf argument at handler".into(),
                ..Default::default()
            },
            _ => protos::Error {
                code: constants::CODE_INTERNAL_ERROR.into(),
                msg: "internal error".into(),
                ..Default::default()
            },
        }
    }
}

#[derive(Debug)]
pub struct Route {
    route_string: String,
}

impl Into<String> for Route {
    fn into(self) -> String {
        self.route_string
    }
}

impl Route {
    pub fn server_kind(&self) -> &str {
        let comps: Vec<&str> = self.route_string.split('.').collect();
        comps[0]
    }

    pub fn handler(&self) -> &str {
        let comps: Vec<&str> = self.route_string.split('.').collect();
        comps[1]
    }

    pub fn method(&self) -> &str {
        let comps: Vec<&str> = self.route_string.split('.').collect();
        comps[2]
    }

    pub fn as_str(&self) -> &str {
        &self.route_string
    }

    pub fn try_from_str(route_string: String) -> Option<Route> {
        let comps: Vec<&str> = route_string.split('.').collect();
        if comps.len() != 3 {
            return None;
        }

        let server_kind = comps[0];
        let handler = comps[1];
        let method = comps[2];

        if server_kind.is_empty() || handler.is_empty() || method.is_empty() {
            return None;
        }

        Some(Route { route_string })
    }
}

pub enum Never {}

pub trait ToError {
    fn to_error(self) -> protos::Error;
}

pub trait ToResponseProto {
    fn to_response_proto(self) -> protos::Response;
}

pub trait ToResponseJson {
    fn to_response_json(self) -> protos::Response;
}

impl<T: serde::Serialize, E: ToError> ToResponseJson for Result<T, E> {
    fn to_response_json(self) -> protos::Response {
        match self {
            Ok(v) => {
                let res_bytes = serde_json::to_vec(&v).expect("should not fail");
                protos::Response {
                    data: res_bytes,
                    error: None,
                }
            }
            Err(e) => protos::Response {
                data: vec![],
                error: Some(e.to_error()),
            },
        }
    }
}

impl<T: prost::Message, E: ToError> ToResponseProto for Result<T, E> {
    fn to_response_proto(self) -> protos::Response {
        match self {
            Ok(v) => {
                let res_bytes = utils::encode_proto(&v);
                protos::Response {
                    data: res_bytes,
                    error: None,
                }
            }
            Err(e) => protos::Response {
                data: vec![],
                error: Some(e.to_error()),
            },
        }
    }
}

impl ToError for Never {
    fn to_error(self) -> protos::Error {
        protos::Error::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_works() {
        let route_str = "server-kind.myhandler.mymethod";
        let route = Route::try_from_str(route_str.to_owned()).expect("should not fail");
        assert_eq!(route.as_str(), route_str);
        assert_eq!(route.server_kind(), "server-kind");
        assert_eq!(route.handler(), "myhandler");
        assert_eq!(route.method(), "mymethod");
    }
}
