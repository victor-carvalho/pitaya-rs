pub mod cluster;
pub mod constants;
pub mod context;
pub mod message;
pub mod metrics;
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
        let comps: Vec<&str> = self.route_string.split(".").collect();
        comps[0]
    }

    pub fn handler(&self) -> &str {
        let comps: Vec<&str> = self.route_string.split(".").collect();
        comps[1]
    }

    pub fn method(&self) -> &str {
        let comps: Vec<&str> = self.route_string.split(".").collect();
        comps[2]
    }

    pub fn as_str(&self) -> &str {
        &self.route_string
    }

    pub fn from_str(route_string: String) -> Option<Route> {
        let comps: Vec<&str> = route_string.split(".").collect();
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
