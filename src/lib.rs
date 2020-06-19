extern crate async_trait;
extern crate etcd_client;
extern crate futures;
extern crate log;
extern crate nats;
extern crate pretty_env_logger;
extern crate prost;
extern crate serde;
extern crate serde_json;
extern crate tokio;

mod cluster;
mod error;
mod server;
mod utils;

use server::{Server, ServerId, ServerKind};
use std::{collections::HashMap, sync::Arc, time};

mod protos {
    include!(concat!(env!("OUT_DIR"), "/protos.rs"));
}

#[derive(Debug)]
struct Route<'a> {
    pub server_kind: &'a str,
    pub handler: &'a str,
    pub method: &'a str,
}

impl<'a> std::convert::TryFrom<&'a str> for Route<'a> {
    type Error = error::Error;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        let comps: Vec<&'a str> = value.split(".").collect();
        if comps.len() == 3 {
            Ok(Route {
                server_kind: comps[0],
                handler: comps[1],
                method: comps[2],
            })
        } else {
            Err(error::Error::InvalidRoute)
        }
    }
}

// Pitaya represent a pitaya server.
// Currently, it only implements cluster mode.
struct Pitaya {
    this_server: Arc<Server>,
    etcd_config: cluster::discovery::EtcdConfig,
    service_discovery: Option<cluster::discovery::EtcdLazy>,
    nats_rpc_client: cluster::rpc_client::NatsClient,
    nats_rpc_server: cluster::rpc_server::NatsRpcServer,
    nats_rpc_server_connection: Option<cluster::rpc_server::NatsServerConnection>,
    runtime: tokio::runtime::Runtime,
    shutdown_timeout: time::Duration,
}

impl Pitaya {
    fn new(
        frontend: bool,
        server_kind: ServerKind,
        etcd_config: cluster::discovery::EtcdConfig,
        rpc_client_config: cluster::rpc_client::Config,
        rpc_server_config: cluster::rpc_server::Config,
        shutdown_timeout: time::Duration,
    ) -> Self {
        let this_server = Arc::new(Server {
            id: ServerId::new(),
            kind: server_kind,
            // TODO(lhahn): fill these options.
            metadata: HashMap::new(),
            hostname: "".to_owned(),
            frontend: frontend,
        });

        // TODO(lhahn): let user parameterize this runtime.
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| error::Error::Tokio(e))
            .expect("failed to create tokio runtime");

        let client = cluster::rpc_client::NatsClient::new(rpc_client_config);
        let server =
            cluster::rpc_server::NatsRpcServer::new(this_server.clone(), rpc_server_config);

        Self {
            this_server: this_server,
            etcd_config: etcd_config,
            service_discovery: None,
            nats_rpc_client: client,
            nats_rpc_server: server,
            nats_rpc_server_connection: None,
            runtime: rt,
            shutdown_timeout: shutdown_timeout,
        }
    }

    pub fn start(&mut self) -> Result<(), error::Error> {
        self.service_discovery.replace({
            let server = self.this_server.clone();
            let config = self.etcd_config.clone();
            self.runtime
                .block_on(async move { cluster::discovery::EtcdLazy::new(server, config).await })
        }?);
        self.nats_rpc_client.connect()?;
        self.nats_rpc_server_connection
            .replace(self.nats_rpc_server.start()?);
        Ok(())
    }

    pub fn shutdown(self) -> Result<(), error::Error> {
        self.runtime.shutdown_timeout(self.shutdown_timeout);
        Ok(())
    }
}

struct PitayaBuilder {
    frontend: bool,
    server_kind: Option<ServerKind>,
    etcd_config: cluster::discovery::EtcdConfig,
    rpc_client_config: cluster::rpc_client::Config,
    rpc_server_config: cluster::rpc_server::Config,
    shutdown_timeout: time::Duration,
}

impl PitayaBuilder {
    pub fn new() -> Self {
        Self {
            frontend: false,
            server_kind: None,
            etcd_config: cluster::discovery::EtcdConfig::default(),
            rpc_client_config: cluster::rpc_client::Config::default(),
            rpc_server_config: cluster::rpc_server::Config::default(),
            shutdown_timeout: time::Duration::from_secs(10),
        }
    }

    pub fn with_frontend(mut self, frontend: bool) -> Self {
        self.frontend = frontend;
        self
    }

    pub fn with_server_kind(mut self, server_kind: &str) -> Self {
        self.server_kind = Some(ServerKind::from(server_kind));
        self
    }

    pub fn with_etcd_config(mut self, c: cluster::discovery::EtcdConfig) -> Self {
        self.etcd_config = c;
        self
    }

    pub fn with_rpc_client_config(mut self, c: cluster::rpc_client::Config) -> Self {
        self.rpc_client_config = c;
        self
    }

    pub fn with_rpc_server_config(mut self, c: cluster::rpc_server::Config) -> Self {
        self.rpc_server_config = c;
        self
    }

    pub fn with_shutdown_timeout(mut self, t: time::Duration) -> Self {
        self.shutdown_timeout = t;
        self
    }

    pub fn build(self) -> Pitaya {
        Pitaya::new(
            self.frontend,
            self.server_kind
                .expect("server kind should be provided to PitayaBuilder"),
            self.etcd_config,
            self.rpc_client_config,
            self.rpc_server_config,
            self.shutdown_timeout,
        )
    }
}
