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
extern crate uuid;

mod cluster;
mod error;
mod server;
mod utils;

pub use cluster::{
    discovery::{EtcdConfig, ServiceDiscovery},
    rpc_client::{Config as RpcClientConfig, RpcClient},
    rpc_server::Config as RpcServerConfig,
    Rpc,
};
pub use error::Error;
use log::{debug, error, info};
use server::{Server, ServerId, ServerKind};
use std::convert::TryFrom;
use std::{collections::HashMap, sync::Arc, time};
use tokio::task;

pub mod protos {
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
            Err(Error::InvalidRoute)
        }
    }
}

// Pitaya represent a pitaya server.
// Currently, it only implements cluster mode.
pub struct Pitaya {
    this_server: Arc<Server>,
    etcd_config: cluster::discovery::EtcdConfig,
    service_discovery: Option<cluster::discovery::EtcdLazy>,
    nats_rpc_client: cluster::rpc_client::NatsClient,
    nats_rpc_server: cluster::rpc_server::NatsRpcServer,
    runtime: tokio::runtime::Runtime,
    shutdown_timeout: time::Duration,
    listen_for_rpc_task: Option<task::JoinHandle<()>>,
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
        let server_id = uuid::Uuid::new_v4().to_string();
        let this_server = Arc::new(Server {
            id: ServerId(server_id),
            kind: server_kind,
            // TODO(lhahn): fill these options.
            metadata: HashMap::new(),
            hostname: "".to_owned(),
            frontend: frontend,
        });

        debug!("this server: {:?}", this_server);

        // TODO(lhahn): let user parameterize this runtime.
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| Error::Tokio(e))
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
            runtime: rt,
            shutdown_timeout: shutdown_timeout,
            listen_for_rpc_task: None,
        }
    }

    pub fn shutdown(mut self) -> Result<(), Error> {
        info!("shutting down pitaya server");
        self.nats_rpc_client.close();
        self.nats_rpc_server.stop()?;
        if let Some(ref mut task) = self.listen_for_rpc_task {
            self.runtime.block_on(async move { task.await })?;
        }
        if let Some(ref mut service_discovery) = self.service_discovery {
            self.runtime
                .block_on(async move { service_discovery.stop().await })?;
        }
        self.runtime.shutdown_timeout(self.shutdown_timeout);
        Ok(())
    }

    pub fn send_rpc(
        &mut self,
        route: &str,
        req: protos::Request,
    ) -> Result<protos::Response, Error> {
        use cluster::discovery::EtcdLazy;

        debug!("sending rpc");

        let route = Route::try_from(route)?;
        let server_kind = ServerKind::from(route.server_kind);

        debug!("getting servers");
        let servers = if let Some(ref mut service_discovery) = self.service_discovery {
            async fn get_servers(
                etcd: &mut EtcdLazy,
                server_kind: &ServerKind,
            ) -> Result<Vec<Arc<Server>>, Error> {
                let servers = etcd.servers_by_kind(server_kind).await?;
                Ok(servers)
            }
            self.runtime
                .block_on(get_servers(service_discovery, &server_kind))?
        } else {
            panic!("etcd should be initialized");
        };

        debug!("getting random server");
        if let Some(random_server) = utils::random_server(&servers) {
            debug!("sending rpc");
            self.nats_rpc_client.call(random_server, req)
        } else {
            error!("found no servers for kind {}", server_kind.0);
            Err(Error::NoServersFound(server_kind))
        }
    }

    fn start<RpcHandler>(&mut self, rpc_handler: RpcHandler) -> Result<(), Error>
    where
        RpcHandler: FnMut(cluster::Rpc) + Send + 'static,
    {
        info!("starting pitaya server");
        async fn start_etcd(
            server: Arc<Server>,
            config: EtcdConfig,
            app_die_sender: tokio::sync::oneshot::Sender<()>,
        ) -> Result<cluster::discovery::EtcdLazy, Error> {
            let mut sd = cluster::discovery::EtcdLazy::new(server, config).await?;
            sd.start(app_die_sender).await?;
            Ok(sd)
        }

        let (app_die_sender, app_die_receiver) = tokio::sync::oneshot::channel();

        self.service_discovery.replace({
            let server = self.this_server.clone();
            let config = self.etcd_config.clone();
            self.runtime
                .block_on(start_etcd(server, config, app_die_sender))
        }?);
        self.nats_rpc_client.connect()?;

        let nats_rpc_server_connection = self.nats_rpc_server.start()?;
        self.listen_for_rpc_task
            .replace(self.runtime.spawn(Self::start_listen_for_rpc_task(
                nats_rpc_server_connection,
                rpc_handler,
            )));

        info!("fnished starting pitaya server");
        Ok(())
    }

    async fn start_listen_for_rpc_task<RpcHandler>(
        mut rpc_server_connection: cluster::rpc_server::NatsServerConnection,
        mut rpc_handler: RpcHandler,
    ) where
        RpcHandler: FnMut(cluster::Rpc) + 'static,
    {
        use cluster::rpc_server::Connection;

        loop {
            match rpc_server_connection.next_rpc().await {
                Some(rpc) => {
                    rpc_handler(rpc);
                }
                None => {
                    debug!("listen rpc task exiting");
                    break;
                }
            }
        }
    }
}

pub struct PitayaBuilder<RpcHandler> {
    frontend: bool,
    server_kind: Option<ServerKind>,
    etcd_config: cluster::discovery::EtcdConfig,
    rpc_client_config: cluster::rpc_client::Config,
    rpc_server_config: cluster::rpc_server::Config,
    shutdown_timeout: time::Duration,
    rpc_handler: Option<RpcHandler>,
}

impl<RpcHandler> PitayaBuilder<RpcHandler>
where
    RpcHandler: FnMut(cluster::Rpc) + Send + 'static,
{
    pub fn new() -> Self {
        Self {
            frontend: false,
            server_kind: None,
            etcd_config: cluster::discovery::EtcdConfig::default(),
            rpc_client_config: cluster::rpc_client::Config::default(),
            rpc_server_config: cluster::rpc_server::Config::default(),
            shutdown_timeout: time::Duration::from_secs(10),
            rpc_handler: None,
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

    pub fn with_rpc_handler(mut self, handler: RpcHandler) -> Self {
        self.rpc_handler.replace(handler);
        self
    }

    pub fn build(self) -> Result<Pitaya, Error> {
        let mut p = Pitaya::new(
            self.frontend,
            self.server_kind
                .expect("server kind should be provided to PitayaBuilder"),
            self.etcd_config,
            self.rpc_client_config,
            self.rpc_server_config,
            self.shutdown_timeout,
        );
        p.start(self.rpc_handler.expect("you should defined a rpc handler!"))?;
        Ok(p)
    }
}
