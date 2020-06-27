extern crate async_trait;
extern crate etcd_client;
extern crate futures;
extern crate nats;
extern crate prost;
extern crate serde;
extern crate serde_json;
extern crate slog;
extern crate slog_async;
extern crate slog_json;
extern crate slog_term;
extern crate tokio;
extern crate uuid;

mod cluster;
mod error;
mod ffi;
mod server;
#[cfg(test)]
mod test_helpers;
mod utils;

pub use cluster::{
    discovery::{EtcdConfig, ServiceDiscovery},
    rpc_client::{Config as RpcClientConfig, RpcClient},
    rpc_server::Config as RpcServerConfig,
    Rpc,
};
pub use error::Error;
use server::{Server, ServerId, ServerKind};
use slog::{debug, error, info, o, warn};
use std::convert::TryFrom;
use std::{collections::HashMap, sync::Arc, time};
use tokio::{
    sync::{mpsc, oneshot},
    task,
};

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
    _shutdown_timeout: time::Duration,
    listen_for_rpc_task: Option<task::JoinHandle<()>>,
    graceful_shutdown_task: Option<task::JoinHandle<()>>,
    pub(crate) logger: slog::Logger,
}

impl Pitaya {
    fn new(
        logger: slog::Logger,
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

        debug!(logger, "this server: {:?}", this_server);

        // TODO(lhahn): let user parameterize this runtime.
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| Error::Tokio(e))
            .expect("failed to create tokio runtime");

        let client = cluster::rpc_client::NatsClient::new(
            logger.new(o!("module" => "rpc_client")),
            rpc_client_config,
        );
        let server = cluster::rpc_server::NatsRpcServer::new(
            logger.new(o!("module" => "rpc_server")),
            this_server.clone(),
            rpc_server_config,
        );

        Self {
            this_server: this_server,
            etcd_config: etcd_config,
            service_discovery: None,
            nats_rpc_client: client,
            nats_rpc_server: server,
            runtime: rt,
            _shutdown_timeout: shutdown_timeout,
            listen_for_rpc_task: None,
            graceful_shutdown_task: None,
            logger: logger,
        }
    }

    pub fn server_by_id(
        &mut self,
        server_id: &ServerId,
        server_kind: &ServerKind,
    ) -> Result<Option<Arc<Server>>, Error> {
        assert!(self.service_discovery.is_some());
        let service_discovery = self.service_discovery.as_mut().unwrap();

        self.runtime
            .block_on(async move { service_discovery.server_by_id(server_id, server_kind).await })
    }

    pub fn shutdown(mut self) -> Result<(), Error> {
        let graceful_shutdown_task = self
            .graceful_shutdown_task
            .take()
            .expect("graceful shutdown task should've been created");
        let mut service_discovery = self
            .service_discovery
            .take()
            .expect("service discovery should've been created");
        let listen_for_rpc_task = self
            .listen_for_rpc_task
            .take()
            .expect("listen for rpc task should've been created");

        info!(self.logger, "shutting down pitaya server");
        self.nats_rpc_client.close();
        self.nats_rpc_server.stop()?;
        self.runtime
            .block_on(async move { listen_for_rpc_task.await })?;
        self.runtime
            .block_on(async move { service_discovery.stop().await })?;
        self.runtime
            .block_on(async move { graceful_shutdown_task.await })?;

        debug!(self.logger, "shutting down tokio runtime");
        // FIXME(lhahn): currently, a bug on Tokio will make shutdown_timeout
        // always wait, so dropping here will make the shutdown faster,
        // however with the risk of blocking the server indefinitely.
        // https://github.com/tokio-rs/tokio/issues/2314
        // self.runtime.shutdown_timeout(self.shutdown_timeout);
        drop(self.runtime);
        Ok(())
    }

    pub fn send_rpc(
        &mut self,
        route: &str,
        req: protos::Request,
    ) -> Result<protos::Response, Error> {
        use cluster::discovery::EtcdLazy;

        debug!(self.logger, "sending rpc");

        let route = Route::try_from(route)?;
        let server_kind = ServerKind::from(route.server_kind);

        debug!(self.logger, "getting servers");
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

        debug!(self.logger, "getting random server");
        if let Some(random_server) = utils::random_server(&servers) {
            debug!(self.logger, "sending rpc");
            self.nats_rpc_client.call(random_server, req)
        } else {
            error!(self.logger, "found no servers for kind"; "kind" => &server_kind.0);
            Err(Error::NoServersFound(server_kind))
        }
    }

    fn start<RpcHandler>(&mut self, rpc_handler: RpcHandler) -> Result<oneshot::Receiver<()>, Error>
    where
        RpcHandler: FnMut(cluster::Rpc) + Send + 'static,
    {
        info!(self.logger, "starting pitaya server");
        async fn start_etcd(
            logger: slog::Logger,
            server: Arc<Server>,
            config: EtcdConfig,
            app_die_sender: mpsc::Sender<()>,
        ) -> Result<cluster::discovery::EtcdLazy, Error> {
            let mut sd = cluster::discovery::EtcdLazy::new(logger, server, config).await?;
            sd.start(app_die_sender).await?;
            Ok(sd)
        }

        let (graceful_shutdown_sender, graceful_shutdown_receiver) = oneshot::channel();
        // NOTE(lhahn): I don't expect that we'll attemp to send more than 20 die messages.
        let (app_die_sender, app_die_receiver) = mpsc::channel(20);

        self.graceful_shutdown_task
            .replace(self.runtime.spawn(Self::graceful_shutdown_task(
                self.logger.new(o!("task" => "graceful_shutdown")),
                graceful_shutdown_sender,
                app_die_receiver,
            )));

        self.service_discovery.replace({
            let server = self.this_server.clone();
            let config = self.etcd_config.clone();
            self.runtime.block_on(start_etcd(
                self.logger.new(o!("module" => "service_discovery")),
                server,
                config,
                app_die_sender,
            ))
        }?);
        self.nats_rpc_client.connect()?;

        let nats_rpc_server_connection = self.nats_rpc_server.start()?;
        self.listen_for_rpc_task
            .replace(self.runtime.spawn(Self::start_listen_for_rpc_task(
                self.logger.new(o!("task" => "start_listen_for_rpc")),
                nats_rpc_server_connection,
                rpc_handler,
            )));

        info!(self.logger, "finshed starting pitaya server");
        Ok(graceful_shutdown_receiver)
    }

    async fn graceful_shutdown_task(
        logger: slog::Logger,
        graceful_shutdown_sender: oneshot::Sender<()>,
        mut app_die_receiver: mpsc::Receiver<()>,
    ) {
        use tokio::signal::unix::{signal, SignalKind};
        let mut signal_hangup =
            signal(SignalKind::hangup()).expect("failed to register signal handling");
        let mut signal_interrupt =
            signal(SignalKind::interrupt()).expect("failed to register signal handling");
        let mut signal_terminate =
            signal(SignalKind::terminate()).expect("failed to register signal handling");

        tokio::select! {
            _ = signal_hangup.recv() => {
                warn!(logger, "received hangup signal");
                if let Err(_) = graceful_shutdown_sender.send(()) {
                    error!(logger, "failed to send graceful shutdown message, receiver already dropped");
                }
                return;
            }
            _ = signal_interrupt.recv() => {
                warn!(logger, "received interrupt signal");
                if let Err(_) = graceful_shutdown_sender.send(()) {
                    error!(logger, "failed to send graceful shutdown message, receiver already dropped");
                }
                return;
            }
            _ = signal_terminate.recv() => {
                warn!(logger, "received terminate signal");
                if let Err(_) = graceful_shutdown_sender.send(()) {
                    error!(logger, "failed to send graceful shutdown message, receiver already dropped");
                }
                return;
            }
            _ = app_die_receiver.recv() => {
                warn!(logger, "received app die message");
                if let Err(_) = graceful_shutdown_sender.send(()) {
                    error!(logger, "failed to send graceful shutdown message, receiver already dropped");
                }
                return;
            }
        }
    }

    async fn start_listen_for_rpc_task<RpcHandler>(
        logger: slog::Logger,
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
                    debug!(logger, "listen rpc task exiting");
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
    logger: Option<slog::Logger>,
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
            logger: None,
        }
    }

    pub fn with_logger(mut self, logger: slog::Logger) -> Self {
        self.logger.replace(logger);
        self
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

    pub fn build(self) -> Result<(Pitaya, oneshot::Receiver<()>), Error> {
        let mut p = Pitaya::new(
            self.logger
                .expect("a logger should be passed to PitayaBuilder"),
            self.frontend,
            self.server_kind
                .expect("server kind should be provided to PitayaBuilder"),
            self.etcd_config,
            self.rpc_client_config,
            self.rpc_server_config,
            self.shutdown_timeout,
        );
        let shutdown_receiver =
            p.start(self.rpc_handler.expect("you should defined a rpc handler!"))?;
        Ok((p, shutdown_receiver))
    }
}
