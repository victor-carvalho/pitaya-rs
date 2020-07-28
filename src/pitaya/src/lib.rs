mod constants;
pub mod context;
mod error;
mod ffi;
pub mod settings;

pub use error::Error;
pub use etcd_nats_cluster::{EtcdLazy, NatsRpcClient, NatsRpcServer};
use pitaya_core::cluster::server::{ServerId, ServerInfo, ServerKind};
use pitaya_core::metrics;
pub use pitaya_core::{cluster, protos, utils};
use slog::{debug, error, info, o, trace, warn};
use std::convert::TryFrom;
use std::{collections::HashMap, sync::Arc};
use tokio::{
    sync::{broadcast, mpsc, oneshot, Mutex},
    task,
};

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

struct Tasks {
    listen_for_rpc: task::JoinHandle<()>,
    graceful_shutdown: task::JoinHandle<()>,
}

struct SharedState {
    tasks: std::sync::Mutex<Option<Tasks>>,
}

// Pitaya represent a pitaya server.
// Currently, it only implements cluster mode.
pub struct Pitaya<D, S, C> {
    discovery: Arc<Mutex<D>>,
    rpc_server: S,
    rpc_client: C,
    shared_state: Arc<SharedState>,
    logger: slog::Logger,
    settings: Arc<settings::Settings>,
    metrics_server: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl<D, S, C> Clone for Pitaya<D, S, C>
where
    D: cluster::Discovery + 'static,
    S: cluster::RpcServer + Clone + 'static,
    C: cluster::RpcClient + Clone + 'static,
{
    fn clone(&self) -> Self {
        Self {
            discovery: self.discovery.clone(),
            rpc_server: self.rpc_server.clone(),
            rpc_client: self.rpc_client.clone(),
            shared_state: self.shared_state.clone(),
            logger: self.logger.clone(),
            settings: self.settings.clone(),
            metrics_server: self.metrics_server.clone(),
        }
    }
}

impl<D, S, C> Pitaya<D, S, C>
where
    D: cluster::Discovery + 'static,
    S: cluster::RpcServer + Clone + 'static,
    C: cluster::RpcClient + Clone + 'static,
{
    async fn new<'a>(
        server_info: Arc<ServerInfo>,
        logger: slog::Logger,
        frontend: bool,
        discovery: Arc<Mutex<D>>,
        rpc_server: S,
        rpc_client: C,
        settings: settings::Settings,
    ) -> Result<Self, Error> {
        if settings.server_kind.trim().is_empty() {
            return Err(Error::InvalidServerKind);
        }

        debug!(logger, "init"; "settings" => ?settings, "server_info" => ?server_info);

        Ok(Self {
            shared_state: Arc::new(SharedState {
                tasks: std::sync::Mutex::new(None),
            }),
            discovery,
            rpc_client,
            rpc_server,
            logger,
            settings: Arc::new(settings),
            metrics_server: Arc::new(Mutex::new(None)),
        })
    }

    pub async fn server_by_id(
        &mut self,
        server_id: &ServerId,
        server_kind: &ServerKind,
    ) -> Result<Option<Arc<ServerInfo>>, Error> {
        let mut discovery = self.discovery.lock().await;
        let server = discovery.server_by_id(server_id, server_kind).await?;
        Ok(server)
    }

    pub async fn shutdown(mut self) -> Result<(), Error> {
        let tasks = self
            .shared_state
            .tasks
            .lock()
            .unwrap()
            .take()
            .expect("tasks should've been created");

        info!(self.logger, "shutting down pitaya server");

        info!(self.logger, "stopping service discovery");
        self.discovery.lock().await.shutdown().await?;
        info!(self.logger, "stopped");

        info!(self.logger, "stopping rpc client");
        self.rpc_client.shutdown().await?;

        info!(self.logger, "stopping rpc server");
        self.rpc_server.shutdown().await?;

        info!(self.logger, "waiting listen for rpc task");
        tasks.listen_for_rpc.await?;

        info!(self.logger, "waiting graceful shutdown task");
        tasks.graceful_shutdown.await?;
        Ok(())
    }

    pub async fn send_rpc_to_server(
        &mut self,
        server_id: &ServerId,
        server_kind: &ServerKind,
        req: protos::Request,
    ) -> Result<protos::Response, Error> {
        debug!(self.logger, "sending rpc");

        let server = {
            debug!(self.logger, "getting servers");
            self.discovery
                .lock()
                .await
                .server_by_id(server_id, server_kind)
                .await?
        };

        if let Some(server) = server {
            debug!(self.logger, "sending rpc");
            let res = self.rpc_client.call(server, req).await.map(|res| {
                trace!(self.logger, "received rpc response"; "res" => ?res);
                res
            })?;
            Ok(res)
        } else {
            Err(Error::NoServersFound(server_kind.clone()))
        }
    }

    pub async fn send_rpc(
        &mut self,
        route: &str,
        req: protos::Request,
    ) -> Result<protos::Response, Error> {
        debug!(self.logger, "sending rpc");

        let route = Route::try_from(route)?;
        let server_kind = ServerKind::from(route.server_kind);

        debug!(self.logger, "getting servers");
        let servers = self
            .discovery
            .lock()
            .await
            .servers_by_kind(&server_kind)
            .await?;

        debug!(self.logger, "getting random server");
        if let Some(random_server) = utils::random_server(&servers) {
            debug!(self.logger, "sending rpc");
            let res = self.rpc_client.call(random_server, req).await.map(|res| {
                trace!(self.logger, "received rpc response"; "res" => ?res);
                res
            })?;
            Ok(res)
        } else {
            error!(self.logger, "found no servers for kind"; "kind" => &server_kind.0);
            Err(Error::NoServersFound(server_kind))
        }
    }

    pub async fn send_kick(
        &mut self,
        server_id: ServerId,
        server_kind: ServerKind,
        kick_msg: protos::KickMsg,
    ) -> Result<protos::KickAnswer, Error> {
        let res = self
            .rpc_client
            .kick_user(server_id, server_kind, kick_msg)
            .await?;
        Ok(res)
    }

    pub async fn send_push_to_user(
        &mut self,
        server_id: ServerId,
        server_kind: ServerKind,
        push_msg: protos::Push,
    ) -> Result<(), Error> {
        self.rpc_client
            .push_to_user(server_id, server_kind, push_msg)
            .await?;
        Ok(())
    }

    async fn start<RpcHandler>(
        &mut self,
        rpc_handler: RpcHandler,
    ) -> Result<oneshot::Receiver<()>, Error>
    where
        RpcHandler: FnMut(cluster::Rpc) + Send + 'static,
    {
        info!(self.logger, "starting pitaya server");

        let (graceful_shutdown_sender, graceful_shutdown_receiver) = oneshot::channel();

        // NOTE(lhahn): I don't expect that we'll attemp to send more than 20 die messages.
        let (app_die_sender, app_die_receiver) = broadcast::channel(20);

        // Start the metrics server first if it is enabled.
        if self.settings.metrics.enabled {
            let addr = self
                .settings
                .metrics
                .url
                .parse()
                .map_err(|_e| Error::InvalidAddress {
                    module: "metrics server".to_string(),
                    address: self.settings.metrics.url.to_string(),
                })?;

            let ms = metrics::start_server(
                self.logger.clone(),
                self.settings.metrics.namespace.clone(),
                addr,
                app_die_receiver,
            );
            self.metrics_server.lock().await.replace(tokio::spawn(ms));
        }

        let graceful_shutdown = tokio::spawn(Self::graceful_shutdown_task(
            self.logger.new(o!("task" => "graceful_shutdown")),
            graceful_shutdown_sender,
            app_die_sender.subscribe(),
        ));

        self.rpc_client.start().await?;

        let rpc_server_connection = self.rpc_server.start().await?;
        let listen_for_rpc = tokio::spawn(Self::start_listen_for_rpc_task(
            self.logger.new(o!("task" => "start_listen_for_rpc")),
            rpc_server_connection,
            rpc_handler,
        ));

        self.shared_state.tasks.lock().unwrap().replace(Tasks {
            listen_for_rpc,
            graceful_shutdown,
        });

        // Always start the service discovery last, since before getting RPCs we need to make
        // sure that the server is set up.
        self.discovery.lock().await.start(app_die_sender).await?;

        info!(self.logger, "finshed starting pitaya server");
        Ok(graceful_shutdown_receiver)
    }

    async fn graceful_shutdown_task(
        logger: slog::Logger,
        graceful_shutdown_sender: oneshot::Sender<()>,
        mut app_die_receiver: broadcast::Receiver<()>,
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
        mut rpc_server_connection: mpsc::Receiver<cluster::Rpc>,
        mut rpc_handler: RpcHandler,
    ) where
        RpcHandler: FnMut(cluster::Rpc) + 'static,
    {
        loop {
            match rpc_server_connection.recv().await {
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

    pub async fn add_cluster_subscriber(
        &mut self,
        mut subscriber: Box<dyn FnMut(cluster::Notification) + Send + 'static>,
    ) {
        let logger = self.logger.new(o!());
        let mut subscription = self.discovery.lock().await.subscribe();

        tokio::spawn(async move {
            loop {
                match subscription.recv().await {
                    Ok(n) => {
                        subscriber(n);
                    }
                    Err(broadcast::RecvError::Lagged(num_skipped_msgs)) => {
                        // This should not happen. The only case where this might be an issue is if the
                        // callback is doing some heavy processing for some reason.
                        warn!(logger, "cluster subscriber lagged behind!"; "num_messages" => num_skipped_msgs);
                    }
                    Err(broadcast::RecvError::Closed) => {
                        debug!(logger, "cluster subscriber channel closed");
                        return;
                    }
                }
            }
        });
    }

    pub fn logger(&self) -> slog::Logger {
        self.logger.clone()
    }
}

pub struct PitayaBuilder<'a, RpcHandler> {
    frontend: bool,
    rpc_handler: Option<RpcHandler>,
    logger: Option<slog::Logger>,
    cluster_subscriber: Option<Box<dyn FnMut(cluster::Notification) + Send + 'static>>,
    env_prefix: Option<&'a str>,
    config_file: Option<&'a str>,
    base_settings: settings::Settings,
}

impl<'a, RpcHandler> PitayaBuilder<'a, RpcHandler>
where
    RpcHandler: FnMut(cluster::Rpc) + Send + 'static,
{
    pub fn new() -> Self {
        Self {
            frontend: false,
            rpc_handler: None,
            logger: None,
            cluster_subscriber: None,
            env_prefix: None,
            config_file: None,
            base_settings: Default::default(),
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

    pub fn with_rpc_handler(mut self, handler: RpcHandler) -> Self {
        self.rpc_handler.replace(handler);
        self
    }

    pub fn with_cluster_subscriber<F>(mut self, subscriber: F) -> Self
    where
        F: FnMut(cluster::Notification) + Send + 'static,
    {
        self.cluster_subscriber.replace(Box::new(subscriber));
        self
    }

    pub fn with_env_prefix(mut self, prefix: &'a str) -> Self {
        self.env_prefix.replace(prefix);
        self
    }

    pub fn with_config_file(mut self, config_file: &'a str) -> Self {
        self.config_file.replace(config_file);
        self
    }

    pub fn with_base_settings(mut self, base_settings: settings::Settings) -> Self {
        self.base_settings = base_settings;
        self
    }

    pub async fn build(
        self,
    ) -> Result<
        (
            Pitaya<EtcdLazy, NatsRpcServer, NatsRpcClient>,
            oneshot::Receiver<()>,
        ),
        Error,
    > {
        let logger = self
            .logger
            .expect("a logger should be passed to PitayaBuilder");
        let settings =
            settings::Settings::new(self.base_settings, self.env_prefix, self.config_file)?;
        let etcd_settings = Arc::new(settings.etcd.clone());
        let nats_settings = Arc::new(settings.nats.clone());
        let server_id = uuid::Uuid::new_v4().to_string();
        let server_info = Arc::new(ServerInfo {
            id: ServerId(server_id),
            kind: ServerKind::from(&settings.server_kind),
            // TODO(lhahn): fill these options.
            metadata: HashMap::new(),
            hostname: "".to_owned(),
            frontend: self.frontend,
        });

        let discovery = Arc::new(Mutex::new(
            etcd_nats_cluster::EtcdLazy::new(logger.clone(), server_info.clone(), etcd_settings)
                .await?,
        ));
        let rpc_server = NatsRpcServer::new(
            logger.clone(),
            server_info.clone(),
            nats_settings.clone(),
            tokio::runtime::Handle::current(),
        );
        let rpc_client = NatsRpcClient::new(logger.clone(), nats_settings);

        let mut p = Pitaya::new(
            server_info,
            logger,
            self.frontend,
            discovery,
            rpc_server,
            rpc_client,
            settings,
        )
        .await?;

        if let Some(subscriber) = self.cluster_subscriber {
            p.add_cluster_subscriber(subscriber).await;
        }

        let shutdown_receiver = p
            .start(self.rpc_handler.expect("you should defined a rpc handler!"))
            .await?;
        Ok((p, shutdown_receiver))
    }
}
