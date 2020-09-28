mod constants;
mod error;
mod ffi;
pub mod settings;

pub use error::Error;
pub use pitaya_core::{
    cluster, context::Context, handler, message, metrics, protos, session::Session, state::State,
    utils, Never,
};
use pitaya_core::{
    cluster::server::{ServerId, ServerInfo, ServerKind},
    constants as core_constants, context, service, Route,
};
pub use pitaya_etcd_nats_cluster::{EtcdLazy, NatsRpcClient, NatsRpcServer};
pub use pitaya_macros::{handlers, json_handler, protobuf_handler};
use slog::{debug, error, info, o, trace, warn};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    sync::{broadcast, mpsc, oneshot, Mutex, RwLock},
    task,
};

struct Tasks {
    listen_for_rpc: task::JoinHandle<()>,
    graceful_shutdown: task::JoinHandle<()>,
}

struct SharedState {
    tasks: std::sync::Mutex<Option<Tasks>>,
}

struct ClusterComponents {
    discovery: Arc<Mutex<Box<dyn cluster::Discovery>>>,
    rpc_server: Arc<Box<dyn cluster::RpcServer>>,
    rpc_client: Arc<Box<dyn cluster::RpcClient>>,
}

/// Pitaya represent a pitaya server.
pub struct Pitaya {
    discovery: Arc<Mutex<Box<dyn cluster::Discovery>>>,
    rpc_server: Arc<Box<dyn cluster::RpcServer>>,
    rpc_client: Arc<Box<dyn cluster::RpcClient>>,
    shared_state: Arc<SharedState>,
    logger: slog::Logger,
    settings: Arc<settings::Settings>,
    metrics_reporter: metrics::ThreadSafeReporter,
    container: Arc<state::Container>,
    remote: Arc<service::Remote>,
}

impl Clone for Pitaya {
    fn clone(&self) -> Self {
        Self {
            discovery: self.discovery.clone(),
            rpc_server: self.rpc_server.clone(),
            rpc_client: self.rpc_client.clone(),
            shared_state: self.shared_state.clone(),
            logger: self.logger.clone(),
            settings: self.settings.clone(),
            metrics_reporter: self.metrics_reporter.clone(),
            container: self.container.clone(),
            remote: self.remote.clone(),
        }
    }
}

impl Pitaya {
    async fn new<'a>(
        server_info: Arc<ServerInfo>,
        logger: slog::Logger,
        cluster_components: ClusterComponents,
        metrics_reporter: metrics::ThreadSafeReporter,
        settings: settings::Settings,
        rpc_dispatch: service::RpcDispatch,
        container: Arc<state::Container>,
    ) -> Result<Self, Error> {
        if settings.server_kind.trim().is_empty() {
            return Err(Error::InvalidServerKind);
        }

        debug!(logger, "init"; "settings" => ?settings, "server_info" => ?server_info);
        let remote = Arc::new(service::Remote::new(
            logger.new(o!()),
            cluster_components.discovery.clone(),
            cluster_components.rpc_client.clone(),
            rpc_dispatch,
        ));

        Ok(Self {
            shared_state: Arc::new(SharedState {
                tasks: std::sync::Mutex::new(None),
            }),
            discovery: cluster_components.discovery,
            rpc_client: cluster_components.rpc_client,
            rpc_server: cluster_components.rpc_server,
            logger,
            settings: Arc::new(settings),
            metrics_reporter,
            container,
            remote,
        })
    }

    pub async fn server_by_id(
        &mut self,
        server_id: &ServerId,
        server_kind: &ServerKind,
    ) -> Result<Option<Arc<ServerInfo>>, Error> {
        let mut discovery = self.discovery.lock().await;
        let server = discovery.server_by_id(server_id, Some(server_kind)).await?;
        Ok(server)
    }

    pub async fn shutdown(self) -> Result<(), Error> {
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
        ctx: context::Context,
        server_id: &ServerId,
        server_kind: Option<&ServerKind>,
        route_str: &str,
        data: Vec<u8>,
    ) -> Result<protos::Response, Error> {
        debug!(self.logger, "sending rpc"; "server_id" => %server_id.0);

        let server_info = {
            debug!(self.logger, "getting servers");
            self.discovery
                .lock()
                .await
                .server_by_id(server_id, server_kind)
                .await?
        };

        if let Some(server_info) = server_info {
            let msg = message::Message {
                kind: message::Kind::Request,
                id: 0,
                route: route_str.to_string(),
                data,
                compressed: false,
                err: false,
            };

            debug!(self.logger, "sending rpc");
            let res = self
                .rpc_client
                .call(ctx, protos::RpcType::User, msg, server_info)
                .await
                .map(|res| {
                    trace!(self.logger, "received rpc response"; "res" => ?res);
                    res
                })?;
            Ok(res)
        } else {
            Err(Error::ServerIdNotFound(server_id.0.clone()))
        }
    }

    pub async fn send_rpc(
        &self,
        ctx: context::Context,
        route_str: &str,
        data: Vec<u8>,
    ) -> Result<protos::Response, Error> {
        debug!(self.logger, "sending rpc");

        let route = Route::try_from_str(route_str.to_string()).ok_or(Error::InvalidRoute)?;
        let server_kind = route
            .server_kind()
            .map(|k| ServerKind::from(k))
            .ok_or(Error::NoServerKindOnRoute(route_str.to_string()))?;

        debug!(self.logger, "getting servers");
        let servers = self
            .discovery
            .lock()
            .await
            .servers_by_kind(&server_kind)
            .await?;

        debug!(self.logger, "getting random server");
        if let Some(random_server_info) = utils::random_server(&servers) {
            debug!(self.logger, "sending rpc");

            let msg = message::Message {
                kind: message::Kind::Request,
                id: 0,
                route: route_str.to_string(),
                data,
                compressed: false,
                err: false,
            };

            let res = self
                .rpc_client
                .call(ctx, protos::RpcType::User, msg, random_server_info)
                .await
                .map(|res| {
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
        &self,
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
        &self,
        server_id: ServerId,
        server_kind: ServerKind,
        push_msg: protos::Push,
    ) -> Result<(), Error> {
        self.rpc_client
            .push_to_user(server_id, server_kind, push_msg)
            .await?;
        Ok(())
    }

    async fn start(&mut self) -> Result<oneshot::Receiver<()>, Error> {
        info!(self.logger, "starting pitaya server");

        let (graceful_shutdown_sender, graceful_shutdown_receiver) = oneshot::channel();
        let (app_die_sender, app_die_receiver) = broadcast::channel(20);

        self.metrics_reporter.write().await.start().await.unwrap();

        let graceful_shutdown = tokio::spawn(Self::graceful_shutdown_task(
            self.logger.new(o!("task" => "graceful_shutdown")),
            graceful_shutdown_sender,
            app_die_receiver,
        ));

        self.rpc_client.start().await?;

        let rpc_server_connection = self.rpc_server.start().await?;
        let listen_for_rpc = tokio::spawn(Self::start_handlers_task(
            self.logger.new(o!("task" => "start_listen_for_rpc")),
            rpc_server_connection,
            self.container.clone(),
            self.remote.clone(),
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
                let _ = graceful_shutdown_sender.send(());
            }
            _ = signal_interrupt.recv() => {
                warn!(logger, "received interrupt signal");
                let _ = graceful_shutdown_sender.send(());
            }
            _ = signal_terminate.recv() => {
                warn!(logger, "received terminate signal");
                let _ = graceful_shutdown_sender.send(());
            }
            _ = app_die_receiver.recv() => {
                warn!(logger, "received app die message");
                let _ = graceful_shutdown_sender.send(());
            }
        }
    }

    async fn start_handlers_task(
        logger: slog::Logger,
        mut rpc_server_connection: mpsc::Receiver<cluster::Rpc>,
        container: Arc<state::Container>,
        remote: Arc<service::Remote>,
    ) {
        loop {
            let maybe_rpc = rpc_server_connection.recv().await;

            if maybe_rpc.is_none() {
                debug!(logger, "listen rpc task exiting");
                break;
            }

            let rpc = maybe_rpc.unwrap();
            let logger = logger.clone();
            let container = container.clone();
            let remote = remote.clone();

            // Spawn task to handle the incoming RPC.
            let _ = tokio::spawn(async move {
                match context::Context::new(rpc.request(), container) {
                    Ok(ctx) => {
                        remote.process_rpc(ctx, rpc).await;
                    }
                    Err(e) => {
                        let response = utils::build_error_response(
                            core_constants::CODE_BAD_FORMAT,
                            format!("invalid request: {}", e),
                        );
                        if !rpc.respond(response) {
                            error!(logger, "failed to respond to rpc");
                        }
                    }
                }
            });
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

    pub async fn register_metrics(&mut self, metrics: Vec<metrics::Opts>) -> Result<(), Error> {
        for metric in metrics {
            if metric.buckets.len() > 0 {
                debug!(self.logger, "registering custom histogram: {}", metric.name);
                // If we have buckets it means this is a histogram.
                self.metrics_reporter
                    .write()
                    .await
                    .register_histogram(metric)?;
            } else {
                debug!(self.logger, "registering custom counter: {}", metric.name);
                self.metrics_reporter
                    .write()
                    .await
                    .register_counter(metric)?;
            }
        }
        Ok(())
    }

    pub async fn inc_counter(&self, name: &str) {
        if let Err(e) = self.metrics_reporter.read().await.inc_counter(name) {
            warn!(self.logger, "failed to increment counter"; "name" => name, "error" => %e);
        }
    }

    pub async fn observe_hist(&self, name: &str, value: f64, labels: &[&str]) {
        if let Err(e) = self
            .metrics_reporter
            .read()
            .await
            .observe_hist(name, value, labels)
        {
            warn!(self.logger, "failed to observe histogram"; "name" => name, "error" => %e);
        }
    }
}

pub struct PitayaBuilder<'a> {
    frontend: bool,
    logger: Option<slog::Logger>,
    cluster_subscriber: Option<Box<dyn FnMut(cluster::Notification) + Send + 'static>>,
    env_prefix: Option<&'a str>,
    config_file: Option<&'a str>,
    base_settings: settings::Settings,
    rpc_handler: Option<
        Box<dyn Fn(context::Context, Option<Session>, cluster::Rpc) + Send + Sync + 'static>,
    >,
    client_handlers: handler::Handlers,
    server_handlers: handler::Handlers,
    container: state::Container,
    metrics: Vec<metrics::Opts>,
}

impl<'a> Default for PitayaBuilder<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> PitayaBuilder<'a> {
    pub fn new() -> Self {
        Self {
            frontend: false,
            rpc_handler: None,
            logger: None,
            cluster_subscriber: None,
            env_prefix: None,
            config_file: None,
            base_settings: Default::default(),
            client_handlers: handler::Handlers::new(),
            server_handlers: handler::Handlers::new(),
            container: state::Container::new(),
            metrics: Vec::new(),
        }
    }

    pub fn with_logger(mut self, logger: slog::Logger) -> Self {
        self.logger.replace(logger);
        self
    }

    pub fn with_client_handlers(mut self, handlers: handler::Handlers) -> Self {
        self.client_handlers = handlers;
        self
    }

    pub fn with_server_handlers(mut self, handlers: handler::Handlers) -> Self {
        self.server_handlers = handlers;
        self
    }

    pub fn with_frontend(mut self, frontend: bool) -> Self {
        self.frontend = frontend;
        self
    }

    pub fn with_rpc_handler(
        mut self,
        handler: Box<
            dyn Fn(context::Context, Option<Session>, cluster::Rpc) + Send + Sync + 'static,
        >,
    ) -> Self {
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

    pub fn with_state<T: Sync + Send + 'static>(self, state: T) -> Self {
        if !self.container.set(state) {
            panic!("cannot set state for the given type, since it was already set");
        }
        self
    }

    pub fn with_custom_metrics(mut self, metrics: Vec<metrics::Opts>) -> Self {
        self.metrics = metrics;
        self
    }

    pub async fn build(mut self) -> Result<(Pitaya, oneshot::Receiver<()>), Error> {
        if self.rpc_handler.is_none()
            && (self.client_handlers.is_empty() && self.server_handlers.is_empty())
        {
            panic!("either client and server handlers should be defined or an RPC handler should be provided");
        }

        let logger = self
            .logger
            .expect("a logger should be passed to PitayaBuilder");
        let settings =
            settings::Settings::merge(self.base_settings, self.env_prefix, self.config_file)?;
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

        let metrics_reporter: metrics::ThreadSafeReporter = if settings.metrics.enabled {
            let metrics_addr =
                settings
                    .metrics
                    .url
                    .parse()
                    .map_err(|_e| Error::InvalidAddress {
                        module: "metrics".to_string(),
                        address: settings.metrics.url.clone(),
                    })?;
            Arc::new(RwLock::new(Box::new(
                prometheus_metrics::PrometheusReporter::new(
                    settings.metrics.namespace.clone(),
                    settings.metrics.const_labels.clone(),
                    logger.clone(),
                    metrics_addr,
                )?,
            )))
        } else {
            Arc::new(RwLock::new(Box::new(metrics::DummyReporter {})))
        };

        let discovery: Arc<Mutex<Box<dyn cluster::Discovery>>> = Arc::new(Mutex::new(Box::new(
            pitaya_etcd_nats_cluster::EtcdLazy::new(
                logger.clone(),
                server_info.clone(),
                etcd_settings,
            )
            .await?,
        )));

        // Freeze state, so we cannot modify it later.
        self.container.freeze();
        let container = Arc::new(self.container);

        let rpc_server: Arc<Box<dyn cluster::RpcServer>> = Arc::new(Box::new(NatsRpcServer::new(
            logger.clone(),
            server_info.clone(),
            nats_settings.clone(),
            tokio::runtime::Handle::current(),
            metrics_reporter.clone(),
        )));
        let rpc_client: Arc<Box<dyn cluster::RpcClient>> = Arc::new(Box::new(NatsRpcClient::new(
            logger.clone(),
            nats_settings,
            server_info.clone(),
        )));

        let rpc_dispatch = if let Some(rpc_handler) = self.rpc_handler {
            service::RpcDispatch::Raw(rpc_handler)
        } else {
            service::RpcDispatch::Handlers {
                client: Arc::new(self.client_handlers),
                server: Arc::new(self.server_handlers),
            }
        };

        let mut p = Pitaya::new(
            server_info,
            logger,
            ClusterComponents {
                discovery,
                rpc_server,
                rpc_client,
            },
            metrics_reporter,
            settings,
            rpc_dispatch,
            container,
        )
        .await?;

        p.register_metrics(self.metrics).await?;

        if let Some(subscriber) = self.cluster_subscriber {
            p.add_cluster_subscriber(subscriber).await;
        }

        let shutdown_receiver = p.start().await?;
        Ok((p, shutdown_receiver))
    }
}
