extern crate async_trait;
extern crate config;
extern crate etcd_client;
extern crate futures;
extern crate humantime_serde;
extern crate nats;
extern crate prometheus;
extern crate prost;
extern crate serde;
extern crate serde_json;
extern crate slog;
extern crate slog_async;
extern crate slog_json;
extern crate slog_term;
extern crate tokio;
extern crate uuid;

pub mod cluster;
mod constants;
mod error;
mod ffi;
mod metrics;
mod server;
pub mod settings;
#[cfg(test)]
mod test_helpers;
mod utils;

pub use cluster::{discovery::ServiceDiscovery, rpc_client::RpcClient, Rpc};
pub use error::Error;
use server::{Server, ServerId, ServerKind};
use slog::{debug, error, info, o, trace, warn};
use std::convert::TryFrom;
use std::{collections::HashMap, sync::Arc};
use tokio::{
    sync::{broadcast, mpsc, oneshot},
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

struct Tasks {
    listen_for_rpc: task::JoinHandle<()>,
    graceful_shutdown: task::JoinHandle<()>,
}

struct SharedState {
    tasks: std::sync::Mutex<Option<Tasks>>,
}

// Pitaya represent a pitaya server.
// Currently, it only implements cluster mode.
#[derive(Clone)]
pub struct Pitaya {
    service_discovery: Arc<tokio::sync::Mutex<cluster::discovery::EtcdLazy>>,
    nats_rpc_client: cluster::rpc_client::NatsClient,
    nats_rpc_server: cluster::rpc_server::NatsRpcServer,
    shared_state: Arc<SharedState>,
    logger: slog::Logger,
    settings: Arc<settings::Settings>,
}

impl Pitaya {
    async fn new<'a>(
        logger: slog::Logger,
        frontend: bool,
        env_prefix: Option<&'a str>,
        config_file: Option<&'a str>,
        base_settings: settings::Settings,
    ) -> Result<Self, Error> {
        let settings = settings::Settings::new(base_settings, env_prefix, config_file)?;
        let nats_settings = Arc::new(settings.nats.clone());
        let etcd_settings = Arc::new(settings.etcd.clone());

        if settings.server_kind.trim().is_empty() {
            return Err(Error::InvalidServerKind);
        }

        let server_id = uuid::Uuid::new_v4().to_string();
        let this_server = Arc::new(Server {
            id: ServerId(server_id),
            kind: ServerKind::from(&settings.server_kind),
            // TODO(lhahn): fill these options.
            metadata: HashMap::new(),
            hostname: "".to_owned(),
            frontend,
        });

        debug!(logger, "init"; "settings" => ?settings, "this_server" => ?this_server);

        let nats_rpc_client = cluster::rpc_client::NatsClient::new(
            logger.new(o!("module" => "rpc_client")),
            nats_settings.clone(),
        );
        let nats_rpc_server = cluster::rpc_server::NatsRpcServer::new(
            logger.new(o!("module" => "rpc_server")),
            this_server.clone(),
            nats_settings,
            tokio::runtime::Handle::current(),
        );
        let service_discovery = {
            let logger = logger.new(o!("module" => "service_discovery"));
            cluster::discovery::EtcdLazy::new(logger, this_server.clone(), etcd_settings).await
        }?;

        Ok(Self {
            shared_state: Arc::new(SharedState {
                tasks: std::sync::Mutex::new(None),
            }),
            service_discovery: Arc::new(tokio::sync::Mutex::new(service_discovery)),
            nats_rpc_client,
            nats_rpc_server,
            logger,
            settings: Arc::new(settings),
        })
    }

    pub async fn server_by_id(
        &mut self,
        server_id: &ServerId,
        server_kind: &ServerKind,
    ) -> Result<Option<Arc<Server>>, Error> {
        let mut service_discovery = self.service_discovery.lock().await;
        service_discovery.server_by_id(server_id, server_kind).await
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
        self.service_discovery.lock().await.stop().await?;
        info!(self.logger, "stopped");

        info!(self.logger, "stopping rpc client");
        self.nats_rpc_client.close();

        info!(self.logger, "stopping rpc server");
        self.nats_rpc_server.stop()?;

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
            self.service_discovery
                .lock()
                .await
                .server_by_id(server_id, server_kind)
                .await?
        };

        if let Some(server) = server {
            debug!(self.logger, "sending rpc");
            self.nats_rpc_client.call(server, req).await.map(|res| {
                trace!(self.logger, "received rpc response"; "res" => ?res);
                res
            })
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
            .service_discovery
            .lock()
            .await
            .servers_by_kind(&server_kind)
            .await?;

        debug!(self.logger, "getting random server");
        if let Some(random_server) = utils::random_server(&servers) {
            debug!(self.logger, "sending rpc");
            self.nats_rpc_client
                .call(random_server, req)
                .await
                .map(|res| {
                    trace!(self.logger, "received rpc response"; "res" => ?res);
                    res
                })
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
        self.nats_rpc_client
            .kick_user(server_id, server_kind, kick_msg)
            .await
    }

    pub async fn send_push_to_user(
        &mut self,
        server_id: ServerId,
        server_kind: ServerKind,
        push_msg: protos::Push,
    ) -> Result<(), Error> {
        self.nats_rpc_client
            .push_to_user(server_id, server_kind, push_msg)
            .await
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
        let (app_die_sender, app_die_receiver) = mpsc::channel(20);

        let graceful_shutdown = tokio::spawn(Self::graceful_shutdown_task(
            self.logger.new(o!("task" => "graceful_shutdown")),
            graceful_shutdown_sender,
            app_die_receiver,
        ));

        self.nats_rpc_client.connect()?;

        let nats_rpc_server_connection = self.nats_rpc_server.start()?;
        let listen_for_rpc = tokio::spawn(Self::start_listen_for_rpc_task(
            self.logger.new(o!("task" => "start_listen_for_rpc")),
            nats_rpc_server_connection,
            rpc_handler,
        ));

        self.shared_state.tasks.lock().unwrap().replace(Tasks {
            listen_for_rpc,
            graceful_shutdown,
        });

        // Always start the service discovery last, since before getting RPCs we need to make
        // sure that the server is set up.
        self.service_discovery
            .lock()
            .await
            .start(app_die_sender)
            .await?;

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

    pub async fn add_cluster_subscriber(
        &mut self,
        mut subscriber: Box<dyn FnMut(cluster::Notification) + Send + 'static>,
    ) {
        let logger = self.logger.new(o!());
        let mut subscription = self.service_discovery.lock().await.subscribe();

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

    pub async fn build(self) -> Result<(Pitaya, oneshot::Receiver<()>), Error> {
        let mut p = Pitaya::new(
            self.logger
                .expect("a logger should be passed to PitayaBuilder"),
            self.frontend,
            self.env_prefix,
            self.config_file,
            self.base_settings,
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
