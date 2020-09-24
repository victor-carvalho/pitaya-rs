use crate::{settings, tasks};
use async_trait::async_trait;
use etcd_client::GetOptions;
use pitaya_core::cluster::{Discovery, Error, Notification, ServerId, ServerInfo, ServerKind};
use slog::{debug, error, info, o, warn};
use std::collections::HashMap;
use std::iter::FromIterator;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;

pub(crate) struct ServersCache {
    servers_by_id: HashMap<ServerId, Arc<ServerInfo>>,
    servers_by_kind: HashMap<ServerKind, HashMap<ServerId, Arc<ServerInfo>>>,
    // Channel for notifying listeners for changes in the cache.
    notification_chan: (
        broadcast::Sender<Notification>,
        broadcast::Receiver<Notification>,
    ),
    logger: slog::Logger,
}

impl ServersCache {
    fn new(logger: slog::Logger, max_chan_size: usize) -> Self {
        Self {
            servers_by_id: HashMap::new(),
            servers_by_kind: HashMap::new(),
            notification_chan: broadcast::channel(max_chan_size),
            logger,
        }
    }

    fn by_id(&self, id: &ServerId) -> Option<Arc<ServerInfo>> {
        self.servers_by_id.get(id).cloned()
    }

    pub(crate) fn insert(&mut self, server: Arc<ServerInfo>) {
        self.servers_by_id
            .insert(server.id.clone(), server.clone())
            .map(|old_val| {
                warn!(
                    self.logger,
                    "cache was stale: updated old server"; "server" => ?old_val
                );
            })
            .or_else(|| {
                debug!(self.logger, "added server to cache"; "server" => ?server);
                self.notify(Notification::ServerAdded(server.clone()));
                None
            });
        self.servers_by_kind
            .entry(server.kind.clone())
            .and_modify(|servers| {
                servers.insert(server.id.clone(), server.clone());
            })
            .or_insert_with(|| HashMap::from_iter([(server.id.clone(), server)].iter().cloned()));
    }

    pub(crate) fn remove(&mut self, server_kind: &ServerKind, server_id: &ServerId) {
        if let Some(server) = self.servers_by_id.remove(server_id) {
            debug!(self.logger, "server removed from cache"; "server_id" => &server_id.0);
            self.notify(Notification::ServerRemoved(server));
        }
        self.servers_by_kind.remove(server_kind);
    }

    fn subscribe(&self) -> broadcast::Receiver<Notification> {
        debug!(self.logger, "adding one more notification subscriber");
        self.notification_chan.0.subscribe()
    }

    fn notify(&self, notification: Notification) {
        let notify_count = self
            .notification_chan
            .0
            .send(notification.clone())
            .unwrap_or(0);
        debug!(self.logger, "notified receivers"; "num" => notify_count, "notification" => ?notification);
    }
}

// This service discovery is a lazy implementation.
pub struct EtcdLazy {
    settings: Arc<settings::Etcd>,
    client: etcd_client::Client,
    this_server: Arc<ServerInfo>,
    lease_id: Option<i64>,
    keep_alive_task: Option<(
        tokio::task::JoinHandle<()>,
        tokio::sync::oneshot::Sender<()>,
    )>,
    watch_task: Option<(tokio::task::JoinHandle<()>, etcd_client::Watcher)>,
    servers_cache: Arc<RwLock<ServersCache>>,
    logger: slog::Logger,
}

impl EtcdLazy {
    pub async fn new(
        logger: slog::Logger,
        server: Arc<ServerInfo>,
        settings: Arc<settings::Etcd>,
    ) -> Result<Self, Error> {
        info!(logger, "connecting to etcd"; "url" => &settings.url);
        let client = etcd_client::Client::connect([&settings.url], None)
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        // TODO(lhahn): remove hardcoded max channel size.
        let max_chan_size = 80;
        Ok(Self {
            settings,
            client,
            this_server: server,
            servers_cache: Arc::new(RwLock::new(ServersCache::new(
                logger.new(o!()),
                max_chan_size,
            ))),
            lease_id: None,
            keep_alive_task: None,
            watch_task: None,
            logger,
        })
    }

    fn server_kind_prefix(&self, server_kind: Option<&ServerKind>) -> String {
        if let Some(kind) = server_kind {
            format!("{}/servers/{}/", self.settings.prefix, kind.0)
        } else {
            format!("{}/servers/", self.settings.prefix)
        }
    }

    async fn revoke_lease(&mut self) -> Result<(), etcd_client::Error> {
        if let Some(lease_id) = self.lease_id {
            self.client.lease_revoke(lease_id).await?;
            info!(self.logger, "lease revoked"; "lease_id" => lease_id);
        } else {
            warn!(self.logger, "lease not found, not revoking");
        }
        Ok(())
    }

    async fn cache_servers(&mut self, server_kind: Option<&ServerKind>) -> Result<(), Error> {
        if let Some(kind) = server_kind {
            debug!(
                self.logger,
                "server id not found in cache, filling cache for kind {}", kind.0,
            );
        } else {
            warn!(
                self.logger,
                "server id not found in cache, filling all ETCD servers",
            );
        }
        let resp = {
            let key_prefix = self.server_kind_prefix(server_kind);
            self.client
                .get(key_prefix, Some(GetOptions::new().with_prefix()))
                .await
                .map_err(|e| Error::ClusterCommunication(e.to_string()))?
        };
        // TODO(lhahn): add a metric here to know how much keys a server is fetching in one
        // single request. This might be useful in the future for debugging issues with
        // ETCD load.
        debug!(self.logger, "etcd returned {} keys", resp.kvs().len());
        for kv in resp.kvs() {
            match kv.value_str() {
                Ok(server_str) => {
                    let new_server: Arc<ServerInfo> =
                        Arc::new(match serde_json::from_str(server_str) {
                            Ok(s) => s,
                            Err(_e) => {
                                warn!(self.logger, "corrupt server"; "server_str" => server_str);
                                continue;
                            }
                        });
                    self.servers_cache.write().unwrap().insert(new_server);
                }
                Err(e) => {
                    warn!(self.logger, "could not get value from etcd key"; "err" => %e);
                }
            }
        }
        Ok(())
    }

    async fn grant_lease(&mut self, app_die_sender: broadcast::Sender<()>) -> Result<(), Error> {
        assert!(self.lease_id.is_none());
        assert!(self.keep_alive_task.is_none());

        let lease_response = self
            .client
            .lease_grant(self.settings.lease_ttl.as_secs() as i64, None)
            .await
            .map_err(|e| Error::ClusterCommunication(e.to_string()))?;
        self.lease_id = Some(lease_response.id());

        let (keeper, stream) = self
            .client
            .lease_keep_alive(lease_response.id())
            .await
            .map_err(|e| Error::ClusterCommunication(e.to_string()))?;
        let (stop_sender, stop_receiver) = tokio::sync::oneshot::channel::<()>();

        self.keep_alive_task = Some((
            tokio::spawn(tasks::lease_keep_alive(
                self.logger.new(o!("task" => "keep_alive")),
                self.settings.lease_ttl,
                keeper,
                stream,
                stop_receiver,
                app_die_sender,
            )),
            stop_sender,
        ));

        Ok(())
    }

    fn get_etcd_server_key(&self) -> String {
        format!(
            "{}/servers/{}/{}",
            self.settings.prefix, self.this_server.kind.0, self.this_server.id.0
        )
    }

    async fn add_server_to_etcd(&mut self) -> Result<(), Error> {
        assert!(self.lease_id.is_some());
        let key = self.get_etcd_server_key();
        let server_json = serde_json::to_vec(&*self.this_server).unwrap();
        let lease_id = self.lease_id.unwrap();
        let options = etcd_client::PutOptions::new().with_lease(lease_id);
        self.client
            .put(key, server_json, Some(options))
            .await
            .map_err(|e| Error::ClusterCommunication(e.to_string()))?;
        info!(self.logger, "added server to etcd");
        Ok(())
    }

    async fn start_watch(&mut self, app_die_sender: broadcast::Sender<()>) -> Result<(), Error> {
        let watch_prefix = format!("{}/servers/", self.settings.prefix);
        let options = etcd_client::WatchOptions::new().with_prefix();
        let (watcher, watch_stream) = self
            .client
            .watch(watch_prefix, Some(options))
            .await
            .map_err(|e| Error::ClusterCommunication(e.to_string()))?;

        info!(self.logger, "starting etcd watch");
        let handle = tokio::spawn(tasks::watch_task(
            self.logger.new(o!("task" => "watch")),
            self.servers_cache.clone(),
            self.settings.prefix.clone(),
            watch_stream,
            app_die_sender,
        ));
        self.watch_task = Some((handle, watcher));

        Ok(())
    }

    // This function only returns the servers without trying to cache servers.
    fn only_servers_by_kind(&mut self, server_kind: &ServerKind) -> Vec<Arc<ServerInfo>> {
        // TODO(lhahn): consider not converting between a HashMap and a vector here
        // and use a vector for storage instead.
        self.servers_cache
            .read()
            .unwrap()
            .servers_by_kind
            .get(server_kind)
            .map(|servers_hash| servers_hash.values().cloned().collect())
            .unwrap_or_default()
    }

    // This function only returns the server without trying to cache servers.
    fn only_server_by_id(&mut self, server_id: &ServerId) -> Option<Arc<ServerInfo>> {
        self.servers_cache.read().unwrap().by_id(server_id)
    }
}

#[async_trait]
impl Discovery for EtcdLazy {
    async fn start(&mut self, app_die_sender: broadcast::Sender<()>) -> Result<(), Error> {
        self.grant_lease(app_die_sender.clone()).await?;
        self.add_server_to_etcd().await?;
        self.start_watch(app_die_sender).await?;
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Error> {
        info!(self.logger, "stopping etcd service discovery");
        if let Some((handle, sender)) = self.keep_alive_task.take() {
            info!(self.logger, "cancelling keep alive task");
            if sender.send(()).is_err() {
                error!(self.logger, "failed to send stop message");
            }
            if let Err(e) = handle.await {
                error!(self.logger, "failed to wait for keep alive task"; "error" => %e);
            }
        }
        if let Some((handle, mut watcher)) = self.watch_task.take() {
            info!(self.logger, "cancelling watcher");
            if let Err(e) = watcher.cancel().await {
                error!(self.logger, "failed to cancel watcher"; "error" => %e);
            }
            if let Err(e) = handle.await {
                error!(self.logger, "failed to wait for watcher"; "error" => %e);
            }
        }
        if let Err(e) = self.revoke_lease().await {
            error!(self.logger, "failed to revoke lease"; "error" => %e);
        }
        Ok(())
    }

    async fn server_by_id(
        &mut self,
        server_id: &ServerId,
        server_kind: Option<&ServerKind>,
    ) -> Result<Option<Arc<ServerInfo>>, Error> {
        if let Some(server) = self.only_server_by_id(server_id) {
            return Ok(Some(server));
        }

        // If a server id was provided, we can cache it from ETCD, otherwise we'll
        // do an expensive search.
        self.cache_servers(server_kind).await?;

        Ok(self.only_server_by_id(server_id))
    }

    async fn servers_by_kind(
        &mut self,
        server_kind: &ServerKind,
    ) -> Result<Vec<Arc<ServerInfo>>, Error> {
        let servers = self.only_servers_by_kind(server_kind);
        if servers.is_empty() {
            // No servers were found, we'll try to fetch servers information from etcd.
            self.cache_servers(Some(server_kind)).await?;
        }
        Ok(self.only_servers_by_kind(server_kind))
    }

    fn subscribe(&mut self) -> broadcast::Receiver<Notification> {
        self.servers_cache.read().unwrap().subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants;
    use std::error::Error as StdError;
    use std::time::Duration;

    const INVALID_ETCD_URL: &str = "localhost:1234";

    fn new_server() -> Arc<ServerInfo> {
        Arc::new(ServerInfo {
            frontend: true,
            hostname: "".to_owned(),
            id: ServerId::new(),
            kind: ServerKind::new(),
            metadata: HashMap::new(),
        })
    }

    #[tokio::test]
    async fn sd_can_be_create() -> Result<(), Box<dyn StdError>> {
        let server = new_server();
        let _sd = EtcdLazy::new(
            test_helpers::get_root_logger(),
            server,
            Arc::new(settings::Etcd {
                prefix: "pitaya".to_owned(),
                url: constants::LOCAL_ETCD_URL.to_owned(),
                lease_ttl: Duration::from_secs(60),
            }),
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[should_panic]
    async fn sd_can_fail_creation() {
        let server = new_server();
        let _sd = EtcdLazy::new(
            test_helpers::get_root_logger(),
            server,
            Arc::new(settings::Etcd {
                prefix: "pitaya".to_owned(),
                url: INVALID_ETCD_URL.to_owned(),
                lease_ttl: Duration::from_secs(60),
            }),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn cache_empty_on_start() -> Result<(), Box<dyn StdError>> {
        let server = new_server();
        let sd = EtcdLazy::new(
            test_helpers::get_root_logger(),
            server,
            Arc::new(settings::Etcd {
                prefix: "pitaya".to_owned(),
                url: constants::LOCAL_ETCD_URL.to_owned(),
                lease_ttl: Duration::from_secs(60),
            }),
        )
        .await?;
        assert_eq!(sd.servers_cache.read().unwrap().servers_by_id.len(), 0);
        assert_eq!(sd.servers_cache.read().unwrap().servers_by_kind.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn server_by_id_works() -> Result<(), Box<dyn StdError>> {
        let mut sd = EtcdLazy::new(
            test_helpers::get_root_logger(),
            new_server(),
            Arc::new(settings::Etcd {
                prefix: "pitaya".to_owned(),
                url: constants::LOCAL_ETCD_URL.to_owned(),
                lease_ttl: Duration::from_secs(60),
            }),
        )
        .await?;

        let server = sd
            .server_by_id(
                &ServerId::from("random-id"),
                Some(&ServerKind::from("room")),
            )
            .await?;
        assert!(server.is_none());
        assert_eq!(sd.servers_cache.read().unwrap().servers_by_id.len(), 1);

        let mut server_id: Option<ServerId> = None;
        for (id, _) in sd.servers_cache.read().unwrap().servers_by_id.iter() {
            server_id = Some(id.clone());
        }

        let server = sd
            .server_by_id(server_id.as_ref().unwrap(), Some(&ServerKind::from("room")))
            .await?;

        assert!(server.is_some());
        assert_eq!(sd.servers_cache.read().unwrap().servers_by_id.len(), 1);
        assert_eq!(sd.servers_cache.read().unwrap().servers_by_kind.len(), 1);
        assert_eq!(
            sd.servers_cache
                .read()
                .unwrap()
                .servers_by_kind
                .get(&ServerKind::from("room"))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(server_id.unwrap(), server.unwrap().id);
        Ok(())
    }

    #[tokio::test]
    async fn server_by_kind_works() -> Result<(), Box<dyn StdError>> {
        let mut sd = EtcdLazy::new(
            test_helpers::get_root_logger(),
            new_server(),
            Arc::new(settings::Etcd {
                prefix: "pitaya".to_owned(),
                url: constants::LOCAL_ETCD_URL.to_owned(),
                lease_ttl: Duration::from_secs(60),
            }),
        )
        .await?;

        let servers = sd.servers_by_kind(&ServerKind::from("room")).await?;
        assert_eq!(servers.len(), 1);

        let servers = sd.servers_by_kind(&ServerKind::from("room2")).await?;
        assert_eq!(servers.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn server_lease_works() -> Result<(), Box<dyn StdError>> {
        let server = new_server();
        let (app_die_sender, _app_die_recv) = broadcast::channel(10);

        let mut sd = EtcdLazy::new(
            test_helpers::get_root_logger(),
            server,
            Arc::new(settings::Etcd {
                prefix: "pitaya".to_owned(),
                url: constants::LOCAL_ETCD_URL.to_owned(),
                lease_ttl: Duration::from_secs(60),
            }),
        )
        .await?;

        sd.start(app_die_sender).await?;
        assert!(sd.lease_id.is_some());
        assert!(sd.keep_alive_task.is_some());
        sd.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn server_watch_works() -> Result<(), Box<dyn StdError>> {
        let server = new_server();
        let mut sd = EtcdLazy::new(
            test_helpers::get_root_logger(),
            server,
            Arc::new(settings::Etcd {
                prefix: "pitaya".to_owned(),
                url: constants::LOCAL_ETCD_URL.to_owned(),
                lease_ttl: Duration::from_secs(60),
            }),
        )
        .await?;

        let mut subscribe_chan = sd.subscribe();

        let (app_die_sender, _app_die_recv) = broadcast::channel(10);
        sd.start(app_die_sender).await?;

        let servers_added = Arc::new(RwLock::new(Vec::new()));
        let servers_removed = Arc::new(RwLock::new(Vec::new()));

        let task_servers_added = servers_added.clone();
        let task_servers_removed = servers_removed.clone();
        tokio::spawn(async move {
            loop {
                match subscribe_chan.recv().await {
                    Ok(Notification::ServerAdded(sv)) => {
                        task_servers_added.write().unwrap().push(sv);
                    }
                    Ok(Notification::ServerRemoved(sv)) => {
                        task_servers_removed.write().unwrap().push(sv);
                    }
                    Err(_) => {
                        return;
                    }
                }
            }
        });

        // Wait a little bit, otherwise we'll have a rece condition reading both
        // RwLocks below.
        tokio::time::delay_for(Duration::from_millis(500)).await;

        assert_eq!(servers_added.read().unwrap().len(), 0);
        assert_eq!(servers_removed.read().unwrap().len(), 0);

        let servers = sd
            .servers_by_kind(&ServerKind::from("unknown-kind"))
            .await?;

        assert!(servers.is_empty());
        assert_eq!(servers_added.read().unwrap().len(), 0);
        assert_eq!(servers_removed.read().unwrap().len(), 0);

        let servers = sd.servers_by_kind(&ServerKind::from("room")).await?;

        // Wait a little bit, otherwise we'll have a rece condition reading both
        // RwLocks below.
        tokio::time::delay_for(Duration::from_millis(50)).await;

        assert_eq!(servers.len(), 1);
        assert_eq!(servers_added.read().unwrap().len(), 1);
        assert_eq!(servers_removed.read().unwrap().len(), 0);

        sd.shutdown().await?;

        Ok(())
    }
}
