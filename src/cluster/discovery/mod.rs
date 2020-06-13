use crate::{error::Error, Server, ServerId, ServerKind};
use async_trait::async_trait;
use etcd_client::GetOptions;
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::iter::FromIterator;
use std::sync::{Arc, Mutex};
use std::time::Duration;

mod tasks;

trait Listener {
    fn server_added(&mut self, server: Server);
    fn server_removed(&mut self, server: Server);
}

#[async_trait]
trait ServiceDiscovery {
    async fn server_by_id(
        &mut self,
        id: &ServerId,
        kind: &ServerKind,
    ) -> Result<Option<Arc<Server>>, Error>;
    async fn servers_by_kind(&mut self, sv_type: &ServerKind) -> Result<Vec<Arc<Server>>, Error>;

    fn add_listener(&mut self, _listener: Box<dyn Listener>) {}
    fn remove_listener(&mut self, _listener: Box<dyn Listener>) {}
}

struct ServersCache {
    servers_by_id: HashMap<ServerId, Arc<Server>>,
    servers_by_kind: HashMap<ServerKind, HashMap<ServerId, Arc<Server>>>,
}

impl ServersCache {
    fn new() -> Self {
        Self {
            servers_by_id: HashMap::new(),
            servers_by_kind: HashMap::new(),
        }
    }

    fn by_id(&self, id: &ServerId) -> Option<Arc<Server>> {
        self.servers_by_id.get(id).map(|s| s.clone())
    }

    fn insert(&mut self, server: Arc<Server>) {
        self.servers_by_id.insert(server.id.clone(), server.clone());
        self.servers_by_kind
            .entry(server.kind.clone())
            .and_modify(|servers| {
                servers.insert(server.id.clone(), server.clone());
            })
            .or_insert(HashMap::from_iter(
                [(server.id.clone(), server)].iter().cloned(),
            ));
    }

    fn remove(&mut self, server_kind: &ServerKind, server_id: &ServerId) {
        self.servers_by_id.remove(server_id);
        self.servers_by_kind.remove(server_kind);
    }
}

// This service discovery is a lazy implementation.
struct EtcdLazy {
    client: etcd_client::Client,
    prefix: String,
    this_server: Server,
    lease_id: Option<i64>,
    listeners: Vec<Box<dyn Listener + Send>>,
    keep_alive_task: Option<(
        tokio::task::JoinHandle<()>,
        tokio::sync::oneshot::Sender<()>,
    )>,
    watch_task: Option<(tokio::task::JoinHandle<()>, etcd_client::Watcher)>,
    lease_ttl: Duration,
    servers_cache: Arc<Mutex<ServersCache>>,
}

impl EtcdLazy {
    pub(crate) async fn new(
        prefix: String,
        server: Server,
        url: &str,
        lease_ttl: Duration,
    ) -> Result<Self, etcd_client::Error> {
        let client = etcd_client::Client::connect([url], None).await?;
        Ok(Self {
            client: client,
            prefix: prefix,
            this_server: server,
            servers_cache: Arc::new(Mutex::new(ServersCache::new())),
            lease_id: None,
            listeners: Vec::new(),
            keep_alive_task: None,
            lease_ttl: lease_ttl,
            watch_task: None,
        })
    }

    pub(crate) async fn start(
        &mut self,
        app_die_sender: tokio::sync::oneshot::Sender<()>,
    ) -> Result<(), Error> {
        self.grant_lease(app_die_sender).await?;
        self.add_server_to_etcd().await?;
        self.start_watch().await?;
        Ok(())
    }

    pub(crate) async fn stop(&mut self) -> Result<(), Error> {
        info!("stopping etcd service discovery");
        if let Some((handle, sender)) = self.keep_alive_task.take() {
            let _ = sender.send(()).map_err(|_| {
                error!("failed to send stop message");
            });
            handle.await?;
        }
        if let Some((handle, mut watcher)) = self.watch_task.take() {
            info!("cancelling watcher");
            watcher.cancel().await?;
            handle.await?;
        }
        info!("revoking lease");
        self.revoke_lease().await?;
        Ok(())
    }

    fn server_kind_prefix(&self, server_kind: &ServerKind) -> String {
        format!("{}/servers/{}/", self.prefix, server_kind.0)
    }

    async fn revoke_lease(&mut self) -> Result<(), Error> {
        if let Some(lease_id) = self.lease_id {
            self.client.lease_revoke(lease_id).await?;
        }
        Ok(())
    }

    async fn cache_server_kind(&mut self, server_kind: &ServerKind) -> Result<(), Error> {
        info!(
            "server id not found in cache, filling cache for kind {}",
            server_kind.0
        );
        let resp = {
            let key_prefix = self.server_kind_prefix(server_kind);
            self.client
                .get(key_prefix, Some(GetOptions::new().with_prefix()))
                .await?
        };
        info!("etcd returned {} keys", resp.kvs().len());
        for kv in resp.kvs() {
            let server_str = kv.value_str()?;
            println!("server string: {}", server_str);
            let new_server: Arc<Server> = Arc::new(serde_json::from_str(server_str)?);
            let new_server_id = new_server.id.clone();

            let mut servers_cache = self.servers_cache.lock().unwrap();
            servers_cache.insert(new_server);
        }
        Ok(())
    }

    async fn grant_lease(
        &mut self,
        app_die_sender: tokio::sync::oneshot::Sender<()>,
    ) -> Result<(), Error> {
        assert!(self.lease_id.is_none());
        assert!(self.keep_alive_task.is_none());

        let lease_response = self
            .client
            .lease_grant(self.lease_ttl.as_secs() as i64, None)
            .await?;
        self.lease_id = Some(lease_response.id());

        let (keeper, stream) = self.client.lease_keep_alive(lease_response.id()).await?;
        let (stop_sender, stop_receiver) = tokio::sync::oneshot::channel::<()>();

        self.keep_alive_task = Some((
            tokio::spawn(tasks::lease_keep_alive(
                self.lease_ttl.clone(),
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
            self.prefix, self.this_server.kind.0, self.this_server.id.0
        )
    }

    async fn add_server_to_etcd(&mut self) -> Result<(), Error> {
        assert!(self.lease_id.is_some());
        let key = self.get_etcd_server_key();
        let server_json = serde_json::to_vec(&self.this_server)?;
        if let Some(lease_id) = self.lease_id {
            let options = etcd_client::PutOptions::new().with_lease(lease_id);
            self.client.put(key, server_json, Some(options)).await?;
        } else {
            unreachable!();
        }
        Ok(())
    }

    async fn start_watch(&mut self) -> Result<(), Error> {
        let watch_prefix = format!("{}/servers/", self.prefix);
        let options = etcd_client::WatchOptions::new().with_prefix();
        let (watcher, watch_stream) = self.client.watch(watch_prefix, Some(options)).await?;

        let handle = tokio::spawn(tasks::watch_task(
            self.servers_cache.clone(),
            self.prefix.clone(),
            watch_stream,
        ));
        self.watch_task = Some((handle, watcher));

        Ok(())
    }

    // This function only returns the servers without trying to cache servers.
    fn only_servers_by_kind(&mut self, server_kind: &ServerKind) -> Vec<Arc<Server>> {
        // TODO(lhahn): consider not converting between a HashMap and a vector here
        // and use a vector for storage instead.
        self.servers_cache
            .lock()
            .unwrap()
            .servers_by_kind
            .get(server_kind)
            .map(|servers_hash| servers_hash.values().map(|v| v.clone()).collect())
            .unwrap_or(Vec::new())
    }

    // This function only returns the server without trying to cache servers.
    fn only_server_by_id(&mut self, server_id: &ServerId) -> Option<Arc<Server>> {
        self.servers_cache
            .lock()
            .unwrap()
            .servers_by_id
            .get(server_id)
            .map(|sv| sv.clone())
    }
}

#[async_trait]
impl ServiceDiscovery for EtcdLazy {
    async fn server_by_id(
        &mut self,
        server_id: &ServerId,
        server_kind: &ServerKind,
    ) -> Result<Option<Arc<Server>>, Error> {
        if let Some(server) = self.only_server_by_id(server_id) {
            return Ok(Some(server));
        }
        let resp = {
            let key_prefix = self.server_kind_prefix(server_kind);
            self.client
                .get(key_prefix, Some(GetOptions::new().with_prefix()))
                .await?
        };
        info!("etcd returned {} keys", resp.kvs().len());
        self.cache_server_kind(server_kind).await?;

        Ok(self.only_server_by_id(server_id))
    }

    async fn servers_by_kind(
        &mut self,
        server_kind: &ServerKind,
    ) -> Result<Vec<Arc<Server>>, Error> {
        let servers = self.only_servers_by_kind(server_kind);
        if servers.len() == 0 {
            // No servers were found, we'll try to fetch servers information from etcd.
            self.cache_server_kind(server_kind).await?;
        }
        Ok(self.only_servers_by_kind(server_kind))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::error::Error as StdError;

    const ETCD_URL: &str = "localhost:2379";
    const INVALID_ETCD_URL: &str = "localhost:1234";

    fn new_server() -> Server {
        Server {
            frontend: true,
            hostname: "".to_owned(),
            id: ServerId::new(),
            kind: ServerKind::new(),
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn sd_can_be_create() -> Result<(), Box<dyn StdError>> {
        let mut rt = tokio::runtime::Runtime::new()?;
        let server = new_server();
        let _sd = rt.block_on(async move {
            EtcdLazy::new(
                "pitaya".to_owned(),
                server,
                ETCD_URL,
                Duration::from_secs(60),
            )
            .await
        })?;
        Ok(())
    }

    #[test]
    #[should_panic]
    fn sd_can_fail_creation() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let server = new_server();
        let _sd = rt
            .block_on(async move {
                EtcdLazy::new(
                    "pitaya".to_owned(),
                    server,
                    INVALID_ETCD_URL,
                    Duration::from_secs(60),
                )
                .await
            })
            .unwrap();
    }

    #[test]
    fn cache_empty_on_start() -> Result<(), Box<dyn StdError>> {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let server = new_server();
        let sd = rt.block_on(async move {
            EtcdLazy::new(
                "pitaya".to_owned(),
                server,
                ETCD_URL,
                Duration::from_secs(60),
            )
            .await
        })?;
        assert_eq!(sd.servers_cache.lock().unwrap().servers_by_id.len(), 0);
        assert_eq!(sd.servers_cache.lock().unwrap().servers_by_kind.len(), 0);
        Ok(())
    }

    async fn server_by_id_main(
        server_id: &ServerId,
    ) -> Result<(EtcdLazy, Option<Arc<Server>>), Box<dyn StdError>> {
        let server = new_server();
        let mut sd = EtcdLazy::new(
            "pitaya".to_owned(),
            server,
            ETCD_URL,
            Duration::from_secs(60),
        )
        .await?;
        let maybe_server = sd
            .server_by_id(server_id, &ServerKind::from("room"))
            .await?;
        Ok((sd, maybe_server))
    }

    #[test]
    fn server_by_id_works() -> Result<(), Box<dyn StdError>> {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let (sd, server) = rt.block_on(server_by_id_main(&ServerId::from("random-id")))?;
        assert!(server.is_none());
        assert_eq!(sd.servers_cache.lock().unwrap().servers_by_id.len(), 1);

        let mut server_id: Option<ServerId> = None;
        for (id, _) in sd.servers_cache.lock().unwrap().servers_by_id.iter() {
            server_id = Some(id.clone());
        }

        let (sd, server) = rt.block_on(server_by_id_main(server_id.as_ref().unwrap()))?;

        assert!(server.is_some());
        assert_eq!(sd.servers_cache.lock().unwrap().servers_by_id.len(), 1);
        assert_eq!(sd.servers_cache.lock().unwrap().servers_by_kind.len(), 1);
        assert_eq!(
            sd.servers_cache
                .lock()
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

    #[test]
    fn server_by_kind_works() -> Result<(), Box<dyn StdError>> {
        // pretty_env_logger::init();
        async fn test(server_kind: &ServerKind) -> Result<(EtcdLazy, Vec<Arc<Server>>), Error> {
            let server = new_server();
            let mut sd = EtcdLazy::new(
                "pitaya".to_owned(),
                server,
                ETCD_URL,
                Duration::from_secs(60),
            )
            .await?;
            let maybe_servers = sd.servers_by_kind(server_kind).await?;
            Ok((sd, maybe_servers))
        }

        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let (_, servers) = rt.block_on(test(&ServerKind::from("room")))?;
        assert_eq!(servers.len(), 1);

        let (_, servers) = rt.block_on(test(&ServerKind::from("room2")))?;
        assert_eq!(servers.len(), 0);

        Ok(())
    }

    async fn lease_test() -> Result<(), Box<dyn StdError>> {
        let server = new_server();
        let (app_die_sender, _app_die_recv) = tokio::sync::oneshot::channel();

        let mut sd = EtcdLazy::new(
            "pitaya".to_owned(),
            server,
            ETCD_URL,
            Duration::from_secs(10),
        )
        .await?;

        sd.start(app_die_sender).await?;

        assert!(sd.lease_id.is_some());
        assert!(sd.keep_alive_task.is_some());

        sd.stop().await?;

        Ok(())
    }

    #[test]
    fn server_lease_works() -> Result<(), Box<dyn StdError>> {
        pretty_env_logger::init();
        // unimplemented!()
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let _ = rt.block_on(lease_test())?;

        Ok(())
    }

    #[test]
    fn server_watch_works() -> Result<(), Box<dyn StdError>> {
        // pretty_env_logger::init();
        // unimplemented!()
        Ok(())
    }
}
