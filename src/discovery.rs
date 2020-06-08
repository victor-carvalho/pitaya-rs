use super::{Error, Server, ServerId, ServerKind};
use async_trait::async_trait;
use etcd_client::GetOptions;
use log::info;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::future::Future;
use std::sync::Arc;

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
    async fn servers_by_type(&mut self, sv_type: &ServerKind) -> Result<Vec<Arc<Server>>, Error>;

    fn add_listener(&mut self, _listener: Box<dyn Listener>) {}
    fn remove_listener(&mut self, _listener: Box<dyn Listener>) {}
}

// This service discovery is a lazy implementation.
struct EtcdLazy {
    client: etcd_client::Client,
    prefix: String,
    this_server: Server,
    lease_id: Option<i64>,
    listeners: Vec<Box<dyn Listener + Send>>,

    // TODO: Shouldn't this fields be mutexes?
    servers_by_id: HashMap<ServerId, Arc<Server>>,
    servers_by_type: HashMap<ServerKind, HashMap<ServerId, Arc<Server>>>,
}

impl EtcdLazy {
    pub(crate) async fn new(
        prefix: String,
        server: Server,
        url: &str,
    ) -> Result<Self, etcd_client::Error> {
        let client = etcd_client::Client::connect([url], None).await?;
        Ok(Self {
            client: client,
            prefix: prefix,
            this_server: server,
            servers_by_id: HashMap::new(),
            servers_by_type: HashMap::new(),
            lease_id: None,
            listeners: Vec::new(),
        })
    }

    fn server_kind_prefix(&self, server_kind: &ServerKind) -> String {
        format!("{}/servers/{}/", self.prefix, server_kind.0)
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
        use std::iter::FromIterator;
        info!("etcd returned {} keys", resp.kvs().len());
        for kv in resp.kvs() {
            let server_str = kv.value_str()?;
            println!("server string: {}", server_str);
            let new_server: Arc<Server> = Arc::new(serde_json::from_str(server_str)?);
            let new_server_id = new_server.id.clone();

            self.servers_by_id
                .insert(new_server.id.clone(), new_server.clone());

            self.servers_by_type
                .entry(new_server.kind.clone())
                .and_modify(|servers| {
                    servers.insert(new_server_id.clone(), new_server.clone());
                })
                .or_insert(HashMap::from_iter(
                    [(new_server_id, new_server)].iter().cloned(),
                ));
        }
        Ok(())
    }
}

#[async_trait]
impl ServiceDiscovery for EtcdLazy {
    async fn server_by_id(
        &mut self,
        server_id: &ServerId,
        server_kind: &ServerKind,
    ) -> Result<Option<Arc<Server>>, Error> {
        if let Some(server) = self.servers_by_id.get(server_id).map(|sv| sv.clone()) {
            return Ok(Some(server));
        }

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
        self.cache_server_kind(server_kind).await?;

        Ok(self.servers_by_id.get(server_id).map(|sv| sv.clone()))
    }

    async fn servers_by_type(&mut self, _server: &ServerKind) -> Result<Vec<Arc<Server>>, Error> {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

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
        let _sd =
            rt.block_on(async move { EtcdLazy::new("pitaya".to_owned(), server, ETCD_URL).await })?;
        Ok(())
    }

    #[test]
    #[should_panic]
    fn sd_can_fail_creation() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let server = new_server();
        let _sd = rt
            .block_on(
                async move { EtcdLazy::new("pitaya".to_owned(), server, INVALID_ETCD_URL).await },
            )
            .unwrap();
    }

    #[test]
    fn cache_empty_on_start() -> Result<(), Box<dyn StdError>> {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let server = new_server();
        let sd =
            rt.block_on(async move { EtcdLazy::new("pitaya".to_owned(), server, ETCD_URL).await })?;
        assert_eq!(sd.servers_by_id.len(), 0);
        assert_eq!(sd.servers_by_type.len(), 0);
        Ok(())
    }

    async fn server_by_id_main(
        server_id: &ServerId,
    ) -> Result<(EtcdLazy, Option<Arc<Server>>), Box<dyn StdError>> {
        let server = new_server();
        let mut sd = EtcdLazy::new("pitaya".to_owned(), server, ETCD_URL).await?;
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
        assert_eq!(sd.servers_by_id.len(), 1);

        let mut server_id: Option<ServerId> = None;
        for (id, _) in sd.servers_by_id.iter() {
            server_id = Some(id.clone());
        }

        let (sd, server) = rt.block_on(server_by_id_main(server_id.as_ref().unwrap()))?;

        assert!(server.is_some());
        assert_eq!(sd.servers_by_id.len(), 1);
        assert_eq!(sd.servers_by_type.len(), 1);
        assert_eq!(
            sd.servers_by_type
                .get(&ServerKind::from("room"))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(server_id.unwrap(), server.unwrap().id);
        Ok(())
    }

    #[test]
    fn server_by_type_works() -> Result<(), Box<dyn StdError>> {
        unimplemented!()
    }

    #[test]
    fn server_lease_works() -> Result<(), Box<dyn StdError>> {
        unimplemented!()
    }

    #[test]
    fn server_watch_works() -> Result<(), Box<dyn StdError>> {
        unimplemented!()
    }
}
