pub(crate) mod discovery;
pub(crate) mod rpc_client;
pub(crate) mod rpc_server;

use crate::{error::Error, protos, utils, Route, ServerKind};
use discovery::ServiceDiscovery;
use log::{debug, error, info};
use rpc_client::RpcClient;
use rpc_server::Connection;
use std::convert::TryFrom;
use tokio::task;

pub struct Cluster<SD, Client, Server> {
    service_discovery: SD,
    client: Client,
    server_connection: Option<Server>,
    server_connection_task: Option<task::JoinHandle<()>>,
}

impl<SD, Client, Server> Cluster<SD, Client, Server>
where
    SD: ServiceDiscovery,
    Client: RpcClient,
    Server: rpc_server::Connection + Send + 'static,
{
    pub fn new(service_discovery: SD, client: Client, server_connection: Server) -> Self {
        Self {
            service_discovery: service_discovery,
            client: client,
            server_connection: Some(server_connection),
            server_connection_task: None,
        }
    }

    pub async fn listen_rpcs(&mut self) {
        assert!(
            self.server_connection.is_some(),
            "server connection should only be initialized once"
        );
        let mut server_connection = self.server_connection.take().unwrap();
        self.server_connection_task = Some(task::spawn(async move {
            loop {
                match server_connection.next_rpc().await {
                    Some(rpc) => {
                        debug!("received rpc from server");
                        let res = protos::Response {
                            data: "HEY, THIS IS THE SERVER".as_bytes().to_owned(),
                            error: None,
                        };
                        info!("ANSWERING RPC====");
                        if !rpc.respond(res) {
                            error!("failed to answer rpc");
                        }
                    }
                    None => {
                        info!("server connection was closed, exiting rpc task");
                        break;
                    }
                }
            }
        }));
    }

    pub async fn send_rpc(
        &mut self,
        route: &str,
        req: protos::Request,
    ) -> Result<protos::Response, Error> {
        let route = Route::try_from(route)?;
        let server_kind = ServerKind::from(route.server_kind);
        let servers = self.service_discovery.servers_by_kind(&server_kind).await?;
        if let Some(random_server) = utils::random_server(&servers) {
            self.client.call(random_server, req)
        } else {
            error!("found no servers for kind {}", server_kind.0);
            Err(Error::NoServersFound(server_kind))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        cluster::{discovery::EtcdLazy, rpc_client},
        Server, ServerId,
    };
    use std::{collections::HashMap, error::Error as StdError, sync::Arc, time::Duration};

    const ETCD_URL: &str = "localhost:2379";
    const NATS_URL: &str = "http://localhost:4222";

    #[test]
    fn cluster_send_rpc() -> Result<(), Box<dyn StdError>> {
        async fn test() {
            let server = Arc::new(Server {
                id: ServerId::from("my-server-id"),
                kind: ServerKind::from("room"),
                hostname: "".to_owned(),
                metadata: HashMap::new(),
                frontend: false,
            });

            let sd = EtcdLazy::new(
                "pitaya".to_owned(),
                server.clone(),
                ETCD_URL,
                Duration::from_secs(50),
            )
            .await
            .unwrap();

            let mut client = rpc_client::NatsClientBuilder::new(NATS_URL.to_owned()).build();
            client.connect().unwrap();

            let mut server = rpc_server::NatsRpcServer::new(server, NATS_URL.to_owned(), 10);
            let server_connection = server.start(100).unwrap();

            let mut cluster = Cluster::new(sd, client, server_connection);

            let res = cluster
                .send_rpc(
                    "room.room.join",
                    protos::Request {
                        r#type: protos::RpcType::User as i32,
                        msg: Some(protos::Msg {
                            r#type: protos::MsgType::MsgRequest as i32,
                            data: "sending some data".as_bytes().to_owned(),
                            route: "room.room.join".to_owned(),
                            ..protos::Msg::default()
                        }),
                        frontend_id: "".to_owned(),
                        metadata: "{}".as_bytes().to_owned(),
                        ..protos::Request::default()
                    },
                )
                .await
                .unwrap();

            assert!(res.error.is_none());
            assert!(String::from_utf8_lossy(&res.data).contains("success"));
        }

        let mut rt = tokio::runtime::Runtime::new()?;
        rt.block_on(test());

        Ok(())
    }
}
