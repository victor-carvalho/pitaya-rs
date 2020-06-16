pub(crate) mod discovery;
pub(crate) mod rpc_client;
pub(crate) mod rpc_server;

use crate::{error::Error, protos, utils, Route, ServerKind};
use discovery::ServiceDiscovery;
use log::error;
use rpc_client::RpcClient;
use rpc_server::RpcServer;
use std::convert::TryFrom;

pub struct Cluster<SD, Client> {
    service_discovery: SD,
    client: Client,
    server: rpc_server::NatsRpcServer,
}

impl<SD, Client> Cluster<SD, Client>
where
    SD: ServiceDiscovery,
    Client: RpcClient,
{
    pub fn new(service_discovery: SD, client: Client, server: rpc_server::NatsRpcServer) -> Self {
        Self {
            service_discovery: service_discovery,
            client: client,
            server: server,
        }
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

            let mut server = rpc_server::NatsRpcServer::new(server, NATS_URL.to_owned(), 10, 100);
            server.start().unwrap();

            let mut cluster = Cluster::new(sd, client, server);

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
            // panic!("received res: {:?}", res);
        }

        let mut rt = tokio::runtime::Runtime::new()?;
        rt.block_on(test());

        Ok(())
    }
}
