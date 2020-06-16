use crate::{error::Error, protos, utils, Server};
use async_trait::async_trait;
use log::{debug, error, info, warn};
use prost::Message;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

pub struct Rpc {
    req: protos::Request,
    responder: oneshot::Sender<protos::Response>,
}

#[async_trait]
pub trait RpcServer {
    async fn next_rpc(&mut self) -> Option<Rpc>;
}

pub struct NatsRpcServer {
    address: String,
    connection: Option<(nats::Connection, nats::subscription::Handler)>,
    max_reconnects: usize,
    this_server: Arc<Server>,
    rpc_channel: (mpsc::Sender<Rpc>, mpsc::Receiver<Rpc>),
    runtime: Arc<Mutex<tokio::runtime::Runtime>>,
}

impl NatsRpcServer {
    pub fn new(
        this_server: Arc<Server>,
        address: String,
        max_reconnects: usize,
        max_rpcs_queued: usize,
    ) -> Self {
        Self {
            address: address,
            connection: None,
            max_reconnects: max_reconnects,
            this_server: this_server,
            rpc_channel: mpsc::channel(max_rpcs_queued),
            runtime: Arc::new(Mutex::new(
                tokio::runtime::Builder::new()
                    .threaded_scheduler()
                    // TODO(lhahn): consider configuring the amount of threads here.
                    // I only used two initially, since the threads job will be mostly waiting
                    // returns from RPCs.
                    .core_threads(2)
                    .build()
                    .expect("failed to create the tokio scheduler"),
            )),
        }
    }

    pub fn start(&mut self) -> Result<(), Error> {
        if let Some(_) = self.connection {
            warn!("nats rpc server was already started!");
            return Ok(());
        }

        // TODO(lhahn): add callbacks here for sending metrics.
        let connection = nats::ConnectionOptions::new()
            .max_reconnects(Some(self.max_reconnects))
            .connect(&self.address)
            .map_err(|e| Error::Nats(e))?;

        let topic = utils::topic_for_server(&self.this_server);
        info!("rpc server subscribing on topic {}", topic);

        let sub = {
            let sender = self.rpc_channel.0.clone();
            let runtime = self.runtime.clone();
            connection
                .subscribe(&topic)
                .map_err(|e| Error::Nats(e))?
                .with_handler(move |message| {
                    let mut sender = sender.clone();
                    let req = Message::decode(message.data.as_ref())?;
                    let (responder, response_receiver) = oneshot::channel();

                    match sender.try_send(Rpc {
                        req: req,
                        responder: responder,
                    }) {
                        Ok(_) => {
                            let runtime = runtime.lock().unwrap();
                            // For the moment we are ignoring the handle returned by the task.
                            // Worst case scenario we will have to kill the task in the middle of its processing
                            // at the end of the program.
                            let _ = runtime.spawn(async move {
                                match response_receiver.await {
                                    Ok(response) => {
                                        // FIXME: this does not compile. Fix issue with lifetime.
                                        // self.respond("", response);
                                    }
                                    Err(e) => {
                                        // Errors happen here if the channel was closed before sending a message.
                                        error!("failed to receive response from RPC: {}", e)
                                    }
                                }
                            });
                            debug!("received request from nats");
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            // TODO(lhahn): respond 502 here, and add metric.
                            warn!("channel is full, dropping request");
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            warn!("rpc channel stoped being listened");
                        }
                    };

                    info!("received msg: {}", &message);
                    Ok(())
                })
        };

        self.connection = Some((connection, sub));
        Ok(())
    }

    fn respond(&self, reply_topic: &str, res: protos::Response) {
        self.connection.as_ref().map(|(conn, _)| {
            let buffer = {
                let mut b = Vec::with_capacity(res.encoded_len());
                res.encode(&mut b).map(|_| b)
            }
            .expect("failed to encode response");
            let _ = conn.publish(reply_topic, buffer).map_err(|err| {
                error!("failed to respond rpc: {}", err);
            });
        });
    }

    pub fn stop(&mut self) -> Result<(), Error> {
        if let Some((connection, sub_handler)) = self.connection.take() {
            sub_handler.unsubscribe().map_err(|e| Error::Nats(e))?;
            connection.close();
        }
        Ok(())
    }
}

#[async_trait]
impl RpcServer for NatsRpcServer {
    async fn next_rpc(&mut self) -> Option<Rpc> {
        let receiver = &mut self.rpc_channel.1;
        receiver.recv().await
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        cluster::rpc_client::{NatsClientBuilder, RpcClient},
        ServerId, ServerKind,
    };
    use std::collections::HashMap;
    use std::error::Error as StdError;
    use std::time::Duration;

    const NATS_URL: &str = "http://localhost:4222";

    #[test]
    fn server_starts_and_stops() -> Result<(), Box<dyn StdError>> {
        pretty_env_logger::init();

        let sv = Arc::new(Server {
            id: ServerId::from("my-id"),
            kind: ServerKind::from("room"),
            metadata: HashMap::new(),
            frontend: false,
            hostname: "".to_owned(),
        });

        let mut rpc_server = NatsRpcServer::new(sv.clone(), NATS_URL.to_owned(), 10, 100);
        rpc_server.start()?;

        {
            let mut client = NatsClientBuilder::new(NATS_URL.to_owned()).build();
            client.connect()?;

            let res = client.call(
                sv.clone(),
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
            )?;

            info!("RESULT: {:?}", res);

            client.close();
        }

        std::thread::sleep(Duration::from_secs(5));

        rpc_server.stop()?;
        Ok(())
    }
}
