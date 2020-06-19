use crate::{error::Error, protos, utils, Server};
use async_trait::async_trait;
use log::{debug, error, info, warn};
use prost::Message;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub struct Rpc {
    req: protos::Request,
    responder: oneshot::Sender<protos::Response>,
}

impl Rpc {
    pub fn respond(self, res: protos::Response) -> bool {
        self.responder.send(res).map(|_| true).unwrap_or(false)
    }
}

#[async_trait]
pub trait Connection {
    async fn next_rpc(&mut self) -> Option<Rpc>;
}

pub struct NatsServerConnection {
    rpc_receiver: mpsc::Receiver<Rpc>,
}

pub struct Config {
    pub address: String,
    pub max_reconnects: usize,
    pub max_rpcs_queued: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            address: String::from("http://localhost:4222"),
            max_reconnects: 5,
            max_rpcs_queued: 100,
        }
    }
}

pub struct NatsRpcServer {
    config: Config,
    connection: Arc<Mutex<Option<(nats::Connection, nats::subscription::Handler)>>>,
    this_server: Arc<Server>,
    runtime: Arc<Mutex<tokio::runtime::Runtime>>,
}

impl NatsRpcServer {
    pub fn new(this_server: Arc<Server>, config: Config) -> Self {
        Self {
            config: config,
            connection: Arc::new(Mutex::new(None)),
            this_server: this_server,
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

    pub fn start(&mut self) -> Result<NatsServerConnection, Error> {
        if self.connection.lock().unwrap().is_some() {
            warn!("nats rpc server was already started!");
            return Err(Error::RpcServerAlreadyStarted);
        }

        // TODO(lhahn): add callbacks here for sending metrics.
        let nats_connection = nats::ConnectionOptions::new()
            .max_reconnects(Some(self.config.max_reconnects))
            .connect(&self.config.address)
            .map_err(|e| Error::Nats(e))?;

        let (rpc_sender, rpc_receiver) = mpsc::channel(self.config.max_rpcs_queued);

        let sub = {
            let topic = utils::topic_for_server(&self.this_server);
            info!("rpc server subscribing on topic {}", topic);

            let sender = rpc_sender;
            let runtime = self.runtime.clone();
            let connection = self.connection.clone();
            nats_connection
                .subscribe(&topic)
                .map_err(|e| Error::Nats(e))?
                .with_handler(move |message| {
                    Self::on_nats_message(message, &sender, &runtime, connection.clone())
                })
        };

        self.connection
            .lock()
            .unwrap()
            .replace((nats_connection, sub));
        Ok(NatsServerConnection {
            rpc_receiver: rpc_receiver,
        })
    }

    fn on_nats_message(
        mut message: nats::Message,
        sender: &mpsc::Sender<Rpc>,
        runtime: &Arc<Mutex<tokio::runtime::Runtime>>,
        conn: Arc<Mutex<Option<(nats::Connection, nats::subscription::Handler)>>>,
    ) -> std::io::Result<()> {
        let mut sender = sender.clone();
        let req = Message::decode(message.data.as_ref())?;
        let (responder, response_receiver) = oneshot::channel();

        assert!(conn.lock().unwrap().is_some());

        let response_topic = match message.reply.take() {
            Some(topic) => topic,
            None => {
                error!("received empty response topic from nats message");
                return Ok(());
            }
        };

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
                            if let Some((ref mut conn, _)) = conn.lock().unwrap().deref_mut() {
                                debug!("responding rpc");
                                Self::respond(conn, &response_topic, response);
                            } else {
                                error!("CONNECTION NOT OPEN, CANNOT ANSWER");
                            }
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
    }

    fn respond(connection: &nats::Connection, reply_topic: &str, res: protos::Response) {
        let buffer = {
            let mut b = Vec::with_capacity(res.encoded_len());
            res.encode(&mut b).map(|_| b)
        }
        .expect("failed to encode response");
        let _ = connection.publish(reply_topic, buffer).map_err(|err| {
            error!("failed to respond rpc: {}", err);
        });
    }

    pub fn stop(&mut self) -> Result<(), Error> {
        if let Some((connection, sub_handler)) = self.connection.lock().unwrap().take() {
            sub_handler.unsubscribe().map_err(|e| Error::Nats(e))?;
            connection.close();
        }
        Ok(())
    }
}

#[async_trait]
impl Connection for NatsServerConnection {
    async fn next_rpc(&mut self) -> Option<Rpc> {
        self.rpc_receiver.recv().await
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        cluster::rpc_client::{self, RpcClient},
        ServerId, ServerKind,
    };
    use std::collections::HashMap;
    use std::error::Error as StdError;

    #[test]
    fn server_starts_and_stops() -> Result<(), Box<dyn StdError>> {
        let sv = Arc::new(Server {
            id: ServerId::from("my-id"),
            kind: ServerKind::from("room"),
            metadata: HashMap::new(),
            frontend: false,
            hostname: "".to_owned(),
        });

        let mut rt = tokio::runtime::Runtime::new()?;

        let mut rpc_server = NatsRpcServer::new(sv.clone(), Config::default());
        let mut rpc_server_conn = rpc_server.start()?;

        let handle = {
            rt.spawn(async move {
                loop {
                    if let Some(rpc) = rpc_server_conn.next_rpc().await {
                        let res = protos::Response {
                            data: "HEY, THIS IS THE SERVER".as_bytes().to_owned(),
                            error: None,
                        };
                        if let Err(_e) = rpc.responder.send(res) {
                            panic!("failed to respond rpc");
                        }
                    } else {
                        break;
                    }
                }
            })
        };

        {
            let mut client = rpc_client::NatsClient::new(rpc_client::Config::default());
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

            assert_eq!(
                String::from_utf8_lossy(&res.data),
                "HEY, THIS IS THE SERVER"
            );
            client.close();
        }

        rpc_server.stop()?;
        rt.block_on(handle)?;
        Ok(())
    }
}
