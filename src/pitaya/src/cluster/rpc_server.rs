use super::Rpc;
use crate::{error::Error, metrics, protos, settings, utils, Server};
use async_trait::async_trait;
use prost::Message;
use slog::{debug, error, info, o, trace, warn};
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};

#[async_trait]
pub trait Connection {
    async fn next_rpc(&mut self) -> Option<Rpc>;
}

pub struct NatsServerConnection {
    rpc_receiver: mpsc::Receiver<Rpc>,
}

#[derive(Clone)]
pub struct NatsRpcServer {
    settings: Arc<settings::Nats>,
    connection: Arc<Mutex<Option<(nats::Connection, nats::subscription::Handler)>>>,
    this_server: Arc<Server>,
    runtime_handle: tokio::runtime::Handle,
    logger: slog::Logger,
}

impl NatsRpcServer {
    pub fn new(
        logger: slog::Logger,
        this_server: Arc<Server>,
        settings: Arc<settings::Nats>,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            settings,
            this_server,
            logger,
            connection: Arc::new(Mutex::new(None)),
            runtime_handle,
        }
    }

    pub fn start(&mut self) -> Result<NatsServerConnection, Error> {
        if self.connection.lock().unwrap().is_some() {
            warn!(self.logger, "nats rpc server was already started!");
            return Err(Error::RpcServerAlreadyStarted);
        }

        // TODO(lhahn): add callbacks here for sending metrics.
        info!(self.logger, "server connecting to nats"; "url" => &self.settings.url);
        let nats_connection = nats::ConnectionOptions::new()
            .max_reconnects(Some(self.settings.max_reconnection_attempts as usize))
            .connect(&self.settings.url)
            .map_err(Error::Nats)?;

        let (rpc_sender, rpc_receiver) = mpsc::channel(self.settings.max_rpcs_queued as usize);

        let sub = {
            let topic = utils::topic_for_server(&self.this_server);
            let logger = self.logger.new(o!());

            info!(self.logger, "rpc server subscribing"; "topic" => &topic);

            let sender = rpc_sender;
            let runtime_handle = self.runtime_handle.clone();
            let connection = self.connection.clone();
            nats_connection
                .subscribe(&topic)
                .map_err(Error::Nats)?
                .with_handler(move |message| {
                    Self::on_nats_message(
                        message,
                        &logger,
                        &sender,
                        runtime_handle.clone(),
                        connection.clone(),
                    )
                })
        };

        self.connection
            .lock()
            .unwrap()
            .replace((nats_connection, sub));
        Ok(NatsServerConnection { rpc_receiver })
    }

    fn on_nats_message(
        mut message: nats::Message,
        logger: &slog::Logger,
        sender: &mpsc::Sender<Rpc>,
        runtime_handle: tokio::runtime::Handle,
        conn: Arc<Mutex<Option<(nats::Connection, nats::subscription::Handler)>>>,
    ) -> std::io::Result<()> {
        debug!(logger, "received nats message"; "message" => %message);

        let rpc_start = Instant::now();
        let mut sender = sender.clone();
        let req: protos::Request = Message::decode(message.data.as_ref())?;
        let (responder, response_receiver) = oneshot::channel();

        let route = if let Some(msg) = req.msg.as_ref() {
            msg.route.to_string()
        } else {
            String::new()
        };

        assert!(conn.lock().unwrap().is_some());

        let response_topic = match message.reply.take() {
            Some(topic) => topic,
            None => {
                error!(logger, "received empty response topic from nats message");
                return Ok(());
            }
        };

        match sender.try_send(Rpc { req, responder }) {
            Ok(_) => {
                // For the moment we are ignoring the handle returned by the task.
                // Worst case scenario we will have to kill the task in the middle of its processing
                // at the end of the program.
                let _ = {
                    let logger = logger.clone();
                    // runtime.spawn(async move {
                    trace!(logger, "spawning response receiver task");
                    runtime_handle.spawn(async move {
                        match response_receiver.await {
                            Ok(response) => {
                                if let Some((ref mut conn, _)) = conn.lock().unwrap().deref_mut() {
                                    debug!(logger, "responding rpc");
                                    if let Err(err) = Self::respond(conn, &response_topic, response)
                                    {
                                        error!(logger, "failed to respond rpc"; "error" => %err);
                                        metrics::record_rpc_duration(rpc_start, &route, "failed");
                                    } else {
                                        metrics::record_rpc_duration(rpc_start, &route, "ok");
                                    }
                                } else {
                                    error!(logger, "connection not open, cannot answer");
                                }
                            }
                            Err(e) => {
                                // Errors happen here if the channel was closed before sending a message.
                                error!(logger, "failed to receive response from RPC"; "error" => %e);
                            }
                        }
                    })
                };
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                // TODO(lhahn): respond 502 here, and add metric.
                warn!(logger, "channel is full, dropping request");
                if let Some((ref mut conn, _)) = conn.lock().unwrap().deref_mut() {
                    let response = protos::Response {
                        error: Some(protos::Error {
                            code: "PIT-502".to_string(),
                            msg: "server is overwhelmed".to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    if let Err(err) = Self::respond(conn, &response_topic, response) {
                        error!(logger, "failed to respond rpc"; "error" => %err);
                    }
                    metrics::record_rpc_duration(rpc_start, &route, "failed");
                } else {
                    error!(logger, "connection not open, cannot answer");
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                warn!(logger, "rpc channel stoped being listened");
            }
        };

        Ok(())
    }

    fn respond(
        connection: &nats::Connection,
        reply_topic: &str,
        res: protos::Response,
    ) -> Result<(), Error> {
        let buffer = utils::encode_proto(&res);
        connection.publish(reply_topic, buffer).map_err(Error::Nats)
    }

    pub fn stop(&mut self) -> Result<(), Error> {
        if let Some((connection, sub_handler)) = self.connection.lock().unwrap().take() {
            sub_handler.unsubscribe().map_err(Error::Nats)?;
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
mod tests {
    use super::*;
    use crate::{
        cluster::{rpc_client, Client},
        test_helpers, ServerId, ServerKind,
    };
    use std::collections::HashMap;
    use std::error::Error as StdError;

    #[tokio::test]
    async fn server_starts_and_stops() -> Result<(), Box<dyn StdError>> {
        let sv = Arc::new(Server {
            id: ServerId::from("my-id"),
            kind: ServerKind::from("room"),
            metadata: HashMap::new(),
            frontend: false,
            hostname: "".to_owned(),
        });

        let mut rpc_server = NatsRpcServer::new(
            test_helpers::get_root_logger(),
            sv.clone(),
            Arc::new(Default::default()),
            tokio::runtime::Handle::current(),
        );
        let mut rpc_server_conn = rpc_server.start()?;

        let handle = {
            tokio::spawn(async move {
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
            let mut client = rpc_client::NatsClient::new(
                test_helpers::get_root_logger(),
                Arc::new(Default::default()),
            );
            client.connect()?;

            let res = client
                .call(
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
                )
                .await?;

            assert_eq!(
                String::from_utf8_lossy(&res.data),
                "HEY, THIS IS THE SERVER"
            );
            client.close();
        }

        rpc_server.stop()?;
        handle.await?;
        Ok(())
    }
}
