use crate::{
    cluster::{self, Discovery, RpcClient, ServerId, ServerInfo},
    context::Context,
    message, protos, utils,
};
use slog::{debug, error};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;

const BIND_ROUTE: &str = "sys.bindsession";

#[derive(Debug, Error)]
pub enum Error {
    #[error("session is already bound")]
    AlreadyBound,

    #[error("{0}")]
    Cluster(#[from] cluster::Error),
}

// Session represents the state of a client connection in a frontend server.
// The session struct is propagated through RPC calls between frontend and backend servers.
pub struct Session {
    // The user id of the session.
    pub uid: String,
    // The server id from which this session belongs to.
    pub frontend_id: ServerId,
    // The id of the session on the frontend server.
    pub frontend_session_id: i64,

    discovery: Arc<Mutex<Box<dyn Discovery>>>,
    rpc_client: Arc<Box<dyn RpcClient>>,
    logger: slog::Logger,
}

impl std::fmt::Display for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "frontend_id={} frontend_session_id={} uid={}",
            self.frontend_id.0, self.frontend_session_id, self.uid
        )
    }
}

impl Session {
    pub fn new(
        logger: slog::Logger,
        req: &protos::Request,
        rpc_client: Arc<Box<dyn RpcClient>>,
        discovery: Arc<Mutex<Box<dyn Discovery>>>,
    ) -> Option<Self> {
        if let Some(s) = req.session.as_ref() {
            Some(Self {
                logger,
                uid: s.uid.clone(),
                frontend_id: ServerId::from(&req.frontend_id),
                frontend_session_id: s.id.clone(),
                rpc_client,
                discovery,
            })
        } else {
            None
        }
    }

    pub async fn bind(&mut self, uid: impl ToString) -> Result<(), Error> {
        if !self.uid.is_empty() {
            return Err(Error::AlreadyBound);
        }

        // TODO(lhahn): whenever we add support for frontend servers,
        // we need to check here when binding the session if the server is front or not.
        let buf = utils::encode_proto(&protos::Session {
            id: self.frontend_session_id,
            uid: uid.to_string(),
            ..Default::default()
        });

        // First we need to find the frontend server from where this session belongs to.
        let frontend_server = match self
            .discovery
            .lock()
            .await
            .server_by_id(
                &self.frontend_id,
                // TODO(lhahn): this is not efficient. Ideallly we should know the server
                // type here, instead of providing None.
                None,
            )
            .await?
        {
            Some(s) => s,
            None => {
                error!(self.logger, "cannot bind session to unexistent frontend server"; "server_id" => %self.frontend_id.0);
                return Err(Error::Cluster(cluster::Error::FrontendServerNotFound(
                    self.frontend_id.clone(),
                )));
            }
        };

        let res = self
            .rpc_client
            .call(
                Context::empty(), // TODO(lhahn): should this not be empty?
                protos::RpcType::User,
                message::Message {
                    kind: message::Kind::Request,
                    id: 0,
                    route: BIND_ROUTE.to_owned(),
                    data: buf,
                    compressed: false,
                    err: false,
                },
                frontend_server,
            )
            .await?;

        debug!(self.logger, "session bind complete"; "res" => ?res);

        // We only set the uid once we succeed with the bind rpc.
        self.uid = uid.to_string();

        Ok(())
    }
}
