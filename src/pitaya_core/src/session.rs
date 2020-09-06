use crate::cluster::ServerId;

// Session represents the state of a client connection in a frontend server.
// The session struct is propagated through RPC calls between frontend and backend servers.
#[derive(Debug)]
pub struct Session {
    // The user id of the session.
    uid: String,
    // If this session is a frontend session.
    is_frontend: bool,
    // The server id from which this session belongs to.
    frontend_id: ServerId,
    // The id of the session on the frontend server.
    frontend_session_id: i64,
}

impl Session {
    pub fn new(frontend_id: ServerId, frontend_session_id: i64, uid: String) -> Self {
        Self {
            is_frontend: false,
            uid,
            frontend_id,
            frontend_session_id,
        }
    }
}
