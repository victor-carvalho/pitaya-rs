// Session represents the state of a client connection in a frontend server.
// The session struct is propagated through RPC calls between frontend and backend servers.
pub struct Session {
    // The unique id of the session.
    _id: i64,
    // The user id of the session.
    _uid: String,
    // The id of the frontend server that own this session.
    _frontend_id: String,
    // If this session is a frontend session.
    _is_frontend: bool,
    // The session id on the frontend server.
    _frontend_session_id: i64,
}
