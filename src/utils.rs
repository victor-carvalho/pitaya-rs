use super::{Server, ServerId, ServerKind};
use rand::Rng;
use std::sync::Arc;

pub(crate) fn user_kick_topic(user_id: &str, server_kind: &ServerKind) -> String {
    format!("pitaya/{}/user/{}/kick", server_kind.0, user_id)
}

pub(crate) fn user_messages_topic(user_id: &str, server_kind: &ServerKind) -> String {
    format!("pitaya/{}/user/{}/push", server_kind.0, user_id)
}

pub(crate) fn topic_for_server(server: &Server) -> String {
    format!("pitaya/servers/{}/{}", server.kind.0, server.id.0)
}

pub(crate) fn server_kind_prefix(server_kind: &ServerKind) -> String {
    format!("pitaya/servers/{}/", server_kind.0)
}

pub(crate) fn random_server(servers: &[Arc<Server>]) -> Option<Arc<Server>> {
    if servers.is_empty() {
        None
    } else {
        let mut rng = rand::thread_rng();
        let random_index: usize = rng.gen_range(0, servers.len());
        Some(servers[random_index].clone())
    }
}
