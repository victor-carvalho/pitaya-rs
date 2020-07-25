use super::ServersCache;
use crate::{Server, ServerId, ServerKind};
use slog::{debug, error, info, warn};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{broadcast, oneshot};

pub(super) async fn lease_keep_alive(
    logger: slog::Logger,
    mut lease_ttl: Duration,
    mut keeper: etcd_client::LeaseKeeper,
    mut stream: etcd_client::LeaseKeepAliveStream,
    mut stop_chan: oneshot::Receiver<()>,
    app_die_chan: broadcast::Sender<()>,
) {
    use tokio::time::timeout;

    info!(logger, "keep alive task started");
    loop {
        let seconds_to_wait = lease_ttl.as_secs() as f32 - (lease_ttl.as_secs() as f32 / 3.0);
        assert!(seconds_to_wait > 0.0);

        debug!(logger, "waiting for {:.2} seconds", seconds_to_wait);

        match timeout(Duration::from_secs(seconds_to_wait as u64), &mut stop_chan).await {
            Err(_) => {
                // TODO(lhahn): currently, the ttl will fail as soon as it loses connection to etcd.
                // Figure out if a more robust retrying scheme is necessary here.
                if let Err(e) = keeper.keep_alive().await {
                    error!(logger, "failed keep alive request: {}", e);
                    if let Err(_) = app_die_chan.send(()) {
                        error!(logger, "failed to send die message");
                    }
                    return;
                }
                match stream.message().await {
                    Err(_) => {
                        error!(logger, "failed to get keep alive response");
                    }
                    Ok(msg) => {
                        if let Some(response) = msg {
                            debug!(
                                logger,
                                "lease renewed with new ttl of {} seconds",
                                response.ttl()
                            );
                            assert!(response.ttl() > 0);
                            lease_ttl = Duration::from_secs(response.ttl() as u64);
                        } else {
                            // TODO(lhahn): what to do here?
                            warn!(logger, "received empty lease keep alive response");
                        }
                    }
                }
            }
            Ok(_) => {
                info!(
                    logger,
                    "received stop message, exiting lease keep alive task"
                );
                return;
            }
        }
    }
}

pub(super) async fn watch_task(
    logger: slog::Logger,
    servers_cache: Arc<RwLock<ServersCache>>,
    prefix: String,
    mut stream: etcd_client::WatchStream,
    app_die_sender: broadcast::Sender<()>,
) {
    loop {
        debug!(logger, "watching for etcd changes...");

        match stream.message().await {
            Ok(Some(watch_response)) => {
                if watch_response.canceled() {
                    info!(logger, "watch was cancelled, exiting task");
                    return;
                }

                for event in watch_response.events() {
                    let kv = match event.kv() {
                        Some(kv) => kv,
                        None => {
                            warn!(logger, "did not get kv for watch event");
                            continue;
                        }
                    };

                    let key_str = match kv.key_str() {
                        Ok(v) => v,
                        Err(_) => {
                            warn!(logger, "invalid key string for watch event");
                            continue;
                        }
                    };

                    match event.event_type() {
                        etcd_client::EventType::Put => {
                            if let None = parse_server_kind_and_id(&prefix, key_str) {
                                continue;
                            };

                            let value_str = match kv.value_str() {
                                Ok(v) => v,
                                Err(_) => continue,
                            };

                            let server = match serde_json::from_str::<Server>(value_str) {
                                Ok(s) => Arc::new(s),
                                Err(e) => {
                                    error!(logger, "server is not valid json: {}", e);
                                    continue;
                                }
                            };

                            servers_cache.write().unwrap().insert(server);
                        }
                        etcd_client::EventType::Delete => {
                            let (server_kind, server_id) =
                                match parse_server_kind_and_id(&prefix, key_str) {
                                    Some(a) => a,
                                    None => {
                                        warn!(logger, "could not parse key on deleted server");
                                        continue;
                                    }
                                };

                            servers_cache
                                .write()
                                .unwrap()
                                .remove(&server_kind, &server_id);
                        }
                    }
                }
            }
            Ok(None) => {
                // TODO(lhahn): if it got a None, is it safe to assume the watch was closed?
                info!(logger, "watch was closed, exiting");
                return;
            }
            Err(e) => {
                // FIXME, TODO(lhahn): should we send an event to kill the pod here?
                // panic!("failed to get watch message: {}", e);
                error!(logger, "watch error"; "error" => %e);
                let _ = app_die_sender.send(()).map_err(|_| {
                    warn!(logger, "receiver side not listening");
                });
                return;
            }
        }
    }
}

fn parse_server_kind_and_id(prefix: &str, string: &str) -> Option<(ServerKind, ServerId)> {
    let components: Vec<&str> = string.split('/').collect();
    match components[..] {
        [key_prefix, "servers", server_kind, server_id] if key_prefix == prefix => {
            Some((ServerKind::from(server_kind), ServerId::from(server_id)))
        }
        _ => None,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_server_kind_and_id() {
        let s = "pitaya/servers/room/912ebcec-71ec-49b9-95f9-e188e16afa51";
        assert_eq!(
            parse_server_kind_and_id("pitaya", s),
            Some((
                ServerKind::from("room"),
                ServerId::from("912ebcec-71ec-49b9-95f9-e188e16afa51")
            ))
        );
        assert_eq!(parse_server_kind_and_id("pit", s), None);
    }
}
