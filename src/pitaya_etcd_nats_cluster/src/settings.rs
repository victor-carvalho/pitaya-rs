use crate::constants;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Nats {
    // The url where Nats is located.
    pub url: String,

    // How long to wait until the connection request times out.
    #[serde(with = "humantime_serde")]
    pub connection_timeout: Duration,

    // How long to wait until the request times out.
    #[serde(with = "humantime_serde")]
    pub request_timeout: Duration,

    // The maximum amount of times the nats client will attempt to reconnect.
    pub max_reconnection_attempts: u32,

    // The maximum amount of RPCs queued that a nats server will have.
    // If this amount is passed, RPCs will fail.
    pub max_rpcs_queued: u32,

    // The NATS connection username.
    pub auth_user: String,

    // The NATS connection password.
    pub auth_pass: String,
}

impl Default for Nats {
    fn default() -> Self {
        Self {
            url: constants::LOCAL_NATS_URL.to_owned(),
            connection_timeout: constants::DEFAULT_NATS_CONN_TIMEOUT,
            request_timeout: constants::DEFAULT_NATS_REQUEST_TIMEOUT,
            max_reconnection_attempts: constants::DEFAULT_NATS_MAX_RECONN_ATTEMPTS,
            max_rpcs_queued: constants::DEFAULT_NATS_MAX_RPCS_QUEUED,
            auth_user: constants::DEFAULT_NATS_AUTH_USER.to_owned(),
            auth_pass: constants::DEFAULT_NATS_AUTH_PASS.to_owned(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Etcd {
    // The URL where the ETCD instance is located at.
    pub url: String,

    // The prefix where all keys are going to be stored in ETCD.
    // This will typically be the name of your app.
    pub prefix: String,

    // How long should the lease TTL be for this server.
    // In practice, long values imply less requests to ETCD to
    // update the lease, however, if something bad happens, like a crash,
    // the server will be registered for service discovery even though it is
    // not running anymore.
    #[serde(with = "humantime_serde")]
    pub lease_ttl: Duration,
}

impl Default for Etcd {
    fn default() -> Self {
        Self {
            url: constants::LOCAL_ETCD_URL.to_owned(),
            prefix: constants::DEFAULT_ETCD_PREFIX.to_owned(),
            lease_ttl: constants::DEFAULT_ETCD_LEASE_TTL,
        }
    }
}
