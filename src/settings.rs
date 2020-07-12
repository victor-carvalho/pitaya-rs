use crate::constants;
use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Nats {
    pub url: String,
    #[serde(with = "humantime_serde")]
    pub connection_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub request_timeout: Duration,
    pub max_reconnection_attempts: u32,
    pub max_pending_messages: u32,
    pub max_rpcs_queued: u32,
}

impl Default for Nats {
    fn default() -> Self {
        Self {
            url: constants::LOCAL_NATS_URL.to_owned(),
            connection_timeout: constants::DEFAULT_NATS_CONN_TIMEOUT,
            request_timeout: constants::DEFAULT_NATS_REQUEST_TIMEOUT,
            max_reconnection_attempts: constants::DEFAULT_NATS_MAX_RECONN_ATTEMPTS,
            max_pending_messages: constants::DEFAULT_NATS_MAX_PENDING_MESSAGES,
            max_rpcs_queued: constants::DEFAULT_NATS_MAX_RPCS_QUEUED,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Etcd {
    pub url: String,
    pub prefix: String,
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

#[derive(Debug, Deserialize, Serialize)]
pub struct Settings {
    pub debug: bool,
    #[serde(with = "humantime_serde")]
    pub shutdown_timeout: Duration,
    // #[serde(default)]
    pub etcd: Etcd,
    // #[serde(default)]
    pub nats: Nats,
    pub server_kind: String,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            debug: true,
            shutdown_timeout: constants::DEFAULT_SHUTDOWN_TIMEOUT,
            etcd: Default::default(),
            nats: Default::default(),
            server_kind: constants::DEFAULT_SERVER_KIND.to_owned(),
        }
    }
}

impl Settings {
    pub fn new(
        base: Self,
        env_prefix: Option<&str>,
        filename: Option<&str>,
    ) -> Result<Self, ConfigError> {
        let mut config = Config::new();
        let base_config = Config::try_from(&base).unwrap();
        let env_prefix = env_prefix.unwrap_or(constants::DEFAULT_ENV_PREFIX);

        // The order for applying configs:
        // 1 - Always start with the base configuration.
        // 2 - Apply the file configuration if provided.
        // 3 - At the end we merge the environment variables.
        config.merge(base_config)?;
        if let Some(filename) = filename {
            config.merge(File::with_name(filename))?;
        }
        config.merge(
            Environment::with_prefix(env_prefix)
                .separator("__")
                .ignore_empty(true),
        )?;

        config.try_into()
    }
}
