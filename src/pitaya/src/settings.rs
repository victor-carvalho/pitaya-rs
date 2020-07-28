use crate::constants;
use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Metrics {
    // The namespace that will be used when reporting metrics. Default "pitaya".
    pub namespace: String,

    // The URL where the metrics server will listen in.
    pub url: String,

    // The path the metrics server will respond to.
    pub path: String,

    // If the metrics are enabled or not (default is false).
    pub enabled: bool,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            namespace: constants::DEFAULT_METRICS_NAMESPACE.to_owned(),
            url: constants::DEFAULT_METRICS_URL.to_owned(),
            path: constants::DEFAULT_METRICS_PATH.to_owned(),
            enabled: false,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Settings {
    // If the server should be running in debug mode.
    pub debug: bool,

    // The shutdown time for a pitaya instance.
    // If this value is surpassed, all existing tasks,
    // will be stopped. This includes RPCs in progress, for example.
    #[serde(with = "humantime_serde")]
    pub shutdown_timeout: Duration,

    // ETCD related settings.
    pub etcd: etcd_nats_cluster::settings::Etcd,

    // NATS related settings.
    pub nats: etcd_nats_cluster::settings::Nats,

    // The kind of this server. For example, "metagame" to represent a metagame server.
    pub server_kind: String,

    // Metrics related settings.
    pub metrics: Metrics,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            debug: true,
            shutdown_timeout: constants::DEFAULT_SHUTDOWN_TIMEOUT,
            etcd: Default::default(),
            nats: Default::default(),
            server_kind: constants::DEFAULT_SERVER_KIND.to_owned(),
            metrics: Default::default(),
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
