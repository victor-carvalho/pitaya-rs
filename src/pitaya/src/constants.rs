use std::time::Duration;

pub const DEFAULT_ENV_PREFIX: &str = "PITAYA";

pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

pub const DEFAULT_METRICS_URL: &str = "127.0.0.1:8000";
pub const DEFAULT_METRICS_PATH: &str = "/metrics";
pub const DEFAULT_METRICS_NAMESPACE: &str = "pitaya";
