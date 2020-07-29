use std::time::Duration;

pub const DEFAULT_ENV_PREFIX: &str = "PITAYA";

pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

pub const DEFAULT_SERVER_KIND: &str = "pitaya";

pub const DEFAULT_METRICS_URL: &str = "127.0.0.1:8000";
pub const DEFAULT_METRICS_PATH: &str = "/metrics";
pub const DEFAULT_METRICS_NAMESPACE: &str = "pitaya";

pub const CODE_INTERNAL_ERROR: &str = "PIT-500";
pub const CODE_SERVICE_UNAVAILABLE: &str = "PIT-502";
pub const CODE_BAD_FORMAT: &str = "PIT-400";
