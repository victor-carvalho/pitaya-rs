use std::time::Duration;

pub const LOCAL_ETCD_URL: &str = "localhost:2379";
pub const LOCAL_NATS_URL: &str = "http://localhost:4222";

pub const DEFAULT_ENV_PREFIX: &str = "PITAYA";

pub const DEFAULT_ETCD_PREFIX: &str = "pitaya";
pub const DEFAULT_ETCD_LEASE_TTL: Duration = Duration::from_secs(60);

pub const DEFAULT_NATS_CONN_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_NATS_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_NATS_MAX_RECONN_ATTEMPTS: u32 = 5;
pub const DEFAULT_NATS_MAX_RPCS_QUEUED: u32 = 100;

pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

pub const DEFAULT_SERVER_KIND: &str = "pitaya";
