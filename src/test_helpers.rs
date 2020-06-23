use slog::{o, Drain};

pub(crate) const ETCD_URL: &str = "localhost:2379";

pub(crate) const NATS_URL: &str = "http://localhost:4222";

pub(crate) fn get_root_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}
