use slog::{o, Drain};

pub fn get_root_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain)
        .chan_size(500)
        .build()
        .filter_level(slog::Level::Error)
        .fuse();
    slog::Logger::root(drain, o!())
}
