use pitaya::{
    metrics::{self, Reporter, ThreadSafeReporter},
    Session, State,
};
use serde::Serialize;
use slog::{error, o, Drain};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

#[derive(Serialize)]
struct JoinResponse {
    code: i32,
    result: String,
}

struct Error {
    msg: String,
}

impl pitaya::ToError for Error {
    fn to_error(self) -> pitaya::protos::Error {
        pitaya::protos::Error {
            code: "PIT-400".to_owned(),
            msg: self.msg,
            metadata: HashMap::new(),
        }
    }
}

#[derive(Serialize)]
struct PushMsg {
    msg: String,
}

#[pitaya::json_handler("room", client)]
async fn hi(session: Session) -> Result<JoinResponse, Error> {
    if !session.is_bound() {
        return Err(Error {
            msg: "session is not bound man!".to_owned(),
        });
    }

    let msg = PushMsg {
        msg: "HELLO, THIS IS A PUSH FROM THE SERVER".to_owned(),
    };

    // Spawn a new non-blocking task.
    tokio::spawn(async move {
        if let Err(e) = session.push_json_msg("my.super.route", msg).await {
            println!("failed to push msg: {}", e);
        }
    });

    Ok(JoinResponse {
        code: 200,
        result: String::new(),
    })
}

#[pitaya::json_handler("room", client)]
async fn entry(mut session: Session) -> Result<JoinResponse, pitaya::Never> {
    println!("received rpc from session: {}", session);

    session.set("MyData", "HELLO WORLD");

    if let Err(e) = session.bind("helroow").await {
        println!("failed to bind session: {}", e);
    }

    if let Err(e) = session.update_in_front().await {
        println!("failed to update session data on front: {}", e);
    }

    if session.is_bound() {
        if let Err(e) = session.kick().await {
            println!("failed to kick session: {}", e);
        }
    }

    Ok(JoinResponse {
        code: 200,
        result: "ok".to_owned(),
    })
}

#[pitaya::json_handler("room", client)]
async fn bind(
    mut session: Session,
    reporter: State<'_, ThreadSafeReporter>,
) -> Result<JoinResponse, pitaya::Never> {
    if let Err(e) = session.bind("helroow").await {
        println!("failed to bind session: {}", e);
    }

    if let Err(e) = reporter.read().await.inc_counter("my_metric", &["false"]) {
        println!("failed to increment counter: {}", e);
    }

    Ok(JoinResponse {
        code: 200,
        result: "SESSION BOUND!".to_owned(),
    })
}

fn init_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain)
        .chan_size(1000)
        .build()
        .filter_level(slog::Level::Debug)
        .fuse();
    slog::Logger::root(drain, o!())
}

#[tokio::main]
async fn main() {
    let logger = init_logger();

    let metrics_reporter: ThreadSafeReporter = Arc::new(RwLock::new(Box::new(
        prometheus_metrics::PrometheusReporter::new(
            "myprefix".into(),
            HashMap::new(),
            logger.clone(),
            "127.0.0.1:8000".parse().unwrap(),
            // addr: SocketAddr,
        )
        .expect("failed to create metrics reporter"),
    )));

    let (pitaya_server, shutdown_receiver) = pitaya::PitayaBuilder::new()
        .with_server_info(Arc::new(pitaya::cluster::ServerInfo {
            id: pitaya::cluster::ServerId::from("random-id"),
            kind: pitaya::cluster::ServerKind::from("random"),
            metadata: HashMap::new(),
            hostname: String::new(),
            frontend: false,
        }))
        .with_logger(logger.clone())
        .with_client_handlers(pitaya::handlers![entry, hi, bind])
        .with_metrics_reporter(metrics_reporter.clone())
        .build()
        .await
        .expect("failed to startup pitaya");

    metrics_reporter
        .write()
        .await
        .register_counter(metrics::Opts {
            kind: metrics::MetricKind::Counter,
            namespace: "mynamespace".into(),
            subsystem: "subsystem".into(),
            name: "metric_name".into(),
            help: "super help".into(),
            variable_labels: vec![],
            buckets: vec![],
        })
        .unwrap();

    metrics_reporter
        .read()
        .await
        .inc_counter("metric_name", &[])
        .unwrap();

    shutdown_receiver
        .await
        .expect("failed to wait for shutdown receiver");

    if let Err(e) = pitaya_server.shutdown().await {
        error!(logger, "failed to shutdown pitaya: {}", e);
    }
}
