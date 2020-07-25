use hyper::{
    header,
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server,
};
use lazy_static::lazy_static;
use prometheus::{exponential_buckets, register_histogram_vec, Encoder, HistogramVec, TextEncoder};
use slog::{error, info};
use std::net::SocketAddr;
use std::time::Instant;
use tokio::sync::broadcast;

lazy_static! {
    static ref RPC_DURATION: HistogramVec = register_histogram_vec!(
        "rpc_duration",
        "Histogram of histogram duration in seconds",
        &["route", "status"],
        exponential_buckets(0.005, 2.0, 20).unwrap()
    )
    .unwrap();
}

async fn metrics_handler(req: Request<Body>) -> Result<Response<Body>, hyper::http::Error> {
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    let mf = prometheus::gather();

    match (req.method(), req.uri().path()) {
        (&Method::GET, "/metrics") => {
            encoder
                .encode(&mf, &mut buffer)
                .expect("failed to encode metrics to buffer");
            Response::builder()
                .header(header::CONTENT_TYPE, encoder.format_type())
                .body(Body::from(buffer))
        }
        (_, "/metrics") => Response::builder()
            .status(hyper::StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::from("")),
        _ => Response::builder()
            .status(hyper::StatusCode::NOT_FOUND)
            .body(Body::from("")),
    }
}

// Starts the metrics server, using a shutdown_signal for graceful shutdown.
pub async fn start_server(
    logger: slog::Logger,
    addr: SocketAddr,
    mut shutdown_signal: broadcast::Receiver<()>,
) {
    let make_svc =
        make_service_fn(|_conn| async { Ok::<_, hyper::http::Error>(service_fn(metrics_handler)) });
    let server = Server::bind(&addr).serve(make_svc);
    info!(logger, "started metrics server"; "addr" => %addr);
    let graceful = server.with_graceful_shutdown(async move {
        match shutdown_signal.recv().await {
            Err(_) => (),
            Ok(_) => (),
        }
    });
    if let Err(err) = graceful.await {
        error!(logger, "server error"; "error" => %err);
    }
}

pub fn record_rpc_duration(start: Instant, route: &str, status: &str) {
    let elapsed = Instant::now() - start;
    RPC_DURATION
        .with_label_values(&[route, status])
        .observe(elapsed.as_secs_f64());
}
