use async_trait::async_trait;
use hyper::{
    header,
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server,
};
use pitaya_core::metrics::{Error, Opts, Reporter};
use prometheus::{Encoder, TextEncoder};
use slog::{error, info};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::{sync::oneshot, task::JoinHandle};

//
// Reporter implementation.
//
#[derive(Debug)]
pub struct PrometheusReporter {
    registry: Arc<prometheus::Registry>,
    server_handle: Option<JoinHandle<()>>,
    logger: slog::Logger,
    addr: SocketAddr,
    shutdown_sender: Option<oneshot::Sender<()>>,
    histograms: HashMap<String, prometheus::HistogramVec>,
    counters: HashMap<String, prometheus::CounterVec>,
    namespace: String,
    const_labels: HashMap<String, String>,
}

impl PrometheusReporter {
    pub fn new(
        prefix: String,
        const_labels: HashMap<String, String>,
        logger: slog::Logger,
        addr: SocketAddr,
    ) -> Result<Self, Error> {
        let registry = Arc::new(
            prometheus::Registry::new_custom(Some(prefix.clone()), Some(const_labels.clone()))
                .map_err(|e| Error::FailedToStartServer(e.to_string()))?,
        );
        Ok(PrometheusReporter {
            registry,
            server_handle: None,
            logger,
            addr,
            shutdown_sender: None,
            histograms: HashMap::new(),
            counters: HashMap::new(),
            const_labels,
            namespace: prefix,
        })
    }
}

#[async_trait]
impl Reporter for PrometheusReporter {
    async fn start(&mut self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();

        let handle = tokio::spawn(start_server(
            self.registry.clone(),
            self.logger.clone(),
            self.addr,
            rx,
        ));

        self.server_handle.replace(handle);
        self.shutdown_sender.replace(tx);

        info!(self.logger, "prometheus metrics server started");
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Error> {
        if let Some(sender) = self.shutdown_sender.take() {
            if sender.send(()).is_err() {
                error!(self.logger, "failed to send shutdown signal");
            }
        }
        if self
            .server_handle
            .as_mut()
            .expect("server handle should exist")
            .await
            .is_err()
        {
            error!(self.logger, "metrics server panicked");
        }
        info!(self.logger, "prometheus metrics server was shut down");
        Ok(())
    }

    fn register_counter(&mut self, opts: Opts) -> Result<(), Error> {
        let name = opts.name.clone();
        let prometheus_opts = prometheus::Opts {
            namespace: opts.namespace,
            subsystem: opts.subsystem,
            name: opts.name,
            help: opts.help,
            const_labels: self.const_labels.clone(),
            variable_labels: vec![],
        };
        let label_names: Vec<&str> = opts.variable_labels.iter().map(|e| e.as_str()).collect();
        let collector = prometheus::CounterVec::new(prometheus_opts, &label_names)
            .map_err(|e| Error::InvalidMetric(e.to_string()))?;
        self.registry
            .register(Box::new(collector.clone()))
            .map_err(|e| Error::InvalidMetric(e.to_string()))?;
        self.counters.insert(name, collector);

        Ok(())
    }

    fn register_histogram(&mut self, opts: Opts) -> Result<(), Error> {
        let name = opts.name.clone();
        let prometheus_opts = prometheus::HistogramOpts {
            common_opts: prometheus::Opts {
                namespace: opts.namespace,
                subsystem: opts.subsystem,
                name: opts.name,
                help: opts.help,
                const_labels: self.const_labels.clone(),
                variable_labels: vec![],
            },
            buckets: opts.buckets,
        };
        let label_names: Vec<&str> = opts.variable_labels.iter().map(|e| e.as_str()).collect();
        let collector = prometheus::HistogramVec::new(prometheus_opts, &label_names)
            .map_err(|e| Error::InvalidMetric(e.to_string()))?;
        self.registry
            .register(Box::new(collector.clone()))
            .map_err(|e| Error::InvalidMetric(e.to_string()))?;

        self.histograms.insert(name, collector);

        Ok(())
    }

    fn inc_counter(&self, _name: &str) -> Result<(), Error> {
        todo!()
    }

    fn observe_hist(&self, name: &str, value: f64, labels: &[&str]) -> Result<(), Error> {
        if let Some(hist) = self.histograms.get(name) {
            hist.with_label_values(&labels).observe(value);
            Ok(())
        } else {
            Err(Error::InvalidMetric(format!(
                "unknown metrics named {}",
                name
            )))
        }
    }
}

async fn start_server(
    registry: Arc<prometheus::Registry>,
    logger: slog::Logger,
    addr: SocketAddr,
    shutdown_signal: oneshot::Receiver<()>,
) {
    let make_svc = make_service_fn(|_conn| {
        let registry = registry.clone();
        async move {
            Ok::<_, hyper::http::Error>(service_fn(move |req| {
                metrics_handler(registry.clone(), req)
            }))
        }
    });
    let server = Server::bind(&addr).serve(make_svc);
    info!(logger, "started metrics server"; "addr" => %addr);
    let graceful = server.with_graceful_shutdown(async move {
        let _ = shutdown_signal.await;
    });
    if let Err(err) = graceful.await {
        error!(logger, "server error"; "error" => %err);
    }
}

async fn metrics_handler(
    registry: Arc<prometheus::Registry>,
    req: Request<Body>,
) -> Result<Response<Body>, hyper::http::Error> {
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    let mf = registry.gather();

    match (req.method(), req.uri().path()) {
        // TODO(lhahn): make this path configurable.
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
