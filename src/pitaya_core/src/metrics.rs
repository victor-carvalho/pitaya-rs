use async_trait::async_trait;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid metric: {0}")]
    InvalidMetric(String),

    #[error("failed to start metrics server: {0}")]
    FailedToStartServer(String),
}

#[derive(Debug, PartialEq)]
pub enum MetricKind {
    Histogram,
    Gauge,
    Counter,
}

pub struct Opts {
    pub kind: MetricKind,
    pub namespace: String,
    pub subsystem: String,
    pub name: String,
    pub help: String,
    pub variable_labels: Vec<String>,
    pub buckets: Vec<f64>,
}

pub type ThreadSafeReporter = Arc<RwLock<Box<dyn Reporter + Send + Sync>>>;

#[async_trait]
pub trait Reporter {
    fn register_counter(&mut self, opts: Opts) -> Result<(), Error>;
    fn register_histogram(&mut self, opts: Opts) -> Result<(), Error>;
    fn register_gauge(&mut self, opts: Opts) -> Result<(), Error>;

    async fn start(&mut self) -> Result<(), Error>;
    async fn shutdown(&mut self) -> Result<(), Error>;

    fn inc_counter(&self, name: &str, labels: &[&str]) -> Result<(), Error>;
    fn observe_hist(&self, name: &str, value: f64, labels: &[&str]) -> Result<(), Error>;
    fn set_gauge(&self, name: &str, value: f64, labels: &[&str]) -> Result<(), Error>;
    fn add_gauge(&self, name: &str, value: f64, labels: &[&str]) -> Result<(), Error>;
}

pub struct DummyReporter {}

#[async_trait]
impl Reporter for DummyReporter {
    fn register_counter(&mut self, _opts: Opts) -> Result<(), Error> {
        Ok(())
    }

    fn register_histogram(&mut self, _opts: Opts) -> Result<(), Error> {
        Ok(())
    }

    fn register_gauge(&mut self, _opts: Opts) -> Result<(), Error> {
        Ok(())
    }

    async fn start(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn inc_counter(&self, _name: &str, _labels: &[&str]) -> Result<(), Error> {
        Ok(())
    }

    fn observe_hist(&self, _name: &str, _value: f64, _labels: &[&str]) -> Result<(), Error> {
        Ok(())
    }

    fn set_gauge(&self, _name: &str, _value: f64, _labels: &[&str]) -> Result<(), Error> {
        Ok(())
    }

    fn add_gauge(&self, _name: &str, _value: f64, _labels: &[&str]) -> Result<(), Error> {
        Ok(())
    }
}

pub fn exponential_buckets(start: f64, factor: f64, count: usize) -> Vec<f64> {
    assert!(count >= 1);
    assert!(start > 0.0);
    assert!(factor > 1.0);

    let mut next = start;
    let mut buckets = Vec::with_capacity(count);
    for _ in 0..count {
        buckets.push(next);
        next *= factor;
    }

    buckets
}

pub async fn record_histogram_duration(
    reporter: ThreadSafeReporter,
    name: &str,
    start: std::time::Instant,
    labels: &[&str],
) {
    let elapsed = std::time::Instant::now() - start;
    reporter
        .read()
        .await
        .observe_hist(name, elapsed.as_secs_f64(), labels)
        .expect("should not fail to observe");
}
