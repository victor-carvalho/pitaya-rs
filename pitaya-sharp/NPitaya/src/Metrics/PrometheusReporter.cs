using System.Collections.Generic;
using NPitaya.Models;
using Prometheus;

namespace NPitaya.Metrics
{
    public class PrometheusReporter
    {
        private const string LabelSeparator = "_";

        readonly string _host;
        readonly int _port;
        readonly MetricServer _server;
        readonly string _namespace;

        readonly Dictionary<string, Counter> _counters;
        readonly Dictionary<string, Gauge> _gauges;
        readonly Dictionary<string, Histogram> _histograms;

        internal PrometheusReporter(MetricsConfiguration config) : this(
            config.Host,
            config.Port,
            config.Namespace
        ){}

        private PrometheusReporter(string host, string port, string @namespace)
        {
            _namespace = @namespace;
            _host = host;
            _port = int.Parse(port);
            _server = new MetricServer(hostname: _host, port: _port);
            _counters = new Dictionary<string, Counter>();
            _gauges = new Dictionary<string, Gauge>();
            _histograms = new Dictionary<string, Histogram>();
        }

        internal void Start()
        {
            Logger.Info("Starting Prometheus metrics server at {0}:{1}", _host, _port);
            _server.Start();
        }

        internal void RegisterCounter(string name, string help = null, string[] labels = null)
        {
            var key = BuildKey(name);
            Logger.Debug($"Registering counter metric {key}");
            var counter = Prometheus.Metrics.CreateCounter(key, help ?? "");
            _counters.Add(key, counter);
        }

        internal void RegisterGauge(string name, string help = null, string[] labels = null)
        {
            var key = BuildKey(name);
            Logger.Debug($"Registering gauge metric {key}");
            var gauge = Prometheus.Metrics.CreateGauge(key, help ?? "");
            _gauges.Add(key, gauge);
        }

        internal void RegisterHistogram(string name, string help = null, string[] labels = null)
        {
            var key = BuildKey(name);
            Logger.Debug($"Registering histogram metric {key}");
            var histogram = Prometheus.Metrics.CreateHistogram(key, help ?? "");
            _histograms.Add(key, histogram);
        }

        internal void IncCounter(string name, string[] labels = null)
        {
            var key = BuildKey(name);
            var counter = _counters[key];
            Logger.Debug($"Incrementing counter {key}");
            counter.Inc();
        }

        internal void SetGauge(string name, double value, string[] labels = null)
        {
            var key = BuildKey(name);
            var gauge = _gauges[key];
            Logger.Debug($"Setting gauge {key} with value {value}");
            gauge.Set(value);
        }

        internal void ObserveHistogram(string name, double value, string[] labels = null)
        {
            var key = BuildKey(name);
            var histogram = _histograms[key];
            Logger.Debug($"Observing histogram {key} with value {value}");
            histogram.Observe(value);
        }

        string BuildKey(string suffix)
        {
            return $"{_namespace}{LabelSeparator}{suffix}";
        }
    }
}