using NPitaya.Models;
using Prometheus;

namespace NPitaya.Metrics
{
    public class PrometheusReporter
    {
        private readonly string _host;
        private readonly int _port;
        private readonly MetricServer _server;
        internal readonly string Namespace;

        internal PrometheusReporter(MetricsConfiguration config) : this(
            config.Host,
            config.Port,
            config.Namespace
        ){}

        private PrometheusReporter(string host, string port, string @namespace)
        {
            Namespace = @namespace;
            _host = host;
            _port = int.Parse(port);
            _server = new MetricServer(hostname: _host, port: _port);
        }

        internal void Start()
        {
            Logger.Info("Starting Prometheus metrics server at {0}:{1}", _host, _port);
            _server.Start();
        }
    }
}