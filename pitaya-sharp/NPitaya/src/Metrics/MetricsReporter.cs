using System;

namespace NPitaya.Metrics
{
    class MetricsReporter
    {
        readonly PrometheusReporter _prometheusServer;
        readonly PitayaReporter _pitayaMetrics ;

        internal MetricsReporter(MetricsConfiguration config)
        {
            _prometheusServer = new PrometheusReporter(config);
            _pitayaMetrics = new PitayaReporter(_prometheusServer);
        }

        internal void Start()
        {
            _prometheusServer.Start();
        }

        internal IntPtr GetPitayaPtr()
        {
            return _pitayaMetrics.Ptr;
        }
    }
}