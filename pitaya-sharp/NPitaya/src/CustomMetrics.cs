using System;

namespace NPitaya
{
    public class CustomMetrics
    {
        string _metricsNamespace;
        internal IntPtr Ptr { get; private set; }

        public CustomMetrics(string metricsNamespace)
        {
            _metricsNamespace = metricsNamespace;
            Ptr = PitayaCluster.pitaya_custom_metrics_new();
        }


        ~CustomMetrics()
        {
            if (Ptr != IntPtr.Zero)
            {
                PitayaCluster.pitaya_custom_metrics_drop(Ptr);
                Ptr = IntPtr.Zero;
            }
        }

        public void AddHistogram(string subsystem, string name, string help, string[] variableLabels, double[] buckets)
        {
            PitayaCluster.pitaya_custom_metrics_add_hist(
                Ptr,
                _metricsNamespace,
                subsystem,
                name,
                help,
                variableLabels,
                (uint)variableLabels.Length,
                buckets,
                (uint)buckets.Length
            );
        }

        public void AddCounter(string subsystem, string name, string help, string[] variableLabels)
        {
            PitayaCluster.pitaya_custom_metrics_add_counter(
                Ptr,
                _metricsNamespace,
                subsystem,
                name,
                help,
                variableLabels,
                (uint)variableLabels.Length
            );
        }

        public void AddGauge(string subsystem, string name, string help, string[] variableLabels)
        {
            PitayaCluster.pitaya_custom_metrics_add_gauge(
                Ptr,
                _metricsNamespace,
                subsystem,
                name,
                help,
                variableLabels,
                (uint)variableLabels.Length
            );
        }
    }
}