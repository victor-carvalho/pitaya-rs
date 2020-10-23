using System;
using System.Runtime.InteropServices;
using NPitaya.Models;

namespace NPitaya.Metrics
{
    public class MetricsReporter
    {
        private const string LabelSeparator = "_";

        public IntPtr Ptr { get { return _metricsReporterPtr; } }

        public MetricsReporter(PrometheusReporter prometheusReporter)
        {
            var handle = GCHandle.Alloc(prometheusReporter, GCHandleType.Normal);
            var prometheusReporter1 = GCHandle.ToIntPtr(handle);
            _metricsReporterPtr = PitayaCluster.pitaya_metrics_reporter_new(
                new PitayaCluster.RegisterCounterFn(RegisterCounterFn),
                new PitayaCluster.RegisterHistogramFn(RegisterHistogramFn),
                new PitayaCluster.RegisterGaugeFn(RegisterGaugeFn),
                new PitayaCluster.IncCounterFn(IncCounterFn),
                new PitayaCluster.ObserveHistFn(ObserveHistFn),
                new PitayaCluster.SetGaugeFn(SetGaugeFn),
                new PitayaCluster.AddGaugeFn(AddGaugeFn),
                // TODO: here we would have the reference to the prometheus server.
                prometheusReporter1);
        }

        ~MetricsReporter()
        {
            PitayaCluster.pitaya_metrics_reporter_drop(_metricsReporterPtr);
        }

        static void RegisterCounterFn(IntPtr prometheusPtr, MetricsOpts opts)
        {
            var ns = Marshal.PtrToStringAnsi(opts.MetricNamespace);
            // TODO (felipe.rodopoulos): prometheus-net does not support subsystem label yet. Add in the future.
            var name = Marshal.PtrToStringAnsi(opts.Name);
            var prometheusReporter = RetrievePrometheus(prometheusPtr);
            var key = BuildKey(prometheusReporter?.Namespace, name);
            Logger.Debug($"Registering Pitaya metric counter {key}");
            Prometheus.Metrics.CreateCounter(name!, "", ((string[]) null)!);
        }

        static void RegisterHistogramFn(IntPtr prometheusPtr, MetricsOpts opts)
        {
            var ns = Marshal.PtrToStringAnsi(opts.MetricNamespace);
            // TODO (felipe.rodopoulos): prometheus-net does not support subsystem label yet. Add in the future.
            var name = Marshal.PtrToStringAnsi(opts.Name);
            var prometheusReporter = RetrievePrometheus(prometheusPtr);
            var key = BuildKey(prometheusReporter?.Namespace, name);
            Logger.Debug($"Registering Pitaya metric histogram {key}");
            Prometheus.Metrics.CreateHistogram(name!, "", ((string[]) null)!);
        }

        static void RegisterGaugeFn(IntPtr prometheusPtr, MetricsOpts opts)
        {
            var ns = Marshal.PtrToStringAnsi(opts.MetricNamespace);
            // TODO (felipe.rodopoulos): prometheus-net does not support subsystem label yet. Add in the future.
            var name = Marshal.PtrToStringAnsi(opts.Name);
            var prometheusReporter = RetrievePrometheus(prometheusPtr);
            var key = BuildKey(prometheusReporter?.Namespace, name);
            Logger.Debug($"Registering Pitaya metric gauge {key}");
            Prometheus.Metrics.CreateGauge(name!, "", ((string[]) null)!);
        }

        static void IncCounterFn(IntPtr userData, IntPtr name, ref IntPtr labels, UInt32 labelsCount)
        {
            // TODO: implementation
            string nameStr = Marshal.PtrToStringAnsi(name);
            Console.WriteLine($"Incrementing counter {nameStr}");
        }

        static void ObserveHistFn(IntPtr userData, IntPtr name, double value, ref IntPtr labels, UInt32 labelsCount)
        {
            // TODO: implementation
            string nameStr = Marshal.PtrToStringAnsi(name);
            Console.WriteLine($"Observing histogram {nameStr} with val {value}");
        }

        static void SetGaugeFn(IntPtr userData, IntPtr name, double value, ref IntPtr labels, UInt32 labelsCount)
        {
            // TODO: implementation
            string nameStr = Marshal.PtrToStringAnsi(name);
            Console.WriteLine($"Setting gauge {nameStr} with val {value}");
        }

        static void AddGaugeFn(IntPtr userData, IntPtr name, double value, ref IntPtr labels, UInt32 labelsCount)
        {
            // TODO: implementation
            string nameStr = Marshal.PtrToStringAnsi(name);
            Console.WriteLine($"Adding gauge {nameStr} with val {value}");
        }

        private static PrometheusReporter RetrievePrometheus(IntPtr ptr)
        {
            var handle = GCHandle.FromIntPtr(ptr);
            return handle.Target as PrometheusReporter;
        }

        private static string BuildKey(string prefix, string suffix)
        {
            return string.Format("{1}{0}{2}", LabelSeparator, prefix, suffix);
        }
    }
}