using System;
using System.Runtime.InteropServices;

namespace NPitaya.Metrics
{
    public class MetricsReporter
    {
        IntPtr _metricsReporterPtr;

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

        static void RegisterCounterFn(IntPtr userData, MetricsOpts opts)
        {
            string ns = Marshal.PtrToStringAnsi(opts.MetricNamespace);
            // TODO (felipe.rodopoulos): prometheus-net does not support subsystem label yet. Add in the future.
            string name = Marshal.PtrToStringAnsi(opts.Name);
            Prometheus.Metrics.CreateCounter(name!, "", ((string[]) null)!);
        }

        static void RegisterHistogramFn(IntPtr userData, MetricsOpts opts)
        {
            string ns = Marshal.PtrToStringAnsi(opts.MetricNamespace);
            // TODO (felipe.rodopoulos): prometheus-net does not support subsystem label yet. Add in the future.
            string name = Marshal.PtrToStringAnsi(opts.Name);
            Prometheus.Metrics.CreateHistogram(name!, "", ((string[]) null)!);
        }

        static void RegisterGaugeFn(IntPtr userData, MetricsOpts opts)
        {
            string ns = Marshal.PtrToStringAnsi(opts.MetricNamespace);
            // TODO (felipe.rodopoulos): prometheus-net does not support subsystem label yet. Add in the future.
            string name = Marshal.PtrToStringAnsi(opts.Name);
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
    }
}