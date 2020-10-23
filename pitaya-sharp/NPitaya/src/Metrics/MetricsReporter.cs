using System;
using System.Runtime.InteropServices;
using NPitaya.Models;

namespace NPitaya.Metrics
{
    public class MetricsReporter
    {
        private const string LabelSeparator = "_";
        private const string PitayaSubsystem = "pitaya";

        public IntPtr Ptr { get; }

        public MetricsReporter(PrometheusReporter prometheusReporter)
        {
            var handle = GCHandle.Alloc(prometheusReporter, GCHandleType.Normal);
            var reporterPtr = GCHandle.ToIntPtr(handle);
            Ptr = PitayaCluster.pitaya_metrics_reporter_new(
                RegisterCounterFn,
                RegisterHistogramFn,
                RegisterGaugeFn,
                IncCounterFn,
                ObserveHistFn,
                SetGaugeFn,
                AddGaugeFn,
                reporterPtr);
        }

        ~MetricsReporter()
        {
            PitayaCluster.pitaya_metrics_reporter_drop(Ptr);
        }

        static void RegisterCounterFn(IntPtr prometheusPtr, MetricsOpts opts)
        {
            var ns = Marshal.PtrToStringAnsi(opts.MetricNamespace);
            // TODO (felipe.rodopoulos): prometheus-net does not support subsystem label yet. We'll use a hardcoded one.
            var name = Marshal.PtrToStringAnsi(opts.Name);
            var prometheusReporter = RetrievePrometheus(prometheusPtr);
            var key = BuildKey(prometheusReporter?.Namespace, PitayaSubsystem, name);
            Logger.Debug($"Registering Pitaya metric counter {key}");
            Prometheus.Metrics.CreateCounter(key, "", ((string[]) null)!);
        }

        static void RegisterHistogramFn(IntPtr prometheusPtr, MetricsOpts opts)
        {
            var ns = Marshal.PtrToStringAnsi(opts.MetricNamespace);
            // TODO (felipe.rodopoulos): prometheus-net does not support subsystem label yet. We'll use a hardcoded one.
            var name = Marshal.PtrToStringAnsi(opts.Name);
            var prometheusReporter = RetrievePrometheus(prometheusPtr);
            var key = BuildKey(prometheusReporter?.Namespace, PitayaSubsystem, name);
            Logger.Debug($"Registering Pitaya metric histogram {key}");
            Prometheus.Metrics.CreateHistogram(key, "", ((string[]) null)!);
        }

        static void RegisterGaugeFn(IntPtr prometheusPtr, MetricsOpts opts)
        {
            var ns = Marshal.PtrToStringAnsi(opts.MetricNamespace);
            // TODO (felipe.rodopoulos): prometheus-net does not support subsystem label yet. We'll use a hardcoded one.
            var name = Marshal.PtrToStringAnsi(opts.Name);
            var prometheusReporter = RetrievePrometheus(prometheusPtr);
            var key = BuildKey(prometheusReporter?.Namespace, PitayaSubsystem, name);
            Logger.Debug($"Registering Pitaya metric gauge {key}");
            Prometheus.Metrics.CreateGauge(key, "", ((string[]) null)!);
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

        private static string BuildKey(string @namespace, string prefix, string suffix)
        {
            return string.Format("{1}{0}{2}{0}{3}", LabelSeparator, @namespace, prefix, suffix);
        }
    }
}