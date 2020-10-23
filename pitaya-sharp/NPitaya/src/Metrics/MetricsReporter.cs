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
            var key = BuildKey(name);
            prometheusReporter.RegisterCounter(key);
        }

        static void RegisterHistogramFn(IntPtr prometheusPtr, MetricsOpts opts)
        {
            var ns = Marshal.PtrToStringAnsi(opts.MetricNamespace);
            // TODO (felipe.rodopoulos): prometheus-net does not support subsystem label yet. We'll use a hardcoded one.
            var name = Marshal.PtrToStringAnsi(opts.Name);
            var prometheusReporter = RetrievePrometheus(prometheusPtr);
            var key = BuildKey(name);
            prometheusReporter.RegisterHistogram(key);
        }

        static void RegisterGaugeFn(IntPtr prometheusPtr, MetricsOpts opts)
        {
            var ns = Marshal.PtrToStringAnsi(opts.MetricNamespace);
            // TODO (felipe.rodopoulos): prometheus-net does not support subsystem label yet. We'll use a hardcoded one.
            var name = Marshal.PtrToStringAnsi(opts.Name);
            var prometheusReporter = RetrievePrometheus(prometheusPtr);
            var key = BuildKey(name);
            prometheusReporter.RegisterGauge(key);
        }

        static void IncCounterFn(IntPtr prometheusPtr, IntPtr name, ref IntPtr labels, UInt32 labelsCount)
        {
            string nameStr = Marshal.PtrToStringAnsi(name);
            var prometheus = RetrievePrometheus(prometheusPtr);
            prometheus.IncCounter(nameStr);
        }

        static void ObserveHistFn(IntPtr prometheusPtr, IntPtr name, double value, ref IntPtr labels, UInt32 labelsCount)
        {
            string nameStr = Marshal.PtrToStringAnsi(name);
            var key = BuildKey(nameStr);
            var prometheus = RetrievePrometheus(prometheusPtr);
            prometheus.ObserveHistogram(key, value);
        }

        static void SetGaugeFn(IntPtr prometheusPtr, IntPtr name, double value, ref IntPtr labels, UInt32 labelsCount)
        {
            string nameStr = Marshal.PtrToStringAnsi(name);
            var prometheus = RetrievePrometheus(prometheusPtr);
            prometheus.SetGauge(nameStr, value);
        }

        static void AddGaugeFn(IntPtr prometheusPtr, IntPtr name, double value, ref IntPtr labels, UInt32 labelsCount)
        {
            string nameStr = Marshal.PtrToStringAnsi(name);
            Logger.Warn($"Adding gauge {nameStr} with val {value}. This method should not be used.");
        }

        private static PrometheusReporter RetrievePrometheus(IntPtr ptr)
        {
            var handle = GCHandle.FromIntPtr(ptr);
            return handle.Target as PrometheusReporter;
        }

        private static string BuildKey(string suffix)
        {
            return string.Format("{1}{0}{2}", LabelSeparator, PitayaSubsystem, suffix);
        }
    }
}