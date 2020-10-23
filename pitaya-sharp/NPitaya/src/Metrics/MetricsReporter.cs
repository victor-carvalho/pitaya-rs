using System;
using System.Runtime.InteropServices;

namespace NPitaya.Metrics
{
    public class MetricsReporter
    {
        IntPtr _metricsReporterPtr;

        public IntPtr Ptr { get { return _metricsReporterPtr; } }

        public MetricsReporter()
        {
            _metricsReporterPtr = PitayaCluster.pitaya_metrics_reporter_new(
                new PitayaCluster.RegisterCounterFn(RegisterCounterFn),
                new PitayaCluster.RegisterHistogramFn(RegisterHistogramFn),
                new PitayaCluster.RegisterGaugeFn(RegisterGaugeFn),
                new PitayaCluster.IncCounterFn(IncCounterFn),
                new PitayaCluster.ObserveHistFn(ObserveHistFn),
                new PitayaCluster.SetGaugeFn(SetGaugeFn),
                new PitayaCluster.AddGaugeFn(AddGaugeFn),
                // TODO: here we would have the reference to the prometheus server.
                IntPtr.Zero);
        }

        ~MetricsReporter()
        {
            PitayaCluster.pitaya_metrics_reporter_drop(_metricsReporterPtr);
        }

        static void RegisterCounterFn(IntPtr userData, MetricsOpts opts)
        {
            // TODO: implementation
            string ns = Marshal.PtrToStringAnsi(opts.MetricNamespace);
            string subsystem = Marshal.PtrToStringAnsi(opts.Subsystem);
            string name = Marshal.PtrToStringAnsi(opts.Name);
            Console.WriteLine($"Registering Counter {ns}.{subsystem}.{name}");
        }

        static void RegisterHistogramFn(IntPtr userData, MetricsOpts opts)
        {
            // TODO: implementation
            string ns = Marshal.PtrToStringAnsi(opts.MetricNamespace);
            string subsystem = Marshal.PtrToStringAnsi(opts.Subsystem);
            string name = Marshal.PtrToStringAnsi(opts.Name);
            Console.WriteLine($"Registering Histogram {ns}.{subsystem}.{name}");
        }

        static void RegisterGaugeFn(IntPtr userData, MetricsOpts opts)
        {
            // TODO: implementation
            string ns = Marshal.PtrToStringAnsi(opts.MetricNamespace);
            string subsystem = Marshal.PtrToStringAnsi(opts.Subsystem);
            string name = Marshal.PtrToStringAnsi(opts.Name);
            Console.WriteLine($"Registering Gauge {ns}.{subsystem}.{name}");
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