using System;
using System.Threading.Tasks;
using System.Runtime.InteropServices;

namespace NPitaya
{
    internal class MetricsReporter
    {
        IntPtr _pitaya;

        internal MetricsReporter(IntPtr pitaya)
        {
            _pitaya = pitaya;
        }

        internal Task IncCounter(string name, string[] labels)
        {
            return Task.Run(() =>
            {
                var callback = new PitayaCluster.NoErrorCallback(Callback);
                var t = new TaskCompletionSource<bool>();
                var handle = GCHandle.Alloc(t, GCHandleType.Normal);

                PitayaCluster.pitaya_metrics_inc_counter(
                    _pitaya,
                    name,
                    labels,
                    (UInt32)labels.Length,
                    callback,
                    GCHandle.ToIntPtr(handle)
                );

                return t.Task;
            });
        }

        internal Task ObserveHist(string name, double value, string[] labels)
        {
            return Task.Run(() =>
            {
                var callback = new PitayaCluster.NoErrorCallback(Callback);
                var t = new TaskCompletionSource<bool>();
                var handle = GCHandle.Alloc(t, GCHandleType.Normal);

                PitayaCluster.pitaya_metrics_observe_hist(
                    _pitaya,
                    name,
                    value,
                    labels,
                    (UInt32)labels.Length,
                    callback,
                    GCHandle.ToIntPtr(handle)
                );

                return t.Task;
            });
        }

        internal Task SetGauge(string name, double value, string[] labels)
        {
            return Task.Run(() =>
            {
                var callback = new PitayaCluster.NoErrorCallback(Callback);
                var t = new TaskCompletionSource<bool>();
                var handle = GCHandle.Alloc(t, GCHandleType.Normal);

                PitayaCluster.pitaya_metrics_set_gauge(
                    _pitaya,
                    name,
                    value,
                    labels,
                    (UInt32)labels.Length,
                    callback,
                    GCHandle.ToIntPtr(handle)
                );

                return t.Task;
            });
        }

        internal Task AddGauge(string name, double value, string[] labels)
        {
            return Task.Run(() =>
            {
                var callback = new PitayaCluster.NoErrorCallback(Callback);
                var t = new TaskCompletionSource<bool>();
                var handle = GCHandle.Alloc(t, GCHandleType.Normal);

                PitayaCluster.pitaya_metrics_add_gauge(
                    _pitaya,
                    name,
                    value,
                    labels,
                    (UInt32)labels.Length,
                    callback,
                    GCHandle.ToIntPtr(handle)
                );

                return t.Task;
            });
        }

        static void Callback(IntPtr userData)
        {
            var handle = GCHandle.FromIntPtr(userData);
            var t = (TaskCompletionSource<bool>)handle.Target;
            t.SetResult(true);
        }
    }
}