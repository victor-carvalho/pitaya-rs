using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace NPitaya
{
    public class Session
    {
        IntPtr _pitaya;
        IntPtr _sessionPtr;

        internal Session(IntPtr pitaya, IntPtr sessionPtr)
        {
            _pitaya = pitaya;
            _sessionPtr = sessionPtr;
        }

        ~Session()
        {
            PitayaCluster.pitaya_session_drop(_sessionPtr);
        }

        public Task<string> Bind(string uid)
        {
            return Task.Run(() =>
            {
                var context = new Context
                {
                    t = new TaskCompletionSource<string>(),
                };

                var handle = GCHandle.Alloc(context, GCHandleType.Normal);
                var callback = new PitayaCluster.FinishCallback(FinishCallback);

                PitayaCluster.pitaya_session_bind(_pitaya, _sessionPtr, uid, callback, GCHandle.ToIntPtr(handle));

                return context.t.Task;
            });
        }

        public Task<string> UpdateInFront()
        {
            return Task.Run(() =>
            {
                var context = new Context
                {
                    t = new TaskCompletionSource<string>(),
                };

                var handle = GCHandle.Alloc(context, GCHandleType.Normal);
                var callback = new PitayaCluster.FinishCallback(FinishCallback);

                PitayaCluster.pitaya_session_update_in_front(_pitaya, _sessionPtr, callback, GCHandle.ToIntPtr(handle));

                return context.t.Task;
            });
        }

        class Context
        {
            public TaskCompletionSource<string> t;
        }

        static void FinishCallback(IntPtr userData, IntPtr errorMsg)
        {
            var handle = GCHandle.FromIntPtr(userData);
            var context = (Context)handle.Target;

            if (errorMsg != IntPtr.Zero)
            {
                string errorMsgStr = Marshal.PtrToStringAnsi(errorMsg);
                context.t.TrySetResult(errorMsgStr);
                return;
            }

            context.t.TrySetResult(null);
        }
    }
}