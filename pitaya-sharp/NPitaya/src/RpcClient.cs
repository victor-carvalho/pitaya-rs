using System;
using Google.Protobuf;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using NPitaya.Serializer;
using NPitaya.Protos;
using static NPitaya.Utils.Utils;

namespace NPitaya
{
    // RpcClient allows sending RPC messages to other Pitaya servers.
    internal class RpcClient
    {
        IntPtr _pitaya;
        ISerializer _serializer;

        public RpcClient(IntPtr pitaya)
        {
            _pitaya = pitaya;
        }

        public Task<T> Rpc<T>(string serverId, Route route, object msg)
        {
            return Task.Run(() =>
            {
                var callback = new PitayaCluster.SendRpcCallback(RpcCallback<T>);
                var t = new TaskCompletionSource<T>();
                var handle = GCHandle.Alloc(t, GCHandleType.Normal);

                unsafe
                {
                    var data = SerializerUtils.SerializeOrRaw(msg, _serializer);
                    fixed (byte* p = data)
                    {
                        IntPtr request = PitayaCluster.pitaya_buffer_new((IntPtr)p, data.Length);
                        PitayaCluster.pitaya_send_rpc(
                            _pitaya,
                            serverId,
                            route.ToString(),
                            request,
                            callback,
                            GCHandle.ToIntPtr(handle));
                    }
                }

                return t.Task;
            });
        }
        public Task SendKickToUser(string frontendId, string serverKind, KickMsg kick)
        {
            return Task.Run(() =>
            {
                var t = new TaskCompletionSource<bool>();
                var handle = GCHandle.Alloc(t, GCHandleType.Normal);
                var del = new PitayaCluster.SendKickCallback(KickCallback);

                unsafe
                {
                    var data = kick.ToByteArray();
                    fixed (byte* p = data)
                    {
                        IntPtr kickBuffer = PitayaCluster.pitaya_buffer_new((IntPtr)p, data.Length);
                        PitayaCluster.pitaya_send_kick(_pitaya, frontendId, serverKind, kickBuffer, del, GCHandle.ToIntPtr(handle));
                    }
                }

                return t.Task;
            });
        }

        public Task SendPushToUser(
            string frontendId,
            string serverKind,
            string route,
            string uid,
            object pushMsg)
        {
            return Task.Run(() =>
            {
                var t = new TaskCompletionSource<bool>();
                var del = new PitayaCluster.SendPushCallback(PushCallback);
                var handle = GCHandle.Alloc(t, GCHandleType.Normal);
                var push = new Push
                {
                    Route = route,
                    Uid = uid,
                    Data = ByteString.CopyFrom(SerializerUtils.SerializeOrRaw(pushMsg, _serializer))
                };

                unsafe
                {
                    var data = push.ToByteArray();
                    fixed (byte* p = data)
                    {
                        IntPtr pushBuffer = PitayaCluster.pitaya_buffer_new((IntPtr)p, data.Length);
                        PitayaCluster.pitaya_send_push_to_user(_pitaya, frontendId, serverKind, pushBuffer, del, GCHandle.ToIntPtr(handle));
                    }
                }

                return t.Task;
            });
        }

        static void RpcCallback<T>(IntPtr userData, IntPtr err, IntPtr responseBuf)
        {
            var handle = GCHandle.FromIntPtr(userData);
            var t = (TaskCompletionSource<T>)handle.Target;

            if (err != IntPtr.Zero)
            {
                var pitayaError = new PitayaError(
                    PitayaCluster.pitaya_error_code(err),
                    PitayaCluster.pitaya_error_message(err));
                t.SetException(new PitayaException($"RPC call failed: ({pitayaError.Code}: {pitayaError.Message})"));
                return;
            }

            Int32 len;
            IntPtr resData = PitayaCluster.pitaya_buffer_data(responseBuf, out len);
            T response = GetProtoMessageFromBuffer<T>(GetDataFromRawPointer(resData, len));
            t.SetResult(response);
        }

        static void KickCallback(IntPtr userData, IntPtr err, IntPtr kickAnswerBuf)
        {
            var handle = GCHandle.FromIntPtr(userData);
            var t = (TaskCompletionSource<bool>)handle.Target;

            if (err != IntPtr.Zero)
            {
                var pe = new PitayaError(
                    PitayaCluster.pitaya_error_code(err),
                    PitayaCluster.pitaya_error_message(err));
                t.SetException(new Exception($"Kick failed: code={pe.Code} message={pe.Message}"));
                return;
            }

            Int32 len;
            IntPtr resData = PitayaCluster.pitaya_buffer_data(kickAnswerBuf, out len);

            var kickAnswer = new KickAnswer();
            kickAnswer.MergeFrom(new CodedInputStream(GetDataFromRawPointer(resData, len)));

            if (!kickAnswer.Kicked)
            {
                t.SetException(new Exception($"Kick failed: received kicked=false from server"));
                return;
            }

            t.SetResult(true);
        }

        static void PushCallback(IntPtr userData, IntPtr err)
        {
            var handle = GCHandle.FromIntPtr(userData);
            var t = (TaskCompletionSource<bool>)handle.Target;

            if (err != IntPtr.Zero)
            {
                var pe = new PitayaError(
                    PitayaCluster.pitaya_error_code(err),
                    PitayaCluster.pitaya_error_message(err));
                t.SetException(new Exception($"Push failed: code={pe.Code}, message={pe.Message}"));
                return;
            }

            t.SetResult(true);
        }
    }
}