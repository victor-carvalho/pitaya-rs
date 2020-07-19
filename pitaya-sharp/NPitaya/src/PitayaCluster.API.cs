using System;
using Google.Protobuf;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using NPitaya.Metrics;
using NPitaya.Models;
using NPitaya.Serializer;
using NPitaya.Protos;
using NPitaya.Utils;
using static NPitaya.Utils.Utils;

// TODO profiling
// TODO better reflection performance in task async call
// TODO support to sync methods
namespace NPitaya
{
    public partial class PitayaCluster
    {
        private static ISerializer _serializer = new ProtobufSerializer();
        public delegate string RemoteNameFunc(string methodName);
        private delegate void OnSignalFunc();
        private static readonly Dictionary<string, RemoteMethod> RemotesDict = new Dictionary<string, RemoteMethod>();
        private static readonly Dictionary<string, RemoteMethod> HandlersDict = new Dictionary<string, RemoteMethod>();
        private static IntPtr pitaya;
        private static HandleRpcCallbackFunc handleRpcCallback;
        private static ClusterNotificationCallbackFunc clusterNotificationCallback;
        private static LogFunction logFunctionCallback;

        private static Action _onSignalEvent;

        public enum ServiceDiscoveryAction
        {
            ServerAdded,
            ServerRemoved,
        }

        public class ServiceDiscoveryListener
        {
            public Action<ServiceDiscoveryAction, Server> onServer;
            public ServiceDiscoveryListener(Action<ServiceDiscoveryAction, Server> onServer)
            {
                Debug.Assert(onServer != null);
                this.onServer = onServer;
            }
        }

        private static ServiceDiscoveryListener _serviceDiscoveryListener;
        private static GCHandle _serviceDiscoveryListenerHandle;

        public static void AddSignalHandler(Action cb)
        {
            _onSignalEvent += cb;
            // OnSignalInternal(OnSignal);
        }

        private static void OnSignal()
        {
            Logger.Info("Invoking signal handler");
            _onSignalEvent?.Invoke();
        }

        private static void ClusterNotificationCallback(IntPtr userData, NotificationType notificationType, IntPtr serverHandle)
        {
            if (_serviceDiscoveryListener == null)
            {
                return;
            }

            var server = new Server(serverHandle);
            _serviceDiscoveryListener.onServer(
                notificationType == NotificationType.ServerAdded
                    ? ServiceDiscoveryAction.ServerAdded
                    : ServiceDiscoveryAction.ServerRemoved,
                server
            );
        }

        private static void HandleRpcCallback(IntPtr userData, IntPtr rpc)
        {
            Int32 len;
            IntPtr rawData = pitaya_rpc_request(rpc, out len);
            byte[] data = GetDataFromRawPointer(rawData, len);

            try
            {
                var req = new Protos.Request();
                req.MergeFrom(new CodedInputStream(data));

                DispatchRpc(rpc, req);
            }
            catch (Exception e)
            {
                Logger.Error("Failed to decode request, error:{0}", e.Message);
            }
        }

        public static void Initialize(string envPrefix,
                                      string configFile,
                                      NativeLogLevel logLevel,
                                      NativeLogKind logKind,
                                      Action<string> logFunction,
                                      ServiceDiscoveryListener serviceDiscoveryListener = null)
        {
            _serviceDiscoveryListener = serviceDiscoveryListener;
            handleRpcCallback = new HandleRpcCallbackFunc(HandleRpcCallback);
            clusterNotificationCallback = new ClusterNotificationCallbackFunc(ClusterNotificationCallback);
            logFunctionCallback = new LogFunction(LogFunctionCallback);
            var logCtx = GCHandle.Alloc(logFunction, GCHandleType.Normal);

            IntPtr err = pitaya_initialize_with_nats(
                IntPtr.Zero,
                envPrefix,
                configFile,
                Marshal.GetFunctionPointerForDelegate(handleRpcCallback),
                Marshal.GetFunctionPointerForDelegate(clusterNotificationCallback),
                logLevel,
                logKind,
                Marshal.GetFunctionPointerForDelegate(logFunctionCallback),
                GCHandle.ToIntPtr(logCtx),
                out pitaya
            );

            if (err != IntPtr.Zero)
            {
                var pitayaError = new PitayaError(pitaya_error_code(err), pitaya_error_message(err));
                pitaya_error_drop(err);
                throw new PitayaException($"Initialization failed: code={pitayaError.Code} msg={pitayaError.Message}");
            }
        }

        static void LogFunctionCallback(IntPtr ctx, IntPtr msg)
        {
            if (ctx == IntPtr.Zero)
            {
                return;
            }
            var handle = GCHandle.FromIntPtr(ctx);
            var logFn = (Action<string>)handle.Target;
            var msgStr = Marshal.PtrToStringAnsi(msg);
            logFn(msgStr);
        }

        public static void RegisterRemote(BaseRemote remote)
        {
            string className = DefaultRemoteNameFunc(remote.GetName());
            RegisterRemote(remote, className, DefaultRemoteNameFunc);
        }

        public static void RegisterRemote(BaseRemote remote, string name)
        {
            RegisterRemote(remote, name, DefaultRemoteNameFunc);
        }

        public static void RegisterRemote(BaseRemote remote, string name, RemoteNameFunc remoteNameFunc) // TODO remote function name func
        {
            Dictionary<string, RemoteMethod> m = remote.GetRemotesMap();
            foreach (KeyValuePair<string, RemoteMethod> kvp in m)
            {
                var rn = remoteNameFunc(kvp.Key);
                var remoteName = $"{name}.{rn}";
                if (RemotesDict.ContainsKey(remoteName))
                {
                    throw new PitayaException($"tried to register same remote twice! remote name: {remoteName}");
                }

                Logger.Info("registering remote {0}", remoteName);
                RemotesDict[remoteName] = kvp.Value;
            }
        }

        public static void RegisterHandler(BaseHandler handler)
        {
            string className = DefaultRemoteNameFunc(handler.GetName());
            RegisterHandler(handler, className, DefaultRemoteNameFunc);
        }

        public static void RegisterHandler(BaseHandler handler, string name)
        {
            RegisterHandler(handler, name, DefaultRemoteNameFunc);
        }

        public static void RegisterHandler(BaseHandler handler, string name, RemoteNameFunc remoteNameFunc)
        {
            Dictionary<string, RemoteMethod> m = handler.GetRemotesMap();
            foreach (KeyValuePair<string, RemoteMethod> kvp in m)
            {
                var rn = remoteNameFunc(kvp.Key);
                var handlerName = $"{name}.{rn}";
                if (HandlersDict.ContainsKey(handlerName))
                {
                    throw new PitayaException($"tried to register same remote twice! remote name: {handlerName}");
                }

                Logger.Info("registering handler {0}", handlerName);
                HandlersDict[handlerName] = kvp.Value;
            }
        }

        public static void SetSerializer(ISerializer s)
        {
            _serializer = s;
        }

        public static void WaitShutdownSignal()
        {
            pitaya_wait_shutdown_signal(pitaya);
        }

        public static void Terminate()
        {
            pitaya_shutdown(pitaya);
            pitaya = IntPtr.Zero;
            MetricsReporters.Terminate();
        }

        class ServerIdContext
        {
            public TaskCompletionSource<Server> t;
            public string serverId;
        }

        private static void GetServerByIdCallback(IntPtr userData, IntPtr serverHandle)
        {
            var handle = GCHandle.FromIntPtr(userData);
            var context = (ServerIdContext)handle.Target;

            if (serverHandle == IntPtr.Zero)
            {
                Logger.Error($"There are no servers with id {context.serverId}");
                context.t.TrySetResult(null);
                return;
            }

            var sv = new Server(serverHandle);
            context.t.TrySetResult(sv);
        }

        public static Task<Server> GetServerById(string serverId, string serverKind)
        {
            return Task.Run(() =>
            {
                var context = new ServerIdContext
                {
                    t = new TaskCompletionSource<Server>(),
                    serverId = serverId,
                };

                var handle = GCHandle.Alloc(context, GCHandleType.Normal);
                var callback = new ServerByIdCallback(GetServerByIdCallback);

                pitaya_server_by_id(pitaya, serverId, serverKind, callback, GCHandle.ToIntPtr(handle));

                return context.t.Task;
            });
        }

        private static void PushCallback(IntPtr userData, IntPtr err)
        {
            var handle = GCHandle.FromIntPtr(userData);
            var t = (TaskCompletionSource<bool>)handle.Target;

            if (err != IntPtr.Zero)
            {
                var pe = new PitayaError(pitaya_error_code(err), pitaya_error_message(err));
                t.SetException(new Exception($"Push failed: code={pe.Code}, message={pe.Message}"));
                return;
            }

            t.SetResult(true);
        }

        public static Task SendPushToUser(
            string frontendId,
            string serverKind,
            string route,
            string uid,
            object pushMsg)
        {
            return Task.Run(() =>
            {
                var t = new TaskCompletionSource<bool>();
                var del = new SendPushCallback(PushCallback);
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
                        IntPtr pushBuffer = pitaya_buffer_new((IntPtr)p, data.Length);
                        pitaya_send_push_to_user(pitaya, frontendId, serverKind, pushBuffer, del, GCHandle.ToIntPtr(handle));
                    }
                }

                return t.Task;
            });
        }

        private static void KickCallback(IntPtr userData, IntPtr err, IntPtr kickAnswerBuf)
        {
            var handle = GCHandle.FromIntPtr(userData);
            var t = (TaskCompletionSource<bool>)handle.Target;

            if (err != IntPtr.Zero)
            {
                var pe = new PitayaError(pitaya_error_code(err), pitaya_error_message(err));
                t.SetException(new Exception($"Kick failed: code={pe.Code} message={pe.Message}"));
                return;
            }

            Int32 len;
            IntPtr resData = pitaya_buffer_data(kickAnswerBuf, out len);

            var kickAnswer = new KickAnswer();
            kickAnswer.MergeFrom(new CodedInputStream(GetDataFromRawPointer(resData, len)));

            if (!kickAnswer.Kicked)
            {
                t.SetException(new Exception($"Kick failed: received kicked=false from server"));
                return;
            }

            t.SetResult(true);
        }

        public static Task SendKickToUser(string frontendId, string serverKind, KickMsg kick)
        {
            return Task.Run(() =>
            {
                var t = new TaskCompletionSource<bool>();
                var handle = GCHandle.Alloc(t, GCHandleType.Normal);
                var del = new SendKickCallback(KickCallback);

                unsafe
                {
                    var data = kick.ToByteArray();
                    fixed (byte* p = data)
                    {
                        IntPtr kickBuffer = pitaya_buffer_new((IntPtr)p, data.Length);
                        pitaya_send_kick(pitaya, frontendId, serverKind, kickBuffer, del, GCHandle.ToIntPtr(handle));
                    }
                }

                return t.Task;
            });
        }

        private static void RpcCallback<T>(IntPtr userData, IntPtr err, IntPtr responseBuf)
        {
            var handle = GCHandle.FromIntPtr(userData);
            var t = (TaskCompletionSource<T>)handle.Target;

            if (err != IntPtr.Zero)
            {
                var pitayaError = new PitayaError(pitaya_error_code(err), pitaya_error_message(err));
                t.SetException(new PitayaException($"RPC call failed: ({pitayaError.Code}: {pitayaError.Message})"));
                return;
            }

            Int32 len;
            IntPtr resData = pitaya_buffer_data(responseBuf, out len);
            T response = GetProtoMessageFromBuffer<T>(GetDataFromRawPointer(resData, len));
            t.SetResult(response);
        }

        public static Task<T> Rpc<T>(string serverId, Route route, object msg)
        {
            return Task.Run(() =>
            {
                var callback = new SendRpcCallback(RpcCallback<T>);
                var t = new TaskCompletionSource<T>();
                var handle = GCHandle.Alloc(t, GCHandleType.Normal);

                unsafe
                {
                    var data = SerializerUtils.SerializeOrRaw(msg, _serializer);
                    fixed (byte* p = data)
                    {
                        IntPtr request = pitaya_buffer_new((IntPtr)p, data.Length);
                        pitaya_send_rpc(pitaya, serverId, route.ToString(), request, callback, GCHandle.ToIntPtr(handle));
                    }
                }

                return t.Task;
            });
        }

        public static Task<T> Rpc<T>(Route route, object msg)
        {
            return Rpc<T>("", route, msg);
        }

        private static void OnServerAddedOrRemovedNativeCb(int serverAdded, IntPtr serverPtr, IntPtr user)
        {
            var pitayaClusterHandle = (GCHandle)user;
            var serviceDiscoveryListener = pitayaClusterHandle.Target as ServiceDiscoveryListener;

            if (serviceDiscoveryListener == null)
            {
                Logger.Warn("The service discovery listener is null!");
                return;
            }

            var server = (Server)Marshal.PtrToStructure(serverPtr, typeof(Server));

            if (serverAdded == 1)
                serviceDiscoveryListener.onServer(ServiceDiscoveryAction.ServerAdded, server);
            else
                serviceDiscoveryListener.onServer(ServiceDiscoveryAction.ServerRemoved, server);
        }
    }
}