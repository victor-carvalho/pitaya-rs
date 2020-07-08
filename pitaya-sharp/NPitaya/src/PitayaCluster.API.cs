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
        // private static readonly int ProcessorsCount = Environment.ProcessorCount;
        private static ISerializer _serializer = new ProtobufSerializer();
        public delegate string RemoteNameFunc(string methodName);
        private delegate void OnSignalFunc();
        private static readonly Dictionary<string, RemoteMethod> RemotesDict = new Dictionary<string, RemoteMethod>();
        private static readonly Dictionary<string, RemoteMethod> HandlersDict = new Dictionary<string, RemoteMethod>();
        private static IntPtr pitaya;
        private static HandleRpcCallbackFunc handleRpcCallback;
        private static ClusterNotificationCallbackFunc clusterNotificationCallback;

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

        private static void DispatchRpc(IntPtr rpc, Protos.Request req)
        {
            Task.Run(async () => {
                var res = new Protos.Response();
                try
                {
                    switch (req.Type)
                    {
                        case RPCType.User:
                            res = await HandleRpc(req, RPCType.User);
                            break;
                        case RPCType.Sys:
                            res = await HandleRpc(req, RPCType.Sys);
                            break;
                        default:
                            throw new Exception($"invalid rpc type, argument:{req.Type}");
                    }
                }
                catch (Exception e)
                {
                    res = GetErrorResponse("PIT-500", e.Message);

                    var innerMostException = e;
                    while (innerMostException.InnerException != null)
                        innerMostException = innerMostException.InnerException;

                    Logger.Error("Exception thrown in handler: {0}", innerMostException.Message);
#if NPITAYA_DEBUG
                    Logger.Error("StackTrace: {0}", e.StackTrace);
#endif
                }
                finally
                {
                    unsafe
                    {
                        byte[] responseData = res.ToByteArray();
                        Int32 responseLen = responseData.Length;

                        fixed (byte* p = responseData)
                        {
                            IntPtr err = pitaya_rpc_respond(rpc, (IntPtr)p, responseLen);
                            if (err != IntPtr.Zero)
                            {
                                pitaya_error_drop(err);
                                Logger.Error("Failed to respond to rpc");
                            }
                        }
                    }
                }
            });
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

        public static void Initialize(NatsConfig natsCfg,
                                      SDConfig sdCfg,
                                      Server server,
                                      NativeLogLevel logLevel,
                                      NativeLogKind logKind,
                                      ServiceDiscoveryListener serviceDiscoveryListener = null)
        {
            IntPtr natsCfgPtr = new StructWrapper(natsCfg);
            IntPtr sdCfgPtr = new StructWrapper(sdCfg);

            _serviceDiscoveryListener = serviceDiscoveryListener;
            handleRpcCallback = new HandleRpcCallbackFunc(HandleRpcCallback);
            clusterNotificationCallback = new ClusterNotificationCallbackFunc(ClusterNotificationCallback);

            IntPtr err = pitaya_initialize_with_nats(
                natsCfgPtr,
                sdCfgPtr,
                server.Handle,
                Marshal.GetFunctionPointerForDelegate(handleRpcCallback),
                IntPtr.Zero,
                logLevel,
                logKind,
                Marshal.GetFunctionPointerForDelegate(clusterNotificationCallback),
                IntPtr.Zero,
                out pitaya
            );

            if (err != IntPtr.Zero)
            {
                // TODO(lhahn): convert error
                pitaya_error_drop(err);
                throw new PitayaException("Initialization failed");
            }
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

        public static void Terminate()
        {
            pitaya_shutdown(pitaya);
            MetricsReporters.Terminate();
        }

        public static Server GetServerById(string serverId, string serverKind)
        {
            IntPtr serverHandle = IntPtr.Zero;
            bool ok = pitaya_server_by_id(pitaya, serverId, serverKind, ref serverHandle);
            if (!ok)
            {
                Logger.Error($"There are no servers with id {serverId}");
                return null;
            }
            return new Server(serverHandle);
        }

        public static unsafe Task<bool> SendPushToUser(string frontendId, string serverType, string route, string uid,
            object pushMsg)
        {
            return Task.Run(() =>
            {
                IntPtr err = IntPtr.Zero;
                MemoryBuffer inMemBuf = new MemoryBuffer();
                IntPtr outMemBufPtr = IntPtr.Zero;
                var retError = new Error();

                var push = new Push
                {
                    Route = route,
                    Uid = uid,
                    Data = ByteString.CopyFrom(SerializerUtils.SerializeOrRaw(pushMsg, _serializer))
                };

                try
                {
                    var data = push.ToByteArray();
                    fixed (byte* p = data)
                    {
                        inMemBuf.data = (IntPtr) p;
                        inMemBuf.size = data.Length;
                        IntPtr inMemBufPtr = new StructWrapper(inMemBuf);

                        // err = PushInternal(frontendId, serverType, inMemBufPtr, out outMemBufPtr);
                        // if (err != IntPtr.Zero) // error
                        {
                            // Logger.Error($"Push failed: ({retError.code}: {retError.msg})");
                            return false;
                        }

                        return true;
                    }
                }
                finally
                {
                    // if (outMemBufPtr != null) FreeMemoryBufferInternal(outMemBufPtr);
                }
            });
        }

        public static unsafe Task<bool> SendKickToUser(string frontendId, string serverKind, KickMsg kick)
        {
            return Task.Run(() =>
            {
                var data = kick.ToByteArray();
                fixed (byte* p = data)
                {
                    IntPtr kickAnswerPtr = IntPtr.Zero;
                    IntPtr kickBuffer = pitaya_buffer_new((IntPtr)p, data.Length);
                    IntPtr err = pitaya_send_kick(pitaya, frontendId, serverKind, kickBuffer, out kickAnswerPtr);
                    pitaya_buffer_drop(kickBuffer);
                    if (err != IntPtr.Zero)
                    {
                        var pitayaError = new PitayaError(pitaya_error_code(err), pitaya_error_message(err));
                        pitaya_error_drop(err);
                        Logger.Error($"Push failed: ({pitayaError.Code}: {pitayaError.Message})");
                        return false;
                    }

                    Int32 len;
                    IntPtr resData = pitaya_buffer_data(kickAnswerPtr, out len);

                    var kickAnswer = new KickAnswer();
                    kickAnswer.MergeFrom(new CodedInputStream(GetDataFromRawPointer(resData, len)));

                    pitaya_buffer_drop(kickAnswerPtr);
                    return kickAnswer.Kicked;
                }
            });
        }

        public static unsafe Task<T> Rpc<T>(string serverId, Route route, object msg)
        {
            return Task.Run(() =>
            {
                IntPtr rpcResponse;
                IntPtr err = IntPtr.Zero;

                var data = SerializerUtils.SerializeOrRaw(msg, _serializer);
                fixed (byte* p = data)
                {
                    IntPtr request = pitaya_buffer_new((IntPtr)p, data.Length);
                    err = pitaya_send_rpc(pitaya, serverId, route.ToString(), request, out rpcResponse);
                    pitaya_buffer_drop(request);
                }

                if (err != IntPtr.Zero)
                {
                    var pitayaError = new PitayaError(pitaya_error_code(err), pitaya_error_message(err));
                    pitaya_error_drop(err);
                    throw new PitayaException($"RPC call failed: ({pitayaError.Code}: {pitayaError.Message})");
                }

                Int32 len;
                IntPtr resData = pitaya_buffer_data(rpcResponse, out len);

                T response = GetProtoMessageFromBuffer<T>(GetDataFromRawPointer(resData, len));
                pitaya_buffer_drop(rpcResponse);
                return response;
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