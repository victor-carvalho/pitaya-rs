using System;
using System.Runtime.InteropServices;

namespace NPitaya
{
    public partial class PitayaCluster
    {
        private delegate void ServerAddedOrRemoved(int serverAdded, IntPtr server, IntPtr user);
        private delegate void SendRpcCallback(IntPtr userData, IntPtr err, IntPtr response);
        private delegate void ServerByIdCallback(IntPtr userData, IntPtr server);
        private delegate void SendKickCallback(IntPtr userData, IntPtr err, IntPtr kickAnswerBuffer);
        private delegate void SendPushCallback(IntPtr userData, IntPtr err);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void HandleRpcCallbackFunc(IntPtr userData, IntPtr rpc);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void ClusterNotificationCallbackFunc(IntPtr userData, NotificationType notificationType, IntPtr data);


        private const string LibName = "libpitaya";

        //
        // Pitaya
        //
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr pitaya_initialize_with_nats(
            string envPrefix,
            string configFile,
            IntPtr handleRpcCallback,
            IntPtr handleRpcData,
            NativeLogLevel logLevel,
            NativeLogKind logKind,
            IntPtr clusterNotificationCallback,
            IntPtr clusterNotificationData,
            out IntPtr pitaya);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern void pitaya_shutdown(IntPtr pitaya);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr pitaya_send_rpc(
            IntPtr pitaya,
            string serverId,
            string route,
            IntPtr req,
            SendRpcCallback callback,
            IntPtr userData);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern void pitaya_wait_shutdown_signal(IntPtr pitaya);

        //
        // Error
        //
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr pitaya_error_drop(IntPtr error);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr pitaya_error_code(IntPtr error);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr pitaya_error_message(IntPtr error);

        //
        // Buffer
        //
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr pitaya_buffer_new(IntPtr data, Int32 len);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr pitaya_buffer_drop(IntPtr buf);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr pitaya_buffer_data(IntPtr buf, out Int32 len);

        //
        // Server
        //
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern bool pitaya_server_by_id(
            IntPtr pitaya,
            string serverId,
            string serverKind,
            ServerByIdCallback callback1,
            IntPtr userData);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr pitaya_server_new(string id, string kind, string metadata, string hostname, Int32 frontend);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr pitaya_server_id(IntPtr server);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr pitaya_server_kind(IntPtr server);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr pitaya_server_metadata(IntPtr server);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr pitaya_server_hostname(IntPtr server);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern Int32 pitaya_server_frontend(IntPtr server);

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void pitaya_server_drop(IntPtr serverPtr);

        //
        // RPC
        //
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr pitaya_rpc_request(IntPtr rpc, out Int32 len);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr pitaya_rpc_respond(IntPtr rpc, IntPtr responseData, Int32 responseLen);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr pitaya_rpc_drop(IntPtr rpc);

        //
        // Kick
        //
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr pitaya_send_kick(
            IntPtr pitaya,
            string serverId,
            string serverKind,
            IntPtr kickBuffer,
            SendKickCallback callback,
            IntPtr userData);

        //
        // Push
        //
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr pitaya_send_push_to_user(
            IntPtr pitaya,
            string serverId,
            string serverKind,
            IntPtr pushBuffer,
            SendPushCallback callback,
            IntPtr userData);
    }

}