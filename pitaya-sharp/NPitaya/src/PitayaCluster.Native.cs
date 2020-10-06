using System;
using System.Runtime.InteropServices;

namespace NPitaya
{
    public partial class PitayaCluster
    {
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void ServerAddedOrRemoved(int serverAdded, IntPtr server, IntPtr user);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate void SendRpcCallback(IntPtr userData, IntPtr err, IntPtr response);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void ServerByIdCallback(IntPtr userData, IntPtr server);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate void SendKickCallback(IntPtr userData, IntPtr err, IntPtr kickAnswerBuffer);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate void SendPushCallback(IntPtr userData, IntPtr err);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void LogFunction(IntPtr context, IntPtr msg);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void HandleRpcCallbackFunc(IntPtr userData, IntPtr ctx, IntPtr rpc);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void ClusterNotificationCallbackFunc(IntPtr userData, NotificationType notificationType, IntPtr data);
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate void FinishCallback(IntPtr userData, IntPtr errorMsg);
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate void NoErrorCallback(IntPtr userData);

        private const string LibName = "libpitaya";

        //
        // Pitaya
        //
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr pitaya_initialize_with_nats(
            IntPtr context,
            string envPrefix,
            string configFile,
            IntPtr handleRpcCallback,
            IntPtr clusterNotificationCallback,
            NativeLogLevel logLevel,
            NativeLogKind logKind,
            IntPtr logFunction,
            IntPtr logCtx,
            IntPtr customMetrics,
            out IntPtr pitaya);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern void pitaya_shutdown(IntPtr pitaya);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr pitaya_send_rpc(
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
        internal static extern IntPtr pitaya_error_drop(IntPtr error);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr pitaya_error_code(IntPtr error);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr pitaya_error_message(IntPtr error);

        //
        // Buffer
        //
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr pitaya_buffer_new(IntPtr data, Int32 len);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr pitaya_buffer_drop(IntPtr buf);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr pitaya_buffer_data(IntPtr buf, out Int32 len);

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
        internal static extern IntPtr pitaya_send_kick(
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
        internal static extern IntPtr pitaya_send_push_to_user(
            IntPtr pitaya,
            string serverId,
            string serverKind,
            IntPtr pushBuffer,
            SendPushCallback callback,
            IntPtr userData);

        //
        // CustomMetrics
        //

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr pitaya_custom_metrics_new();

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void pitaya_custom_metrics_drop(IntPtr customMetrics);

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void pitaya_custom_metrics_add_hist(
            IntPtr customMetrics,
            string metricNamespace,
            string subsystem,
            string name,
            string help,
            string[] variableLabels,
            UInt32 variableLabelsCount,
            double[] buckets,
            UInt32 buckets_count
        );

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void pitaya_custom_metrics_add_counter(
            IntPtr customMetrics,
            string metricNamespace,
            string subsystem,
            string name,
            string help,
            string[] variableLabels,
            UInt32 variableLabelsCount
        );

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void pitaya_metrics_inc_counter(
            IntPtr pitaya,
            string name,
            string[] labels,
            UInt32 labelsCount,
            NoErrorCallback callback,
            IntPtr userData
        );

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void pitaya_metrics_observe_hist(
            IntPtr pitaya,
            string name,
            double value,
            string[] labels,
            UInt32 labelsCount,
            NoErrorCallback callback,
            IntPtr userData
        );
    }
}