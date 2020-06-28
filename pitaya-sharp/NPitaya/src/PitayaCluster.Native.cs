using System;
using System.Runtime.InteropServices;

namespace NPitaya
{
    public partial class PitayaCluster
    {
        private delegate void ServerAddedOrRemoved(int serverAdded, IntPtr server, IntPtr user);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void HandleRpcCallbackFunc(IntPtr userData, IntPtr rpc);


        private const string LibName = "libpitaya";

        //
        // Pitaya
        //
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr pitaya_initialize_with_nats(
            IntPtr natsCfg,
            IntPtr sdCfg,
            IntPtr server,
            IntPtr handleRpcCallback,
            IntPtr handleRpcData,
            NativeLogLevel logLevel,
            NativeLogKind logKind,
            out IntPtr pitaya);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern void pitaya_shutdown(IntPtr pitaya);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern unsafe IntPtr pitaya_send_rpc(
            IntPtr pitaya,
            string serverId,
            string route,
            IntPtr req,
            out IntPtr res);

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
        private static extern bool pitaya_server_by_id(IntPtr pitaya, string serverId, string serverKind, ref Server server);

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern void pitaya_server_drop(IntPtr serverPtr);

        //
        // RPC
        //
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr pitaya_rpc_request(IntPtr rpc, out Int32 len);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr pitaya_rpc_respond(IntPtr rpc, IntPtr responseData, Int32 responseLen);
        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr pitaya_rpc_drop(IntPtr rpc);


        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "tfg_pitc_FreeMemoryBuffer")]
        private static extern unsafe void FreeMemoryBufferInternal(MemoryBuffer *ptr);

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "tfg_pitc_OnSignal")]
        private static extern void OnSignalInternal(OnSignalFunc ptr);

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "tfg_pitc_WaitForRpc")]
        private static extern IntPtr tfg_pitc_WaitForRpc();

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "tfg_pitc_FinishRpcCall")]
        private static extern void tfg_pitc_FinishRpcCall(IntPtr responseMemoryBufferPtr, IntPtr crpcPtr);

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "tfg_pitc_SendPushToUser")]
        private static extern unsafe IntPtr PushInternal(string serverId, string serverType, IntPtr pushData, out IntPtr buffer);

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "tfg_pitc_SendKickToUser")]
        private static extern unsafe IntPtr KickInternal(string serverId, string serverType, IntPtr pushData, out IntPtr buffer);

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr tfg_pitc_AddServiceDiscoveryListener(ServerAddedOrRemoved cb, IntPtr user);

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern void tfg_pitc_RemoveServiceDiscoveryListener(IntPtr listener);
    }
}