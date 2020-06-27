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

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "pitaya_initialize_with_nats")]
        private static extern IntPtr InitializeWithNatsInternal(
            IntPtr natsCfg,
            IntPtr sdCfg,
            IntPtr server,
            IntPtr handleRpcCallback,
            IntPtr handleRpcData,
            NativeLogLevel logLevel,
            NativeLogKind logKind,
            out IntPtr pitaya);

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr pitaya_error_drop(IntPtr error);

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "pitaya_shutdown")]
        private static extern void TerminateInternal(IntPtr pitaya);

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "pitaya_server_by_id")]
        private static extern bool GetServerByIdInternal(IntPtr pitaya, string serverId, string serverKind, ref Server server);

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "pitaya_server_drop")]
        private static extern void FreeServerInternal(IntPtr serverPtr);


        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "tfg_pitc_RPC")]
        private static extern unsafe bool RPCInternal(string serverId, string route, IntPtr data, int dataSize, MemoryBuffer** buffer, ref Error retErr);

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "tfg_pitc_FreeMemoryBuffer")]
        private static extern unsafe void FreeMemoryBufferInternal(MemoryBuffer *ptr);

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "tfg_pitc_OnSignal")]
        private static extern void OnSignalInternal(OnSignalFunc ptr);

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "tfg_pitc_FreePitayaError")]
        private static extern unsafe void FreePitayaErrorInternal(ref Error err);

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "tfg_pitc_WaitForRpc")]
        private static extern IntPtr tfg_pitc_WaitForRpc();

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "tfg_pitc_FinishRpcCall")]
        private static extern void tfg_pitc_FinishRpcCall(IntPtr responseMemoryBufferPtr, IntPtr crpcPtr);

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "tfg_pitc_SendPushToUser")]
        private static extern unsafe bool PushInternal(string serverId, string serverType, IntPtr pushData, MemoryBuffer** buffer, ref Error retErr);

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "tfg_pitc_SendKickToUser")]
        private static extern unsafe bool KickInternal(string serverId, string serverType, IntPtr pushData, MemoryBuffer** buffer, ref Error retErr);

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr tfg_pitc_AddServiceDiscoveryListener(ServerAddedOrRemoved cb, IntPtr user);

        [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
        private static extern void tfg_pitc_RemoveServiceDiscoveryListener(IntPtr listener);
    }
}