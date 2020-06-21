use crate::{cluster, protos, EtcdConfig, Pitaya, PitayaBuilder, RpcClientConfig, RpcServerConfig};
use lazy_static::lazy_static;
use log::error;
use std::{os::raw::c_char, sync::Mutex, time};
use tokio::sync::oneshot;

#[repr(C)]
struct CPitayaError {
    code: *mut c_char,
    msg: *mut c_char,
}

#[repr(C)]
struct CServer {
    id: *mut c_char,
    kind: *mut c_char,
    metadata: *mut c_char,
    hostname: *mut c_char,
    frontend: i32,
}

#[repr(C)]
enum LogLevel {
    Debug = 0,
    Info = 1,
    Warn = 2,
    Error = 3,
    Critical = 4,
}

#[repr(C)]
struct CSDConfig {
    endpoints: *mut c_char,
    etcdPrefix: *mut c_char,
    serverTypeFilters: *mut c_char,
    heartbeatTTLSec: i32,
    logHeartbeat: i32,
    logServerSync: i32,
    logServerDetails: i32,
    syncServersIntervalSec: i32,
    maxNumberOfRetries: i32,
}

#[repr(C)]
struct CNATSConfig {
    addr: *mut c_char,
    connectionTimeoutMs: i64,
    requestTimeoutMs: i32,
    serverShutdownDeadlineMs: i32,
    serverMaxNumberOfRpcs: i32,
    maxReconnectionAttempts: i32,
    maxPendingMsgs: i32,
}

#[repr(C)]
struct Rpc {
    request: *mut u8,
    responder: Box<oneshot::Sender<protos::Response>>,
}

struct PitayaServer {
    pitaya_server: Pitaya,
    shutdown_receiver: oneshot::Receiver<()>,
}

#[no_mangle]
pub extern "C" fn pitaya_initialize_with_nats(
    nc: *mut CNATSConfig,
    sdConfig: *mut CSDConfig,
    sv: *mut CServer,
    logLevel: LogLevel,
) -> *mut PitayaServer {
    log::info!("initializing global pitaya server");

    pretty_env_logger::init();

    let (pitaya_server, shutdown_receiver) = PitayaBuilder::new()
        .with_server_kind("random-kind")
        .with_rpc_client_config(RpcClientConfig {
            request_timeout: time::Duration::from_secs(4),
            ..Default::default()
        })
        .with_rpc_server_config(RpcServerConfig {
            ..Default::default()
        })
        .with_etcd_config(EtcdConfig {
            prefix: String::from("pitaya"),
            ..EtcdConfig::default()
        })
        .with_rpc_handler(move |mut rpc| {
            log::info!("!!!!!!!! received rpc req: {:?}", rpc.request());
            let res = protos::Response {
                data: "HEY, THIS IS THE SERVER".as_bytes().to_owned(),
                error: None,
            };
            if !rpc.respond(res) {
                log::error!("failed to respond to the server");
            }
        })
        .build()
        .expect("failed to start pitaya server");

    Box::into_raw(Box::new(PitayaServer {
        pitaya_server: pitaya_server,
        shutdown_receiver: shutdown_receiver,
    }))
}

#[no_mangle]
pub extern "C" fn pitaya_shutdown(pitaya_server: *mut PitayaServer) {
    assert!(!pitaya_server.is_null());
    let pitaya_server = unsafe { Box::from_raw(pitaya_server) };

    if let Err(e) = pitaya_server.pitaya_server.shutdown() {
        error!("failed to shutdown pitaya server: {}", e);
    }
}
