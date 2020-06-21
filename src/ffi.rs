use crate::{cluster, protos, EtcdConfig, Pitaya, PitayaBuilder, RpcClientConfig, RpcServerConfig};
use lazy_static::lazy_static;
use std::{os::raw::c_char, sync::Mutex, time};
use tokio::sync::oneshot;

#[repr(C)]
pub struct CPitayaError {
    code: *mut c_char,
    msg: *mut c_char,
}

#[repr(C)]
pub struct CServer {
    id: *mut c_char,
    kind: *mut c_char,
    metadata: *mut c_char,
    hostname: *mut c_char,
    frontend: i32,
}

#[repr(C)]
pub enum PitayaLogLevel {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
}

#[repr(C)]
pub struct CSDConfig {
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
pub struct CNATSConfig {
    addr: *mut c_char,
    connectionTimeoutMs: i64,
    requestTimeoutMs: i32,
    serverShutdownDeadlineMs: i32,
    serverMaxNumberOfRpcs: i32,
    maxReconnectionAttempts: i32,
    maxPendingMsgs: i32,
}

#[repr(C)]
pub struct Rpc {
    request: *mut u8,
    responder: Box<oneshot::Sender<protos::Response>>,
}

pub struct PitayaServer {
    pitaya_server: Pitaya,
    shutdown_receiver: Option<oneshot::Receiver<()>>,
}

#[no_mangle]
pub extern "C" fn pitaya_initialize_with_nats(
    nc: *mut CNATSConfig,
    sdConfig: *mut CSDConfig,
    sv: *mut CServer,
    log_level: PitayaLogLevel,
) -> *mut PitayaServer {
    // FIXME(lhahn): this is really gambeta.
    match log_level {
        PitayaLogLevel::Trace => {
            std::env::set_var("RUST_LOG", "pitaya=trace");
        }
        PitayaLogLevel::Debug => {
            std::env::set_var("RUST_LOG", "pitaya=debug");
        }
        PitayaLogLevel::Info => {
            std::env::set_var("RUST_LOG", "pitaya=info");
        }
        PitayaLogLevel::Warn => {
            std::env::set_var("RUST_LOG", "pitaya=warn");
        }
        PitayaLogLevel::Error => {
            std::env::set_var("RUST_LOG", "pitaya=error");
        }
    }
    pretty_env_logger::init();

    log::info!("initializing global pitaya server");

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
        shutdown_receiver: Some(shutdown_receiver),
    }))
}

#[no_mangle]
pub extern "C" fn pitaya_wait_shutdown_signal(pitaya_server: *mut PitayaServer) {
    assert!(!pitaya_server.is_null());
    let mut pitaya_server = unsafe { Box::from_raw(pitaya_server) };

    let mut rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
    if let Some(shutdown_receiver) = pitaya_server.shutdown_receiver.take() {
        rt.block_on(async move {
            let _ = shutdown_receiver.await.map_err(|e| {
                log::error!("failed to wait for shutdown signal: {}", e);
            });
        });
    }

    // We don't want to deallocate the pitaya server.
    let _ = Box::into_raw(pitaya_server);
}

#[no_mangle]
pub extern "C" fn pitaya_shutdown(pitaya_server: *mut PitayaServer) {
    assert!(!pitaya_server.is_null());
    let pitaya_server = unsafe { Box::from_raw(pitaya_server) };

    if let Err(e) = pitaya_server.pitaya_server.shutdown() {
        log::error!("failed to shutdown pitaya server: {}", e);
    }
}
