use crate::{cluster, protos, EtcdConfig, Pitaya, PitayaBuilder, RpcClientConfig, RpcServerConfig};
use prost::Message;
use std::{ffi::CString, mem, os::raw::c_char, slice, time};
use tokio::sync::oneshot;

#[repr(C)]
pub struct PitayaError {
    code: *mut c_char,
    message: *mut c_char,
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
    etcd_prefix: *mut c_char,
    server_type_filters: *mut c_char,
    heartbeat_ttl_sec: i32,
    log_heartbeat: i32,
    log_server_sync: i32,
    log_server_details: i32,
    sync_servers_interval_sec: i32,
    max_number_of_retries: i32,
}

#[repr(C)]
pub struct CNATSConfig {
    addr: *mut c_char,
    connection_timeout_ms: i64,
    request_timeout_ms: i32,
    server_shutdown_deadline_ms: i32,
    server_max_number_of_rpcs: i32,
    max_reconnection_attempts: i32,
    max_pending_msgs: i32,
}

#[repr(C)]
pub struct PitayaRpcRequest {
    data: *const u8,
    len: i64,
}

#[repr(C)]
pub struct PitayaRpcResponse {
    data: *const u8,
    len: i64,
}

#[repr(C)]
pub struct PitayaRpc {
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
    sd_config: *mut CSDConfig,
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

#[no_mangle]
pub extern "C" fn pitaya_send_rpc(
    pitaya_server: *mut PitayaServer,
    route: *mut c_char,
    request: *const PitayaRpcRequest,
    response: *mut PitayaRpcResponse,
) -> *mut PitayaError {
    assert!(!request.is_null());
    assert!(!response.is_null());
    assert!(!pitaya_server.is_null());

    let mut pitaya_server = unsafe { Box::from_raw(pitaya_server) };
    let route = unsafe { std::ffi::CStr::from_ptr(route) };
    let request_data: &[u8] =
        unsafe { slice::from_raw_parts((*request).data, (*request).len as usize) };

    let route: &str = match route.to_str() {
        Ok(route) => route,
        Err(_) => {
            log::error!("route is invalid UTF8");
            let _ = Box::into_raw(pitaya_server);
            return Box::into_raw(Box::new(PitayaError {
                code: CString::into_raw(CString::new("PIT-400").unwrap()),
                message: CString::into_raw(CString::new("route is invalid utf8").unwrap()),
            }));
        }
    };

    let request: protos::Request = match Message::decode(request_data) {
        Ok(r) => r,
        Err(_) => {
            log::error!("could not decode request");
            let _ = Box::into_raw(pitaya_server);
            return Box::into_raw(Box::new(PitayaError {
                code: CString::into_raw(CString::new("PIT-400").unwrap()),
                message: CString::into_raw(CString::new("invalid request bytes").unwrap()),
            }));
        }
    };

    let res = match pitaya_server.pitaya_server.send_rpc(route, request) {
        Ok(r) => r,
        Err(e) => {
            log::error!("RPC failed");
            let _ = Box::into_raw(pitaya_server);
            return Box::into_raw(Box::new(PitayaError {
                code: CString::into_raw(CString::new("PIT-500").unwrap()),
                message: CString::into_raw(CString::new(format!("rpc error: {}", e)).unwrap()),
            }));
        }
    };

    // We don't drop response buffer because we'll pass it to the C code.
    let response_buffer: mem::ManuallyDrop<Vec<u8>> = {
        let mut b = Vec::with_capacity(res.encoded_len());
        match res.encode(&mut b).map(|_| b) {
            Ok(b) => mem::ManuallyDrop::new(b),
            Err(e) => {
                log::error!("received invalid response proto!");
                let _ = Box::into_raw(pitaya_server);
                return Box::into_raw(Box::new(PitayaError {
                    code: CString::into_raw(CString::new("PIT-500").unwrap()),
                    message: CString::into_raw(
                        CString::new(format!("invalid response proto: {}", e)).unwrap(),
                    ),
                }));
            }
        }
    };

    unsafe {
        (*response).data = response_buffer.as_ptr();
        (*response).len = response_buffer.len() as i64;
    }

    // We don't want to deallocate the pitaya server.
    let _ = Box::into_raw(pitaya_server);

    std::ptr::null_mut()
}
