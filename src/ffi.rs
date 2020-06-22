use crate::{protos, EtcdConfig, PitayaBuilder, RpcClientConfig, RpcServerConfig};
use prost::Message;
use std::{
    collections::HashMap,
    convert::TryFrom,
    ffi::{c_void, CStr, CString},
    mem,
    os::raw::c_char,
    slice, time,
};
use tokio::sync::oneshot;

#[repr(C)]
pub struct PitayaError {
    code: *mut c_char,
    message: *mut c_char,
}

#[repr(C)]
pub struct PitayaServer {
    id: *mut c_char,
    kind: *mut c_char,
    metadata: *mut c_char,
    hostname: *mut c_char,
    frontend: i32,
}

impl std::convert::TryFrom<*mut PitayaServer> for crate::Server {
    type Error = *mut PitayaError;

    fn try_from(value: *mut PitayaServer) -> Result<Self, Self::Error> {
        unsafe {
            let id = CStr::from_ptr((*value).id).to_str();
            let kind = CStr::from_ptr((*value).kind).to_str();
            let metadata = CStr::from_ptr((*value).metadata).to_str();
            let hostname = CStr::from_ptr((*value).hostname).to_str();
            let frontend = (*value).frontend == 1;

            match (id, kind, metadata, hostname) {
                (Ok(id), Ok(kind), Ok(metadata), Ok(hostname)) => Ok(crate::Server {
                    id: crate::ServerId::from(id),
                    kind: crate::ServerKind::from(kind),
                    frontend: frontend,
                    // TODO(lhahn): parse metadata from json.
                    metadata: HashMap::new(),
                    hostname: hostname.to_owned(),
                }),
                _ => Err(Box::into_raw(Box::new(PitayaError {
                    code: CString::into_raw(CString::new("PIT-400").unwrap()),
                    message: CString::into_raw(CString::new("route is invalid utf8").unwrap()),
                }))),
            }
        }
    }
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
pub struct PitayaSDConfig {
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
pub struct PitayaNATSConfig {
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

pub struct PitayaRpc {
    request: Vec<u8>,
    responder: oneshot::Sender<protos::Response>,
}

pub type PitayaHandleRpcCallback = extern "C" fn(*mut c_void, *mut PitayaRpc);
pub struct PitayaHandleRpcData(*mut c_void);

pub struct Pitaya {
    pitaya_server: crate::Pitaya,
    shutdown_receiver: Option<oneshot::Receiver<()>>,
}

#[no_mangle]
pub extern "C" fn pitaya_error_drop(error: *mut PitayaError) {
    let _ = unsafe { Box::from_raw(error) };
}

#[no_mangle]
pub extern "C" fn pitaya_rpc_request(rpc: *mut PitayaRpc, len: *mut i64) -> *mut u8 {
    assert!(!rpc.is_null());
    assert!(!len.is_null());
    unsafe {
        *len = (*rpc).request.len() as i64;
        (*rpc).request.as_mut_ptr()
    }
}

#[no_mangle]
pub extern "C" fn pitaya_rpc_respond(
    rpc: *mut PitayaRpc,
    response_data: *const u8,
    response_len: i64,
) -> *mut PitayaError {
    assert!(!rpc.is_null());
    assert!(!response_data.is_null());

    let rpc = unsafe { Box::from_raw(rpc) };

    let response_slice = unsafe { slice::from_raw_parts(response_data, response_len as usize) };

    let response = match Message::decode(response_slice) {
        Ok(r) => r,
        Err(e) => {
            return Box::into_raw(Box::new(PitayaError {
                code: CString::into_raw(CString::new("PIT-400").unwrap()),
                message: CString::into_raw(
                    CString::new(format!("invalid response bytes: {}", e)).unwrap(),
                ),
            }));
        }
    };

    let sent = rpc.responder.send(response).map(|_| true).unwrap_or(false);

    if sent {
        std::ptr::null_mut()
    } else {
        Box::into_raw(Box::new(PitayaError {
            code: CString::into_raw(CString::new("PIT-500").unwrap()),
            message: CString::into_raw(CString::new("could not answer rpc").unwrap()),
        }))
    }
}

unsafe impl Send for PitayaHandleRpcData {}

#[no_mangle]
pub extern "C" fn pitaya_initialize_with_nats(
    nc: *mut PitayaNATSConfig,
    sd_config: *mut PitayaSDConfig,
    sv: *mut PitayaServer,
    log_level: PitayaLogLevel,
    handle_rpc_cb: PitayaHandleRpcCallback,
    handle_rpc_data: *mut c_void,
    pitaya: *mut *mut Pitaya,
) -> *mut PitayaError {
    assert!(!pitaya.is_null());
    assert!(!sv.is_null());
    assert!(!nc.is_null());
    assert!(!sd_config.is_null());

    // This wrapper type is necessary in order to send it to
    // another thread.
    let handle_rpc_data = PitayaHandleRpcData(handle_rpc_data);

    let server = match crate::Server::try_from(sv) {
        Ok(sv) => sv,
        Err(err) => {
            return err;
        }
    };

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
        .with_server_kind(&server.kind.0)
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
            let request_buffer: Vec<u8> = {
                let mut b = Vec::with_capacity(rpc.request().encoded_len());
                rpc.request()
                    .encode(&mut b)
                    .expect("failed to encode request");
                b
            };
            let rpc = Box::into_raw(Box::new(PitayaRpc {
                request: request_buffer,
                responder: rpc.responder(),
            }));
            handle_rpc_cb(handle_rpc_data.0, rpc);
        })
        .build()
        .expect("failed to start pitaya server");

    unsafe {
        (*pitaya) = Box::into_raw(Box::new(Pitaya {
            pitaya_server: pitaya_server,
            shutdown_receiver: Some(shutdown_receiver),
        }));
    }

    std::ptr::null_mut()
}

#[no_mangle]
pub extern "C" fn pitaya_wait_shutdown_signal(pitaya_server: *mut Pitaya) {
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
pub extern "C" fn pitaya_shutdown(pitaya_server: *mut Pitaya) {
    assert!(!pitaya_server.is_null());
    let pitaya_server = unsafe { Box::from_raw(pitaya_server) };

    if let Err(e) = pitaya_server.pitaya_server.shutdown() {
        log::error!("failed to shutdown pitaya server: {}", e);
    }
}

#[no_mangle]
pub extern "C" fn pitaya_send_rpc(
    pitaya_server: *mut Pitaya,
    route: *mut c_char,
    request: *const PitayaRpcRequest,
    response: *mut PitayaRpcResponse,
) -> *mut PitayaError {
    assert!(!request.is_null());
    assert!(!response.is_null());
    assert!(!pitaya_server.is_null());

    let mut pitaya_server = unsafe { Box::from_raw(pitaya_server) };
    let route = unsafe { CStr::from_ptr(route) };
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
