use crate::{
    cluster, protos, EtcdConfig, PitayaBuilder, RpcClientConfig, RpcServerConfig, ServerId,
    ServerKind,
};
use prost::Message;
use slog::{error, info, o, Drain};
use std::{
    borrow::Cow,
    collections::HashMap,
    convert::TryFrom,
    ffi::{c_void, CStr, CString},
    mem,
    os::raw::c_char,
    slice,
    sync::{Arc, Mutex},
    time,
};
use tokio::sync::oneshot;

pub struct PitayaError {
    code: String,
    message: String,
}

#[no_mangle]
pub extern "C" fn pitaya_error_code(err: *mut PitayaError) -> *const c_char {
    let err = unsafe { mem::ManuallyDrop::new(Box::from_raw(err)) };
    let code = err.code.as_ptr();
    code as *const c_char
}

#[no_mangle]
pub extern "C" fn pitaya_error_message(err: *mut PitayaError) -> *const c_char {
    let err = unsafe { mem::ManuallyDrop::new(Box::from_raw(err)) };
    let code = err.message.as_ptr();
    code as *const c_char
}

#[repr(C)]
pub struct PitayaServer {
    id: *mut c_char,
    kind: *mut c_char,
    metadata: *mut c_char,
    hostname: *mut c_char,
    frontend: i32,
}

impl PitayaServer {
    fn copy_from(&mut self, server: &crate::Server) {
        let metadata_str =
            serde_json::to_string(&server.metadata).expect("metadata should be a valid hashmap");

        self.id = CString::into_raw(
            CString::new(server.id.0.as_str()).expect("string should be valid utf8"),
        );
        self.kind = CString::into_raw(
            CString::new(server.kind.0.as_str()).expect("string should be valid utf8"),
        );
        self.metadata =
            CString::into_raw(CString::new(metadata_str).expect("string should be valid utf8"));
        self.hostname = CString::into_raw(
            CString::new(server.hostname.as_str()).expect("string should be valid utf8"),
        );
        self.frontend = if server.frontend { 1 } else { 0 };
    }
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
                (Ok(id), Ok(kind), Ok(_metadata), Ok(hostname)) => Ok(crate::Server {
                    id: crate::ServerId::from(id),
                    kind: crate::ServerKind::from(kind),
                    frontend: frontend,
                    // TODO(lhahn): parse metadata from json.
                    metadata: HashMap::new(),
                    hostname: hostname.to_owned(),
                }),
                _ => Err(Box::into_raw(Box::new(PitayaError {
                    code: "PIT-400".to_owned(),
                    message: "route is invalid utf8".to_owned(),
                }))),
            }
        }
    }
}

#[repr(C)]
#[allow(dead_code)]
pub enum PitayaLogKind {
    Console = 0,
    Json = 1,
}

#[repr(C)]
#[allow(dead_code)]
pub enum PitayaLogLevel {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
    Critical = 5,
}

impl std::convert::Into<slog::Level> for PitayaLogLevel {
    fn into(self) -> slog::Level {
        match self {
            PitayaLogLevel::Trace => slog::Level::Trace,
            PitayaLogLevel::Debug => slog::Level::Debug,
            PitayaLogLevel::Info => slog::Level::Info,
            PitayaLogLevel::Warn => slog::Level::Warning,
            PitayaLogLevel::Error => slog::Level::Error,
            PitayaLogLevel::Critical => slog::Level::Critical,
        }
    }
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

pub struct PitayaBuffer {
    data: Vec<u8>,
}

#[no_mangle]
pub extern "C" fn pitaya_buffer_new(data: *const u8, len: i32) -> *mut PitayaBuffer {
    let slice = unsafe { slice::from_raw_parts(data, len as usize) };
    Box::into_raw(Box::new(PitayaBuffer {
        data: slice.to_vec(),
    }))
}

pub struct PitayaRpc {
    request: Vec<u8>,
    responder: oneshot::Sender<protos::Response>,
}

#[repr(C)]
#[allow(dead_code)]
pub enum PitayaClusterNotification {
    ServerAdded = 0,
    ServerRemoved = 1,
}

pub type PitayaClusterNotificationCallback =
    extern "C" fn(*mut c_void, PitayaClusterNotification, *mut c_void);

pub type PitayaHandleRpcCallback = extern "C" fn(*mut c_void, *mut PitayaRpc);

struct PitayaUserData(*mut c_void);

pub struct Pitaya {
    pitaya_server: crate::Pitaya,
    shutdown_receiver: Option<oneshot::Receiver<()>>,
}

#[no_mangle]
pub extern "C" fn pitaya_server_by_id(
    pitaya_server: *mut Pitaya,
    server_id: *mut c_char,
    server_kind: *mut c_char,
    server: *mut PitayaServer,
) -> bool {
    assert!(!server.is_null());
    assert!(!pitaya_server.is_null());
    let mut pitaya_server = unsafe { Box::from_raw(pitaya_server) };
    let logger = pitaya_server.pitaya_server.logger.clone();

    let server_id = unsafe { CStr::from_ptr(server_id) };
    let server_kind = unsafe { CStr::from_ptr(server_kind) };

    let (server_id, server_kind) = match (server_id.to_str(), server_kind.to_str()) {
        (Ok(id), Ok(kind)) => (id, kind),
        _ => {
            error!(logger, "invalid utf8 parameters");
            let _ = Box::into_raw(pitaya_server);
            return false;
        }
    };

    let ok = match pitaya_server
        .pitaya_server
        .server_by_id(&ServerId::from(server_id), &ServerKind::from(server_kind))
    {
        Ok(Some(sv)) => {
            unsafe {
                (&mut *server).copy_from(&sv);
            }
            true
        }
        Ok(None) => false,
        Err(e) => {
            error!(logger, "failed to get server by id"; "error" => %e);
            false
        }
    };

    let _ = Box::into_raw(pitaya_server);
    ok
}

#[no_mangle]
pub extern "C" fn pitaya_server_drop(pitaya_server: *mut PitayaServer) {
    let _ = unsafe { Box::from_raw(pitaya_server) };
}

#[no_mangle]
pub extern "C" fn pitaya_error_drop(error: *mut PitayaError) {
    let _ = unsafe { Box::from_raw(error) };
}

#[no_mangle]
pub extern "C" fn pitaya_rpc_request(rpc: *mut PitayaRpc, len: *mut i32) -> *mut u8 {
    assert!(!rpc.is_null());
    assert!(!len.is_null());
    unsafe {
        *len = (*rpc).request.len() as i32;
        (*rpc).request.as_mut_ptr()
    }
}

#[no_mangle]
pub extern "C" fn pitaya_rpc_respond(
    rpc: *mut PitayaRpc,
    response_data: *const u8,
    response_len: i32,
) -> *mut PitayaError {
    assert!(!rpc.is_null());
    assert!(!response_data.is_null());

    let rpc = unsafe { Box::from_raw(rpc) };

    let response_slice = unsafe { slice::from_raw_parts(response_data, response_len as usize) };

    let response = match Message::decode(response_slice) {
        Ok(r) => r,
        Err(e) => {
            return Box::into_raw(Box::new(PitayaError {
                code: "PIT-400".to_owned(),
                message: format!("invalid response bytes: {}", e),
            }));
        }
    };

    let sent = rpc.responder.send(response).map(|_| true).unwrap_or(false);

    if sent {
        std::ptr::null_mut()
    } else {
        Box::into_raw(Box::new(PitayaError {
            code: "PIT-500".to_owned(),
            message: "could not answer rpc".to_owned(),
        }))
    }
}

#[no_mangle]
pub extern "C" fn pitaya_rpc_drop(rpc: *mut PitayaRpc) {
    let _ = unsafe { Box::from_raw(rpc) };
}

// We are telling rust here that we know it is safe to send the
// PitayaUserData to another thread and use it in multiple threads.
// In reality, this is a void* provided by the user, so it could definitely
// crash the program depending on how the value is used outside of the rust code.
unsafe impl Send for PitayaUserData {}
unsafe impl Sync for PitayaUserData {}

#[no_mangle]
pub extern "C" fn pitaya_initialize_with_nats(
    nc: *mut PitayaNATSConfig,
    sd_config: *mut PitayaSDConfig,
    sv: *mut PitayaServer,
    handle_rpc_cb: PitayaHandleRpcCallback,
    handle_rpc_data: *mut c_void,
    log_level: PitayaLogLevel,
    log_kind: PitayaLogKind,
    cluster_notification_callback: PitayaClusterNotificationCallback,
    cluster_notification_data: *mut c_void,
    pitaya: *mut *mut Pitaya,
) -> *mut PitayaError {
    assert!(!pitaya.is_null());
    assert!(!sv.is_null());
    assert!(!nc.is_null());
    assert!(!sd_config.is_null());

    // This wrapper type is necessary in order to send it to
    // another thread.
    let handle_rpc_data = PitayaUserData(handle_rpc_data);
    let cluster_notification_data = PitayaUserData(cluster_notification_data);

    let server = match crate::Server::try_from(sv) {
        Ok(sv) => sv,
        Err(err) => {
            return err;
        }
    };

    let root_logger = match log_kind {
        PitayaLogKind::Console => {
            let decorator = slog_term::TermDecorator::new().build();
            let drain = slog_term::CompactFormat::new(decorator).build();
            let drain = slog::LevelFilter::new(drain, log_level.into()).fuse();
            let drain = slog_async::Async::new(drain).build().fuse();
            slog::Logger::root(drain, o!())
        }
        PitayaLogKind::Json => slog::Logger::root(
            Mutex::new(slog_json::Json::default(std::io::stderr())).map(slog::Fuse),
            o!("version" => env!("CARGO_PKG_VERSION")),
        ),
    };

    let logger = root_logger.clone();
    let rpc_handler_logger = root_logger.new(o!());

    info!(root_logger, "initializing global pitaya server");

    let res = PitayaBuilder::new()
        .with_server_kind(&server.kind.0)
        .with_logger(root_logger)
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
            info!(
                rpc_handler_logger,
                "!!!!!!!! received rpc req: {:?}",
                rpc.request()
            );
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
        .with_cluster_subscriber({
            let logger = logger.clone();
            move |notification| match notification {
                cluster::Notification::ServerAdded(server) => {
                    let raw_server = Arc::into_raw(server);
                    cluster_notification_callback(
                        cluster_notification_data.0,
                        PitayaClusterNotification::ServerAdded,
                        raw_server as *mut c_void,
                    );
                    let _ = unsafe { Arc::from_raw(raw_server) };
                }
                cluster::Notification::ServerRemoved(server_id) => {
                    let _ = CString::new(server_id.0.as_str())
                        .map(|server_id| {
                            cluster_notification_callback(
                                cluster_notification_data.0,
                                PitayaClusterNotification::ServerRemoved,
                                server_id.as_ptr() as *mut c_void,
                            );
                        })
                        .map_err(|_| {
                            error!(logger, "failed to convert server id to raw c string");
                        });
                }
            }
        })
        .build();

    match res {
        Ok((pitaya_server, shutdown_receiver)) => unsafe {
            (*pitaya) = Box::into_raw(Box::new(Pitaya {
                pitaya_server,
                shutdown_receiver: Some(shutdown_receiver),
            }));
            std::ptr::null_mut()
        },
        Err(e) => {
            error!(logger, "failed to create pitaya server"; "error" => %e);
            Box::into_raw(Box::new(PitayaError {
                code: "PIT-500".to_owned(),
                message: format!("failed to create pitaya server: {}", e),
            }))
        }
    }
}

#[no_mangle]
pub extern "C" fn pitaya_wait_shutdown_signal(pitaya_server: *mut Pitaya) {
    assert!(!pitaya_server.is_null());
    let mut pitaya_server = unsafe { Box::from_raw(pitaya_server) };
    let logger = pitaya_server.pitaya_server.logger.clone();

    let mut rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
    if let Some(shutdown_receiver) = pitaya_server.shutdown_receiver.take() {
        rt.block_on(async move {
            let _ = shutdown_receiver.await.map_err(|e| {
                error!(logger, "failed to wait for shutdown signal: {}", e);
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
    let logger = pitaya_server.pitaya_server.logger.clone();

    if let Err(e) = pitaya_server.pitaya_server.shutdown() {
        error!(logger, "failed to shutdown pitaya server: {}", e);
    }
}

#[no_mangle]
pub extern "C" fn pitaya_buffer_data(buf: *mut PitayaBuffer, len: *mut i32) -> *const u8 {
    assert!(!buf.is_null());
    let pb = unsafe { Box::from_raw(buf) };

    let data = pb.data.as_ptr();
    unsafe {
        *len = pb.data.len() as i32;
    }

    let _ = Box::into_raw(pb);
    data
}

#[no_mangle]
pub extern "C" fn pitaya_buffer_drop(buf: *mut PitayaBuffer) {
    let _ = unsafe { Box::from_raw(buf) };
}

#[no_mangle]
pub extern "C" fn pitaya_send_rpc(
    pitaya_server: *mut Pitaya,
    server_id: *mut c_char,
    route: *mut c_char,
    request_buffer: *mut PitayaBuffer,
    response_buffer: *mut *mut PitayaBuffer,
) -> *mut PitayaError {
    assert!(!request_buffer.is_null());
    assert!(!response_buffer.is_null());
    assert!(!pitaya_server.is_null());

    let server_id = if server_id.is_null() {
        Cow::default()
    } else {
        unsafe { CStr::from_ptr(server_id).to_string_lossy() }
    };

    let mut pitaya_server = unsafe { mem::ManuallyDrop::new(Box::from_raw(pitaya_server)) };
    let logger = pitaya_server.pitaya_server.logger.clone();

    let route = unsafe { CStr::from_ptr(route).to_string_lossy() };
    let request_buffer = unsafe { mem::ManuallyDrop::new(Box::from_raw(request_buffer)) };

    let request = protos::Request {
        r#type: protos::RpcType::User as i32,
        msg: Some(protos::Msg {
            r#type: protos::MsgType::MsgRequest as i32,
            data: request_buffer.data.clone(),
            route: route.as_ref().to_owned(),
            ..protos::Msg::default()
        }),
        ..protos::Request::default()
    };

    let result = if server_id.len() > 0 {
        crate::Route::try_from(route.as_ref()).and_then(|route: crate::Route| {
            pitaya_server.pitaya_server.send_rpc_to_server(
                &ServerId::from(server_id.as_ref()),
                &ServerKind::from(route.server_kind),
                request,
            )
        })
    } else {
        pitaya_server
            .pitaya_server
            .send_rpc(route.as_ref(), request)
    };

    let res = match result {
        Ok(r) => r,
        Err(e) => {
            error!(logger, "RPC failed");
            return Box::into_raw(Box::new(PitayaError {
                code: "PIT-500".to_owned(),
                message: format!("rpc error: {}", e),
            }));
        }
    };

    // We don't drop response buffer because we'll pass it to the C code.
    let response_data: Vec<u8> = {
        let mut b = Vec::with_capacity(res.encoded_len());
        match res.encode(&mut b).map(|_| b) {
            Ok(b) => b,
            Err(e) => {
                error!(logger, "received invalid response proto!");
                return Box::into_raw(Box::new(PitayaError {
                    code: "PIT-500".to_owned(),
                    message: format!("invalid response proto: {}", e),
                }));
            }
        }
    };

    unsafe {
        (*response_buffer) = Box::into_raw(Box::new(PitayaBuffer {
            data: response_data,
        }));
    }

    std::ptr::null_mut()
}
