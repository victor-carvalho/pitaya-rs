use crate::{
    cluster, protos, utils, EtcdConfig, PitayaBuilder, RpcClientConfig, RpcServerConfig, ServerId,
    ServerKind,
};
use prost::Message;
use slog::{error, info, o, Drain};
use std::{
    borrow::Cow,
    convert::TryFrom,
    ffi::{c_void, CStr, CString},
    mem,
    os::raw::c_char,
    ptr::null_mut,
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

#[no_mangle]
pub extern "C" fn pitaya_error_drop(error: *mut PitayaError) {
    let _ = unsafe { Box::from_raw(error) };
}

//
// PitayaServer struct
//
pub struct PitayaServer {
    id: CString,
    kind: CString,
    metadata: CString,
    hostname: CString,
    frontend: i32,
}

impl PitayaServer {
    fn new(server: Arc<crate::Server>) -> Self {
        let metadata = serde_json::to_string(&server.metadata)
            .expect("should not fail to convert hashmap to json string");
        Self {
            id: CString::new(server.id.0.as_str())
                .expect("should not fail to convert rust string to c string"),
            kind: CString::new(server.kind.0.as_str())
                .expect("should not fail to convert rust string to c string"),
            metadata: CString::new(metadata)
                .expect("should not fail to convert rust string to c string"),
            hostname: CString::new(server.hostname.as_str())
                .expect("should not fail to convert rust string to c string"),
            frontend: server.frontend as i32,
        }
    }
}

#[no_mangle]
pub extern "C" fn pitaya_server_new(
    id: *mut c_char,
    kind: *mut c_char,
    metadata: *mut c_char,
    hostname: *mut c_char,
    frontend: i32,
) -> *mut PitayaServer {
    let id = unsafe { CStr::from_ptr(id) };
    let kind = unsafe { CStr::from_ptr(kind) };
    let metadata_str = unsafe { CStr::from_ptr(metadata) };
    let hostname = unsafe { CStr::from_ptr(hostname) };
    Box::into_raw(Box::new(PitayaServer {
        id: id.to_owned(),
        kind: kind.to_owned(),
        metadata: metadata_str.to_owned(),
        hostname: hostname.to_owned(),
        frontend,
    }))
}

#[no_mangle]
pub extern "C" fn pitaya_server_id(server: *mut PitayaServer) -> *const c_char {
    let server = unsafe { mem::ManuallyDrop::new(Box::from_raw(server)) };
    let id = server.id.as_ptr();
    id as *const c_char
}

#[no_mangle]
pub extern "C" fn pitaya_server_kind(server: *mut PitayaServer) -> *const c_char {
    let server = unsafe { mem::ManuallyDrop::new(Box::from_raw(server)) };
    let kind = server.kind.as_ptr();
    kind as *const c_char
}

#[no_mangle]
pub extern "C" fn pitaya_server_metadata(server: *mut PitayaServer) -> *const c_char {
    let server = unsafe { mem::ManuallyDrop::new(Box::from_raw(server)) };
    let metadata = server.metadata.as_ptr();
    metadata as *const c_char
}

#[no_mangle]
pub extern "C" fn pitaya_server_hostname(server: *mut PitayaServer) -> *const c_char {
    let server = unsafe { mem::ManuallyDrop::new(Box::from_raw(server)) };
    let hostname = server.hostname.as_ptr();
    hostname as *const c_char
}

#[no_mangle]
pub extern "C" fn pitaya_server_frontend(server: *mut PitayaServer) -> i32 {
    let server = unsafe { mem::ManuallyDrop::new(Box::from_raw(server)) };
    server.frontend
}

#[no_mangle]
pub extern "C" fn pitaya_server_drop(pitaya_server: *mut PitayaServer) {
    let _ = unsafe { Box::from_raw(pitaya_server) };
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

//
// PitayaBuffer struct
//
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
    extern "C" fn(*mut c_void, PitayaClusterNotification, *mut PitayaServer);

pub type PitayaHandleRpcCallback = extern "C" fn(*mut c_void, *mut PitayaRpc);

struct PitayaUserData(*mut c_void);

pub struct Pitaya {
    pitaya_server: crate::Pitaya,
    shutdown_receiver: Option<oneshot::Receiver<()>>,
    runtime: tokio::runtime::Runtime,
}

#[no_mangle]
pub extern "C" fn pitaya_server_by_id(
    p: *mut Pitaya,
    server_id: *mut c_char,
    server_kind: *mut c_char,
    user_data: *mut c_void,
    callback: extern "C" fn(*mut c_void, *mut PitayaServer),
) {
    let p = unsafe { mem::ManuallyDrop::new(Box::from_raw(p)) };
    let logger = p.pitaya_server.logger.clone();

    let server_id = unsafe { CStr::from_ptr(server_id) };
    let server_kind = unsafe { CStr::from_ptr(server_kind) };

    let user_data = PitayaUserData(user_data);

    let (server_id, server_kind) = match (server_id.to_str(), server_kind.to_str()) {
        (Ok(id), Ok(kind)) => (id, kind),
        _ => {
            error!(logger, "invalid utf8 parameters");
            p.runtime.spawn(async move {
                callback(user_data.0, null_mut());
            });
            return;
        }
    };

    let mut pitaya_server = p.pitaya_server.clone();
    p.runtime.spawn(async move {
        match pitaya_server
            .server_by_id(&ServerId::from(server_id), &ServerKind::from(server_kind))
            .await
        {
            Ok(Some(sv)) => {
                callback(user_data.0, Box::into_raw(Box::new(PitayaServer::new(sv))));
            }
            Ok(None) => {
                callback(user_data.0, null_mut());
            }
            Err(e) => {
                error!(logger, "failed to get server by id"; "error" => %e);
                callback(user_data.0, null_mut());
            }
        }
    });
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
        null_mut()
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

    let server = unsafe { mem::ManuallyDrop::new(Box::from_raw(sv)) };

    let root_logger = match log_kind {
        PitayaLogKind::Console => {
            let decorator = slog_term::TermDecorator::new().build();
            let drain = slog_term::CompactFormat::new(decorator).build();
            let drain = slog::LevelFilter::new(drain, log_level.into()).fuse();
            let drain = slog_async::Async::new(drain).build().fuse();
            slog::Logger::root(drain, o!("version" => env!("CARGO_PKG_VERSION")))
        }
        PitayaLogKind::Json => slog::Logger::root(
            Mutex::new(slog_json::Json::default(std::io::stderr())).map(slog::Fuse),
            o!("version" => env!("CARGO_PKG_VERSION")),
        ),
    };

    let logger = root_logger.clone();
    let rpc_handler_logger = root_logger.new(o!());

    info!(root_logger, "initializing global pitaya server");

    let mut runtime = tokio::runtime::Runtime::new().expect("should not fail to create a runtime");

    let res = runtime.block_on(async move {
        PitayaBuilder::new()
            .with_server_kind(&server.kind.to_string_lossy())
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
                let request_buffer = utils::encode_proto(rpc.request());
                let rpc = Box::into_raw(Box::new(PitayaRpc {
                    request: request_buffer,
                    responder: rpc.responder(),
                }));
                handle_rpc_cb(handle_rpc_data.0, rpc);
            })
            .with_cluster_subscriber({
                move |notification| match notification {
                    cluster::Notification::ServerAdded(server) => {
                        let raw_server = Box::into_raw(Box::new(PitayaServer::new(server)));
                        cluster_notification_callback(
                            cluster_notification_data.0,
                            PitayaClusterNotification::ServerAdded,
                            raw_server,
                        );
                    }
                    cluster::Notification::ServerRemoved(server) => {
                        let raw_server = Box::into_raw(Box::new(PitayaServer::new(server)));
                        cluster_notification_callback(
                            cluster_notification_data.0,
                            PitayaClusterNotification::ServerRemoved,
                            raw_server,
                        );
                    }
                }
            })
            .build()
            .await
    });

    match res {
        Ok((pitaya_server, shutdown_receiver)) => unsafe {
            (*pitaya) = Box::into_raw(Box::new(Pitaya {
                pitaya_server,
                shutdown_receiver: Some(shutdown_receiver),
                runtime,
            }));
            null_mut()
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
    let mut pitaya_server = unsafe { mem::ManuallyDrop::new(Box::from_raw(pitaya_server)) };
    let logger = pitaya_server.pitaya_server.logger.clone();

    let mut rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
    if let Some(shutdown_receiver) = pitaya_server.shutdown_receiver.take() {
        rt.block_on(async move {
            let _ = shutdown_receiver.await.map_err(|e| {
                error!(logger, "failed to wait for shutdown signal: {}", e);
            });
        });
    }
}

#[no_mangle]
pub extern "C" fn pitaya_shutdown(p: *mut Pitaya) {
    assert!(!p.is_null());
    let mut p = unsafe { Box::from_raw(p) };
    let logger = p.pitaya_server.logger.clone();

    let pitaya_server = p.pitaya_server.clone();
    p.runtime.block_on(async move {
        if let Err(e) = pitaya_server.shutdown().await {
            error!(logger, "failed to shutdown pitaya server: {}", e);
        }
    });
}

#[no_mangle]
pub extern "C" fn pitaya_send_rpc(
    p: *mut Pitaya,
    server_id: *mut c_char,
    route_str: *mut c_char,
    request_buffer: *mut PitayaBuffer,
    user_data: *mut c_void,
    callback: extern "C" fn(*mut c_void, *mut PitayaError, *mut PitayaBuffer),
) {
    assert!(!request_buffer.is_null());
    assert!(!p.is_null());

    let user_data = PitayaUserData(user_data);
    let server_id = if server_id.is_null() {
        Cow::default()
    } else {
        unsafe { CStr::from_ptr(server_id).to_string_lossy() }
    };

    let p = unsafe { mem::ManuallyDrop::new(Box::from_raw(p)) };
    let logger = p.pitaya_server.logger();

    let route_str = unsafe { CStr::from_ptr(route_str).to_string_lossy() };
    let request_buffer = unsafe { mem::ManuallyDrop::new(Box::from_raw(request_buffer)) };

    let request = protos::Request {
        r#type: protos::RpcType::User as i32,
        msg: Some(protos::Msg {
            r#type: protos::MsgType::MsgRequest as i32,
            data: request_buffer.data.clone(),
            route: route_str.as_ref().to_owned(),
            ..protos::Msg::default()
        }),
        // TODO(lhahn): send metadata here or add something relevant (Jaeger?).
        metadata: "{}".as_bytes().to_owned(),
        ..protos::Request::default()
    };

    let route_str = route_str.to_string();
    let mut pitaya_server = p.pitaya_server.clone();
    p.runtime.spawn(async move {
        let route = match crate::Route::try_from(route_str.as_ref()) {
            Ok(r) => r,
            Err(e) => {
                error!(logger, "failed to convert route"; "error" => %e);
                callback(user_data.0, null_mut(), null_mut());
                return;
            }
        };

        let result = if server_id.len() > 0 {
            pitaya_server
                .send_rpc_to_server(
                    &ServerId::from(server_id.as_ref()),
                    &ServerKind::from(route.server_kind),
                    request,
                )
                .await
        } else {
            pitaya_server.send_rpc(route_str.as_ref(), request).await
        };

        let res = match result {
            Ok(r) => r,
            Err(e) => {
                error!(logger, "RPC failed");
                callback(
                    user_data.0,
                    Box::into_raw(Box::new(PitayaError {
                        code: "PIT-500".to_owned(),
                        message: format!("rpc error: {}", e),
                    })),
                    null_mut(),
                );
                return;
            }
        };

        // We don't drop response buffer because we'll pass it to the C code.
        let response_data = utils::encode_proto(&res);

        callback(
            user_data.0,
            null_mut(),
            Box::into_raw(Box::new(PitayaBuffer {
                data: response_data,
            })),
        );
    });
}

#[no_mangle]
pub extern "C" fn pitaya_send_kick(
    pitaya_server: *mut Pitaya,
    server_id: *mut c_char,
    server_kind: *mut c_char,
    kick_buffer: *mut PitayaBuffer,
    kick_answer: *mut *mut PitayaBuffer,
) -> *mut PitayaError {
    assert!(!pitaya_server.is_null());
    assert!(!server_id.is_null());
    assert!(!server_kind.is_null());
    assert!(!kick_buffer.is_null());
    assert!(!kick_answer.is_null());

    let mut pitaya_server = unsafe { mem::ManuallyDrop::new(Box::from_raw(pitaya_server)) };
    let kick_buffer = unsafe { mem::ManuallyDrop::new(Box::from_raw(kick_buffer)) };
    let server_id = ServerId::from(unsafe { CStr::from_ptr(server_id).to_string_lossy() });
    let server_kind = ServerKind::from(unsafe { CStr::from_ptr(server_kind).to_string_lossy() });

    let kick_msg: protos::KickMsg = match Message::decode(kick_buffer.data.as_ref()) {
        Ok(m) => m,
        Err(e) => {
            return Box::into_raw(Box::new(PitayaError {
                code: "PIT-400".to_owned(),
                message: format!("invalid kick buffer: {}", e),
            }));
        }
    };

    match pitaya_server
        .pitaya_server
        .send_kick(&server_id, &server_kind, kick_msg)
    {
        Ok(answer) => {
            let buffer = utils::encode_proto(&answer);
            unsafe {
                *kick_answer = Box::into_raw(Box::new(PitayaBuffer { data: buffer }));
            }
            null_mut()
        }
        Err(e) => Box::into_raw(Box::new(PitayaError {
            code: "PIT-500".to_owned(),
            message: format!("failed to send kick: {}", e),
        })),
    }
}

#[no_mangle]
pub extern "C" fn pitaya_send_push_to_user(
    pitaya_server: *mut Pitaya,
    server_id: *mut c_char,
    server_kind: *mut c_char,
    push_buffer: *mut PitayaBuffer,
) -> *mut PitayaError {
    assert!(!pitaya_server.is_null());
    assert!(!server_id.is_null());
    assert!(!server_kind.is_null());
    assert!(!push_buffer.is_null());

    let mut pitaya_server = unsafe { mem::ManuallyDrop::new(Box::from_raw(pitaya_server)) };
    let push_buffer = unsafe { mem::ManuallyDrop::new(Box::from_raw(push_buffer)) };
    let server_id = ServerId::from(unsafe { CStr::from_ptr(server_id).to_string_lossy() });
    let server_kind = ServerKind::from(unsafe { CStr::from_ptr(server_kind).to_string_lossy() });

    let push_msg: protos::Push = match Message::decode(push_buffer.data.as_ref()) {
        Ok(m) => m,
        Err(e) => {
            return Box::into_raw(Box::new(PitayaError {
                code: "PIT-400".to_owned(),
                message: format!("invalid push buffer: {}", e),
            }));
        }
    };

    match pitaya_server
        .pitaya_server
        .send_push_to_user(&server_id, &server_kind, push_msg)
    {
        Ok(_) => null_mut(),
        Err(e) => Box::into_raw(Box::new(PitayaError {
            code: "PIT-500".to_owned(),
            message: format!("failed to send push: {}", e),
        })),
    }
}
