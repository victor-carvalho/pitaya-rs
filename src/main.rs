extern crate pitaya;
extern crate slog;
extern crate slog_async;
extern crate slog_term;
extern crate tokio;

use slog::{error, info, o, Drain};
use std::time;

fn init_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}

fn main() {
    let root_logger = init_logger();
    let rpc_handler_logger = root_logger.clone();
    let logger = root_logger.clone();

    let (mut pitaya_server, shutdown_receiver) = pitaya::PitayaBuilder::new()
        .with_server_kind("random-kind")
        .with_logger(root_logger)
        .with_rpc_client_config(pitaya::RpcClientConfig {
            request_timeout: time::Duration::from_secs(4),
            ..pitaya::RpcClientConfig::default()
        })
        .with_etcd_config(pitaya::EtcdConfig {
            prefix: String::from("pitaya"),
            ..pitaya::EtcdConfig::default()
        })
        .with_rpc_handler(move |mut rpc| {
            info!(
                rpc_handler_logger,
                "!!!!!!!! received rpc req: {:?}",
                rpc.request()
            );
            let res = pitaya::protos::Response {
                data: "HEY, THIS IS THE SERVER".as_bytes().to_owned(),
                error: None,
            };
            if !rpc.respond(res) {
                error!(rpc_handler_logger, "failed to respond to the server");
            }
        })
        .build()
        .expect("failed to start pitaya server");

    info!(logger, "sending rpc");

    let res = pitaya_server
        .send_rpc(
            "random-kind.room.join",
            pitaya::protos::Request {
                r#type: pitaya::protos::RpcType::User as i32,
                msg: Some(pitaya::protos::Msg {
                    r#type: pitaya::protos::MsgType::MsgRequest as i32,
                    data: "sending some data".as_bytes().to_owned(),
                    route: "room.room.join".to_owned(),
                    ..pitaya::protos::Msg::default()
                }),
                frontend_id: "".to_owned(),
                metadata: "{}".as_bytes().to_owned(),
                ..pitaya::protos::Request::default()
            },
        )
        .expect("rpc failed");

    info!(
        logger,
        "received response: {:?}",
        String::from_utf8_lossy(&res.data)
    );

    let mut rt = tokio::runtime::Runtime::new().expect("failed to create runtime");
    rt.block_on(async move { shutdown_receiver.await })
        .expect("failed to await on shutdown receiver");

    let _ = pitaya_server.shutdown().map_err(|e| {
        error!(logger, "failed to shutdown pitaya: {}", e);
    });
}
