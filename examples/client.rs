extern crate pitaya;
extern crate slog;
extern crate slog_async;
extern crate slog_term;
extern crate tokio;

use slog::{error, info, o, Drain};

fn init_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build();
    let drain = slog::LevelFilter::new(drain, slog::Level::Trace).fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}

#[tokio::main]
async fn main() {
    let root_logger = init_logger();
    let logger = root_logger.clone();

    let (mut pitaya_server, shutdown_receiver) = pitaya::PitayaBuilder::new()
        .with_config_file("examples/config/production.yaml")
        .with_logger(root_logger)
        .with_rpc_handler(|_rpc| {})
        .build()
        .await
        .expect("failed to start pitaya server");

    info!(logger, "sending rpc");

    let res = pitaya_server
        .send_rpc(
            "csharp.room.join",
            pitaya::protos::Request {
                r#type: pitaya::protos::RpcType::User as i32,
                msg: Some(pitaya::protos::Msg {
                    r#type: pitaya::protos::MsgType::MsgRequest as i32,
                    data: b"sending some data".to_vec(),
                    route: "csharp.room.join".to_owned(),
                    ..pitaya::protos::Msg::default()
                }),
                frontend_id: "".to_owned(),
                metadata: b"{}".to_vec(),
                ..pitaya::protos::Request::default()
            },
        )
        .await
        .expect("rpc failed");

    info!(
        logger,
        "received response: {:?}",
        String::from_utf8_lossy(&res.data)
    );

    shutdown_receiver
        .await
        .expect("failed to wait for shutdown receiver");

    if let Err(e) = pitaya_server.shutdown().await {
        error!(logger, "failed to shutdown pitaya: {}", e);
    }
}
