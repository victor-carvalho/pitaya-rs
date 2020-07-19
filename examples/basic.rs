extern crate pitaya;
extern crate slog;
extern crate slog_async;
extern crate slog_term;
extern crate tokio;

use slog::{error, info, o, Drain};

fn init_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build();
    let drain = slog::LevelFilter::new(drain, slog::Level::Trace).fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}

#[tokio::main]
async fn main() {
    let root_logger = init_logger();
    let logger = root_logger.clone();

    let (mut pitaya_server, shutdown_receiver) = pitaya::PitayaBuilder::new()
        .with_env_prefix("MY_ENV")
        .with_config_file("examples/config/production.yaml")
        .with_logger(root_logger)
        .with_rpc_handler({
            let logger = logger.clone();
            move |mut rpc| {
                info!(logger, "!!!!!!!! received rpc req: {:?}", rpc.request());
                let res = pitaya::protos::Response {
                    data: "HEY, THIS IS THE SERVER".as_bytes().to_owned(),
                    error: None,
                };
                if !rpc.respond(res) {
                    error!(logger, "failed to respond to the server");
                }
            }
        })
        .with_cluster_subscriber({
            let logger = logger.clone();
            move |notification| match notification {
                pitaya::cluster::Notification::ServerAdded(server) => {
                    info!(logger, "[subscriber] server added"; "server" => ?server);
                }
                pitaya::cluster::Notification::ServerRemoved(server_id) => {
                    info!(logger, "[subscriber] server removed"; "server_id" => ?server_id);
                }
            }
        })
        .build()
        .await
        .expect("failed to start pitaya server");

    info!(logger, "sending rpc");

    let res = pitaya_server
        .send_rpc(
            "room.room.join",
            pitaya::protos::Request {
                r#type: pitaya::protos::RpcType::User as i32,
                msg: Some(pitaya::protos::Msg {
                    r#type: pitaya::protos::MsgType::MsgRequest as i32,
                    data: "sending some data".as_bytes().to_owned(),
                    route: "csharp.room.join".to_owned(),
                    ..pitaya::protos::Msg::default()
                }),
                frontend_id: "".to_owned(),
                metadata: "{}".as_bytes().to_owned(),
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

    let _ = pitaya_server.shutdown().await.map_err(|e| {
        error!(logger, "failed to shutdown pitaya: {}", e);
    });
}
