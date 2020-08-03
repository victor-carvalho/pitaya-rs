use criterion::{criterion_group, criterion_main, Criterion};
use pitaya::{EtcdLazy, NatsRpcClient, NatsRpcServer};
use slog::{error, info, o, Drain};

async fn send_rpc(
    mut pitaya_server: pitaya::Pitaya<EtcdLazy, NatsRpcServer, NatsRpcClient>,
    msg: Vec<u8>,
) {
    if let Err(e) = pitaya_server
        .send_rpc(
            "room.room.join",
            pitaya::protos::Request {
                r#type: pitaya::protos::RpcType::User as i32,
                msg: Some(pitaya::protos::Msg {
                    r#type: pitaya::protos::MsgType::MsgRequest as i32,
                    data: msg,
                    route: "room.room.join".to_owned(),
                    ..pitaya::protos::Msg::default()
                }),
                frontend_id: "".to_owned(),
                metadata: "{}".as_bytes().to_owned(),
                ..pitaya::protos::Request::default()
            },
        )
        .await
    {
        println!("RPC FAILED: {}", e);
    }
}

fn init_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain)
        .chan_size(1000)
        .build()
        .filter_level(slog::Level::Warning)
        .fuse();
    slog::Logger::root(drain, o!())
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    let root_logger = init_logger();
    let logger = root_logger.clone();

    let (pitaya_server, _) = runtime.block_on(async move {
        pitaya::PitayaBuilder::new()
            .with_base_settings(pitaya::settings::Settings {
                server_kind: String::from("SuperKind"),
                ..Default::default()
            })
            .with_logger(root_logger)
            .with_rpc_handler({
                let logger = logger.clone();
                move |_ctx, rpc| {
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
            .expect("failed to start pitaya server")
    });

    c.bench_function("send_rpc", |b| {
        b.iter(|| {
            // let msg = RpcMsg::default();
            // let msg_data = pitaya::utils::encode_proto(&msg);
            runtime.block_on(send_rpc(pitaya_server.clone(), vec![]));
        })
    });

    runtime.block_on(pitaya_server.shutdown()).unwrap();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
