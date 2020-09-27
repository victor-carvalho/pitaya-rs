use pitaya::{Context, State};
use slog::{error, info, o, Drain};
use std::sync::{Arc, Mutex};
use tokio::{
    sync::watch,
    time::{Duration, Instant},
};

async fn send_rpc(pitaya_server: pitaya::Pitaya, msg: Vec<u8>, rx: watch::Receiver<bool>) {
    loop {
        if *rx.borrow() {
            // Received signal to quit.
            break;
        }

        let msg = msg.clone();

        match pitaya_server
            .send_rpc(Context::empty(), "SuperKind.hello.hello_method", msg)
            .await
        {
            Ok(res) => {
                if let Some(err) = res.error {
                    println!("RPC RETURNED ERROR: code={}, msg={}", err.code, err.msg);
                } else {
                    println!("RPC SUCCEEDED: {}", String::from_utf8_lossy(&res.data));
                }
            }
            Err(e) => {
                println!("RPC FAILED: {}", e);
            }
        }

        tokio::time::delay_until(Instant::now() + Duration::from_secs(1)).await;
    }
}

fn init_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain)
        .chan_size(1000)
        .build()
        .filter_level(slog::Level::Info)
        .fuse();
    slog::Logger::root(drain, o!())
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RpcMsg {
    #[prost(string, tag = "1")]
    pub route: std::string::String,
    #[prost(string, tag = "2")]
    pub msg: std::string::String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct MyResponse {
    #[prost(string, tag = "1")]
    pub my_message: String,
}

struct Counter {
    value: Arc<Mutex<i32>>,
}

#[pitaya::protobuf_handler("hello", server, with_args)]
async fn hello_method(
    rpc_msg: RpcMsg,
    counter: State<'_, Counter>,
) -> Result<MyResponse, pitaya::Never> {
    let mut count = counter.value.lock().unwrap();
    *count = *count + 1;
    Ok(MyResponse {
        my_message: format!(
            "my awesome response: route={} msg={} count={}",
            rpc_msg.route, rpc_msg.msg, count
        ),
    })
}

#[tokio::main]
async fn main() {
    let root_logger = init_logger();
    let logger = root_logger.clone();

    let (pitaya_server, shutdown_receiver) = pitaya::PitayaBuilder::new()
        .with_env_prefix("MY_ENV")
        .with_config_file("examples/config/production.yaml")
        .with_logger(root_logger)
        .with_server_handlers(pitaya::handlers![hello_method])
        .with_state(Counter {
            value: Arc::new(Mutex::new(20)),
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

    let (tx, rx) = watch::channel(false);

    let msg = RpcMsg {
        route: "client.route".into(),
        msg: "message from client :D".into(),
    };

    let msg_data = pitaya::utils::encode_proto(&msg);

    const NUM_CONCURRENT_TASKS: usize = 50;

    let mut tasks = Vec::with_capacity(NUM_CONCURRENT_TASKS);
    println!("spawning tasks...");
    for _ in 0..NUM_CONCURRENT_TASKS {
        let task = tokio::spawn({
            let pitaya_server = pitaya_server.clone();
            let msg_data = msg_data.clone();
            let rx = rx.clone();
            async move {
                send_rpc(pitaya_server.clone(), msg_data.clone(), rx).await;
            }
        });
        tasks.push(task);
    }

    println!("waiting");
    shutdown_receiver
        .await
        .expect("failed to wait for shutdown receiver");
    println!("done waiting");

    println!("broadcasting");
    tx.broadcast(true).unwrap();
    println!("done broadcasting");

    futures::future::join_all(tasks).await;

    std::thread::sleep(std::time::Duration::from_secs(1));

    if let Err(e) = pitaya_server.shutdown().await {
        error!(logger, "failed to shutdown pitaya: {}", e);
    }
}
