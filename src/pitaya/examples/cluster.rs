use serde::{Deserialize, Serialize};
use slog::{error, o, Drain};

// #[derive(Deserialize)]
// struct UserMessage {
//     name: String,
//     content: String,
// }

// #[derive(Deserialize)]
// struct NewUser {
//     content: String,
// }

// #[derive(Deserialize)]
// struct AllMembers {
//     members: Vec<String>,
// }

#[derive(Serialize)]
struct JoinResponse {
    code: i32,
    result: String,
}

#[derive(Deserialize)]
struct Req {
    oi: String,
}

#[pitaya::json_handler("room", with_args)]
async fn entry(req: Req) -> Result<JoinResponse, pitaya::Never> {
    Ok(JoinResponse {
        code: 200,
        result: format!("server: {}", req.oi),
    })
}

// func (r *Room) Entry(ctx context.Context, msg []byte) (*JoinResponse, error) {
//     logger := pitaya.GetDefaultLoggerFromCtx(ctx) // The default logger contains a requestId, the route being executed and the sessionId
//     s := pitaya.GetSessionFromCtx(ctx)
//
//     err := s.Bind(ctx, "helroow")
//     if err != nil {
//         logger.Error("Failed to bind session")
//         logger.Error(err)
//         return nil, pitaya.Error(err, "RH-000", map[string]string{"failed": "bind"})
//     }
//     return &JoinResponse{Result: "ok"}, nil
// }

fn init_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain)
        .chan_size(1000)
        .build()
        .filter_level(slog::Level::Debug)
        .fuse();
    slog::Logger::root(drain, o!())
}

#[tokio::main]
async fn main() {
    let logger = init_logger();

    let (pitaya_server, shutdown_receiver) = pitaya::PitayaBuilder::new()
        .with_base_settings(pitaya::settings::Settings {
            server_kind: "room".into(),
            ..Default::default()
        })
        .with_logger(logger.clone())
        .with_handlers(pitaya::handlers![room::entry])
        .build()
        .await
        .expect("failed to startup pitaya");

    shutdown_receiver
        .await
        .expect("failed to wait for shutdown receiver");

    if let Err(e) = pitaya_server.shutdown().await {
        error!(logger, "failed to shutdown pitaya: {}", e);
    }
}
