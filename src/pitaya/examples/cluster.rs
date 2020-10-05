use pitaya::Session;
use serde::Serialize;
use slog::{error, o, Drain};
use std::collections::HashMap;

#[derive(Serialize)]
struct JoinResponse {
    code: i32,
    result: String,
}

struct Error {
    msg: String,
}

impl pitaya::ToError for Error {
    fn to_error(self) -> pitaya::protos::Error {
        pitaya::protos::Error {
            code: "PIT-400".to_owned(),
            msg: self.msg,
            metadata: HashMap::new(),
        }
    }
}

#[derive(Serialize)]
struct PushMsg {
    msg: String,
}

#[pitaya::json_handler("room", client)]
async fn hi(session: Session) -> Result<JoinResponse, Error> {
    if !session.is_bound() {
        return Err(Error {
            msg: "session is not bound man!".to_owned(),
        });
    }

    let msg = PushMsg {
        msg: "HELLO, THIS IS A PUSH FROM THE SERVER".to_owned(),
    };

    // Spawn a new non-blocking task.
    tokio::spawn(async move {
        if let Err(e) = session.push_json_msg("my.super.route", msg).await {
            println!("failed to push msg: {}", e);
        }
    });

    Ok(JoinResponse {
        code: 200,
        result: String::new(),
    })
}

#[pitaya::json_handler("room", client)]
async fn entry(mut session: Session) -> Result<JoinResponse, pitaya::Never> {
    println!("received rpc from session: {}", session);

    session.set("MyData", "HELLO WORLD");

    if let Err(e) = session.bind("helroow").await {
        println!("failed to bind session: {}", e);
    }

    if let Err(e) = session.update_in_front().await {
        println!("failed to update session data on front: {}", e);
    }

    if session.is_bound() {
        if let Err(e) = session.kick().await {
            println!("failed to kick session: {}", e);
        }
    }

    Ok(JoinResponse {
        code: 200,
        result: "ok".to_owned(),
    })
}

#[pitaya::json_handler("room", client)]
async fn bind(mut session: Session) -> Result<JoinResponse, pitaya::Never> {
    if let Err(e) = session.bind("helroow").await {
        println!("failed to bind session: {}", e);
    }

    Ok(JoinResponse {
        code: 200,
        result: "SESSION BOUND!".to_owned(),
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
        .with_client_handlers(pitaya::handlers![entry, hi, bind])
        // .with_server_handlers(pitaya::handlers![room::entry])
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
