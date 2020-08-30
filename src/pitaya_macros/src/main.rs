#![feature(decl_macro)]

use pitaya_core::state::State;
use pitaya_macros::{handlers, json_handler};
use serde::Serialize;

#[derive(Serialize)]
struct JsonRes {
    message: String,
}

#[allow(dead_code)]
#[json_handler("SuperHandler")]
async fn my_handler_method(
    counter: State<'_, std::sync::Arc<i32>>,
) -> Result<JsonRes, pitaya_core::Never> {
    println!("HELLO FRIEND: {}", *counter);

    Ok(JsonRes {
        message: "random response message".into(),
    })
}

fn main() {
    let _handlers = handlers![my_handler_method];

    println!("HELLO MAN");
}
