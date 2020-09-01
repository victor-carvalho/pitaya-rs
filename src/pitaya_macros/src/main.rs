#![feature(decl_macro)]

use pitaya_core::state::State;
use pitaya_macros::{handlers, json_handler};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct JsonInput {
    name: String,
}

#[derive(Serialize)]
struct JsonRes {
    message: String,
}

#[allow(dead_code)]
#[json_handler("SuperHandler", with_args)]
async fn my_handler_method(
    input: JsonInput,
    counter: State<'_, std::sync::Arc<i32>>,
) -> Result<JsonRes, pitaya_core::Never> {
    println!("HELLO FRIEND: {}", *counter);
    println!("MY INPUT NAME: {}", input.name);

    // let req = pitaya_core::protos::Request {
    //     msg: Some(pitaya_core::protos::Msg { data: vec![] }),
    // };

    Ok(JsonRes {
        message: format!("random response message: {}", input.name),
    })
}

fn main() {
    let _handlers = handlers![my_handler_method];

    println!("HELLO MAN");
}
