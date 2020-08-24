use pitaya_macros::{handlers, json_handler};
use serde::Serialize;

#[derive(Serialize)]
struct JsonRes {
    message: String,
}

#[allow(dead_code)]
#[json_handler("SuperHandler")]
async fn my_handler_method() -> Result<JsonRes, pitaya_core::Never> {
    println!("HELLO FRIEND");
    Ok(JsonRes {
        message: "random response message".into(),
    })
}

fn main() {
    let _handlers = handlers![my_handler_method];

    println!("HELLO MAN");
}
