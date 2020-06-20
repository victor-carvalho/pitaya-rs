extern crate log;
extern crate pitaya;
extern crate pretty_env_logger;

use std::{thread, time};

fn main() {
    pretty_env_logger::init();

    let mut pitaya_server = pitaya::PitayaBuilder::new()
        .with_server_kind("random-kind")
        .with_rpc_client_config(pitaya::RpcClientConfig {
            request_timeout: time::Duration::from_secs(4),
            ..pitaya::RpcClientConfig::default()
        })
        .with_etcd_config(pitaya::EtcdConfig {
            prefix: String::from("pitaya"),
            ..pitaya::EtcdConfig::default()
        })
        .with_rpc_handler(move |mut rpc| {
            log::info!("!!!!!!!! received rpc req: {:?}", rpc.request());
            let res = pitaya::protos::Response {
                data: "HEY, THIS IS THE SERVER".as_bytes().to_owned(),
                error: None,
            };
            if !rpc.respond(res) {
                log::error!("failed to respond to the server");
            }
        })
        .build()
        .expect("failed to start pitaya server");

    log::info!("sending rpc");

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

    log::info!(
        "received response: {:?}",
        String::from_utf8_lossy(&res.data)
    );

    thread::sleep(time::Duration::from_secs(10));

    let _ = pitaya_server.shutdown().map_err(|e| {
        log::error!("failed to shutdown pitaya: {}", e);
    });
}
