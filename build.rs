extern crate cbindgen;

use std::env;

fn main() {
    prost_build::compile_protos(
        &[
            "./pitaya-protos/request.proto",
            "./pitaya-protos/response.proto",
        ],
        &["./pitaya-protos"],
    )
    .expect("failed to compile protos!");

    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    let config = cbindgen::Config {
        language: cbindgen::Language::C,
        enumeration: cbindgen::EnumConfig {
            prefix_with_name: true,
            ..Default::default()
        },
        ..Default::default()
    };

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_config(config)
        .generate()
        .expect("Unable to pitaya C bindings")
        .write_to_file("pitaya.h");
}
