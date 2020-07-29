use std::env;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    let config = cbindgen::Config {
        tab_width: 4,
        // FIXME(lhahn): this is a workaround for cbindgen, since it was not generating
        // the forward declarations for pitaya.
        after_includes: Some(String::from("\ntypedef struct Pitaya Pitaya;")),
        language: cbindgen::Language::C,
        enumeration: cbindgen::EnumConfig {
            prefix_with_name: true,
            ..Default::default()
        },
        ..Default::default()
    };

    match cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_config(config)
        .generate()
    {
        Ok(bindings) => {
            bindings.write_to_file("pitaya.h");
        }
        Err(err) => {
            println!("failed to generate bindings: {}", err);
        }
    }
}
