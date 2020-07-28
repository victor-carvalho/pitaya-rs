use std::env;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    let config = cbindgen::Config {
        tab_width: 4,
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
