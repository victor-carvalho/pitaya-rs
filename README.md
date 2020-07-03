# Overview
pitaya-rs attemps to recreate a subset [Pitaya](https://github.com/topfreegames/pitaya) in Rust, with the objective of leveraging a common C API in order to create Pitaya servers in multiple languages.

# Building
You can build normally as any rust crate:
```
cargo build --release
```

# Testing
This project uses cargo-make in order to create a cross platform Makefile. First install it:
```
cargo install carg-make
```

After that, you need to setup dependencies and then you can run the standard test command:
```
cargo make deps
cargo test
cargo make undeps
```
