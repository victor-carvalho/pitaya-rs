# Overview
pitaya-rs attemps to recreate [Pitaya](https://github.com/topfreegames/pitaya) in Rust, with the objective of leveraging a common C API in order to create Pitaya servers in multiple languages. By leveraging the stable architecture that is already used by the project, we can expand its reach into different languages other than Go.

# Building
You can build normally as any rust crate:
```
# For development
cargo build
# For release
cargo build --release
```

# Testing
This project uses cargo-make in order to create a cross platform Makefile. First install it:
```
cargo install --force cargo-make
```

After that, you need to setup dependencies and then you can run the standard test command:
```
makers deps
cargo test
makers undeps
```

# FAQ
## Go can be compiled into a dynamic library as well, why not use that?
Even Go can be compiled into a shared library and used with a C FFI interface as well, in practice we found many problems. The main issues are the conflict between the host language and the Go language runtime that is embedded into the final compiled binary. We can avoid that by using Rust, since it does not come with a runtime.