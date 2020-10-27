[![Actions Status](https://github.com/tfgco/pitaya-rs/workflows/Rust/badge.svg)](https://github.com/tfgco/pitaya-rs/actions)

# Overview
pitaya-rs attemps to recreate [Pitaya](https://github.com/topfreegames/pitaya) in Rust, with the objective of leveraging a common C API in order to create Pitaya servers in multiple languages. By leveraging the stable architecture that is already used by the project, we can expand its reach into different languages other than Go.

You can find the C# implementation [here](./pitaya-sharp/README.md).

# Requirements
The library needs to be build with the minimum Rust version of 1.46. Also, the library
uses ETCD and NATS for implementing service discovery and RPC communication between servers.

# Cloning
You have to clone with recursive flag to clone all submodules too.
```
git clone --recursive https://github.com/tfgco/pitaya-rs
```

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

# Examples
There are rust examples that can be run with the traditional cargo commands:
```
cargo run --example <example-name>
```

There are also one example in C that can be found in the `examples/c` directory. You can run that on MacOS using the following commands:
```
cd examples/c
./build.sh && ./example
```

# C# Examples

The C# example is found in the `pitaya-sharp/exampleapp` directory and can be run with the following commands:
```
cd pitaya-sharp
dotnet run -p exampleapp
```

To build `NPitaya` you need both a `libpitaya.dylib` and `libpitaya.so` in your target folder.
So to run on OSX you may need to create an empty `libpitaya.so`:
```
cd pitaya-rs
touch ./target/release/libpitaya.so
```

# Benchmarks
The default command for benchmarks is the following:
```
cargo bench --verbose
```

**NOTE**: Currently benchmarks are not really helpful, since they depend on external systems, that way we're not only testing the rust code, which is not that helpful. An improvement here will be by using stubs instead of the real implementations.

# FAQ
## Go can be compiled into a dynamic library as well, why not use that?
Even though Go can be compiled into a shared library and used with a C FFI interface as well, in practice we found many problems. The main issues are the conflict between the host language and the Go language runtime that is embedded into the final compiled binary. We can avoid that by using Rust, since it does not come with a runtime.