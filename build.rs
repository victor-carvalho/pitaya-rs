fn main() {
    prost_build::compile_protos(
        &[
            "./pitaya-protos/request.proto",
            "./pitaya-protos/response.proto",
        ],
        &["./pitaya-protos"],
    )
    .unwrap();
}
