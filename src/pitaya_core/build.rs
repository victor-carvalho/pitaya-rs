fn main() {
    prost_build::compile_protos(
        &[
            "./pitaya-protos/request.proto",
            "./pitaya-protos/response.proto",
            "./pitaya-protos/kick.proto",
            "./pitaya-protos/push.proto",
        ],
        &["./pitaya-protos"],
    )
    .expect("failed to compile protos!");
}
