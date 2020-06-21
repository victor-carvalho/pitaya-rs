#include <stdio.h>
#include <pitaya.h>

int main()
{
    CNATSConfig nats_config = {0};
    nats_config.addr = "http://localhost:4222";
    nats_config.connectionTimeoutMs = 5000;
    nats_config.requestTimeoutMs = 5000;
    nats_config.serverShutdownDeadlineMs = 5000;
    nats_config.serverMaxNumberOfRpcs = 100;
    nats_config.maxReconnectionAttempts = 20;
    nats_config.maxPendingMsgs = 50;

    CSDConfig sd_config = {0};
    sd_config.endpoints = "localhost:2379";
    sd_config.etcdPrefix = "pitaya";

    CServer server = {0};
    server.id = "my-server-id-from-c";
    server.kind = "my-server-kind-from-c";
    server.metadata = "random-metadata";
    server.hostname = "";
    server.frontend = 0;

    PitayaServer *pitaya = pitaya_initialize_with_nats(
        &nats_config,
        &sd_config,
        &server,
        PitayaLogLevel_Trace
    );

    printf("Will send RPC...\n");

    uint8_t request_data[30];

    PitayaRpcRequest request = {0};
    request.data = request_data;
    request.len = 30;

    PitayaRpcResponse response = {0};

    PitayaError *error = pitaya_send_rpc(pitaya, "room.room.join", &request, &response);
    if (error) {
        printf("ERROR ON RPC: code=%s, message=%s\n", error->code, error->message);
    } else {
        printf("RPC successful\n");
    }


    pitaya_wait_shutdown_signal(pitaya);

    pitaya_shutdown(pitaya);
}
