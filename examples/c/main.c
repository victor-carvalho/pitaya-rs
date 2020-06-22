#include <stdio.h>
#include <pitaya.h>

#include "nanopb/pb.h"
#include "nanopb/pb_common.h"
#include "nanopb/pb_common.c"
#include "nanopb/pb_encode.c"
#include "nanopb/pb_decode.c"

#include "error.pb.h"
#include "error.pb.c"
#include "msg.pb.c"
#include "session.pb.c"
#include "pitaya.pb.c"
#include "request.pb.c"
#include "response.pb.c"

int main()
{
    CNATSConfig nats_config = {0};
    nats_config.addr = "http://localhost:4222";
    nats_config.connection_timeout_ms = 5000;
    nats_config.request_timeout_ms = 5000;
    nats_config.server_shutdown_deadline_ms = 5000;
    nats_config.server_max_number_of_rpcs = 100;
    nats_config.max_reconnection_attempts = 20;
    nats_config.max_pending_msgs = 50;

    CSDConfig sd_config = {0};
    sd_config.endpoints = "localhost:2379";
    sd_config.etcd_prefix = "pitaya";

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
