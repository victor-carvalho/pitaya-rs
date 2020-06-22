#include <assert.h>
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

static bool
write_string(pb_ostream_t *stream, const pb_field_t *field, void * const *arg)
{
    return pb_encode_tag_for_field(stream, field) &&
           pb_encode_string(stream, *arg, strlen(*arg));
}

static bool
read_string(pb_istream_t *stream, const pb_field_t *field, void **arg)
{
    uint8_t *buf = calloc(stream->bytes_left+1, 1);

    if (!pb_read(stream, buf, stream->bytes_left)) {
        free(buf);
        return false;
    }

    *arg = buf;
    return true;
}

static void
handle_rpc(void *data, PitayaRpc *rpc)
{
    printf("======================= RECEIVED RPC!!!!\n");
    fflush(stdout);

    protos_Response protos_response = protos_Response_init_zero;
    protos_response.has_error = 0;
    protos_response.data.funcs.encode = write_string;
    protos_response.data.arg = "RESPONSE FROM C";

    uint8_t response_data[256];
    pb_ostream_t stream = pb_ostream_from_buffer(response_data, sizeof(response_data));
    assert(pb_encode(&stream, protos_Response_fields, &protos_response));
    int64_t response_len = stream.bytes_written;

    PitayaError *err = pitaya_rpc_respond(rpc, response_data, response_len);
    if (err) {
        printf("error on respond: code=%s, message=%s\n", err->code, err->message);
        pitaya_error_drop(err);
    }
}

int main()
{
    PitayaNATSConfig nats_config = {0};
    nats_config.addr = "http://localhost:4222";
    nats_config.connection_timeout_ms = 5000;
    nats_config.request_timeout_ms = 5000;
    nats_config.server_shutdown_deadline_ms = 5000;
    nats_config.server_max_number_of_rpcs = 100;
    nats_config.max_reconnection_attempts = 20;
    nats_config.max_pending_msgs = 50;

    PitayaSDConfig sd_config = {0};
    sd_config.endpoints = "localhost:2379";
    sd_config.etcd_prefix = "pitaya";

    PitayaServer server = {0};
    server.id = "my-server-id-from-c";
    server.kind = "my-server-kind-from-c";
    server.metadata = "random-metadata";
    server.hostname = "";
    server.frontend = 0;

    PitayaError *err = NULL;
    Pitaya *pitaya = NULL;

    err = pitaya_initialize_with_nats(
        &nats_config,
        &sd_config,
        &server,
        PitayaLogLevel_Trace,
        handle_rpc,
        NULL,
        &pitaya
    );

    if (err) {
        printf("failed to initialize pitaya: code=%s, message=%s\n", err->code, err->message);
        pitaya_error_drop(err);
        return 1;
    }

    printf("Will send RPC...\n");

    protos_Request protos_request = protos_Request_init_zero;
    protos_request.type = protos_RPCType_User;
    protos_request.has_msg = 1;
    protos_request.msg.type = protos_MsgType_MsgRequest;
    protos_request.msg.data.funcs.encode = write_string;
    protos_request.msg.data.arg = "Some data to be sent";
    protos_request.msg.route.funcs.encode = write_string;
    protos_request.msg.route.arg = "my-server-kind-from-c.room.join";
    protos_request.metadata.funcs.encode = write_string;
    protos_request.metadata.arg = "{}";

    uint8_t request_data[256];
    pb_ostream_t stream = pb_ostream_from_buffer(request_data, sizeof(request_data));
    assert(pb_encode(&stream, protos_Request_fields, &protos_request));

    PitayaRpcRequest request = {0};
    request.data = request_data;
    request.len = stream.bytes_written;

    PitayaRpcResponse response = {0};

    err = pitaya_send_rpc(pitaya, "my-server-kind-from-c.room.join", &request, &response);
    if (err) {
        printf("ERROR ON RPC: code=%s, message=%s\n", err->code, err->message);
        pitaya_error_drop(err);
    } else {
        printf("RPC successful\n");

        protos_Response protos_response = protos_Response_init_zero;
        protos_response.data.funcs.decode = read_string;
        protos_response.error.code.funcs.decode = read_string;
        protos_response.error.msg.funcs.decode = read_string;
        protos_response.error.metadata.funcs.decode = read_string;

        pb_istream_t stream = pb_istream_from_buffer(response.data, response.len);
        assert(pb_decode(&stream, protos_Response_fields, &protos_response));

        printf("Received response from server: %s\n", (char*)protos_response.data.arg);
    }

    pitaya_wait_shutdown_signal(pitaya);

    pitaya_shutdown(pitaya);
}
