#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef enum {
    PitayaLogKind_Console = 0,
    PitayaLogKind_Json = 1,
} PitayaLogKind;

typedef enum {
    PitayaLogLevel_Trace = 0,
    PitayaLogLevel_Debug = 1,
    PitayaLogLevel_Info = 2,
    PitayaLogLevel_Warn = 3,
    PitayaLogLevel_Error = 4,
    PitayaLogLevel_Critical = 5,
} PitayaLogLevel;

typedef struct Pitaya Pitaya;

typedef struct PitayaRpc PitayaRpc;

typedef struct {
    char *code;
    char *message;
} PitayaError;

typedef struct {
    char *addr;
    int64_t connection_timeout_ms;
    int32_t request_timeout_ms;
    int32_t server_shutdown_deadline_ms;
    int32_t server_max_number_of_rpcs;
    int32_t max_reconnection_attempts;
    int32_t max_pending_msgs;
} PitayaNATSConfig;

typedef struct {
    char *endpoints;
    char *etcd_prefix;
    char *server_type_filters;
    int32_t heartbeat_ttl_sec;
    int32_t log_heartbeat;
    int32_t log_server_sync;
    int32_t log_server_details;
    int32_t sync_servers_interval_sec;
    int32_t max_number_of_retries;
} PitayaSDConfig;

typedef struct {
    char *id;
    char *kind;
    char *metadata;
    char *hostname;
    int32_t frontend;
} PitayaServer;

typedef void (*PitayaHandleRpcCallback)(void*, PitayaRpc*);

typedef struct {
    const uint8_t *data;
    int64_t len;
} PitayaRpcRequest;

typedef struct {
    const uint8_t *data;
    int64_t len;
} PitayaRpcResponse;

void pitaya_error_drop(PitayaError *error);

PitayaError *pitaya_initialize_with_nats(PitayaNATSConfig *nc,
                                         PitayaSDConfig *sd_config,
                                         PitayaServer *sv,
                                         PitayaHandleRpcCallback handle_rpc_cb,
                                         void *handle_rpc_data,
                                         Pitaya **pitaya,
                                         PitayaLogLevel log_level,
                                         PitayaLogKind log_kind);

uint8_t *pitaya_rpc_request(PitayaRpc *rpc, int64_t *len);

PitayaError *pitaya_rpc_respond(PitayaRpc *rpc, const uint8_t *response_data, int64_t response_len);

PitayaError *pitaya_send_rpc(Pitaya *pitaya_server,
                             char *route,
                             const PitayaRpcRequest *request,
                             PitayaRpcResponse *response);

void pitaya_shutdown(Pitaya *pitaya_server);

void pitaya_wait_shutdown_signal(Pitaya *pitaya_server);
