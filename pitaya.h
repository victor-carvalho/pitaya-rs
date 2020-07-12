#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#define DEFAULT_NATS_MAX_PENDING_MESSAGES 100

#define DEFAULT_NATS_MAX_RECONN_ATTEMPTS 5

#define DEFAULT_NATS_MAX_RPCS_QUEUED 100

typedef enum {
    PitayaClusterNotification_ServerAdded = 0,
    PitayaClusterNotification_ServerRemoved = 1,
} PitayaClusterNotification;

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

typedef struct PitayaBuffer PitayaBuffer;

typedef struct PitayaError PitayaError;

typedef struct PitayaRpc PitayaRpc;

typedef struct PitayaServer PitayaServer;

typedef void (*PitayaHandleRpcCallback)(void*, PitayaRpc*);

typedef void (*PitayaClusterNotificationCallback)(void*, PitayaClusterNotification, PitayaServer*);

const uint8_t *pitaya_buffer_data(PitayaBuffer *buf, int32_t *len);

void pitaya_buffer_drop(PitayaBuffer *buf);

PitayaBuffer *pitaya_buffer_new(const uint8_t *data, int32_t len);

const char *pitaya_error_code(PitayaError *err);

void pitaya_error_drop(PitayaError *error);

const char *pitaya_error_message(PitayaError *err);

PitayaError *pitaya_initialize_with_nats(char *env_prefix,
                                         char *config_file,
                                         PitayaHandleRpcCallback handle_rpc_cb,
                                         void *handle_rpc_data,
                                         PitayaLogLevel log_level,
                                         PitayaLogKind log_kind,
                                         PitayaClusterNotificationCallback cluster_notification_callback,
                                         void *cluster_notification_data,
                                         Pitaya **pitaya);

void pitaya_rpc_drop(PitayaRpc *rpc);

uint8_t *pitaya_rpc_request(PitayaRpc *rpc, int32_t *len);

PitayaError *pitaya_rpc_respond(PitayaRpc *rpc, const uint8_t *response_data, int32_t response_len);

void pitaya_send_kick(Pitaya *p,
                      char *server_id,
                      char *server_kind,
                      PitayaBuffer *kick_buffer,
                      void (*callback)(void*, PitayaError*, PitayaBuffer*),
                      void *user_data);

void pitaya_send_push_to_user(Pitaya *p,
                              char *server_id,
                              char *server_kind,
                              PitayaBuffer *push_buffer,
                              void (*callback)(void*, PitayaError*),
                              void *user_data);

void pitaya_send_rpc(Pitaya *p,
                     char *server_id,
                     char *route_str,
                     PitayaBuffer *request_buffer,
                     void (*callback)(void*, PitayaError*, PitayaBuffer*),
                     void *user_data);

void pitaya_server_by_id(Pitaya *p,
                         char *server_id,
                         char *server_kind,
                         void (*callback)(void*, PitayaServer*),
                         void *user_data);

void pitaya_server_drop(PitayaServer *pitaya_server);

int32_t pitaya_server_frontend(PitayaServer *server);

const char *pitaya_server_hostname(PitayaServer *server);

const char *pitaya_server_id(PitayaServer *server);

const char *pitaya_server_kind(PitayaServer *server);

const char *pitaya_server_metadata(PitayaServer *server);

PitayaServer *pitaya_server_new(char *id,
                                char *kind,
                                char *metadata,
                                char *hostname,
                                int32_t frontend);

void pitaya_shutdown(Pitaya *p);

void pitaya_wait_shutdown_signal(Pitaya *pitaya_server);
