#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct Pitaya Pitaya;

typedef enum {
    PitayaClusterNotification_ServerAdded = 0,
    PitayaClusterNotification_ServerRemoved = 1,
} PitayaClusterNotification;

typedef enum {
    PitayaLogKind_Console = 0,
    PitayaLogKind_Json = 1,
    PitayaLogKind_Function = 2,
} PitayaLogKind;

typedef enum {
    PitayaLogLevel_Trace = 0,
    PitayaLogLevel_Debug = 1,
    PitayaLogLevel_Info = 2,
    PitayaLogLevel_Warn = 3,
    PitayaLogLevel_Error = 4,
    PitayaLogLevel_Critical = 5,
} PitayaLogLevel;

/**
 * Pitaya represent a pitaya server.
 */
typedef struct Pitaya Pitaya;

typedef struct PitayaBuffer PitayaBuffer;

typedef struct PitayaContext PitayaContext;

typedef struct PitayaCustomMetrics PitayaCustomMetrics;

typedef struct PitayaError PitayaError;

typedef struct PitayaRpc PitayaRpc;

typedef struct PitayaServerInfo PitayaServerInfo;

typedef void (*PitayaHandleRpcCallback)(void*, PitayaContext*, PitayaRpc*);

typedef void (*PitayaClusterNotificationCallback)(void*, PitayaClusterNotification, PitayaServerInfo*);

const uint8_t *pitaya_buffer_data(PitayaBuffer *buf, int32_t *len);

void pitaya_buffer_drop(PitayaBuffer *buf);

PitayaBuffer *pitaya_buffer_new(const uint8_t *data, int32_t len);

void pitaya_custom_metrics_add_counter(PitayaCustomMetrics *m,
                                       char *namespace_,
                                       char *subsystem,
                                       char *name,
                                       char *help,
                                       char **variable_labels,
                                       uint32_t variable_labels_count);

void pitaya_custom_metrics_add_hist(PitayaCustomMetrics *m,
                                    char *namespace_,
                                    char *subsystem,
                                    char *name,
                                    char *help,
                                    char **variable_labels,
                                    uint32_t variable_labels_count,
                                    double *buckets,
                                    uint32_t buckets_count);

void pitaya_custom_metrics_drop(PitayaCustomMetrics *m);

PitayaCustomMetrics *pitaya_custom_metrics_new(void);

const char *pitaya_error_code(PitayaError *err);

void pitaya_error_drop(PitayaError *error);

const char *pitaya_error_message(PitayaError *err);

PitayaError *pitaya_initialize_with_nats(void *user_ctx,
                                         char *env_prefix,
                                         char *config_file,
                                         PitayaHandleRpcCallback handle_rpc_cb,
                                         PitayaClusterNotificationCallback cluster_notification_callback,
                                         PitayaLogLevel log_level,
                                         PitayaLogKind log_kind,
                                         void (*log_function)(void*, char*),
                                         void *log_ctx,
                                         PitayaCustomMetrics *custom_metrics,
                                         Pitaya **pitaya);

void pitaya_metrics_inc_counter(Pitaya *p, char *name, void (*callback)(void*), void *user_data);

void pitaya_metrics_observe_hist(Pitaya *p,
                                 char *name,
                                 double value,
                                 char **labels,
                                 uintptr_t labels_count,
                                 void (*callback)(void*),
                                 void *user_data);

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
                         void (*callback)(void*, PitayaServerInfo*),
                         void *user_data);

void pitaya_server_drop(PitayaServerInfo *pitaya_server);

int32_t pitaya_server_frontend(PitayaServerInfo *server);

const char *pitaya_server_hostname(PitayaServerInfo *server);

const char *pitaya_server_id(PitayaServerInfo *server);

const char *pitaya_server_kind(PitayaServerInfo *server);

const char *pitaya_server_metadata(PitayaServerInfo *server);

PitayaServerInfo *pitaya_server_new(char *id,
                                    char *kind,
                                    char *metadata,
                                    char *hostname,
                                    int32_t frontend);

void pitaya_shutdown(Pitaya *p);

void pitaya_wait_shutdown_signal(Pitaya *p);
