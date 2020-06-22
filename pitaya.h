#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef enum {
  PitayaLogLevel_Trace = 0,
  PitayaLogLevel_Debug = 1,
  PitayaLogLevel_Info = 2,
  PitayaLogLevel_Warn = 3,
  PitayaLogLevel_Error = 4,
} PitayaLogLevel;

typedef struct PitayaServer PitayaServer;

typedef struct {
  char *addr;
  int64_t connection_timeout_ms;
  int32_t request_timeout_ms;
  int32_t server_shutdown_deadline_ms;
  int32_t server_max_number_of_rpcs;
  int32_t max_reconnection_attempts;
  int32_t max_pending_msgs;
} CNATSConfig;

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
} CSDConfig;

typedef struct {
  char *id;
  char *kind;
  char *metadata;
  char *hostname;
  int32_t frontend;
} CServer;

typedef struct {
  char *code;
  char *message;
} PitayaError;

typedef struct {
  const uint8_t *data;
  int64_t len;
} PitayaRpcRequest;

typedef struct {
  const uint8_t *data;
  int64_t len;
} PitayaRpcResponse;

PitayaServer *pitaya_initialize_with_nats(CNATSConfig *nc,
                                          CSDConfig *sd_config,
                                          CServer *sv,
                                          PitayaLogLevel log_level);

PitayaError *pitaya_send_rpc(PitayaServer *pitaya_server,
                             char *route,
                             const PitayaRpcRequest *request,
                             PitayaRpcResponse *response);

void pitaya_shutdown(PitayaServer *pitaya_server);

void pitaya_wait_shutdown_signal(PitayaServer *pitaya_server);
