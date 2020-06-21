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
  int64_t connectionTimeoutMs;
  int32_t requestTimeoutMs;
  int32_t serverShutdownDeadlineMs;
  int32_t serverMaxNumberOfRpcs;
  int32_t maxReconnectionAttempts;
  int32_t maxPendingMsgs;
} CNATSConfig;

typedef struct {
  char *endpoints;
  char *etcdPrefix;
  char *serverTypeFilters;
  int32_t heartbeatTTLSec;
  int32_t logHeartbeat;
  int32_t logServerSync;
  int32_t logServerDetails;
  int32_t syncServersIntervalSec;
  int32_t maxNumberOfRetries;
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
                                          CSDConfig *sdConfig,
                                          CServer *sv,
                                          PitayaLogLevel log_level);

PitayaError *pitaya_send_rpc(PitayaServer *pitaya_server,
                             char *route,
                             const PitayaRpcRequest *request,
                             PitayaRpcResponse *response);

void pitaya_shutdown(PitayaServer *pitaya_server);

void pitaya_wait_shutdown_signal(PitayaServer *pitaya_server);
