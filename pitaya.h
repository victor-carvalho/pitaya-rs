#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>


typedef enum LogLevel {
  Debug = 0,
  Info = 1,
  Warn = 2,
  Error = 3,
  Critical = 4,
} LogLevel;

typedef struct PitayaServer PitayaServer;

typedef struct CNATSConfig {
  char *addr;
  int64_t connectionTimeoutMs;
  int32_t requestTimeoutMs;
  int32_t serverShutdownDeadlineMs;
  int32_t serverMaxNumberOfRpcs;
  int32_t maxReconnectionAttempts;
  int32_t maxPendingMsgs;
} CNATSConfig;

typedef struct CSDConfig {
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

typedef struct CServer {
  char *id;
  char *kind;
  char *metadata;
  char *hostname;
  int32_t frontend;
} CServer;

PitayaServer *pitaya_initialize_with_nats(CNATSConfig *nc,
                                          CSDConfig *sdConfig,
                                          CServer *sv,
                                          LogLevel logLevel);

void pitaya_shutdown(PitayaServer *pitaya_server);
