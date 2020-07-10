using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using exampleapp.Handlers;
using exampleapp.remotes;
using NPitaya;
using NPitaya.Metrics;
using NPitaya.Models;

namespace PitayaCSharpExample
{
  class Example
  {
    static void Main(string[] args)
    {
      Logger.SetLevel(LogLevel.DEBUG);

      Console.WriteLine("c# prog running");

      string serverId = System.Guid.NewGuid().ToString();

      var sdConfig = new SDConfig(
        endpoints: "http://127.0.0.1:2379",
        etcdPrefix: "pitaya/",
        serverTypeFilters: new List<string>(),
        heartbeatTTLSec: 60,
        logHeartbeat: true,
        logServerSync: true,
        logServerDetails: true,
        syncServersIntervalSec: 30,
        maxNumberOfRetries: 0);

      var sv = new Server(
          id: serverId,
          kind: "csharp",
          metadata: "",
          hostname: "localhost",
          frontend: false);

      var natsConfig = new NatsConfig(
          endpoint: "127.0.0.1:4222",
          connectionTimeoutMs: 2000,
          requestTimeoutMs: 1000,
          serverShutdownDeadlineMs: 3,
          serverMaxNumberOfRpcs: 100,
          maxConnectionRetries: 3,
          maxPendingMessages: 1000);

      var constantTags = new Dictionary<string, string>
      {
          {"game", "game"},
          {"serverType", "svType"}
      };

    //   var statsdMR = new StatsdMetricsReporter("localhost", 5000, "game", constantTags);
    //   MetricsReporters.AddMetricReporter(statsdMR);
    //   var prometheusMR = new PrometheusMetricsReporter("default", "game", 9090);
    //   MetricsReporters.AddMetricReporter(prometheusMR);

      try
      {
          PitayaCluster.Initialize(
            natsConfig,
            sdConfig,
            sv,
            NativeLogLevel.Trace,
            NativeLogKind.Console,
            new PitayaCluster.ServiceDiscoveryListener((action, server) =>
            {
                switch (action)
                {
                    case PitayaCluster.ServiceDiscoveryAction.ServerAdded:
                        Console.WriteLine("Server was added");
                        Console.WriteLine("    id: " + server.Id);
                        Console.WriteLine("  kind: " + server.Kind);
                        break;
                    case PitayaCluster.ServiceDiscoveryAction.ServerRemoved:
                        Console.WriteLine("Server was removed");
                        Console.WriteLine("    id: " + server.Id);
                        Console.WriteLine("  kind: " + server.Kind);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(action), action, null);
                }
            }));
        //PitayaCluster.Initialize(natsConfig, sdConfig, sv, NativeLogLevel.Debug, "");
      }
      catch (PitayaException exc)
      {
        Logger.Error("Failed to create cluster: {0}", exc.Message);
        Environment.Exit(1);
      }

      new Thread(() => {
        Console.WriteLine("Waiting shutdown signal...");
        PitayaCluster.WaitShutdownSignal();
        Console.WriteLine("Done waiting...");
        PitayaCluster.Terminate();
        Environment.Exit(1);
        // Environment.FailFast("oops");
      }).Start();

      Logger.Info("pitaya lib initialized successfully :)");

      var tr = new TestRemote();
      PitayaCluster.RegisterRemote(tr);
      var th = new TestHandler();
      PitayaCluster.RegisterHandler(th);

      TrySendRpc();
      Console.ReadKey();
      PitayaCluster.Terminate();
    }

    static async void TrySendRpc()
    {
        Logger.Info("Sending RPC....");
        try
        {
            // var res = await PitayaCluster.Rpc<NPitaya.Protos.MyResponse>(Route.FromString("csharp.testRemote.remote"), new NPitaya.Protos.RPCMsg {
                // Route = "random.route.man",
                // Msg = "HEY",
            // });
            var res = await PitayaCluster.Rpc<NPitaya.Protos.MyResponse>(Route.FromString("room.room.join"), null);
            Console.WriteLine("GOT MESSAGE!!!");
            Console.WriteLine($"Code: {res.Code}");
            Console.WriteLine($"Msg: {res.Msg}");
        }
        catch (PitayaException e)
        {
            Logger.Error("Error sending RPC Call: {0}", e.Message);
        }
    }
  }
}
