using System;
using System.Collections.Generic;
using System.Threading;
using exampleapp.Handlers;
using exampleapp.remotes;
using NPitaya;
using NPitaya.Models;

namespace PitayaCSharpExample
{
  class Example
  {
    static void Main(string[] args)
    {
      Logger.SetLevel(LogLevel.DEBUG);

      Console.WriteLine("c# prog running");

      var constantTags = new Dictionary<string, string>
      {
          {"game", "game"},
          {"serverType", "svType"}
      };

    //   var statsdMR = new StatsdMetricsReporter("localhost", 5000, "game", constantTags);
    //   MetricsReporters.AddMetricReporter(statsdMR);
    //   var prometheusMR = new PrometheusMetricsReporter("default", "game", 9090);
    //   MetricsReporters.AddMetricReporter(prometheusMR);

      AppDomain.CurrentDomain.ProcessExit += (object sender, EventArgs args) =>
      {
          Console.WriteLine("shutting down pitaya cluster");
          PitayaCluster.Terminate();
      };

      try
      {
          PitayaCluster.Initialize(
            "CSHARP",
            "csharp.toml",
            NativeLogLevel.Info,
            NativeLogKind.Console,
            (msg) =>
            {
                Console.Write($"C# Log: {msg}");
            },
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
      }
      catch (PitayaException exc)
      {
        Logger.Error("Failed to create cluster: {0}", exc.Message);
        Environment.Exit(1);
      }

      Logger.Info("pitaya lib initialized successfully :)");

      var tr = new TestRemote();
      PitayaCluster.RegisterRemote(tr);
      var th = new TestHandler();
      PitayaCluster.RegisterHandler(th);

      TrySendRpc();

      PitayaCluster.WaitShutdownSignal();
    }

    static async void TrySendRpc()
    {
        Logger.Info("Sending RPC....");
        try
        {
            var res = await PitayaCluster.Rpc<NPitaya.Protos.MyResponse>(Route.FromString("csharp.testRemote.remote"), new NPitaya.Protos.RPCMsg {
                Route = "random.route.man",
                Msg = "HEY",
            });
            // var res = await PitayaCluster.Rpc<NPitaya.Protos.MyResponse>(Route.FromString("room.room.join"), null);
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
