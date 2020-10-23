using System.Reflection;
using System.Reflection.Metadata;

namespace NPitaya.Metrics
{
    public class MetricsConfiguration
    {
        internal readonly string Host;
        internal string Port;
        internal string Namespace;

        public MetricsConfiguration(string host, string port, string ns)
        {
            Host = host;
            Port = port;
            Namespace = ns;
        }
    }
}