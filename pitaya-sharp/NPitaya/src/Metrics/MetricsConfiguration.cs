using System.Reflection;
using System.Reflection.Metadata;

namespace NPitaya.Metrics
{
    public class MetricsConfiguration
    {
        internal bool IsEnabled;
        internal readonly string Host;
        internal string Port;
        internal string Namespace;
        private CustomMetrics CustomMetrics;

        public MetricsConfiguration(bool isEnabled, string host, string port, string ns, CustomMetrics customMetrics = null)
        {
            IsEnabled = isEnabled;
            Host = host;
            Port = port;
            Namespace = ns;
            CustomMetrics = customMetrics;
        }
    }

    public class CustomMetrics
    {

    }
}