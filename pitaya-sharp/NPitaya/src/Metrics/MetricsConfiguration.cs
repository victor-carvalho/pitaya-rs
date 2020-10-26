namespace NPitaya.Metrics
{
    public class MetricsConfiguration
    {
        internal readonly bool IsEnabled;
        internal readonly string Host;
        internal readonly string Port;
        internal readonly string Namespace;

        public MetricsConfiguration(bool isEnabled, string host, string port, string ns)
        {
            IsEnabled = isEnabled;
            Host = host;
            Port = port;
            Namespace = ns;
        }
    }
}