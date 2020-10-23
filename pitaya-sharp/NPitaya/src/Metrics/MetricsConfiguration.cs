namespace NPitaya.Metrics
{
    public class MetricsConfiguration
    {
        internal readonly string Host;
        internal readonly string Port;
        internal readonly string Namespace;

        public MetricsConfiguration(string host, string port, string ns)
        {
            Host = host;
            Port = port;
            Namespace = ns;
        }
    }
}