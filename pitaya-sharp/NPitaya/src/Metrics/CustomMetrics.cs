using System.Collections.Generic;

namespace NPitaya.Metrics
{
    internal readonly struct MetricSpec
    {
        internal readonly string Name;
        internal readonly string Help;
        internal readonly string[] Labels;

        internal MetricSpec(string name, string help, string[] labels)
        {
            Name = name;
            Help = help;
            Labels = labels;
        }
    }

    public class CustomMetrics
    {
        internal readonly List<MetricSpec> Counters;
        internal readonly List<MetricSpec> Gauges;
        internal readonly List<MetricSpec> Histograms;

        public CustomMetrics()
        {
            Counters = new List<MetricSpec>();
            Gauges = new List<MetricSpec>();
            Histograms = new List<MetricSpec>();
        }

        public void AddCounter(string name, string help = null, string[] labels = null)
        {
            Counters.Add(new MetricSpec(name, help, labels));
        }

        public void AddGauge(string name, string help = null, string[] labels = null)
        {
            Gauges.Add(new MetricSpec(name, help, labels));
        }

        public void AddHistogram(string name, string help = null, string[] labels = null)
        {
            Histograms.Add(new MetricSpec(name, help, labels));
        }
    }
}