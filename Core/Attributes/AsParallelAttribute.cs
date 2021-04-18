using System;

namespace AspNetCore.Kafka.Attributes
{
    [Flags]
    public enum ParallelBy
    {
        Partition,
        Key,
        Field
    }

    public class AsParallelAttribute : MessageBlockAttribute
    {
        public int DegreeOfParallelism { get; set; }

        public ParallelBy ParallelBy { get; set; }

        public string FieldName { get; set; }
    }
}