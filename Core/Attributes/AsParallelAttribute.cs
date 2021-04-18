using System;

namespace AspNetCore.Kafka.Attributes
{
    [Flags]
    public enum GroupBy
    {
        Partition,
        Key,
        Field
    }

    public class AsParallelAttribute : MessageBlockAttribute
    {
        public int DegreeOfParallelism { get; set; }

        public GroupBy GroupBy { get; set; }

        public string FieldName { get; set; }
    }
}