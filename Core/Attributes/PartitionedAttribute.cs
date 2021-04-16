namespace AspNetCore.Kafka.Attributes
{
    public class PartitionedAttribute : MessageBlockAttribute
    {
        public int MaxDegreeOfParallelism { get; set; } = -1;
    }
}