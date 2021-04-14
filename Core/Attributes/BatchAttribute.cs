namespace AspNetCore.Kafka.Attributes
{
    public class BatchAttribute : MessageBlockAttribute
    {
        public int Size { get; set; }
        
        public int Time { get; set; }
    }
}