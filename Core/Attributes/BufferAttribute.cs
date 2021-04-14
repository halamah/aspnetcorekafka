namespace AspNetCore.Kafka.Attributes
{
    public class BufferAttribute : MessageBlockAttribute
    {
        public int Size { get; set; }
    }
}