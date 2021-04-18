namespace AspNetCore.Kafka.Automation.Attributes
{
    public class BufferAttribute : MessageBlockAttribute
    {
        public BufferAttribute()
        {
        }
        
        public BufferAttribute(int size)
        {
            Size = size;
        }
        
        public int Size { get; set; }
    }
}