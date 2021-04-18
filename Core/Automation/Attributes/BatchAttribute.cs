namespace AspNetCore.Kafka.Automation.Attributes
{
    public class BatchAttribute : MessageBlockAttribute
    {
        public BatchAttribute()
        {
        }
        
        public BatchAttribute(int size, int time = 0)
        {
            Size = size;
            Time = time;
        }
        
        public int Size { get; set; }
        
        public int Time { get; set; }
    }
}