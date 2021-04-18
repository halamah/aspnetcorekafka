using System;

namespace AspNetCore.Kafka.Automation.Attributes
{
    public class PartitionedAttribute : MessageBlockAttribute
    {
        public PartitionedAttribute()
        {
        }
        
        public PartitionedAttribute(int maxDegreeOfParallelism)
        {
            if (maxDegreeOfParallelism < -1 || maxDegreeOfParallelism == 0)
                throw new ArgumentException("MaxDegreeOfParallelism must be greater than 0 or -1");
                    
            MaxDegreeOfParallelism = maxDegreeOfParallelism;
        }
        
        public int MaxDegreeOfParallelism { get; set; } = -1;
    }
}