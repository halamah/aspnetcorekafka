using System;
using AspNetCore.Kafka.Data;

namespace AspNetCore.Kafka.Automation.Attributes
{
    public class ParallelAttribute : MessagePolicyAttribute
    {
        public ParallelAttribute() { }
        
        public ParallelAttribute(int degreeOfParallelism) : this(By.Partition, degreeOfParallelism) { }

        public ParallelAttribute(By by) : this(by, -1) { }
            
        public ParallelAttribute(By by, int degreeOfParallelism)
        {
            if (degreeOfParallelism < -1 || degreeOfParallelism == 0)
                throw new ArgumentException("DegreeOfParallelism must be greater than 0 or -1");

            By = by;
            DegreeOfParallelism = degreeOfParallelism;
        }
        
        public By By { get; set; } = By.Partition;
        
        public int DegreeOfParallelism { get; set; } = -1;
    }
}