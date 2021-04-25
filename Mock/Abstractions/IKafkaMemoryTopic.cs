using System.Threading.Tasks;

namespace AspNetCore.Kafka.Mock.Abstractions
{
    public interface IKafkaMemoryTopic
    {
        string Name { get; }
        
        long ProducedCount { get; }
        
        long ConsumedCount { get; }
        
        long CommitCount { get; }
        
        int PartitionsCount { get; set; }

        public Task WhenConsumedAny();

        public Task WhenConsumedAll();
    }
}