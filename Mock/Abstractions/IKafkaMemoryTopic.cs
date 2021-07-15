using System.Threading.Tasks;

namespace AspNetCore.Kafka.Mock.Abstractions
{
    public interface IKafkaMemoryTopic
    {
        string Name { get; }

        int PartitionsCount { get; set; }

        public Task WhenConsumedAny();

        public Task WhenConsumedAll();
        
        IKafkaMemoryMessageList<object, object> Produced { get; }

        IKafkaMemoryMessageList<object, object> Consumed { get; }
    }
}