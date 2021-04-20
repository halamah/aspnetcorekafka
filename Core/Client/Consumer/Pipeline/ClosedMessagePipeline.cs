using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Client.Consumer.Pipeline
{
    internal delegate ITargetBlock<IMessage<TContract>> BuildFunc<in TContract>();
    
    internal class ClosedMessagePipeline<TContract> : IMessagePipelineSource<TContract>
    {
        private readonly BuildFunc<TContract> _factory;

        public ClosedMessagePipeline(IKafkaConsumer consumer, BuildFunc<TContract> factory)
        {
            _factory = factory;
            Consumer = consumer;
        }

        public ITargetBlock<IMessage<TContract>> BuildTarget() => _factory();

        public IKafkaConsumer Consumer { get; }

        public bool IsEmpty => _factory is null;
    }
}