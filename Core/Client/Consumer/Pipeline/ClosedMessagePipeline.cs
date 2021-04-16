using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Client.Consumer.Pipeline
{
    internal delegate ITargetBlock<IMessage<TContract>> BuildFunc<in TContract>();
    
    internal class ClosedMessagePipeline<TContract> : IMessagePipelineSource<TContract>
    {
        private readonly BuildFunc<TContract> _build;

        public ClosedMessagePipeline(IKafkaConsumer consumer, BuildFunc<TContract> build)
        {
            _build = build;
            Consumer = consumer;
        }

        public ITargetBlock<IMessage<TContract>> Build() => _build();

        public IKafkaConsumer Consumer { get; }
    }
}