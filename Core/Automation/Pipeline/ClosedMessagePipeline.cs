using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Automation.Pipeline
{
    internal delegate PipelinePropagator<TContract> BuildFunc<TContract>();
    
    internal class ClosedMessagePipeline<TContract> : IMessagePipeline<TContract>
    {
        public BuildFunc<TContract> Factory { get; }

        public ClosedMessagePipeline(IKafkaConsumer consumer, BuildFunc<TContract> factory)
        {
            Factory = factory;
            Consumer = consumer;
        }

        public PipelinePropagator<TContract> Build(ICompletionSource completion) => Factory();

        public IKafkaConsumer Consumer { get; }

        public bool IsEmpty => Factory is null;
    }
}