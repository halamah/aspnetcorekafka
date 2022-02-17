using System.Threading;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Automation.Pipeline
{
    internal delegate PipelinePropagator<TContract> BuildFunc<TContract>();
    
    internal class ClosedMessagePipeline<TContract> : IMessagePipeline<TContract>
    {
        public BuildFunc<TContract> Factory { get; }

        public CancellationTokenSource CancellationToken { get; }
        
        public ClosedMessagePipeline(IKafkaConsumer consumer, CancellationTokenSource cancellationToken, BuildFunc<TContract> factory)
        {
            Factory = factory;
            Consumer = consumer;
            CancellationToken = cancellationToken;
        }

        public PipelinePropagator<TContract> Build(ICompletionSource completion) => Factory();

        public IKafkaConsumer Consumer { get; }

        public bool IsEmpty => Factory is null;
    }
}