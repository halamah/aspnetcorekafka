using System;
using System.Threading;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Automation.Pipeline;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessagePipeline
    {
        IKafkaConsumer Consumer { get; }
        
        CancellationTokenSource CancellationToken { get; }
        
        bool IsEmpty { get; }
    }
    
    public interface IMessagePipeline<TContract> : IMessagePipeline
    {
        PipelinePropagator<TContract> Build(ICompletionSource completion);
    }
    
    public interface IMessagePipeline<TContract, out TDestination> : IMessagePipeline<TContract>
    {
        IMessagePipeline<TContract, T> Block<T>(Func<IPropagatorBlock<TDestination, T>> blockFunc);

        public IMessagePipeline<TContract> Block(Func<ITargetBlock<TDestination>> block);
    }
}