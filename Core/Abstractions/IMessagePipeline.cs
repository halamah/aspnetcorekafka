using System;
using System.Threading.Tasks.Dataflow;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessagePipeline
    {
        IKafkaConsumer Consumer { get; }
        
        bool IsEmpty { get; }
    }
    
    public interface IMessagePipelineSource<in TContract> : IMessagePipeline
    {
        ITargetBlock<IMessage<TContract>> BuildTarget();
    }
    
    public interface IMessagePipelineDestination<out TDestination> : IMessagePipeline
    {
        ISourceBlock<TDestination> BuildSource();
    }
    
    public interface IMessagePipeline<in TContract, out TDestination> : 
        IMessagePipelineSource<TContract>,
        IMessagePipelineDestination<TDestination>
    {
        IPropagatorBlock<IMessage<TContract>, TDestination> Build();
        
        IMessagePipeline<TContract, T> Block<T>(Func<IPropagatorBlock<TDestination, T>> blockFunc);

        public IMessagePipelineSource<TContract> Block(Func<ITargetBlock<TDestination>> block);
    }
}