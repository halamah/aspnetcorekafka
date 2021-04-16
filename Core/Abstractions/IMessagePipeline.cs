using System;
using System.Threading.Tasks.Dataflow;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessagePipeline
    {
        IKafkaConsumer Consumer { get; }
    }
    
    public interface IMessagePipelineSource<in TSource> : IMessagePipeline
    {
        ITargetBlock<TSource> Build();
    }
    
    public interface IMessagePipelineDestination<out TDestination> : IMessagePipeline
    {
    }
    
    public interface IMessagePipeline<in TSource, out TDestination> : 
        IMessagePipelineSource<TSource>,
        IMessagePipelineDestination<TDestination>
    {
        IMessagePipeline<TSource, T> Block<T>(Func<IPropagatorBlock<TDestination, T>> blockFunc);
        
        internal IMessagePipelineSource<TSource> Block(Func<ITargetBlock<TDestination>> block);
    }
}