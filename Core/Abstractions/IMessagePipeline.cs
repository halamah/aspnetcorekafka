using System.Threading.Tasks.Dataflow;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessagePipeline<TSource>
    {
        internal IMessagePipeline<TSource> DynamicBlock<T>(ITargetBlock<T> block);

        IMessageSubscription Subscribe();
    }
    
    public interface IMessagePipeline<TSource, out TDestination> : IMessagePipeline<TSource>
    {
        IMessagePipeline<TSource, T> Block<T>(IPropagatorBlock<TDestination, T> block);
    }
}