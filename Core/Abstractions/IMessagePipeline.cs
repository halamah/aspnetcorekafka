using System.Threading.Tasks.Dataflow;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessagePipeline
    {
        IMessageSubscription Subscribe();
    }
    
    public interface IMessagePipeline<TSource, out TDestination> : IMessagePipeline
    {
        IMessagePipeline<TSource, T> Block<T>(IPropagatorBlock<TDestination, T> block);
    }
}