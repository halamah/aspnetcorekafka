using System;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Data;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessagePipeline
    {
        IKafkaConsumer Consumer { get; }
    }
    
    public interface IMessagePipelineSource<in TContract> : IMessagePipeline
    {
        ITargetBlock<IMessage<TContract>> Build();
    }
    
    public interface IMessagePipelineDestination<out TDestination> : IMessagePipeline
    {
        IObservable<TDestination> AsObservable(string topic = null, SubscriptionOptions options = null);

        IObservable<TDestination> AsObservable(SubscriptionOptions options);
    }
    
    public interface IMessagePipeline<in TContract, out TDestination> : 
        IMessagePipelineSource<TContract>,
        IMessagePipelineDestination<TDestination>
    {
        IMessagePipeline<TContract, T> Block<T>(Func<IPropagatorBlock<TDestination, T>> blockFunc);

        public IMessagePipelineSource<TContract> Block(Func<ITargetBlock<TDestination>> block);
    }
}