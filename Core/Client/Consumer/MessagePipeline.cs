using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Client.Consumer
{
    internal class MessagePipeline<TSource, TDestination> : IMessagePipeline<TSource, TDestination>
    {
        private readonly Func<Func<TSource, Task>, IMessageSubscription> _subscription;
        private readonly IPropagatorBlock<TSource, TDestination> _pipeline;

        private MessagePipeline(
            Func<Func<TSource, Task>, IMessageSubscription> subscription,
            IPropagatorBlock<TSource, TDestination> pipeline = null)
        {
            _pipeline = pipeline;
            _subscription = subscription;
        }

        public MessagePipeline(Func<Func<TSource, Task>, IMessageSubscription> subscription)
        {
            _subscription = subscription;
        }

        public IMessagePipeline<TSource, T> Block<T>(IPropagatorBlock<TDestination, T> block)
        {
            if(_pipeline is null)
                return new MessagePipeline<TSource, T>(_subscription, (IPropagatorBlock<TSource, T>) block);
                    
            _pipeline.LinkTo(block);

            return new MessagePipeline<TSource, T>(_subscription, DataflowBlock.Encapsulate(_pipeline, block));
        }

        public IMessageSubscription Subscribe()
        {
            _pipeline.LinkTo(new ActionBlock<TDestination>(_ => { }, new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = 1,
                EnsureOrdered = true
            }));
            
            return _subscription(x => _pipeline.SendAsync(x));
        }
    }
}