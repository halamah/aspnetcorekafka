using System;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;

namespace AspNetCore.Kafka.Client.Consumer.Pipeline
{
    internal delegate IPropagatorBlock<IMessage<TContract>, TDestination> BuildFunc<in TContract, out TDestination>();
    
    internal class MessagePipeline<TContract, TDestination> : IMessagePipeline<TContract, TDestination>
    {
        internal BuildFunc<TContract, TDestination>  Factory { get; }

        public MessagePipeline(IKafkaConsumer consumer, BuildFunc<TContract, TDestination> factory = null)
        {
            Consumer = consumer;
            Factory = factory;
        }

        public virtual IMessagePipeline<TContract, T> Block<T>(Func<IPropagatorBlock<TDestination, T>> blockFunc)
        {
            if (Factory is null)
                return new MessagePipeline<TContract, T>(Consumer, () => (IPropagatorBlock<IMessage<TContract>, T>) blockFunc());

            return new MessagePipeline<TContract, T>(Consumer, () =>
            {
                var pipeline = Factory();
                var block = blockFunc();
                pipeline.LinkTo(block);
                return DataflowBlock.Encapsulate(pipeline, block);
            });
        }

        public virtual IMessagePipelineSource<TContract> Block(Func<ITargetBlock<TDestination>> blockFunc)
        {
            if (Factory is null)
                return new ClosedMessagePipeline<TContract>(Consumer,
                    () => (ITargetBlock<IMessage<TContract>>) blockFunc());
            
            return new ClosedMessagePipeline<TContract>(Consumer, () =>
            {
                var pipeline = Factory();
                var block = blockFunc();
                pipeline.LinkTo(block);
                return pipeline;
            });
        }

        public virtual ITargetBlock<IMessage<TContract>> Build()
        {
            if (Factory is null)
                throw new InvalidOperationException("Pipeline is empty");
            
            var pipeline = Factory();
            
            pipeline.LinkTo(DataflowBlock.NullTarget<TDestination>());

            return pipeline;
        }

        public IObservable<TDestination> AsObservable(string topic = null, SubscriptionOptions options = null)
        {
            var block = Factory is null
                ? new BufferBlock<TDestination>(
                    new DataflowBlockOptions
                    {
                        BoundedCapacity = 1,
                        EnsureOrdered = true,
                    }) as IPropagatorBlock<IMessage<TContract>, TDestination>
                : Factory();

            Consumer.Subscribe<TContract>(
                string.IsNullOrEmpty(topic) ? TopicDefinition.FromType<TContract>().Topic : topic,
                x => block!.SendAsync(x),
                options);

            return block!.AsObservable();
        }

        public IObservable<TDestination> AsObservable(SubscriptionOptions options)
            => AsObservable(TopicDefinition.FromType<TContract>().Topic, options);

        public IKafkaConsumer Consumer { get; }
    }
}