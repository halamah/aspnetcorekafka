using System;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;

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
                pipeline.LinkTo(block, new DataflowLinkOptions {PropagateCompletion = true });
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
                pipeline.LinkTo(block, new DataflowLinkOptions {PropagateCompletion = true });
                return pipeline;
            });
        }

        public virtual ITargetBlock<IMessage<TContract>> BuildTarget()
        {
            var pipeline = Factory?.Invoke() ?? throw new InvalidOperationException("Pipeline is empty");

            pipeline.LinkTo(
                DataflowBlock.NullTarget<TDestination>(),
                new DataflowLinkOptions {PropagateCompletion = true});

            return pipeline;
        }
        
        public ISourceBlock<TDestination> BuildSource()
            => Factory?.Invoke() ?? throw new InvalidOperationException("Pipeline is empty");

        public IPropagatorBlock<IMessage<TContract>, TDestination> Build()
            => Factory?.Invoke() ?? throw new InvalidOperationException("Pipeline is empty");
        
        public IKafkaConsumer Consumer { get; }

        public bool IsEmpty => Factory is null;
    }
}