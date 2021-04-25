using System;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Automation.Pipeline
{
    internal delegate PipelinePropagator<TContract, TDestination> BuildFunc<TContract, TDestination>();
    
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
            return IsEmpty
                ? new MessagePipeline<TContract, T>(Consumer,
                    () => new PipelinePropagator<TContract, T>((IPropagatorBlock<IMessage<TContract>, T>) blockFunc()))
                : new MessagePipeline<TContract, T>(Consumer,
                    () => Factory().Link(blockFunc()));
        }

        public virtual IMessagePipeline<TContract> Block(Func<ITargetBlock<TDestination>> blockFunc)
        {
            var next = blockFunc();

            if (!IsEmpty)
                return new ClosedMessagePipeline<TContract>(Consumer, () => Factory().Link(next));

            return new ClosedMessagePipeline<TContract>(Consumer,
                () => CreateBufferPropagator().Link((ITargetBlock<IMessage<TContract>>) blockFunc()));
        }

        private static PipelinePropagator<TContract, IMessage<TContract>> CreateBufferPropagator()
        {
            var buffer = new BufferBlock<IMessage<TContract>>(new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = 1,
                EnsureOrdered = true,
            });
                
            return new PipelinePropagator<TContract, IMessage<TContract>>(buffer, buffer);   
        }

        public virtual PipelinePropagator<TContract> Build(ICompletionSource completion)
        {
            var result = Factory?.Invoke() ?? throw new InvalidOperationException("Pipeline is empty");
            
            completion.Register(() =>
            {
                result.Input.Complete();
                return result.Output.Completion;
            });
            
            result.Link(DataflowBlock.NullTarget<TDestination>());
            return result.Close();
        }

        public IKafkaConsumer Consumer { get; }

        public bool IsEmpty => Factory is null;
    }
}