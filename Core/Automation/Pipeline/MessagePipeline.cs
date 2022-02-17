using System;
using System.Threading;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Automation.Pipeline
{
    internal delegate PipelinePropagator<TContract, TDestination> BuildFunc<TContract, TDestination>();
    
    internal class MessagePipeline<TContract, TDestination> : IMessagePipeline<TContract, TDestination>
    {
        internal BuildFunc<TContract, TDestination>  Factory { get; }

        public CancellationTokenSource CancellationToken { get; }
        
        public MessagePipeline(IKafkaConsumer consumer, CancellationTokenSource cancellationToken, BuildFunc<TContract, TDestination> factory = null)
        {
            Consumer = consumer;
            CancellationToken = cancellationToken;
            Factory = factory;
        }

        public virtual IMessagePipeline<TContract, T> Block<T>(Func<IPropagatorBlock<TDestination, T>> blockFunc)
        {
            return IsEmpty
                ? new MessagePipeline<TContract, T>(
                    Consumer,
                    CancellationToken,
                    () => new PipelinePropagator<TContract, T>((IPropagatorBlock<IMessage<TContract>, T>) blockFunc()))
                : new MessagePipeline<TContract, T>(
                    Consumer, 
                    CancellationToken,
                    () => Factory().Link(blockFunc()));
        }

        public virtual IMessagePipeline<TContract> Block(Func<ITargetBlock<TDestination>> blockFunc)
        {
            if (!IsEmpty)
                return new ClosedMessagePipeline<TContract>(
                    Consumer,
                    CancellationToken,
                    () => Factory().Link(blockFunc()));

            return new ClosedMessagePipeline<TContract>(
                Consumer,
                CancellationToken,
                () => CreateBufferPropagator(CancellationToken.Token).Link((ITargetBlock<IMessage<TContract>>) blockFunc()));
        }

        private static PipelinePropagator<TContract, IMessage<TContract>> CreateBufferPropagator(CancellationToken cancellationToken)
        {
            var buffer = new BufferBlock<IMessage<TContract>>(new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = 1,
                EnsureOrdered = true,
                CancellationToken = cancellationToken,
            });
                
            return new PipelinePropagator<TContract, IMessage<TContract>>(buffer, buffer);   
        }

        public virtual PipelinePropagator<TContract> Build(ICompletionSource completion)
        {
            var result = Factory?.Invoke() ?? throw new InvalidOperationException("Pipeline is empty");
            
            completion.Add(() =>
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