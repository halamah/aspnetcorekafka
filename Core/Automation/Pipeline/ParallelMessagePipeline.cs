using System;
using System.Collections.Concurrent;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;

namespace AspNetCore.Kafka.Automation.Pipeline
{
    internal class ParallelMessagePipeline<TContract, TDestination> : MessagePipeline<TContract, TDestination>
    {
        private readonly IMessagePipeline<TContract, IMessage<TContract>> _sourcePipeline;
        private readonly By _by;
        private readonly int _degreeOfParallelism;

        public ParallelMessagePipeline(
            IMessagePipeline<TContract, IMessage<TContract>> sourcePipeline, 
            By by,
            int degreeOfParallelism,
            BuildFunc<TContract, TDestination> factory = null) : base(sourcePipeline.Consumer, factory)
        {
            if(by != By.Partition)
                throw new ArgumentException("Only parallel By.Partition supported");
            
            _sourcePipeline = sourcePipeline;
            _by = by;
            _degreeOfParallelism = degreeOfParallelism;
        }
        
        public override IMessagePipeline<TContract, T> Block<T>(Func<IPropagatorBlock<TDestination, T>> blockFunc)
        {
            var next = (MessagePipeline<TContract, T>) base.Block(blockFunc);
            return new ParallelMessagePipeline<TContract, T>(_sourcePipeline, _by, _degreeOfParallelism, next.Factory);
        }

        public override IMessagePipeline<TContract> Block(Func<ITargetBlock<TDestination>> blockFunc)
        {
            var next = (MessagePipeline<TContract, TDestination>) base.Block(blockFunc);
            
            return new ParallelMessagePipeline<TContract, TDestination>(
                _sourcePipeline,
                _by,
                _degreeOfParallelism,
                next.Factory);
        }

        public override PipelinePropagator<TContract> Build(ICompletionSource completion)
        {
            var streams = new ConcurrentDictionary<int, ITargetBlock<IMessage<TContract>>>();

            return _sourcePipeline.Block(() => new ActionBlock<IMessage<TContract>>(async x =>
                    {
                        var partition = _degreeOfParallelism < 0
                            ? x.Partition 
                            : x.Partition % _degreeOfParallelism;

                        await streams.GetOrAdd(
                                partition, 
                                _ => base.Build(completion).Input).SendAsync(x).ConfigureAwait(false);
                    },
                    new ExecutionDataflowBlockOptions
                    {
                        BoundedCapacity = 1,
                        EnsureOrdered = true
                    }))
                .Build(completion);
        }
    }
}