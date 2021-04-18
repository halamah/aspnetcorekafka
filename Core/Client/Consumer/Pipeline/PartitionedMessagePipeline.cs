using System;
using System.Collections.Concurrent;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Client.Consumer.Pipeline
{
    internal class PartitionedMessagePipeline<TContract, TDestination> : MessagePipeline<TContract, TDestination>
    {
        private readonly IMessagePipeline<TContract, IMessage<TContract>> _sourcePipeline;
        private readonly int _maxDegreeOfParallelism;

        public PartitionedMessagePipeline(
            IMessagePipeline<TContract, IMessage<TContract>> sourcePipeline, 
            int maxDegreeOfParallelism,
            BuildFunc<TContract, TDestination> factory = null) : base(sourcePipeline.Consumer, factory)
        {
            _sourcePipeline = sourcePipeline;
            _maxDegreeOfParallelism = maxDegreeOfParallelism;
        }
        
        public override IMessagePipeline<TContract, T> Block<T>(Func<IPropagatorBlock<TDestination, T>> blockFunc)
        {
            var next = (MessagePipeline<TContract, T>) base.Block(blockFunc);
            return new PartitionedMessagePipeline<TContract, T>(_sourcePipeline, _maxDegreeOfParallelism, next.Factory);
        }

        public override IMessagePipelineSource<TContract> Block(Func<ITargetBlock<TDestination>> blockFunc)
        {
            var next = (MessagePipeline<TContract, TDestination>) base.Block(blockFunc);
            return new PartitionedMessagePipeline<TContract, TDestination>(
                _sourcePipeline,
                _maxDegreeOfParallelism,
                next.Factory);
        }

        public override ITargetBlock<IMessage<TContract>> Build()
        {
            var streams = new ConcurrentDictionary<int, ITargetBlock<IMessage<TContract>>>();

            return _sourcePipeline.Block(() => new ActionBlock<IMessage<TContract>>(async x =>
                    {
                        var partition = _maxDegreeOfParallelism < 0
                            ? x.Partition 
                            : x.Partition % _maxDegreeOfParallelism;
                        
                        await streams.GetOrAdd(partition, _ => base.Build()).SendAsync(x);
                    },
                    new ExecutionDataflowBlockOptions
                    {
                        BoundedCapacity = 1,
                        EnsureOrdered = true
                    }))
                .Build();
        }
    }
}