using System;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Client.Consumer
{
    internal delegate IPropagatorBlock<TSource, TDestination> BuildFunc<in TSource, out TDestination>();
    
    internal class MessagePipeline<TSource, TDestination> : IMessagePipeline<TSource, TDestination>
    {
        private readonly BuildFunc<TSource, TDestination>  _build;

        public MessagePipeline(IKafkaConsumer consumer, BuildFunc<TSource, TDestination> build = null)
        {
            Consumer = consumer;
            _build = build;
        }

        public IMessagePipeline<TSource, T> Block<T>(Func<IPropagatorBlock<TDestination, T>> blockFunc)
        {
            if (_build is null)
                return new MessagePipeline<TSource, T>(Consumer, () => (IPropagatorBlock<TSource, T>) blockFunc());

            return new MessagePipeline<TSource, T>(Consumer, () =>
            {
                var pipeline = _build();
                var block = blockFunc();
                pipeline.LinkTo(block);
                return DataflowBlock.Encapsulate(pipeline, block);
            });
        }

        IMessagePipelineSource<TSource> IMessagePipeline<TSource, TDestination>.Block(Func<ITargetBlock<TDestination>> blockFunc)
        {
            if (_build is null)
                throw new InvalidOperationException("Cannot attach null target to empty pipeline.");
            
            return new MessagePipeline<TSource, TDestination>(Consumer, () =>
            {
                var pipeline = _build();
                var block = blockFunc();
                pipeline.LinkTo(block);
                return pipeline;
            });
        }

        public ITargetBlock<TSource> Build()
        {
            if (_build is null)
                throw new InvalidOperationException("Pipeline is empty.");
            
            var pipeline = _build();
            
            pipeline.LinkTo(DataflowBlock.NullTarget<TDestination>());

            return pipeline;
        }
        
        public IKafkaConsumer Consumer { get; }
    }
}