using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Client.Consumer
{
    public static class MessagePipelineExtensions
    {
        public static IMessagePipeline<TSource, TDestination> Action<TSource, TDestination>(
            this IMessagePipeline<TSource, TDestination> pipeline,
            params Func<TDestination, Task>[] handlers)
        {
            return pipeline.Action(null, handlers);
        }
        
        public static IMessagePipeline<TSource, TDestination> Action<TSource, TDestination>(
            this IMessagePipeline<TSource, TDestination> pipeline,
            ILogger log,
            params Func<TDestination, Task>[] handlers)
        {
            return pipeline.Block(() => new TransformBlock<TDestination, TDestination>(async x =>
                {
                    foreach (var handler in handlers)
                    {
                        try
                        {
                            await handler(x);
                        }
                        catch (Exception e)
                        {
                            log?.LogError(e, "Message handler failure");
                        }
                    }

                    return x;
                },
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = 1,
                    EnsureOrdered = true
                }));
        }
        
        /*
        public static IMessagePipeline<IMessage<T>, IMessage<T>> Partitioned<T>(
            this IMessagePipeline<IMessage<T>, IMessage<T>> pipeline,
            int maxDegreeOfParallelism = -1)
        {
            if (maxDegreeOfParallelism == 1)
                return pipeline;
            
            ConcurrentDictionary<int, ITargetBlock<IMessage<T>>> streams = new();
            var chain = new MessagePipeline<IMessage<T>, IMessage<T>>(pipeline);

            pipeline.Block(() => new ActionBlock<IMessage<T>>(async x =>
                {
                    var id = maxDegreeOfParallelism < 0 ? x.Partition : x.Partition % maxDegreeOfParallelism;
                    var stream = streams.GetOrAdd(id, _ => chain.Build());
                    await stream.SendAsync(x);
                },
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = 1,
                    EnsureOrdered = true
                }));

            return chain;
        }*/

        public static IMessagePipeline<TSource, TDestination> Buffer<TSource, TDestination>(
            this IMessagePipeline<TSource, TDestination> pipeline, 
            int size)
        {
            if (size <= 1)
                throw new ArgumentException("Buffer size must be greater that 1");

            return pipeline.Block(() => new BufferBlock<TDestination>(new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = size,
                EnsureOrdered = true
            }));
        }
        
        public static IMessagePipeline<TSource, IMessageOffset> Commit<TSource>(
            this IMessagePipeline<TSource, IMessageOffset> pipeline)
        {
            return pipeline.Block(() => new TransformBlock<IMessageOffset, IMessageOffset>(x =>
                {
                    x.Commit(true);
                    return x;
                },
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = 1,
                    EnsureOrdered = true
                }));
        }

        public static IMessagePipeline<TSource, TResult> Select<TSource, TDestination, TResult>(
            this IMessagePipeline<TSource, TDestination> pipeline, Func<TDestination, TResult> transform)
        {
            return pipeline.Block(() => new TransformBlock<TDestination, TResult>(transform, new ExecutionDataflowBlockOptions
            {
                EnsureOrdered = true,
                BoundedCapacity = 1
            }));
        }
        
        public static IMessagePipeline<TSource, TResult> Select<TSource, TDestination, TResult>(
            this IMessagePipeline<TSource, TDestination> pipeline, Func<TDestination, Task<TResult>> transform)
        {
            return pipeline.Block(() => new TransformBlock<TDestination, TResult>(transform, new ExecutionDataflowBlockOptions
            {
                EnsureOrdered = true,
                BoundedCapacity = 1
            }));
        }
        
        public static IMessagePipeline<IMessage<T>, IMessageEnumerable<T>> Batch<T>(
            this IMessagePipeline<IMessage<T>, IMessage<T>> pipeline,
            int size,
            int time)
        {
            return pipeline.Batch(size, TimeSpan.FromMilliseconds(time));
        }
        
        public static IMessagePipeline<IMessage<T>, IMessageEnumerable<T>> Batch<T>(
            this IMessagePipeline<IMessage<T>, IMessage<T>> pipeline,
            int size, 
            TimeSpan time)
        {
            if (size <= 1)
                throw new ArgumentException("Buffer size must be greater that 1");

            return pipeline.Block(() =>
            {
                var batch = new BatchBlock<IMessage<T>>(size, new GroupingDataflowBlockOptions
                {
                    EnsureOrdered = true,
                    BoundedCapacity = size
                });

                var timer = time.TotalMilliseconds > 0
                    ? new Timer(_ => batch.TriggerBatch(), null, (int) time.TotalMilliseconds, Timeout.Infinite)
                    : null;

                var transform = new TransformBlock<IMessage<T>[], IMessageEnumerable<T>>(x =>
                    {
                        timer?.Change((int) time.TotalMilliseconds, Timeout.Infinite);
                        return new KafkaMessageEnumerable<T>(x);
                    },
                    new ExecutionDataflowBlockOptions
                    {
                        EnsureOrdered = true,
                        BoundedCapacity = 1
                    });
            
                batch.LinkTo(transform);
                
                return DataflowBlock.Encapsulate(batch, transform);
            });
        }

        public static IMessagePipeline<IMessage<TSource>, IMessage<TSource>> Pipeline<TSource>(this IKafkaConsumer consumer)
        {
            return new MessagePipeline<IMessage<TSource>, IMessage<TSource>>(consumer);
        }
        
        public static IMessageSubscription Subscribe<TSource>(
            this IMessagePipelineSource<IMessage<TSource>> pipeline,
            SubscriptionOptions options = null) where TSource : class
        {
            var block = pipeline.Build();
            
            return pipeline.Consumer.Subscribe<TSource>(
                TopicDefinition.FromType<TSource>().Topic,
                x => block.SendAsync(x),
                options);
        }
        
        public static IMessageSubscription Subscribe<TSource>(
            this IMessagePipelineSource<IMessage<TSource>> pipeline,
            string topic,
            SubscriptionOptions options = null) where TSource : class
        {
            var block = pipeline.Build();
            
            return pipeline.Consumer.Subscribe<TSource>(topic, x => block.SendAsync(x), options);
        }
    }
}