using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Client.Consumer.Pipeline
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

        public static IMessagePipeline<TContract, IMessage<TContract>> Partitioned<TContract>(
            this IMessagePipeline<TContract, IMessage<TContract>> pipeline,
            int maxDegreeOfParallelism = -1)
        {
            if(maxDegreeOfParallelism == 0)
                throw new ArgumentException("MaxDegreeOfParallelism cannot be zero");
            
            return maxDegreeOfParallelism == 1
                ? pipeline
                : new PartitionedMessagePipeline<TContract, IMessage<TContract>>(pipeline, maxDegreeOfParallelism);
        }

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
        
        public static IMessagePipeline<T, IMessageEnumerable<T>> Batch<T>(
            this IMessagePipeline<T, IMessage<T>> pipeline,
            int size,
            int time)
        {
            return pipeline.Batch(size, TimeSpan.FromMilliseconds(time));
        }
        
        public static IMessagePipeline<T, IMessageEnumerable<T>> Batch<T>(
            this IMessagePipeline<T, IMessage<T>> pipeline,
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

        public static IMessagePipeline<TContract, IMessage<TContract>> Pipeline<TContract>(this IKafkaConsumer consumer)
        {
            return new MessagePipeline<TContract, IMessage<TContract>>(consumer);
        }
        
        public static IMessageSubscription Subscribe<TContract>(
            this IMessagePipelineSource<TContract> pipeline,
            SubscriptionOptions options = null) where TContract : class
        {
            var block = pipeline.Build();
            
            return pipeline.Consumer.Subscribe<TContract>(
                TopicDefinition.FromType<TContract>().Topic,
                x => block.SendAsync(x),
                options);
        }
        
        public static IMessageSubscription Subscribe<TContract>(
            this IMessagePipelineSource<TContract> pipeline,
            string topic,
            SubscriptionOptions options = null) where TContract : class
        {
            var block = pipeline.Build();
            
            return pipeline.Consumer.Subscribe<TContract>(
                string.IsNullOrEmpty(topic) ? TopicDefinition.FromType<TContract>().Topic : topic, 
                x => block.SendAsync(x), options);
        }
    }
}