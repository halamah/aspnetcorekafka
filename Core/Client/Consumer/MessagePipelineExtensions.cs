using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Groups;
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

        public static IMessagePipeline<IMessage<T>, IGroupedMessage<T>> GroupBy<T>(
            this IMessagePipeline<IMessage<T>, IMessage<T>> pipeline,
            Func<IGroupingBehaviourFactory<T>, IGroupingBehaviour<T>> by,
            int degreeOfParallelism = 1)
        {
            var factory = new GroupingBehaviourFactory<T>(degreeOfParallelism);
            var parallelBehaviour = by?.Invoke(factory) ?? factory.ByPartition();

            return pipeline.Block(new TransformBlock<IMessage<T>, IGroupedMessage<T>>(
                x => new GroupedMessage<T>(x, parallelBehaviour.SelectGroup(x)), new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = 1,
                    EnsureOrdered = true
                }));
        }

        public static IMessagePipeline<TSource, IGroupedMessage<TDestination>>
            Action<TSource, TDestination>(
                this IMessagePipeline<TSource, IGroupedMessage<TDestination>> pipeline,
                params Func<IMessage<TDestination>, Task>[] handlers)
        {
            var dict = new ConcurrentDictionary<int, SemaphoreSlim>();
            var block = new TransformBlock<IGroupedMessage<TDestination>, IGroupedMessage<TDestination>>(
                async x =>
                {
                    var s = dict.GetOrAdd(x.Group, k => new SemaphoreSlim(1));
                    await s.WaitAsync();
                    foreach (var handler in handlers)
                    {
#pragma warning disable 4014
                        Task.Run(() => handler(x).ContinueWith(_ => s.Release()));
#pragma warning restore 4014
                    }

                    return x;
                },
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = 1,
                    EnsureOrdered = true,
                    MaxDegreeOfParallelism = 100
                });

            return pipeline.Block(block);
        }

        public static IMessagePipeline<TSource, TDestination> Action<TSource, TDestination>(
            this IMessagePipeline<TSource, TDestination> pipeline,
            ILogger log,
            params Func<TDestination, Task>[] handlers)
        {
            return pipeline.Block(new TransformBlock<TDestination, TDestination>(async x =>
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

        public static IMessagePipeline<TSource, TDestination> Buffer<TSource, TDestination>(
            this IMessagePipeline<TSource, TDestination> pipeline,
            int size)
        {
            if (size <= 1)
                throw new ArgumentException("Buffer size must be greater that 1");

            return pipeline.Block(new TransformBlock<TDestination, TDestination>(x => x,
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = size,
                    EnsureOrdered = true
                }));
        }

        public static IMessagePipeline<TSource, IMessageOffset> Commit<TSource>(
            this IMessagePipeline<TSource, IMessageOffset> pipeline)
        {
            return pipeline.Block(new TransformBlock<IMessageOffset, IMessageOffset>(x =>
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

        /*
        public static IMessagePipeline<TSource, IEnumerable<TDestination>> Batch<TSource, TDestination>(
            this IMessagePipeline<TSource, TDestination> pipeline,
            int size, 
            int time = 0)
        {
            if (size <= 1)
                throw new ArgumentException("Buffer size must be greater that 1");
            
            var batch = new BatchBlock<TDestination>(size, new GroupingDataflowBlockOptions
            {
                EnsureOrdered = true,
                BoundedCapacity = size
            });
            
            var timer = new Timer(_ => batch.TriggerBatch(), null, time, Timeout.Infinite);

            var transform = new TransformBlock<TDestination[], TDestination[]>(x =>
                {
                    timer.Change(time, Timeout.Infinite);
                    return x;
                },
                new ExecutionDataflowBlockOptions
                {
                    EnsureOrdered = true,
                    BoundedCapacity = 1
                });
            
            batch.LinkTo(transform);

            return pipeline.Block(DataflowBlock.Encapsulate(batch, transform));
        }*/

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

            var batch = new BatchBlock<IMessage<T>>(size, new GroupingDataflowBlockOptions
            {
                EnsureOrdered = true,
                BoundedCapacity = size,
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

            return pipeline.Block(DataflowBlock.Encapsulate(batch, transform));
        }
    }
}