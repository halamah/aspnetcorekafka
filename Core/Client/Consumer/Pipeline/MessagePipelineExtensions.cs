using System;
using System.Runtime.ExceptionServices;
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
            Func<TDestination, Task> handler,
            Failure policy = Failure.Retry)
        {
            const int retryDelay = 10000;
            
            return pipeline.Block(() => new TransformBlock<TDestination, TDestination>(async x =>
                {
                    var count = 0;
                    
                    while (true)
                    {
                        try
                        {
                            await handler(x);
                            break;
                        }
                        catch (Exception e)
                        {
                            pipeline.Consumer.Logger.LogError(e, "Message handler failure");

                            if (policy == Failure.Skip)
                                break;

                            var delay = (int) Math.Pow(2, count) * retryDelay;
                            
                            if(TimeSpan.FromMilliseconds(delay) > TimeSpan.FromMinutes(5))
                                ExceptionDispatchInfo.Capture(e).Throw();

                            await Task.Delay(delay);
                        }
                        finally
                        {
                            ++count;
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

        public static IMessagePipeline<TContract, IMessage<TContract>> AsParallel<TContract>(
            this IMessagePipeline<TContract, IMessage<TContract>> pipeline,
            By by = By.Partition,
            int degreeOfParallelism = -1)
        {
            if(by != By.Partition)
                throw new ArgumentException("Only parallel By.Partition supported");
            
            if(degreeOfParallelism == 0)
                throw new ArgumentException("DegreeOfParallelism cannot be zero");
            
            return degreeOfParallelism == 1
                ? pipeline
                : new ParallelMessagePipeline<TContract, IMessage<TContract>>(pipeline, by, degreeOfParallelism);
        }

        public static IMessagePipeline<TSource, TDestination> Buffer<TSource, TDestination>(
            this IMessagePipeline<TSource, TDestination> pipeline, 
            int size)
        {
            if (size <= 1)
                throw new ArgumentException("Buffer size must be greater than 1");

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
                throw new ArgumentException("Buffer size must be greater than 1");

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
            
                batch.LinkTo(transform, new DataflowLinkOptions {PropagateCompletion = true });
                
                return DataflowBlock.Encapsulate(batch, transform);
            });
        }

        public static IMessagePipeline<TContract, IMessage<TContract>> Message<TContract>(this IKafkaConsumer consumer)
            => new MessagePipeline<TContract, IMessage<TContract>>(consumer);

        public static IMessageSubscription Subscribe<TContract, T>(
            this IMessagePipeline<TContract, T> pipeline,
            SourceOptions options = null) where TContract : class => pipeline.Subscribe(null, options);
        
        public static IMessageSubscription Subscribe<TContract>(
            this IMessagePipelineSource<TContract> pipeline,
            string topic,
            SourceOptions options = null)
        {
            var target = pipeline.BuildTarget();
            
            return pipeline.Consumer.Subscribe<TContract>(
                string.IsNullOrEmpty(topic) ? TopicDefinition.FromType<TContract>().Topic : topic, 
                x => target.SendAsync(x), options);
        }

        public static IObservable<T> AsObservable<TContract, T>(this IMessagePipeline<TContract, T> pipeline)
            => pipeline.SubscribeObservable(null);
        
        public static IObservable<T> SubscribeObservable<TContract, T>(
            this IMessagePipeline<TContract, T> pipeline,
            SourceOptions options = null) => pipeline.SubscribeObservable(null, options);
        
        public static IObservable<T> SubscribeObservable<TContract, T>(
            this IMessagePipeline<TContract, T> pipeline, 
            string topic = null,
            SourceOptions options = null)
        {
            pipeline = pipeline.IsEmpty ? pipeline.Buffer(1) : pipeline;

            var propagator = pipeline.Build();
            
            pipeline.Consumer.Subscribe<TContract>(
                string.IsNullOrEmpty(topic) ? TopicDefinition.FromType<TContract>().Topic : topic,
                x => propagator.SendAsync(x),
                options);

            return propagator!.AsObservable();
        }
    }
}