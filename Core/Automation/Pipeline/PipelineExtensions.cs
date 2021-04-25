using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;
using Microsoft.Extensions.Logging;
using MoreLinq;

namespace AspNetCore.Kafka.Automation.Pipeline
{
    public static class PipelineExtensions
    {
        public static IMessagePipeline<TSource, TDestination> Action<TSource, TDestination>(
            this IMessagePipeline<TSource, TDestination> pipeline,
            Action<TDestination> handler,
            Failure policy = default) where TDestination : ICommittable
        {
            return pipeline.Action(x =>
                {
                    handler(x);
                    return Task.CompletedTask;
                },
                policy);
        }

        public static IMessagePipeline<TSource, TDestination> Action<TSource, TDestination>(
            this IMessagePipeline<TSource, TDestination> pipeline,
            Func<TDestination, Task> handler,
            Failure policy = default) where TDestination : ICommittable
        {
            const int retryDelay = 10000;
            
            return pipeline.Block(() => new TransformBlock<TDestination, TDestination>(async message =>
                {
                    var count = 0;
                    
                    while (true)
                    {
                        try
                        {
                            await handler(message).ConfigureAwait(false);
                            break;
                        }
                        catch (Exception e)
                        {
                            pipeline.Consumer.Log.LogError(e, "Message handler failure");
                            pipeline.Consumer.Interceptors.ForEach(x => x.ConsumeAsync(message, e));

                            if (policy == Failure.Skip)
                                break;

                            await Task.Delay(
                                    Math.Min((int) Math.Pow(2, count) * retryDelay, 60 * 1000))
                                .ConfigureAwait(false);
                        }
                        finally
                        {
                            ++count;
                        }
                    }

                    return message;
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
        
        public static IMessagePipeline<TSource, ICommittable> Commit<TSource>(
            this IMessagePipeline<TSource, ICommittable> pipeline)
        {
            return pipeline.Block(() => new TransformBlock<ICommittable, ICommittable>(x =>
                {
                    x.Commit();
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
            this IMessagePipeline<TContract> pipeline,
            string topic,
            SourceOptions options = null)
        {
            var propagator = pipeline.Build(pipeline.Consumer);
            
            pipeline.Consumer.RegisterCompletionSource(() =>
            {
                propagator.Input.Complete();
                return propagator.Output.Completion;
            });
            
            return pipeline.Consumer.Subscribe<TContract>(
                string.IsNullOrEmpty(topic) ? TopicDefinition.FromType<TContract>().Topic : topic, 
                x => propagator.Input.SendAsync(x), options);
        }

        public static IObservable<IMessage<TContract>> AsObservable<TContract>(this IKafkaConsumer consumer)
            => consumer.SubscribeObservable<TContract>(null);
        
        public static IObservable<IMessage<TContract>> SubscribeObservable<TContract>(
            this IKafkaConsumer consumer,
            SourceOptions options = null) => consumer.SubscribeObservable<TContract>(null, options);
        
        public static IObservable<IMessage<TContract>> SubscribeObservable<TContract>(
            this IKafkaConsumer consumer, 
            string topic = null,
            SourceOptions options = null)
        {
            var buffer = new BufferBlock<IMessage<TContract>>(new DataflowBlockOptions
            {
                BoundedCapacity = 1,
                EnsureOrdered = true,
            });
            
            consumer.RegisterCompletionSource(() =>
            {
                buffer.Complete();
                return buffer.Completion;
            });
            
            consumer.Subscribe<TContract>(
                string.IsNullOrEmpty(topic) ? TopicDefinition.FromType<TContract>().Topic : topic,
                x => buffer.SendAsync(x),
                options);

            return buffer.AsObservable();
        }
    }
}