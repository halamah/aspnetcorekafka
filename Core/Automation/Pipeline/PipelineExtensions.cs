using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation.Attributes;
using AspNetCore.Kafka.Data;
using Microsoft.Extensions.Logging;
using MoreLinq.Extensions;

namespace AspNetCore.Kafka.Automation.Pipeline
{
    public static class PipelineExtensions
    {
        public static IMessagePipeline<TSource, TDestination> Action<TSource, TDestination>(
            this IMessagePipeline<TSource, TDestination> pipeline,
            Action<TDestination> handler,
            Option flags = Option.None) where TDestination : ICommittable
        {
            return pipeline.Action(x =>
                {
                    handler(x);
                    return Task.CompletedTask;
                },
                flags);
        }

        public static IMessagePipeline<TContract, TDestination> Action<TContract, TDestination>(
            this IMessagePipeline<TContract, TDestination> pipeline,
            Func<TDestination, Task> handler,
            Option flags = Option.None) where TDestination : ICommittable
        {
            const int retryDelay = 10000;
            
            return pipeline.Block(() => new TransformBlock<TDestination, TDestination>(async message =>
                {
                    var count = 0;
                    var stopWatch = new Stopwatch();
                    
                    while (true)
                    {
                        if (flags.IsSet(Option.SkipNullMessages) && message is null)
                            break;
                        
                        try
                        {
                            try
                            {
                                stopWatch.Start();
                                await handler(message).ConfigureAwait(false);
                            }
                            finally
                            {
                                stopWatch.Stop();
                            }

                            try
                            {
                                pipeline.Consumer.Interceptors.ForEach(x => x.ConsumeAsync(new KafkaInterception
                                {
                                    Messages = message.Messages.Select(msg => new InterceptedMessage(msg)),
                                    Metrics = new InterceptionMetrics
                                    {
                                        ProcessingTime = stopWatch.Elapsed
                                    }
                                }));
                            }
                            catch(Exception e)
                            {
                                pipeline.Consumer.Log.LogError(e, "Message interceptor failure");
                            }
                            
                            break;
                        }
                        catch (Exception e)
                        {
                            var skipFailure = flags.IsSet(Option.SkipFailure);
                            var actualRetryDelay = Math.Min((int)Math.Pow(2, count) * retryDelay, 60 * 1000);

                            pipeline.Consumer.Log.LogError(e, 
                                "Message handler failure. Retry {RetryState}",
                                skipFailure ? "disabled" : $"in {actualRetryDelay} ms");
                            
                            pipeline.Consumer.Interceptors.ForEach(x => x.ConsumeAsync(new KafkaInterception
                            {
                                Exception = e,
                                Messages = message.Messages.Select(msg => new InterceptedMessage(msg)),
                                Metrics = new InterceptionMetrics
                                {
                                    ProcessingTime = stopWatch.Elapsed
                                }
                            }));
                            
                            if (skipFailure)
                                break;

                            await Task
                                .Delay(actualRetryDelay)
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
        
        public static IMessagePipeline<TContract, IMessage<TContract>> Where<TContract>(
            this IMessagePipeline<TContract, IMessage<TContract>> pipeline, Func<TContract, bool> predicate)
        {
            return pipeline.Block(() => new TransformManyBlock<IMessage<TContract>, IMessage<TContract>>(
                message => !predicate(message.Value)
                    ? Enumerable.Empty<IMessage<TContract>>()
                    : new[] {message},
                new ExecutionDataflowBlockOptions
                {
                    EnsureOrdered = true,
                    BoundedCapacity = 1
                }));
        }
        
        public static IMessagePipeline<TContract, IMessage<TContract>> Where<TContract>(
            this IMessagePipeline<TContract, IMessage<TContract>> pipeline, Func<TContract, Task<bool>> predicate)
        {
            return pipeline.Block(() => new TransformManyBlock<IMessage<TContract>, IMessage<TContract>>(
                async message => ! await predicate(message.Value).ConfigureAwait(false)
                    ? Enumerable.Empty<IMessage<TContract>>()
                    : new[] {message},
                new ExecutionDataflowBlockOptions
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
                throw new ArgumentException("Batch size must be greater than 1");

            return pipeline.Block(() =>
            {
                var batch = new BatchBlock<IMessage<T>>(size, new GroupingDataflowBlockOptions
                {
                    EnsureOrdered = true,
                    BoundedCapacity = size
                });

                var timer = time.TotalMilliseconds > 0
                    ? new Timer(_ => batch.TriggerBatch(), null, Timeout.Infinite, Timeout.Infinite)
                    : null;
                
                void Track() => timer?.Change(time, time);

                var transform = new TransformBlock<IMessage<T>[], IMessageEnumerable<T>>(x =>
                    {
                        Track();
                        return new KafkaMessageEnumerable<T>(x);
                    },
                    new ExecutionDataflowBlockOptions
                    {
                        EnsureOrdered = true,
                        BoundedCapacity = 1
                    });
            
                batch.LinkTo(transform, new DataflowLinkOptions {PropagateCompletion = true });
                
                Track();
                
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

        public static IObservable<IMessage<TContract>> AsObservable<TContract>(
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