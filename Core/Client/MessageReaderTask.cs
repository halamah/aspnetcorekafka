using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Client
{
    public class MessageReaderTask<TKey, TValue, TContract>
    {
        private readonly IEnumerable<IMessageInterceptor> _interceptors;
        private readonly ILogger _log;
        private readonly IConsumer<TKey, TValue> _consumer;
        private readonly string _topic;
        private readonly CancellationTokenSource _cancellationToken = new();
        private readonly DefaultKafkaMessageParser _parser;
        private readonly TaskCompletionSource _shutdown = new();

        public MessageReaderTask(
            IEnumerable<IMessageInterceptor> interceptors,
            IJsonMessageSerializer jsonSerializer,
            IAvroMessageSerializer avroSerializer,
            ILogger logger,
            IConsumer<TKey, TValue> consumer,
            string topic)
        {
            _interceptors = interceptors.ToList();
            _log = logger;
            _consumer = consumer;
            _topic = topic;
            _parser = new(jsonSerializer, avroSerializer);
        }

        public MessageReaderTask(SubscriptionConfiguration subscription, IConsumer<TKey, TValue> consumer)
        {
            var provider = subscription.Scope.ServiceProvider;
            
            _interceptors = provider.GetServices<IMessageInterceptor>().ToList();
            _log = provider.GetRequiredService<ILogger<KafkaConsumer>>();
            _consumer = consumer;
            _topic = subscription.Topic;
            _parser = new(
                provider.GetRequiredService<IJsonMessageSerializer>(), 
                provider.GetRequiredService<IAvroMessageSerializer>());
        }

        public IMessageSubscription Run(Func<IMessage<TContract>, Task> handler)
        {
            Task.Factory.StartNew(
                () => Handler(handler, _cancellationToken.Token),
                default,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
            
            return new MessageSubscription<TKey, TValue>(_consumer, _topic, _cancellationToken, _log, _shutdown);
        }

        private async Task Handler(Func<IMessage<TContract>, Task> handler, CancellationToken token)
        {
            using var _ = _log.BeginScope(new
            {
                _consumer.Name,
                Topic = _topic,
            });
            
            _consumer.Subscribe(_topic);

            _log.LogInformation("Started consuming");

            try
            {
                while (true)
                {
                    token.ThrowIfCancellationRequested();

                    IMessage<TContract> message = null;
                    Exception exception = null;

                    try
                    {
                        var raw = _consumer.Consume(token);
                        
                        var value = _parser.Parse<TContract>(raw.Message.Value);
                        var key = raw.Message?.Key?.ToString();
                        
                        message = new KafkaMessage<TContract>(() => Commit(raw))
                        {
                            Value = value,
                            Partition = raw.Partition.Value,
                            Offset = raw.Offset.Value,
                            Key = key,
                            Topic = _topic,
                        };
                        
                        await handler(message).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (ConsumeException e)
                    {
                        exception = e;
                        _log.LogError(e, "Consumer failure: {Reason}", e.Error.Reason);
                    }
                    catch (Exception e)
                    {
                        exception = e;
                        _log.LogError(e, "Consumer failure");
                    }
                    finally
                    {
                        try
                        {
                            await Task.WhenAll(
                                    _interceptors.Select(async x => await x.ConsumeAsync(message, exception)))
                                .ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            _log.LogError(e, "Consumer  interceptor failure");
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _log.LogInformation("Consumer requested to shut down");
            }
            catch (Exception e)
            {
                _log.LogError(e, "Consumer fatal exception");
            }
            finally
            {
                _consumer.Close();
                _log.LogInformation("Consumer shutdown");
                _shutdown.SetResult();
            }   
        }
        
        private bool Commit(ConsumeResult<TKey, TValue> result)
        {
            try
            {
                _consumer.Commit(result);
            }
            catch (KafkaException e)
            {
                _log.LogError(e, "Commit failure {Reason}", e.Error.Reason);
                return false;
            }
            catch (Exception e)
            {
                _log.LogError(e, "Commit failure");
                return false;
            }

            return true;
        }
    }
}