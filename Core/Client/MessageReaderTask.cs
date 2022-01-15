using System;
using System.Dynamic;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Client
{
    public class MessageReaderTask<TKey, TValue, TContract>
    {
        private readonly ILogger _log;
        private readonly SubscriptionConfiguration _subscription;
        private readonly IConsumer<TKey, TValue> _consumer;
        private readonly CancellationTokenSource _cancellationToken = new();
        private readonly TaskCompletionSource _shutdown = new();
        private readonly IMessageSerializer<TValue> _deserializer;
        
        public MessageReaderTask(
            ILogger log,
            SubscriptionConfiguration subscription, 
            IMessageSerializer<TValue> deserializer,
            IConsumer<TKey, TValue> consumer)
        {
            _log = log;
            _subscription = subscription;
            _consumer = consumer;
            _deserializer = deserializer;
        }

        public IMessageSubscription Run(Func<IMessage<TContract>, Task> handler)
        {
            Task.Factory.StartNew(
                () => Handler(handler, _cancellationToken.Token),
                default,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
            
            return new MessageSubscription<TKey, TValue>(_consumer, _subscription.Topic, _cancellationToken, _log, _shutdown);
        }

        private async Task Handler(Func<IMessage<TContract>, Task> handler, CancellationToken token)
        {
            using var _ = _log.BeginScope(new {_consumer.Name, _subscription.Topic});
            
            _consumer.Subscribe(_subscription.Topic);

            _log.LogInformation("Started consuming {Topic}. Group: {GroupId}", _subscription.Topic, _subscription.Group);

            try
            {
                while (true)
                {
                    token.ThrowIfCancellationRequested();

                    try
                    {
                        var raw = _consumer.Consume(token);

                        var value = raw.Message.Value switch
                        {
                            null => default,
                            _ when typeof(TContract) == typeof(object) => (TContract) (object) _deserializer.Deserialize<ExpandoObject>(raw.Message.Value),
                            TContract x => x,
                            _ => _deserializer.Deserialize<TContract>(raw.Message.Value)
                        };
                        
                        var key = raw.Message?.Key?.ToString();
                        
                        IMessage<TContract> message = new KafkaMessage<TContract>(() => Commit(raw))
                        {
                            Value = value,
                            Partition = raw.Partition.Value,
                            Offset = raw.Offset.Value,
                            Key = key,
                            Topic = _subscription.Topic,
                            Group = _subscription.Group,
                            Name = _subscription.Options?.Name,
                        };
                        
                        await handler(message).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (ConsumeException e)
                    {
                        _log.LogError(e, "Consumer failure: {Reason}", e.Error.Reason);
                    }
                    catch (Exception e)
                    {
                        _log.LogError(e, "Consumer failure");
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