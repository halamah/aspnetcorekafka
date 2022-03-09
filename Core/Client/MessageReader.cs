using System;
using System.Dynamic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Client
{
    public class MessageReader<TKey, TValue, TContract>
    {
        private readonly ILogger _log;
        private readonly SubscriptionConfiguration _subscription;
        private readonly IConsumer<TKey, TValue> _consumer;
        private readonly RevokeHandler _revokeHandler;
        private readonly CancellationTokenSource _cancellationToken;
        private readonly IMessageSerializer<TValue> _deserializer;
        
        public MessageReader(ILogger log,
            SubscriptionConfiguration subscription,
            IMessageSerializer<TValue> deserializer,
            IConsumer<TKey, TValue> consumer, 
            RevokeHandler revokeHandler)
        {
            _log = log;
            _subscription = subscription;
            _consumer = consumer;
            _revokeHandler = revokeHandler;
            _deserializer = deserializer;
            _cancellationToken = subscription.Options.CancellationToken;
        }

        public IMessageSubscription Run(Func<IMessage<TContract>, CancellationToken, Task> handler)
        {
            var task = Task.Factory.StartNew(
                () => Handler(handler, _cancellationToken.Token),
                default,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
            
            return new MessageSubscription<TKey, TValue>(_consumer, _revokeHandler, _subscription.Topic, _cancellationToken, _log, task.Unwrap());
        }

        private async Task Handler(Func<IMessage<TContract>, CancellationToken, Task> handler, CancellationToken cancellationToken)
        {
            using var _ = _log.BeginScope(new {_consumer.Name, _subscription.Topic});
            
            _consumer.Subscribe(_subscription.Topic);
            
            _log.LogInformation("Started MessageReader {Topic}. Group: {GroupId}", _subscription.Topic, _subscription.Group);

            try
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    try
                    {
                        var raw = _consumer.Consume(cancellationToken);

                        var value = raw.Message.Value switch
                        {
                            null => default,
                            _ when typeof(TContract) == typeof(object) => (TContract) (object) _deserializer.Deserialize<ExpandoObject>(raw.Message.Value),
                            TContract x => x,
                            _ => _deserializer.Deserialize<TContract>(raw.Message.Value)
                        };
                        
                        var key = raw.Message?.Key?.ToString();
                        
                        IMessage<TContract> message = new KafkaMessage<TContract>(() => Commit(raw), () => Store(raw))
                        {
                            Value = value,
                            Partition = raw.Partition.Value,
                            Offset = raw.Offset.Value,
                            Key = key,
                            Topic = _subscription.Topic,
                            Group = _subscription.Group,
                            Name = _subscription.Options?.Name,
                            Timestamp = new DateTimeOffset(raw.Message.Timestamp.UtcDateTime, TimeSpan.Zero),
                            Headers = raw.Message.Headers?.ToDictionary(x => x.Key, x => x.GetValueBytes())
                        };
                        
                        await handler(message, _cancellationToken.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (ConsumeException e)
                    {
                        _log.LogError(e, "MessageReader failure: {Error}", e.Error.Reason);
                    }
                    catch (Exception e)
                    {
                        _log.LogError(e, "MessageReader failure: {Error}", e.Message);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _log.LogInformation("MessageReader requested to shut down");
            }
            catch (Exception e)
            {
                _log.LogError(e, "MessageReader fatal exception");
            }
            finally
            {
                _log.LogInformation("MessageReader close");
                // will close and revoke partitions
                _consumer.Close();
                _log.LogInformation("MessageReader shutdown");
            }   
        }
        
        private bool Store(ConsumeResult<TKey, TValue> result)
        {
            try
            {
                _consumer.StoreOffset(result);
            }
            catch (KafkaException e)
            {
                _log.LogError(e, "Store failure {Reason}", e.Error.Reason);
                return false;
            }
            catch (Exception e)
            {
                _log.LogError(e, "Store failure");
                return false;
            }

            return true;
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