using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using static LanguageExt.Prelude;

namespace AspNetCore.Kafka.Core.Consumer
{
    public class SubscriptionWorker<TKey, TValue, TContract> where TContract : class
    {
        private readonly IServiceScope _scope;
        private readonly ILogger _logger;
        private readonly IConsumer<TKey, TValue> _consumer;
        private readonly bool _manualCommit;
        private readonly string _topic;
        private readonly AutoResetEvent _signal = new(false);
        private readonly CancellationTokenSource _cancellationToken = new();

        public SubscriptionWorker(
            IServiceScope scope,
            ILogger logger,
            IConsumer<TKey, TValue> consumer,
            bool manualCommit,
            string topic)
        {
            _scope = scope;
            _logger = logger;
            _consumer = consumer;
            _manualCommit = manualCommit;
            _topic = topic;
        }
        
        public IMessageSubscription Run(Func<IMessage<TContract>, Task> handler)
        {
            Task.Factory.StartNew(
                () => Handler(handler, _cancellationToken.Token),
                _cancellationToken.Token,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
            
            return new MessageSubscription<TKey, TValue>(_consumer, _topic, _cancellationToken, _signal, _logger);
        }

        private async Task Handler(Func<IMessage<TContract>, Task> handler, CancellationToken token)
        {
            using var _ = _logger.BeginScope(new
            {
                _consumer.Name,
                Topic = _topic,
            });

            _logger.LogInformation("Started consuming");

            var parser = new MessageParser<TKey, TValue>();

            try
            {
                var interceptors = _scope.ServiceProvider.GetServices<IMessageInterceptor>().ToList();
                    
                while (true)
                {
                    token.ThrowIfCancellationRequested();

                    IMessage<TContract> message = null;
                    Exception exception = null;
                        
                    try
                    {
                        var raw = _consumer.Consume(token);
                        var value = parser.Parse<TContract>(raw);
                        var key = raw.Message?.Key?.ToString();
                        
                        message = new MessagePayload<TContract>(() => Commit(_consumer, raw))
                        {
                            Value = value,
                            Partition = raw.Partition.Value,
                            Offset = raw.Offset.Value,
                            Key = key,
                            Topic = _topic,
                        };

                        var either = await TryAsync(handler(message)).Try().ConfigureAwait(false);
                            
                        either.IfFail(x => _logger.LogError(x, "Consumer handler failure"));
                        either.IfSucc(_ =>
                        {
                            if (_manualCommit)
                                message.Commit();
                        });
                    }
                    catch (ConsumeException e)
                    {
                        exception = e;
                        _logger.LogError(e, "Consumer failure: {Reason}", e.Error.Reason);
                    }
                    catch (Exception e)
                    {
                        exception = e;
                        _logger.LogError(e, "Consumer failure");
                    }
                    finally
                    {
                        var result = await TryAsync(
                                Task.WhenAll(interceptors.Select(async x =>
                                    await x.InterceptAsync(message, exception)))).Try()
                            .ConfigureAwait(false);

                        result.IfFail(x => _logger.LogError(x, "Consumer interceptor failure"));
                        
                        _signal.Set();
                    }
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Consumer interrupted");
            }
            finally
            {
                _consumer.Close();
                _logger.LogInformation("Consumer shutdown");
            }   
        }
        
        private bool Commit(IConsumer<TKey, TValue> consumer, ConsumeResult<TKey, TValue> result)
        {
            try
            {
                consumer.Commit(result);
            }
            catch (KafkaException e)
            {
                _logger.LogError(e, "Commit failure {Reason}", e.Error.Reason);
                return false;
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Commit failure");
                return false;
            }

            return true;
        }
    }
}