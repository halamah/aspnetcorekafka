using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Client.Consumer
{
    public class MessageReaderTask<TKey, TValue, TContract> where TContract : class
    {
        private readonly IEnumerable<IMessageInterceptor> _interceptors;
        private readonly ILogger _log;
        private readonly IConsumer<TKey, TValue> _consumer;
        private readonly string _topic;
        private readonly int _buffer;
        private readonly CancellationTokenSource _cancellationToken = new();
        private readonly MessageParser<TKey, TValue> _parser;

        public MessageReaderTask(
            IEnumerable<IMessageInterceptor> interceptors,
            IMessageSerializer serializer,
            ILogger logger,
            IConsumer<TKey, TValue> consumer,
            string topic,
            int buffer)
        {
            _interceptors = interceptors.ToList();
            _log = logger;
            _consumer = consumer;
            _topic = topic;
            _buffer = buffer;
            _parser = new(serializer);
        }
        
        public IMessageSubscription Run(Func<IMessage<TContract>, Task> handler)
        {
            Task.Factory.StartNew(
                () => Handler(handler, _cancellationToken.Token),
                _cancellationToken.Token,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
            
            return new MessageSubscription<TKey, TValue>(_consumer, _topic, _cancellationToken, _log);
        }

        private async Task Handler(Func<IMessage<TContract>, Task> handler, CancellationToken token)
        {
            using var _ = _log.BeginScope(new
            {
                _consumer.Name,
                Topic = _topic,
            });

            _log.LogInformation("Started consuming");

            try
            {
                var action = new ActionBlock<IMessage<TContract>>(handler, new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = Math.Max(_buffer, 1)
                });
                    
                while (true)
                {
                    token.ThrowIfCancellationRequested();

                    IMessage<TContract> message = null;
                    Exception exception = null;

                    try
                    {
                        var raw = _consumer.Consume(token);
                        var value = _parser.Parse<TContract>(raw);
                        var key = raw.Message?.Key?.ToString();
                        
                        message = new KafkaMessage<TContract>(() => Commit(_consumer, raw))
                        {
                            Value = value,
                            Partition = raw.Partition.Value,
                            Offset = raw.Offset.Value,
                            Key = key,
                            Topic = _topic,
                        };
                        
                        await action.SendAsync(message, token);
                    }
                    catch (ConsumeException e)
                    {
                        exception = e;
                        _log.LogError(e, "Consume failure: {Reason}", e.Error.Reason);
                    }
                    catch (Exception e)
                    {
                        exception = e;
                        _log.LogError(e, "Consume failure");
                    }
                    finally
                    {
                        try { await Task.WhenAll(_interceptors.Select(async x => await x.ConsumeAsync(message, exception))).ConfigureAwait(false); }
                        catch (Exception e) { _log.LogError(e, "Consume  interceptor failure"); }
                    }
                }
            }
            catch (Exception e)
            {
                _log.LogError(e, "Consume interrupted");
            }
            finally
            {
                _consumer.Close();
                _log.LogInformation("Consume shutdown");
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