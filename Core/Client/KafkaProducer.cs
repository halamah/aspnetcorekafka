using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Options;
using AspNetCore.Kafka.Utility;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AspNetCore.Kafka.Client
{
    internal class KafkaProducer : IKafkaProducer
    {
        private readonly ILogger _log;
        private readonly IKafkaEnvironment _environment;
        private readonly IEnumerable<IMessageInterceptor> _interceptors;
        private readonly IProducer<string, string> _producer;
        private readonly IKafkaMessageJsonSerializer _serializer;

        public KafkaProducer(
            IOptions<KafkaOptions> options, 
            ILogger<KafkaProducer> log, 
            IKafkaEnvironment environment,
            IEnumerable<IMessageInterceptor> interceptors,
            IKafkaMessageJsonSerializer serializer, 
            IServiceProvider provider)
        {
            _log = log;
            _environment = environment;
            _interceptors = interceptors;
            _serializer = serializer;

            if(string.IsNullOrEmpty(options.Value?.Server))
                throw new ArgumentException("Kafka connection string is not defined");

            // https://github.com/confluentinc/confluent-kafka-dotnet/issues/423
            // using (var producer = new Producer<string, GenericRecord>(producerConfig, new AvroSerializer<string>(), new AvroSerializer<GenericRecord>()))
            
            _producer = provider.GetService<IKafkaClientFactory>()?.CreateProducer<string, string>(options.Value);

            if (_producer is null)
                throw new ArgumentNullException(nameof(_producer), "Producer build failure");
        }

        async Task IKafkaProducer.ProduceInternalAsync<T>(string topic, T message, string key)
        {
            Exception exception = null;

            try
            {
                using var _ = _log.BeginScope(new {Topic = topic});

                topic = _environment.ExpandTemplate(topic);

                await _producer.ProduceAsync(topic, new Message<string, string>
                    {
                        Value = _serializer.Serialize(message),
                        Key = key
                    })
                    .ConfigureAwait(false);
            }
            catch (Exception e)
            {
                exception = e;
                throw;
            }
            finally
            {
                try
                {
                    await Task.WhenAll(_interceptors.Select(async x => await x.ProduceAsync(new KafkaInterception
                        {
                            Exception = exception,
                            Messages = new[]
                            {
                                new InterceptedMessage
                                {
                                    Topic = topic,
                                    Key = key,
                                    Value = message,
                                }
                            }
                        })))
                        .ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    _log.LogError(e, "Produce interceptor failure");
                }
            }
        }

        public int Flush(TimeSpan? timeout) => timeout is null
            ? _producer.Flush(TimeSpan.MaxValue)
            : _producer.Flush(timeout.Value);

        ILogger IKafkaClient.Log => _log;

        IEnumerable<IMessageInterceptor> IKafkaClient.Interceptors => _interceptors;
        
        public void Dispose() => _producer?.Dispose();
    }
}
