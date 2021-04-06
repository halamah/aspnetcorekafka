using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Options;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AspNetCore.Kafka.Client
{
    internal class KafkaProducer : KafkaClient, IKafkaProducer
    {
        private readonly ILogger _log;
        private readonly IProducer<string, string> _producer;
        private readonly IEnumerable<IMessageInterceptor> _interceptors;
        private readonly IMessageSerializer _serializer;

        public KafkaProducer(
            IOptions<KafkaOptions> options, 
            ILogger<KafkaProducer> logger, 
            IHostEnvironment environment,
            IEnumerable<IMessageInterceptor> interceptors,
            IMessageSerializer serializer)
            : base(logger, options.Value, environment)
        {
            _log = logger;
            _interceptors = interceptors;
            _serializer = serializer;

            if(string.IsNullOrEmpty(options.Value?.Server))
                throw new ArgumentException("Kafka connection string is not defined");
            
            _producer = new ProducerBuilder<string, string>(new ProducerConfig(options.Value.Configuration?.Producer ?? new())
                {
                    BootstrapServers = options.Value.Server,
                })
                .SetLogHandler(LogHandler)
                .Build();
        }

        public async Task ProduceAsync<T>(string topic, object key, T message)
        {
            Exception exception = null;

            try
            {
                using var _ = Logger.BeginScope(new {Topic = topic});

                topic = ExpandTemplate(topic);

                await _producer.ProduceAsync(topic, new Message<string, string>
                    {
                        Value = _serializer.Serialize(message),
                        Key = key?.ToString()
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
                try { await Task.WhenAll(_interceptors.Select(async x => await x.ProduceAsync(topic, key, message, exception))).ConfigureAwait(false); }
                catch (Exception e) { _log.LogError(e, "Produce interceptor failure"); }
            }
        }

        public int Flush(TimeSpan? timeout) => timeout is null
            ? _producer.Flush(TimeSpan.MaxValue)
            : _producer.Flush(timeout.Value);

        public void Dispose() => _producer?.Dispose();
    }
}
