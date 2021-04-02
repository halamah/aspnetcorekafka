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
using Newtonsoft.Json;
using static LanguageExt.Prelude;

namespace AspNetCore.Kafka.Client
{
    internal class KafkaProducer : KafkaClient, IKafkaProducer
    {
        private readonly ILogger _logger;
        private readonly IProducer<string, string> _producer;
        private readonly IEnumerable<IMessageInterceptor> _interceptors;

        public KafkaProducer(
            IOptions<KafkaOptions> options, 
            ILogger<KafkaProducer> logger, 
            IHostEnvironment environment,
            IEnumerable<IMessageInterceptor> interceptors)
            : base(logger, options.Value, environment)
        {
            _logger = logger;
            _interceptors = interceptors;

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
                        Value = JsonConvert.SerializeObject(message, CoreExtensions.JsonSerializerSettings),
                        Key = key.ToString()
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
                var result = await TryAsync(
                        Task.WhenAll(_interceptors.Select(async x =>
                            await x.ProduceAsync(topic, key, message, exception)))).Try()
                    .ConfigureAwait(false);
                
                result.IfFail(x => _logger.LogError(x, "Produce  interceptor failure"));
            }
        }

        public int Flush(TimeSpan? timeout) => timeout is null
            ? _producer.Flush(TimeSpan.MaxValue)
            : _producer.Flush(timeout.Value);

        public void Dispose() => _producer?.Dispose();
    }
}
