using System;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Options;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace AspNetCore.Kafka.Core
{
    internal class KafkaProducer : KafkaClient, IKafkaProducer
    {
        private readonly IProducer<string, string> _producer;

        public KafkaProducer(IOptions<KafkaOptions> options, ILogger<KafkaProducer> logger, IHostEnvironment environment)
            : base(logger, options.Value, environment)
        {
            _producer = new ProducerBuilder<string, string>(new ProducerConfig(options.Value.Configuration.Producer)
                {
                    BootstrapServers = options.Value.Server,
                })
                .SetLogHandler(LogHandler)
                .Build();
        }

        public async Task ProduceAsync<T>(string topic, object key, T message)
        {
            using var _ = Logger.BeginScope(new {Topic = topic});

                topic = ExpandTemplate(topic);

                await _producer.ProduceAsync(topic, new Message<string, string>
                    {
                        Value = JsonConvert.SerializeObject(message,
                            Kafka.CoreExtensions.JsonSerializerSettings),
                        Key = key.ToString()
                    })
                    .ConfigureAwait(false);
        }

        public int Flush(TimeSpan? timeout) => timeout is null
            ? _producer.Flush(TimeSpan.MaxValue)
            : _producer.Flush(timeout.Value);

        public void Dispose() => _producer?.Dispose();
    }
}
