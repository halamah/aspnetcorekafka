using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Client
{
    public class KafkaHostedService : IHostedService
    {
        private readonly IKafkaConsumer _consumer;
        private readonly ILogger<KafkaHostedService> _log;

        public KafkaHostedService(IKafkaConsumer consumer, ILogger<KafkaHostedService> log)
        {
            _consumer = consumer;
            _log = log;
        }

        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            _log.LogInformation("Kafka consumer termination");
            
            await _consumer.DisposeAsync();
            
            _log.LogInformation("Kafka consumer termination completed");
        }
    }
}