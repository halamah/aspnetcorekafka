using System;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Automation
{
    public class ConsumerHostedService : IHostedService
    {
        private readonly ISubscriptionManager _manager;
        private readonly IKafkaConsumer _consumer;
        private readonly ILogger _log;
        
        public ConsumerHostedService(
            IKafkaConsumer consumer,
            ILogger<ConsumerHostedService> log,
            ISubscriptionManager manager)
        {
            _consumer = consumer;
            _log = log;
            _manager = manager;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _log.LogInformation("Subscription service started");
            
            await _manager.SubscribeFromAssembliesAsync().ConfigureAwait(false);
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            _log.LogInformation("Subscription service shutdown started");

            var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
            await _consumer.Complete(cts.Token).ConfigureAwait(false);
            
            _log.LogInformation("Subscription service shutdown completed");
        }
    }
}