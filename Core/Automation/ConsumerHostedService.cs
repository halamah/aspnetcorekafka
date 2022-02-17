using System;
using System.Collections.Generic;
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
        private readonly ILogger _log;
        private readonly IEnumerable<Type> _handlers;
        private readonly IKafkaConsumer _consumer;
        
        public ConsumerHostedService(
            ILogger<ConsumerHostedService> log,
            ISubscriptionManager manager, 
            IEnumerable<Type> handlers, 
            IKafkaConsumer consumer)
        {
            _log = log;
            _manager = manager;
            _handlers = handlers;
            _consumer = consumer;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _log.LogInformation("Subscription service started");
            await _manager.SubscribeFromTypesAsync(_handlers).ConfigureAwait(false);
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            _log.LogInformation("Kafka consumer termination");
            
            await _consumer.DisposeAsync();
            
            _log.LogInformation("Kafka consumer termination completed");
        }
    }
}