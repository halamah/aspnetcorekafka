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
        
        public ConsumerHostedService(
            ILogger<ConsumerHostedService> log,
            ISubscriptionManager manager, 
            IEnumerable<Type> handlers)
        {
            _log = log;
            _manager = manager;
            _handlers = handlers;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _log.LogInformation("Subscription service started");
            await _manager.SubscribeFromTypesAsync(_handlers).ConfigureAwait(false);
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}