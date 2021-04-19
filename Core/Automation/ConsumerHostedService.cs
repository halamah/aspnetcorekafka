using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Automation
{
    public class ConsumerHostedService : IHostedService
    {
        private readonly ILogger<ConsumerHostedService> _log;
        private readonly ISubscriptionService _service;
        
        public ConsumerHostedService(ILogger<ConsumerHostedService> log, ISubscriptionService service)
        {
            _log = log;
            _service = service;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
            => await _service.SubscribeConfiguredAssembliesAsync();

        public async Task StopAsync(CancellationToken cancellationToken)
            => await _service.UnsubscribeAllAsync();
    }
}