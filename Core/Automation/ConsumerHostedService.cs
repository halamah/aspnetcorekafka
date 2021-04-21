using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using Microsoft.Extensions.Hosting;

namespace AspNetCore.Kafka.Automation
{
    public class ConsumerHostedService : IHostedService
    {
        private readonly ISubscriptionService _service;
        
        public ConsumerHostedService(ISubscriptionService service) => _service = service;

        public async Task StartAsync(CancellationToken cancellationToken)
            => await _service.SubscribeConfiguredAssembliesAsync();

        public async Task StopAsync(CancellationToken cancellationToken)
            => await _service.Shutdown();
    }
}