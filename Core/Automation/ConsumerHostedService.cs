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

        public Task StartAsync(CancellationToken cancellationToken) => _service.SubscribeConfiguredAssembliesAsync();

        public Task StopAsync(CancellationToken cancellationToken) => _service.Shutdown();
    }
}