using AspNetCore.Kafka;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Tests
{
    public class Program
    {
        private readonly IConfiguration _config;

        public static void Main(string[] args)
            => Host.CreateDefaultBuilder()
                .ConfigureWebHostDefaults(webBuilder => webBuilder.UseStartup<Program>())
                .Build()
                .Run();
        
        public Program(IConfiguration config)
        {
            _config = config;
        }

        public void ConfigureServices(IServiceCollection services)
        {
            services.AddKafka(_config);
        }

        public void Configure(IApplicationBuilder app)
        {
            
        }
    }
}