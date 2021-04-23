using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Options;
using Avro.Generic;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AspNetCore.Kafka.Client.Consumer
{
    internal class KafkaConsumer : KafkaClient, IKafkaConsumer
    {
        private readonly IServiceScopeFactory _factory;
        private readonly ISubscriptionService _service;

        public KafkaConsumer(
            IOptions<KafkaOptions> options,
            ILogger<KafkaConsumer> logger,
            IHostEnvironment environment, 
            IServiceScopeFactory factory,
            IEnumerable<IMessageInterceptor> interceptors,
            ISubscriptionService service) : base(logger, options.Value, environment)
        {
            _factory = factory;
            _service = service;
            
            Interceptors = interceptors;
        }

        IMessageSubscription IKafkaConsumer.SubscribeInternal<T>(
            string topic,
            Func<IMessage<T>, Task> handler, 
            SourceOptions options)
        {
            if (options?.Offset?.DateOffset is not null &&
                (options.Offset?.Bias is not null || options.Offset?.Offset is not null))
                throw new ArgumentException(
                    $"Ambiguous offset configuration for topic '{topic}'. Only DateOffset alone or Offset/Bias must be set.");
            
            topic = ExpandTemplate(topic);

            if (string.IsNullOrWhiteSpace(topic))
                throw new ArgumentException($"Missing topic name for subscription type {typeof(T).Name}");
            
            if(_service.Subscriptions.Any(x => x.Topic == topic))
                throw new ArgumentException($"Duplicate subscription for topic subscription {topic}");

            options ??= new SourceOptions();
            options.Offset ??= new MessageOffset();
            
            options = options with
            {
                Offset = options.Offset with
                {
                    Offset = options.Offset?.Offset ?? TopicOffset.Stored,
                    Bias = options.Offset?.Bias ?? 0
                },
                Format = options.Format == TopicFormat.Unset ? TopicFormat.String : options.Format,
            };

            var group = ExpandTemplate(Options?.Configuration?.Group)?.ToLowerInvariant();
            
            #if (DEBUG)
                if (!string.IsNullOrEmpty(group))
                    group += "-";
                
                group += Environment.MachineName;
            #endif
            
            using var _ = Logger.BeginScope(new
            {
                Topic = topic,
                Group = group,
                Options = options
            });

            try
            {
                Logger.LogInformation(
                    "* Subscribe topic {Topic}, {Options}, group: {Group}, commit: {CommitMode}",
                    topic, options, group, Options.IsManualCommit() ? "manual" : "auto");

                using var scope = _factory.CreateScope();
                
                var definition = new SubscriptionConfiguration
                {
                    Topic = topic,
                    Options = options,
                    Group = group,
                    LogHandler = LogHandler,
                    Scope = scope,
                };

                var clientFactory = scope.ServiceProvider.GetService<IKafkaClientFactory>();

                var subscription = options.Format == TopicFormat.Avro
                    ? new SubscriptionBuilder<string, GenericRecord, T>(Options, clientFactory).Build(definition)
                        .Run(handler)
                    : new SubscriptionBuilder<string, string, T>(Options, clientFactory).Build(definition)
                        .Run(handler);
                
                _service.Register(subscription);

                return subscription;
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Subscription failed");
                throw;
            }
        }
        
        public override IEnumerable<IMessageInterceptor> Interceptors { get; }

        public void Dispose()
        {
        }
    }
}