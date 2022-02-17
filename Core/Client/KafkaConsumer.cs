using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Options;
using AspNetCore.Kafka.Utility;
using Avro.Generic;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AspNetCore.Kafka.Client
{
    internal class KafkaConsumer : IKafkaConsumer
    {
        private readonly KafkaOptions _options;
        private readonly ILogger _log;
        private readonly IServiceScopeFactory _factory;
        private readonly IEnumerable<IMessageInterceptor> _interceptors;
        private readonly ConcurrentBag<IMessageSubscription> _subscriptions = new();

        public KafkaConsumer(
            IOptions<KafkaOptions> options,
            ILogger<KafkaConsumer> log,
            IServiceScopeFactory factory,
            IEnumerable<IMessageInterceptor> interceptors)
        {
            _options = options.Value;
            _log = log;
            _factory = factory;
            _interceptors = interceptors;
        }

        IMessageSubscription IKafkaConsumer.SubscribeInternal<T>(
            string topic,
            Func<IMessage<T>, CancellationToken, Task> handler, 
            SourceOptions options)
        {
            if (options?.Offset?.DateOffset is not null &&
                (options.Offset?.Bias is not null || options.Offset?.Offset is not null))
                throw new ArgumentException(
                    $"Ambiguous offset configuration for topic '{topic}'. Only DateOffset alone or Offset/Bias must be set.");

            var name = _options.ExpandTemplate(options?.Name);
            topic = _options.ExpandTemplate(topic);

            if (string.IsNullOrWhiteSpace(topic))
                throw new ArgumentException($"Missing topic name for subscription type {typeof(T).Name}");
            
            options ??= new SourceOptions();
            
            options = options with
            {
                Format = options.Format == TopicFormat.Unset ? TopicFormat.String : options.Format,
            };

            var group = _options.Configuration.ClientCommon.GetValueOrDefault(KafkaConfiguration.GroupId) ??
                        _options.Configuration.Consumer.GetValueOrDefault(KafkaConfiguration.GroupId);

            using var _ = _log.BeginScope(new
            {
                Name = name,
                Topic = topic,
                Options = options,
                Group = group
            });

            try
            {
                _log.LogInformation("* Subscribe topic {Topic}, {Options}", topic, options);

                using var scope = _factory.CreateScope();

                var configuration = new SubscriptionConfiguration
                {
                    Topic = topic,
                    Options = options,
                    Scope = scope,
                    Group = group
                };
                
                var clientFactory = scope.ServiceProvider.GetService<IKafkaClientFactory>();
                
                IMessageSubscription Run<TSerialized>()
                    => new SubscriptionBuilder<string, TSerialized, T>(
                            _log,
                            _options, 
                            scope.ServiceProvider.GetRequiredService<IMessageSerializer<TSerialized>>(),
                            clientFactory)
                        .Build(configuration)
                        .Run(handler);

                var subscription = options.Format == TopicFormat.Avro ? Run<GenericRecord>() : Run<string>();

                _subscriptions.Add(subscription);

                return subscription;
            }
            catch (Exception e)
            {
                _log.LogError(e, "Subscription failed");
                throw;
            }
        }

        private async Task UnsubscribeAllAsync()
        {
            if (_subscriptions.Any())
            {
                _log.LogInformation("Consumer unsubscribe all");
            }
            else
            {
                _log.LogInformation("Consumer has no subscriptions");
            }

            while (_subscriptions.TryTake(out var subscription))
            {
                _log.LogInformation("Topic {Topic} unsubscribe", subscription.Topic);
                await subscription.UnsubscribeAsync().ConfigureAwait(false);
            }
        }

        public async ValueTask DisposeAsync() => await UnsubscribeAllAsync().ConfigureAwait(false);

        ILogger IKafkaClient.Log => _log;

        IEnumerable<IMessageInterceptor> IKafkaClient.Interceptors => _interceptors;
    }
}