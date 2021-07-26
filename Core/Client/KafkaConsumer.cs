using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
        private readonly IKafkaEnvironment _environment;
        private readonly IServiceScopeFactory _factory;
        private readonly IEnumerable<IMessageInterceptor> _interceptors;
        private readonly ConcurrentBag<Func<Task>> _completions = new();

        public KafkaConsumer(
            IOptions<KafkaOptions> options,
            ILogger<KafkaConsumer> log,
            IKafkaEnvironment environment, 
            IServiceScopeFactory factory,
            IEnumerable<IMessageInterceptor> interceptors)
        {
            _options = options.Value;
            _log = log;
            _environment = environment;
            _factory = factory;
            _interceptors = interceptors;
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

            var name = _environment.ExpandTemplate(options?.Name);
            topic = _environment.ExpandTemplate(topic);

            if (string.IsNullOrWhiteSpace(topic))
                throw new ArgumentException($"Missing topic name for subscription type {typeof(T).Name}");
            
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
                _log.LogInformation(
                    "* Subscribe topic {Topic} with {CommitMode} commit, {Options}",
                    topic, 
                    _options.IsManualCommit() ? "manual" : "auto", 
                    options);

                using var scope = _factory.CreateScope();
                
                var configuration = new SubscriptionConfiguration
                {
                    Topic = topic,
                    Options = options,
                    Scope = scope,
                    Group = group
                };

                var clientFactory = scope.ServiceProvider.GetService<IKafkaClientFactory>();

                var subscription = options.Format == TopicFormat.Avro
                    ? new SubscriptionBuilder<string, GenericRecord, T>(_options, clientFactory).Build(configuration)
                        .Run(handler)
                    : new SubscriptionBuilder<string, string, T>(_options, clientFactory).Build(configuration)
                        .Run(handler);

                RegisterCompletionSource(subscription.Unsubscribe);

                return subscription;
            }
            catch (Exception e)
            {
                _log.LogError(e, "Subscription failed");
                throw;
            }
        }

        public void Dispose()
        {
            Complete(default).GetAwaiter().GetResult();
        }

        public async Task Complete(CancellationToken ct)
        {
            _log.LogInformation("Waiting to complete processing");

            while (_completions.TryTake(out var completion))
            {
                await Task.WhenAny(completion(), ct.AsTask()).ConfigureAwait(false);
                ct.ThrowIfCancellationRequested();
            }

            _log.LogInformation("Processing completed");
        }

        public void RegisterCompletionSource(Func<Task> completion) => _completions.Add(completion);

        public async ValueTask DisposeAsync() => await Complete(default).ConfigureAwait(false);

        ILogger IKafkaClient.Log => _log;

        IEnumerable<IMessageInterceptor> IKafkaClient.Interceptors => _interceptors;
    }
}