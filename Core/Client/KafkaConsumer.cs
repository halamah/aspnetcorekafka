using System;
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
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AspNetCore.Kafka.Client
{
    internal class KafkaConsumer : KafkaClient, IKafkaConsumer
    {
        private readonly IServiceScopeFactory _factory;
        private readonly List<Func<Task>> _completions = new();
        
        public KafkaConsumer(
            IOptions<KafkaOptions> options,
            ILogger<KafkaConsumer> log,
            IHostEnvironment environment, 
            IServiceScopeFactory factory,
            IEnumerable<IMessageInterceptor> interceptors) : base(log, options.Value, environment)
        {
            _factory = factory;
            
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
            
            using var _ = Log.BeginScope(new
            {
                Topic = topic,
                Group = group,
                Options = options
            });

            try
            {
                Log.LogInformation(
                    "* Subscribe topic {Topic} with {CommitMode} commit, {Options}, group: {Group}",
                    topic, Options.IsManualCommit() ? "manual" : "auto", options, group);

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

                Register(subscription.Unsubscribe);

                return subscription;
            }
            catch (Exception e)
            {
                Log.LogError(e, "Subscription failed");
                throw;
            }
        }

        public override IEnumerable<IMessageInterceptor> Interceptors { get; }

        public void Dispose()
        {
            Complete(default).GetAwaiter().GetResult();
        }

        public async Task Complete(CancellationToken ct)
        {
            Log.LogInformation("Waiting to complete processing");
            
            await Task.WhenAny(Task.WhenAll(_completions.Select(x => x())), ct.AsTask());
            
            Log.LogInformation("Processing completed");
        }

        public void Register(Func<Task> completion) => _completions.Add(completion);

        public async ValueTask DisposeAsync() => await Complete(default);   
    }
}