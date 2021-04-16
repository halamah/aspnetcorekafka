using System;
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
        private readonly IMessageSerializer _serializer;
        private readonly IServiceScopeFactory _factory;

        public KafkaConsumer(
            IOptions<KafkaOptions> options,
            ILogger<KafkaConsumer> logger,
            IHostEnvironment environment, 
            IMessageSerializer serializer,
            IServiceScopeFactory factory) : base(logger, options.Value, environment)
        {
            _serializer = serializer;
            _factory = factory;
        }

        public IMessageSubscription Subscribe<T>(
            string topic,
            Func<IMessage<T>, Task> handler, 
            SubscriptionOptions options = null)
        {
            topic = ExpandTemplate(topic);
            
            var group = ExpandTemplate(Options?.Configuration?.Group)?.ToLowerInvariant();
            var format = options?.Format ?? TopicFormat.String;
            var offset = options?.Offset is var o and not null and not TopicOffset.Unset
                ? o.Value
                : Options?.Configuration?.Offset ?? TopicOffset.Unset;

            if (offset == TopicOffset.Unset)
                offset = TopicOffset.Stored;
            
            var bias = options?.Bias ?? Options?.Configuration?.Bias ?? 0;
            
            #if (DEBUG)
                if (!string.IsNullOrEmpty(group))
                    group += "-";
                
                group += Environment.MachineName;
            #endif
            
            using var _ = Logger.BeginScope(new
            {
                Topic = topic,
                Group = group,
                Offset = offset,
                Bias = bias,
            });

            try
            {
                Logger.LogInformation("* Subscribe topic {Topic} from date: {DateOffset} time: {TimeOffset}, offset: {Offset}, group: {Group}, commit: {CommitMode}", 
                    topic, options?.DateOffset, options?.NegativeTimeOffset, offset, group, Options.IsManualCommit() ? "manual" : "auto");

                using var scope = _factory.CreateScope();
                
                var subscription = new SubscriptionConfiguration
                {
                    Topic = topic,
                    DateOffset = options?.DateOffset,
                    TimeOffset = options?.NegativeTimeOffset ?? TimeSpan.Zero, 
                    Offset = offset,
                    Bias = bias,
                    Group = group,
                    Logger = Logger,
                    TopicFormat = format,
                    LogHandler = LogHandler,
                    Scope = scope,
                    Serializer = _serializer,
                };

                var clientFactory = scope.ServiceProvider.GetService<IKafkaClientFactory>();

                return format == TopicFormat.Avro
                    ? new SubscriptionBuilder<string, GenericRecord, T>(Options, clientFactory).Build(subscription)
                        .Run(handler)
                    : new SubscriptionBuilder<string, string, T>(Options, clientFactory).Build(subscription)
                        .Run(handler);
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Subscription failed");
                throw;
            }
        }

        public void Dispose()
        {
        }
    }
}