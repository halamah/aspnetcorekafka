using System;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Core.Consumer;
using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.Options;
using Avro.Generic;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AspNetCore.Kafka.Core
{
    internal class KafkaConsumer : KafkaClient, IKafkaConsumer
    {
        private readonly IServiceScopeFactory _factory;

        public KafkaConsumer(
            IOptions<KafkaOptions> options,
            ILogger<KafkaConsumer> logger,
            IHostEnvironment environment, 
            IServiceScopeFactory factory) : base(logger, options.Value, environment)
        {
            _factory = factory;
        }

        public IMessageSubscription Subscribe<T>(
            string topic,
            Func<IMessage<T>, Task> handler, 
            SubscriptionOptions options = null) where T : class
        {
            topic = ExpandTemplate(topic);
            
            var group = ExpandTemplate(Options.Configuration.Group).ToLowerInvariant();
            var format = options?.TopicFormat ?? TopicFormat.String;
            var offset = options?.Offset is var o and not null and not TopicOffset.Unset
                ? o.Value
                : Options.Configuration.Offset;

            if (offset == TopicOffset.Unset)
                offset = TopicOffset.Stored;
            
            var bias = options?.Bias ?? Options.Configuration.Bias;
            
            #if (DEBUG)
                group += "-" + System.Environment.MachineName;
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
                Logger.LogInformation("Subscribe topic {Topic} from {Offset}, group: {Group}, commit: {CommitMode}", 
                    topic, offset, group, Options.IsManualCommit() ? "manual" : "auto");

                var subscription = new SubscriptionConfiguration
                {
                    Topic = topic,
                    Offset = offset,
                    Bias = bias,
                    Group = group,
                    Logger = Logger,
                    TopicFormat = format,
                    LogHandler = LogHandler,
                    Scope = _factory.CreateScope(),
                };

                if (format == TopicFormat.Avro)
                {
                    return new SubscriptionBuilder<string, GenericRecord, T>(Options)
                        .Build(subscription)
                        .Run(handler);
                }

                return new SubscriptionBuilder<string, string, T>(Options)
                    .Build(subscription)
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