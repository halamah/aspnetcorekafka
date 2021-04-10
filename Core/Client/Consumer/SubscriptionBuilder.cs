using System;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Options;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;

namespace AspNetCore.Kafka.Client.Consumer
{
    internal class SubscriptionBuilder<TKey, TValue, TContract> where TContract : class
    {
        private readonly KafkaOptions _options;
        private readonly IKafkaClientFactory _clientFactory;

        public SubscriptionBuilder(KafkaOptions options, IKafkaClientFactory clientFactory)
        {
            _options = options;
            _clientFactory = clientFactory;
        }

        public MessageReaderTask<TKey, TValue, TContract> Build(SubscriptionConfiguration subscription)
        {
            var group = subscription.Group ?? _options.Configuration?.Group;
            
            if(string.IsNullOrEmpty(_options.Server))
                throw new ArgumentException("Kafka connection string is not defined");
            
            var config = new ConsumerConfig(_options.Configuration?.Consumer ?? new())
            {
                BootstrapServers = _options.Server,
                GroupId = group,
            };

            var consumer = _clientFactory?.CreateConsumer<TKey, TValue>(_options, subscription);

            if (consumer is null)
                throw new ArgumentNullException(nameof(consumer), "Consumer build failure");
            
            consumer.Subscribe(subscription.Topic);

            return new MessageReaderTask<TKey, TValue, TContract>(
                subscription.Scope.ServiceProvider.GetServices<IMessageInterceptor>(),
                subscription.Serializer,
                subscription.Logger,
                consumer,
                subscription.Topic,
                subscription.Buffer);
        }
    }
}