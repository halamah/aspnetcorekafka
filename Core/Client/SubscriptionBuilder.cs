using System;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Options;

namespace AspNetCore.Kafka.Client
{
    internal class SubscriptionBuilder<TKey, TValue, TContract>
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

            var consumer = _clientFactory?.CreateConsumer<TKey, TValue>(_options, subscription);

            if (consumer is null)
                throw new ArgumentNullException(nameof(consumer), "Consumer build failure");

            return new MessageReaderTask<TKey, TValue, TContract>(subscription, consumer);
        }
    }
}