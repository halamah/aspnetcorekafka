using System;
using System.Collections.Generic;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Options;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Client
{
    internal class SubscriptionBuilder<TKey, TValue, TContract>
    {
        private readonly ILogger _log;
        private readonly KafkaOptions _options;
        private readonly IMessageSerializer<TValue> _serializer;
        private readonly IKafkaClientFactory _clientFactory;

        public SubscriptionBuilder(
            ILogger log,
            KafkaOptions options, 
            IMessageSerializer<TValue> serializer,
            IKafkaClientFactory clientFactory)
        {
            _log = log;
            _options = options;
            _serializer = serializer;
            _clientFactory = clientFactory;
        }

        public MessageReader<TKey, TValue, TContract> Build(SubscriptionConfiguration subscription)
        {
            if(string.IsNullOrEmpty(_options.Server))
                throw new ArgumentException("Kafka connection string is not defined");

            var revokeHandler = new RevokeHandler();

            void Revoke(IConsumer<TKey, TValue> consumer, List<TopicPartitionOffset> tp)
            {
                _log.LogInformation("Topic {Topic} partitions revoke", subscription.Topic);
                revokeHandler.OnRevoke().GetAwaiter().GetResult();
            }

            var consumer = _clientFactory?.CreateConsumer<TKey, TValue>(_options, Revoke, subscription);

            if (consumer is null)
                throw new ArgumentNullException(nameof(consumer), "Consumer build failure");

            return new MessageReader<TKey, TValue, TContract>(_log, subscription, _serializer, consumer, revokeHandler);
        }
    }
}