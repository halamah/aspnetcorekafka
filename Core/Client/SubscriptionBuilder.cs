using System;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Options;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Client
{
    internal class SubscriptionBuilder<TKey, TValue, TContract>
    {
        private readonly ILogger _log;
        private readonly KafkaOptions _options;
        private readonly IKafkaMessageSerializer<TValue> _serializer;
        private readonly IKafkaClientFactory _clientFactory;

        public SubscriptionBuilder(
            ILogger log,
            KafkaOptions options, 
            IKafkaMessageSerializer<TValue> serializer,
            IKafkaClientFactory clientFactory)
        {
            _log = log;
            _options = options;
            _serializer = serializer;
            _clientFactory = clientFactory;
        }

        public MessageReaderTask<TKey, TValue, TContract> Build(SubscriptionConfiguration subscription)
        {
            if(string.IsNullOrEmpty(_options.Server))
                throw new ArgumentException("Kafka connection string is not defined");

            var consumer = _clientFactory?.CreateConsumer<TKey, TValue>(_options, subscription);

            if (consumer is null)
                throw new ArgumentNullException(nameof(consumer), "Consumer build failure");

            return new MessageReaderTask<TKey, TValue, TContract>(_log, subscription, _serializer, consumer);
        }
    }
}