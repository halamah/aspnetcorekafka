using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Mock.Abstractions;
using Confluent.Kafka;

namespace AspNetCore.Kafka.Mock.InMemory
{
    public class InMemoryKafkaProducer<TKey, TValue> : IProducer<TKey, TValue>
    {
        private readonly KafkaMemoryBroker _broker;
        private readonly InMemoryTopicCollection<TKey, TValue> _topics;

        public InMemoryKafkaProducer(IKafkaMemoryBroker broker, InMemoryTopicCollection<TKey, TValue> topics)
        {
            _broker = (KafkaMemoryBroker) broker;
            _topics = topics;
        }

        public void Dispose() { }

        public int AddBrokers(string brokers) => 0;

        public Handle Handle => default;

        public string Name => "KafkaProducerMock";
        
        public Task<DeliveryResult<TKey, TValue>> ProduceAsync(string topic, Message<TKey, TValue> message, CancellationToken cancellationToken = new())
        {
            _topics.GetTopic(topic).Put(message);
            ++_broker.ProduceCount;
            return Task.FromResult(new DeliveryResult<TKey, TValue>());
        }

        public Task<DeliveryResult<TKey, TValue>> ProduceAsync(TopicPartition topicPartition, Message<TKey, TValue> message, CancellationToken cancellationToken = new())
        {
            _topics.GetTopic(topicPartition.Topic).Put(message);
            ++_broker.ProduceCount;
            return Task.FromResult(new DeliveryResult<TKey, TValue>());
        }

        public void Produce(string topic, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler = null)
        {
            _topics.GetTopic(topic).Put(message);
            ++_broker.ProduceCount;
            deliveryHandler?.Invoke(new DeliveryReport<TKey, TValue>());
        }

        public void Produce(TopicPartition topicPartition, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler = null)
        {
            _topics.GetTopic(topicPartition.Topic).Put(message);
            ++_broker.ProduceCount;
            deliveryHandler?.Invoke(new DeliveryReport<TKey, TValue>());
        }

        public int Poll(TimeSpan timeout) => 0;

        public int Flush(TimeSpan timeout) => 0;

        public void Flush(CancellationToken cancellationToken = new CancellationToken())
        { }

        public void InitTransactions(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public void BeginTransaction()
        {
            throw new NotImplementedException();
        }

        public void CommitTransaction(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public void CommitTransaction()
        {
            throw new NotImplementedException();
        }

        public void AbortTransaction(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public void AbortTransaction()
        {
            throw new NotImplementedException();
        }

        public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }
    }
}