using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace AspNetCore.Kafka.Mock.InMemory
{
    internal class KafkaMemoryProducer<TKey, TValue> : IProducer<TKey, TValue>
    {
        private readonly KafkaMemoryBroker _broker;
        
        public KafkaMemoryProducer(KafkaMemoryBroker broker)
        {
            _broker = broker;
        }

        public void Dispose() { }

        public int AddBrokers(string brokers) => 0;

        public Handle Handle => default;

        public string Name => "KafkaProducerMock";
        
        public Task<DeliveryResult<TKey, TValue>> ProduceAsync(string topic, Message<TKey, TValue> message, CancellationToken cancellationToken = new())
        {
            _broker.GetTopic<TKey, TValue>(topic).Put(message);
            return Task.FromResult(new DeliveryResult<TKey, TValue>());
        }

        public Task<DeliveryResult<TKey, TValue>> ProduceAsync(TopicPartition topicPartition, Message<TKey, TValue> message, CancellationToken cancellationToken = new())
        {
            _broker.GetTopic<TKey, TValue>(topicPartition.Topic).Put(message);
            return Task.FromResult(new DeliveryResult<TKey, TValue>());
        }

        public void Produce(string topic, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler = null)
        {
            _broker.GetTopic<TKey, TValue>(topic).Put(message);
            deliveryHandler?.Invoke(new DeliveryReport<TKey, TValue>());
        }

        public void Produce(TopicPartition topicPartition, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler = null)
        {
            _broker.GetTopic<TKey, TValue>(topicPartition.Topic).Put(message);
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