using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using AspNetCore.Kafka.Client;

namespace AspNetCore.Kafka.Mock.Abstractions
{
    public interface IKafkaMemoryTopic<out TKey, out TValue>
    {
        string Name { get; }

        int PartitionsCount { get; set; }

        public Task WhenConsumedAny();

        public Task WhenConsumedAll();
        
        internal IKafkaMemoryTopic<TKey, T> Parse<T>(KafkaMessageParser parser, Func<T, bool> selector = null);
            
        IEnumerable<IKafkaMemoryMessage<TKey, TValue>> Produced { get; }

        IEnumerable<IKafkaMemoryMessage<TKey, TValue>> Consumed { get; }
    }
}