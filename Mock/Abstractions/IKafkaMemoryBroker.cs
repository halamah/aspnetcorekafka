using System;
using System.Collections.Generic;

namespace AspNetCore.Kafka.Mock.Abstractions
{
    public interface IKafkaMemoryBroker
    {
        public IKafkaMemoryTopic<string, string> GetTopic(string topic);
        
        public IKafkaMemoryTopic<string, T> GetTopic<T>(Func<T, bool> selector = null);
        
        public IEnumerable<IKafkaMemoryTopic<object, object>> Topics { get; }

        public IKafkaMemoryBroker Bounce();
    }
}