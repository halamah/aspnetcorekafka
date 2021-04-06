using System;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;
using Confluent.Kafka;

namespace AspNetCore.Kafka.Client
{
    public static class Extensions
    {
        /// <summary>Publish message</summary>
        public static Task ProduceAsync<T>(this IKafkaProducer producer, T message) 
            where T : class
        {
            var definition = TopicDefinition.FromType<T>();
            var key = definition.GetMessageKey(message);
            return producer.ProduceAsync(definition.Topic, key, message);
        }
        
        public static IMessageSubscription Subscribe<T>(
            this IKafkaConsumer consumer,
            Func<IMessage<T>, 
                Task> handler)
            where T : class
            => consumer.Subscribe(TopicDefinition.FromType<T>().Topic, handler);

        public static Offset Otherwise(this Offset x, Offset fallback)
            => x == Offset.Unset ? fallback : x;
        
        public static Offset Move(this Offset x, long value)
            => x == Offset.Unset ? x : new Offset(x.Value + value);
    }
}