using System;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;
using Confluent.Kafka;

namespace AspNetCore.Kafka.Client
{
    public static class Extensions
    {
        /// <summary>
        /// Publish event payload.
        /// Event topic and key will be taken from type of <paramref name="payload"/>.
        /// If payload is null than topic will be taken from <typeparamref name="T"/>.
        /// </summary>
        public static Task ProduceAsync<T>(this IKafkaProducer producer, T payload) 
            where T : class
        {
            var message = TopicDefinition.FromType<T>();
            var key = message.GetMessageKey(payload);
            return producer.ProduceAsync(message.Topic, key, payload);
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