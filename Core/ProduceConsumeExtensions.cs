using System;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;

namespace AspNetCore.Kafka
{
    public static class ProduceConsumeExtensions
    {
        public static IMessageSubscription Subscribe<T>(
            this IKafkaConsumer client,
            string topic,
            Func<IMessage<T>, Task> handler,
            SourceOptions options = null)
        {
            ValidateOptions<T>(topic, options);
            return client.Subscribe(topic, handler, options);
        }

        public static IMessageSubscription Subscribe<T>(
            this IKafkaConsumer client,
            Func<IMessage<T>, Task> handler,
            SourceOptions options = null)
        {
            return client.Subscribe(TopicDefinition.FromType<T>().Topic, handler, options);
        }

        public static Task ProduceAsync<T>(
            this IKafkaProducer client,
            string topic, 
            T message,
            string key = null)
        {
            ValidateOptions<T>(topic, null);
            
            key ??= TopicDefinition.FromType<T>().GetMessageKey(message);
            
            return client.ProduceAsync(TopicDefinition.FromType<T>().Topic, message, key);   
        }
        
        private static void ValidateOptions<T>(string topic, SourceOptions options)
        {
            var messageType = typeof(T);
            var definition = TopicDefinition.FromType(messageType);

            if (definition is not null)
            {
                if(!string.Equals(topic, definition.Topic))
                    throw new ArgumentException(
                        $"Ambiguous topic set for message '{messageType.Name}' ({definition.Topic} and {topic})");
            }
            
            if (definition is not null && options is not null)
            {
                if (definition.Format != options.Format && 
                    definition.Format != TopicFormat.Unset &&
                    options.Format != TopicFormat.Unset)
                    throw new ArgumentException(
                        $"Ambiguous topic format set for message '{messageType.Name}' ({definition.Format} and {options.Format})");
            }
        }
    }
}