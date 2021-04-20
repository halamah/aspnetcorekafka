using System;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation.Attributes;
using AspNetCore.Kafka.Data;

namespace AspNetCore.Kafka
{
    public static class ProduceConsumeExtensions
    {
        public static Task ProduceAsync<T>(this IKafkaProducer client, T message, string key = null)
            => client.ProduceInternalAsync(null, message, key);
        
        public static IMessageSubscription Subscribe<T>(
            this IKafkaConsumer client, Func<IMessage<T>, Task> handler, SourceOptions options = null)
            => client.SubscribeInternal(null, handler, options);
        
        public static IMessageSubscription Subscribe<T>(
            this IKafkaConsumer client,
            string topic,
            Func<IMessage<T>, Task> handler,
            SourceOptions options = null)
        {
            var definition = ValidateOptions<T>(topic, options);

            if (string.IsNullOrWhiteSpace(topic))
                topic = definition.Topic;

            options ??= new SourceOptions();

            if (options.Format == TopicFormat.Unset && definition.Format != TopicFormat.Unset)
                options.Format = definition.Format;
            
            return client.SubscribeInternal(topic, handler, options);
        }

        public static Task ProduceAsync<T>(
            this IKafkaProducer client,
            string topic, 
            T message,
            string key = null)
        {
            var definition = ValidateOptions<T>(topic, null);
            
            if (string.IsNullOrWhiteSpace(topic))
                topic = definition.Topic;

            key ??= definition.GetMessageKey(message);
            
            return client.ProduceInternalAsync(topic, message, key);   
        }
        
        private static MessageAttribute ValidateOptions<T>(string topic, SourceOptions options)
        {
            var messageType = typeof(T);
            var definition = TopicDefinition.FromType(messageType);

            if (!string.IsNullOrWhiteSpace(topic) &&
                !string.IsNullOrWhiteSpace(definition?.Topic) && topic != definition.Topic)
                throw new ArgumentException(
                    $"Ambiguous topic name for message '{messageType.Name}' ({definition.Topic} and {topic})");

            if (definition is not null && options is not null &&
                definition.Format != TopicFormat.Unset && options.Format != TopicFormat.Unset &&
                definition.Format != options.Format)
                throw new ArgumentException(
                    $"Ambiguous topic format for message '{messageType.Name}' ({definition.Format} and {options.Format})");

            return definition;
        }
    }
}