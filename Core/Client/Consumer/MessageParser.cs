using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Avro;
using Avro.Generic;
using Confluent.Kafka;

namespace AspNetCore.Kafka.Client.Consumer
{
    public class MessageParser<TKey, TValue>
    {
        private readonly IMessageSerializer _serializer;

        public MessageParser(IMessageSerializer serializer)
        {
            _serializer = serializer;
        }

        public TContract Parse<TContract>(ConsumeResult<TKey, TValue> message)
        {
            if (message.Message.Value == null)
                return default;

            return message.Message.Value switch
            {
                TContract value => value,
                GenericRecord value => value.ToObject<TContract>(),
                _ => message.Message.Value.ToString() is var json and not null
                    ? _serializer.Deserialize<TContract>(json)
                    : default
            };
        }
    }
}