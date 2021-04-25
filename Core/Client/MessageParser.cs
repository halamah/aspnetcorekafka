using System;
using AspNetCore.Kafka.Abstractions;
using Avro.Generic;
using Confluent.Kafka;

namespace AspNetCore.Kafka.Client
{
    public class MessageParser<TKey, TValue>
    {
        private readonly IJsonMessageSerializer _jsonSerializer;
        private readonly IAvroMessageSerializer _avroSerializer;

        public MessageParser(IJsonMessageSerializer jsonSerializer, IAvroMessageSerializer avroSerializer)
        {
            _jsonSerializer = jsonSerializer;
            _avroSerializer = avroSerializer;
        }

        public TContract Parse<TContract>(ConsumeResult<TKey, TValue> result)
        {
            if (result.Message.Value == null)
                return default;

            return result.Message.Value switch
            {
                null => default,
                TContract value => value,
                GenericRecord value => _avroSerializer.Deserialize<TContract>(value),
                string value => _jsonSerializer.Deserialize<TContract>(value),
                _ => throw new ArgumentException($"Could not deserialize type '{typeof(TValue).Name}'")
            };
        }
    }
}