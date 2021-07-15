using System;
using AspNetCore.Kafka.Abstractions;
using Avro.Generic;

namespace AspNetCore.Kafka.Client
{
    public class DefaultKafkaMessageParser
    {
        private readonly IKafkaMessageJsonSerializer _jsonSerializer;
        private readonly IKafkaMessageAvroSerializer _avroSerializer;

        public DefaultKafkaMessageParser(IKafkaMessageJsonSerializer jsonSerializer, IKafkaMessageAvroSerializer avroSerializer)
        {
            _jsonSerializer = jsonSerializer;
            _avroSerializer = avroSerializer;
        }

        public TContract Parse<TContract>(object input)
        {
            if (input == null)
                return default;

            return input switch
            {
                null => default,
                TContract value => value,
                GenericRecord value => _avroSerializer.Deserialize<TContract>(value),
                string value => _jsonSerializer.Deserialize<TContract>(value),
                _ => throw new ArgumentException($"Could not deserialize type '{input.GetType().Name}'")
            };
        }
    }
}