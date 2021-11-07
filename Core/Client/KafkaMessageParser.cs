using System;
using AspNetCore.Kafka.Abstractions;
using Avro.Generic;

namespace AspNetCore.Kafka.Client
{
    public class KafkaMessageParser
    {
        private readonly IKafkaMessageJsonSerializer _json;
        private readonly IKafkaMessageAvroSerializer _avro;

        public KafkaMessageParser(
            IKafkaMessageJsonSerializer json, 
            IKafkaMessageAvroSerializer avro)
        {
            _json = json;
            _avro = avro;
        }

        public TContract Parse<TContract>(object input)
        {
            if (input == null)
                return default;

            return input switch
            {
                null => default,
                TContract value => value,
                GenericRecord value => _avro.Deserialize<TContract>(value),
                string value => _json.Deserialize<TContract>(value),
                _ => throw new ArgumentException($"Could not deserialize type '{input.GetType().Name}'")
            };
        }
    }
}