using System;
using AspNetCore.Kafka.Abstractions;
using Avro.Generic;

namespace AspNetCore.Kafka.Client
{
    public class KafkaMessageParser
    {
        private readonly IKafkaMessageSerializer<string> _text;
        private readonly IKafkaMessageSerializer<GenericRecord> _avro;

        public KafkaMessageParser(
            IKafkaMessageSerializer<string> text, 
            IKafkaMessageSerializer<GenericRecord> avro)
        {
            _text = text;
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
                string value => _text.Deserialize<TContract>(value),
                _ => throw new ArgumentException($"Could not deserialize type '{input.GetType().Name}'")
            };
        }
    }
}