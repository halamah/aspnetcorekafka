using Avro.Generic;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IAvroMessageSerializer
    {
        string Serialize<T>(T value);
        
        T Deserialize<T>(GenericRecord value);
    }
}