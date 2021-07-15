using Avro.Generic;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IKafkaMessageAvroSerializer
    {
        string Serialize<T>(T value);
        
        T Deserialize<T>(GenericRecord value);
    }
}