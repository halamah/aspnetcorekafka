namespace AspNetCore.Kafka.Abstractions
{
    public interface IKafkaEnvironment
    {
        string EnvironmentName { get; }
    }
}