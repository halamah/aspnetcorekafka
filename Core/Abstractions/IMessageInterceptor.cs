using System.Threading.Tasks;
using AspNetCore.Kafka.Data;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageInterceptor
    {
        Task ConsumeAsync(KafkaInterception interception);

        Task ProduceAsync(KafkaInterception interception);
    }
}