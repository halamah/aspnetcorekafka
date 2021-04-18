using System;
using System.Threading.Tasks;
using AspNetCore.Kafka.Data;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IKafkaConsumer : IDisposable
    {
        internal IMessageSubscription Subscribe<T>(
            string topic,
            Func<IMessage<T>, Task> handler,
            SourceOptions options = null);
    }
}