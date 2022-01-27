using System;
using System.Threading.Tasks;
using AspNetCore.Kafka.Data;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IKafkaConsumer : IKafkaClient, IAsyncDisposable
    {
        internal IMessageSubscription SubscribeInternal<T>(
            string topic,
            Func<IMessage<T>, Task> handler,
            SourceOptions options = null);
    }
}