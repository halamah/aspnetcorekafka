using System;
using System.Threading.Tasks;
using AspNetCore.Kafka.Data;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IKafkaConsumer : IKafkaClient, IDisposable
    {
        internal IMessageSubscription SubscribeInternal<T>(
            string topic,
            Func<IMessage<T>, Task> handler,
            SourceOptions options = null);
    }
}