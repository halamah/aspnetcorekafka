using System;
using System.Threading.Tasks;
using AspNetCore.Kafka.Data;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IKafkaConsumer : IDisposable
    {
        IMessageSubscription Subscribe<T>(
            string topic,
            Func<IMessage<T>, Task> handler,
            SubscriptionOptions options = null) where T : class;
        
        IMessagePipeline<IMessage<T>, IMessage<T>> Pipeline<T>(string topic, SubscriptionOptions options = null) where T : class;
    }
}