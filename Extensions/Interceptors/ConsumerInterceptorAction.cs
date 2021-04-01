using System;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Extensions.Interceptors
{
    public class ConsumerInterceptorAction : IMessageInterceptor
    {
        private readonly Func<IMessage<object>, Task> _func;

        public ConsumerInterceptorAction(Func<IMessage<object>, Task> func)
        {
            _func = func ?? throw new ArgumentNullException(nameof(func));
        }

        public Task InterceptAsync(IMessage<object> payload, Exception exception) => _func?.Invoke(payload);
    }
}