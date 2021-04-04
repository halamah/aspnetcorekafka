using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Automation
{
    internal class DefaultMessageConverter : IMessageConverter
    {
        private readonly Delegate _sink;
        
        public DefaultMessageConverter(object instance, MethodInfo method)
        {
            PayloadType = method.GetParameters().FirstOrDefault()?.ParameterType;

            if (PayloadType?.GetGenericTypeDefinition() != typeof(IMessage<>))
                throw new ArgumentException($"Unsupported handler type {method}");
            
            _sink = Delegate.CreateDelegate(
                typeof(Func<,>).MakeGenericType(PayloadType, method.ReturnType), instance, method);
        }

        public Task HandleAsync(IMessage message) => _sink.DynamicInvoke(message) as Task;
        
        public Type PayloadType { get; }
    }
}