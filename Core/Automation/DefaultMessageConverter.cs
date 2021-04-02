using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Automation
{
    internal class DefaultMessageConverter : IMessageConverter
    {
        public DefaultMessageConverter(MethodInfo methodInfo)
        {
            PayloadType = TargetType = methodInfo.GetParameters().FirstOrDefault()?.ParameterType;

            if (PayloadType?.GetGenericTypeDefinition() != typeof(IMessage<>))
                throw new ArgumentException($"Unsupported handler type {methodInfo}");
        }

        public Task HandleAsync(Delegate actualHandler, IMessage message) => actualHandler.DynamicInvoke(message) as Task;
        
        public Type PayloadType { get; }
        public Type TargetType { get; }
    }
}