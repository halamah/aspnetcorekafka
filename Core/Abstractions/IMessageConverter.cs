using System;
using System.Linq;
using System.Threading.Tasks;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageConverter
    {
        Task HandleAsync(Delegate actualHandler, IMessage message);
        
        Type PayloadType { get; }
        
        Type MessageType => PayloadType.GetGenericArguments().FirstOrDefault();

        Type TargetType { get; }

        string Info => null;
    }
}