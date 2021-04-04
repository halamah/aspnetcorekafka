using System;
using System.Linq;
using System.Threading.Tasks;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageConverter
    {
        Task HandleAsync(IMessage message);
        
        Type PayloadType { get; }
        
        Type MessageType => PayloadType.GetGenericArguments().FirstOrDefault();

        string Info => null;
    }
}