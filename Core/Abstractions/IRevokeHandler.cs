using System.Threading.Tasks;

namespace AspNetCore.Kafka.Abstractions
{
    public delegate Task RevokeHandlerDelegate();
    
    public interface IRevokeHandler
    {
        event RevokeHandlerDelegate Revoke;
    }
}