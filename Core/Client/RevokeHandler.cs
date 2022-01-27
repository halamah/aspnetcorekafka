using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Client
{
    public class RevokeHandler
    {
        public event RevokeHandlerDelegate Revoke;

        public virtual Task OnRevoke() => Revoke?.Invoke() ?? Task.CompletedTask;
    }
}