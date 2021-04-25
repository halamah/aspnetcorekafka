using System;
using System.Threading;
using System.Threading.Tasks;

namespace AspNetCore.Kafka.Abstractions
{
    public interface ICompletionSource
    {
        Task Complete(CancellationToken ct = default);

        Task Complete(int timeout) => Complete(new CancellationTokenSource(timeout).Token);
        
        void Register(Func<Task> completion);
    }
}