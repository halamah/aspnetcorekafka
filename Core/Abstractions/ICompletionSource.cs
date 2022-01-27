using System;
using System.Threading;
using System.Threading.Tasks;

namespace AspNetCore.Kafka.Abstractions
{
    public interface ICompletionSource
    {
        Task CompleteAsync(CancellationToken ct = default);

        Task CompleteAsync(int timeout) => CompleteAsync(new CancellationTokenSource(timeout).Token);
        
        void Add(Func<Task> completion);
    }
}