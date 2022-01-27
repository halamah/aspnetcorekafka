using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Utility;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Client
{
    public class CompletionSource : ICompletionSource
    {
        private readonly ConcurrentBag<Func<Task>> _completions = new();
        private readonly ILogger _log;

        public CompletionSource(ILogger log)
        {
            _log = log;
        }

        public async Task CompleteAsync(CancellationToken ct = default)
        {
            _log.LogInformation("Waiting to complete processing");

            await Task.WhenAny(Task.WhenAll(_completions.ToList().Select(x => x())), ct.AsTask()).ConfigureAwait(false);
            
            _completions.Clear();
            
            _log.LogInformation("Processing completed");
        }

        public void Add(Func<Task> completion) => _completions.Add(completion);
    }
}