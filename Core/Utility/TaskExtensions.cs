using System.Threading;
using System.Threading.Tasks;

namespace AspNetCore.Kafka.Utility
{
    public static class TaskExtensions
    {
        public static Task AsTask(this CancellationToken ct)
        {
            var cts = new TaskCompletionSource();
            ct.Register(() => cts.TrySetCanceled(), false);
            return cts.Task;
        }
    }
}