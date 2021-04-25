using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace Tests.Data
{
    public class Generator
    {
        public static async Task<int> Run(Func<Task> action, TimeSpan duration, TimeSpan factor)
        {
            return await Task.Run(async () =>
            {
                var random = new Random(DateTimeOffset.UtcNow.Millisecond);
                var sw = Stopwatch.StartNew();
                var result = 0;

                for (; sw.Elapsed < duration; ++result)
                {
                    await action();
                    
                    await Task.Delay(random.Next(
                        (int) (factor.TotalMilliseconds * 0.1),
                        (int) (factor.TotalMilliseconds * 0.2)));
                }

                return result;
            });
        }
    }
}