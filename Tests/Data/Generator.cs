using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace Tests.Data
{
    public class Generator
    {
        public static async Task<int> Run(ITestOutputHelper log, Func<Task> action, TimeSpan duration, TimeSpan factor)
        {
            return await Task.Run(async () =>
            {
                var random = new Random(DateTimeOffset.UtcNow.Millisecond);
                var sw = Stopwatch.StartNew();
                var swd = Stopwatch.StartNew();
                var delay = random.Next((int) duration.TotalMilliseconds / 10, (int) duration.TotalMilliseconds / 5);
                var result = 0;
                log.WriteLine($"Delay {delay}");

                for (; sw.Elapsed < duration; ++result)
                {
                    await action();

                    if (swd.ElapsedMilliseconds >= delay)
                    {
                        var timeout = random.Next((int) factor.TotalMilliseconds / 2, (int) factor.TotalMilliseconds);
                        log.WriteLine($"Delay {timeout}");
                        await Task.Delay(timeout);
                        swd.Restart();
                    }
                }

                return result;
            });
        }
    }
}