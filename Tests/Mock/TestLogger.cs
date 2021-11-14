using System;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace Tests.Mock
{
    public class TestLogger<T> : ILogger<T>
    {
        private readonly ITestOutputHelper _output;

        public TestLogger(ITestOutputHelper output) => _output = output;

        public IDisposable BeginScope<TState>(TState state) => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            try { _output.WriteLine($"[{logLevel}] {state} {exception}"); }
            catch { }
        }
    }
}