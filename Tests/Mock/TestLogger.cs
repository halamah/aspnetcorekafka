using System;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace Tests.Mock
{
    public class TestLogger<T> : ILogger<T>
    {
        private readonly ITestOutputHelper _log;

        public TestLogger(ITestOutputHelper log)
        {
            _log = log;
        }

        public IDisposable BeginScope<TState>(TState state) => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            #if (DEBUG)
                _log.WriteLine($"[{logLevel}] {state} {exception}");
            #endif
        }
    }
}