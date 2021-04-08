using System;

namespace AspNetCore.Kafka.Data
{
    public class Disposable : IDisposable
    {
        private readonly Action _action;

        private Disposable(Action action) => _action = action;

        public static IDisposable Create(Action action) => new Disposable(action);

        public void Dispose() => _action();
    }
}