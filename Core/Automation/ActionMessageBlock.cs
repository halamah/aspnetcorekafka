using System;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Automation
{
    public class ActionMessageBlock
    {
        public Func<IMessage<T>, Task> Create<T>(Func<IMessage<T>, Task> next) => next;

        public override string ToString() => "Handle()";
    }
}