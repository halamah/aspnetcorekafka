using System;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Groups
{
    internal class GroupingBehaviour<T> : IGroupingBehaviour<T>
    {
        private readonly Func<IMessage<T>, int> _func;
        private readonly int _maxParallelGroups;

        public GroupingBehaviour(Func<IMessage<T>, int> func, int maxParallelGroups)
        {
            _func = func;
            _maxParallelGroups = maxParallelGroups;
        }

        public int SelectGroup(IMessage<T> msg) => (_func?.Invoke(msg) ?? 1) % _maxParallelGroups;

        public IGroupingBehaviourFactory<T> And => new GroupingBehaviourFactory<T>(_func, _maxParallelGroups);
    }
}