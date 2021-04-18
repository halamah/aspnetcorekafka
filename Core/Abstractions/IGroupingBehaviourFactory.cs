using System;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IGroupingBehaviourFactory<T>
    {
        int MaxParallelGroups { get; }

        IGroupingBehaviour<T> None { get; }

        IGroupingBehaviour<T> ByPartition();

        IGroupingBehaviour<T> ByKey();

        IGroupingBehaviour<T> ByField(Func<T, object> fieldSelector);
    }
}