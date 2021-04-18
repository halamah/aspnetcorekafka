using System;

namespace AspNetCore.Kafka.Abstractions
{
    /// <summary>
    /// Provides basic methods for Kafka message grouping
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IGroupingBehaviourFactory<T>
    {
        /// <summary>
        /// Number of max parallel groups
        /// </summary>
        int MaxParallelGroups { get; }

        /// <summary>
        /// No grouping: Add all messages to the same group (1).
        /// </summary>
        IGroupingBehaviour<T> None { get; }

        /// <summary>
        /// Group messages by partition.
        /// </summary>
        /// <returns></returns>
        IGroupingBehaviour<T> ByPartition();

        /// <summary>
        /// Group messages by key.
        /// </summary>
        /// <returns></returns>
        IGroupingBehaviour<T> ByKey();

        /// <summary>
        /// Group messages by field.
        /// </summary>
        /// <param name="fieldSelector"></param>
        /// <returns></returns>
        IGroupingBehaviour<T> ByField(Func<T, object> fieldSelector);
    }
}