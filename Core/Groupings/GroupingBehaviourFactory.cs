using System;
using System.Security.Cryptography;
using System.Text;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Groupings
{
    public class GroupingBehaviourFactory<T> : IGroupingBehaviourFactory<T>
    {
        private readonly Func<IMessage<T>, int> _func;
        public int MaxParallelGroups { get; }
        
        public IGroupingBehaviour<T> None => new GroupingBehaviour<T>(null, MaxParallelGroups);

        public GroupingBehaviourFactory(int maxParallelGroups)
        {
            MaxParallelGroups = maxParallelGroups;
        }
        
        public GroupingBehaviourFactory(Func<IMessage<T>, int> func, int maxParallelGroups)
        {
            _func = func;
            MaxParallelGroups = maxParallelGroups;
        }

        public IGroupingBehaviour<T> ByPartition()
        {
            int PartitionSelector(IMessage<T> msg) => msg.Partition;

            return new GroupingBehaviour<T>(_func != null ? m => _func(m) & PartitionSelector(m) 
                : PartitionSelector, MaxParallelGroups);
        }

        public IGroupingBehaviour<T> ByKey()
        {
            int KeySelector(IMessage<T> msg)
            {
                if (string.IsNullOrEmpty(msg.Key)) return 0;

                if (int.TryParse(msg.Key, out var i)) return i;

                // fallback -> try to get hash
                var md5Hasher = MD5.Create();
                var hashed = md5Hasher.ComputeHash(Encoding.UTF8.GetBytes(msg.Key));
                return BitConverter.ToInt32(hashed, 0);
            }

            return new GroupingBehaviour<T>(_func != null ? m => _func(m) & KeySelector(m) 
                : KeySelector, MaxParallelGroups);
        }

        public IGroupingBehaviour<T> ByField(Func<T, object> fieldSelector)
        {
            int FieldSelector(IMessage<T> msg) => fieldSelector(msg.Value).GetHashCode();

            return new GroupingBehaviour<T>(_func != null ? m => _func(m) & FieldSelector(m)
                : FieldSelector, MaxParallelGroups);
        }
    }
}