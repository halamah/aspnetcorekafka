using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;

namespace AspNetCore.Kafka.Client
{
    public static class Extensions
    {
        public static Offset Otherwise(this Offset x, Offset fallback)
            => x == Offset.Unset ? fallback : x;
        
        public static Offset Move(this Offset x, long value)
            => x == Offset.Unset ? x : new Offset(x.Value + value);

        public static Dictionary<string, string> Merge(this IDictionary<string, string> lhs, IDictionary<string, string> rhs)
        {
            var result = lhs.ToDictionary(x => x.Key, x => x.Value);

            foreach (var (key, value) in rhs)
                result[key] = value;

            return result;
        }
    }
}