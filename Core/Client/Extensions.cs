using Confluent.Kafka;

namespace AspNetCore.Kafka.Client
{
    public static class Extensions
    {
        public static Offset Otherwise(this Offset x, Offset fallback)
            => x == Offset.Unset ? fallback : x;
        
        public static Offset Move(this Offset x, long value)
            => x == Offset.Unset ? x : new Offset(x.Value + value);
    }
}