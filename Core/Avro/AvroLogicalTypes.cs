using Avro.Util;

namespace AspNetCore.Kafka.Avro
{
    public static class AvroLogicalTypes
    {
        public static void Register()
        {
            LogicalTypeFactory.Instance.Register(new UuidLogicalType());
            LogicalTypeFactory.Instance.Register(new TimestampMillisLogicalType());
        }
    }
}