using System;
using Avro;
using Avro.Util;

namespace AspNetCore.Kafka.Avro
{
    public class TimestampMillisLogicalType : LogicalUnixEpochType<TimeSpan>
    {
        /// <summary>Initializes a new TimeMillisecond logical type.</summary>
        public TimestampMillisLogicalType()
            : base("timestamp-millis")
        {
        }

        /// <inheritdoc />
        public override void ValidateSchema(LogicalSchema schema)
        {
            if (Schema.Type.Int != schema.BaseSchema.Tag && Schema.Type.Long != schema.BaseSchema.Tag)
                throw new AvroTypeException("'timestamp-millis' can only be used with an underlying int/long type");
        }

        /// <inheritdoc />
        public override object ConvertToBaseValue(object logicalValue, LogicalSchema schema)
        {
            var date = (DateTimeOffset) logicalValue;
            return date.ToUnixTimeMilliseconds();
        }

        /// <inheritdoc />
        public override object ConvertToLogicalValue(object baseValue, LogicalSchema schema)
        {
            return DateTimeOffset.FromUnixTimeMilliseconds((long) baseValue);
        }
    }
}