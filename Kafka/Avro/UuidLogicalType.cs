using System;
using Avro;
using Avro.Util;

namespace AspNetCore.Kafka.Avro
{
    public class UuidLogicalType : LogicalType
    {
        public UuidLogicalType() : base("UUID")
        {
        }

        public override object ConvertToBaseValue(object logicalValue, LogicalSchema schema) => logicalValue;

        public override object ConvertToLogicalValue(object baseValue, LogicalSchema schema) => baseValue.ToString();

        public override Type GetCSharpType(bool nullable) => typeof(string);

        public override bool IsInstanceOfLogicalType(object logicalValue) => logicalValue is string;
    }
}