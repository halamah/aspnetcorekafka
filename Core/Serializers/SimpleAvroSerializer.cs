using System;
using System.Collections.Generic;
using System.Reflection;
using AspNetCore.Kafka.Abstractions;
using Avro.Generic;

namespace AspNetCore.Kafka.Serializers
{
    public class SimpleAvroSerializer : IAvroMessageSerializer
    {
        public T Deserialize<T>(GenericRecord record)
        {
            var type = typeof(T);
            var result = (T) Activator.CreateInstance(typeof(T));

            foreach (var field in record.Schema.Fields)
            {
                try
                {
                    if (!record.TryGetValue(field.Name, out var value))
                        continue;
                    
                    if (result is Dictionary<string, object> obj)
                    {
                        obj.Add(field.Name, value);
                    }
                    else
                    {
                        if (type.GetProperty(
                            field.Name,
                            BindingFlags.Public |
                            BindingFlags.Instance |
                            BindingFlags.SetProperty |
                            BindingFlags.IgnoreCase) is var property and not null)
                        {
                            if (property.PropertyType.IsEnum && value is string)
                            {
                                property.SetValue(result,
                                    Enum.TryParse(property.PropertyType, value.ToString(), true, out var x)
                                        ? x
                                        : Activator.CreateInstance(property.PropertyType));
                            }
                            else
                            {
                                property.SetValue(result, value);
                            }
                        }
                    }
                }
                catch (Exception)
                {
                    // ignore
                }
            }
            
            return result;
        }

        public string Serialize<T>(T value)
        {
            throw new NotImplementedException();
        }
    }
}