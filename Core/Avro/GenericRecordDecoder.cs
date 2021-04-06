using System;
using System.Collections.Generic;
using System.Reflection;
using Avro.Generic;

namespace AspNetCore.Kafka.Avro
{
    public static class GenericRecordDecoder
    {
        public static T ToObject<T>(this GenericRecord x) where T : class
        {
            var type = typeof(T);
            var result = (T) Activator.CreateInstance(typeof(T));

            foreach (var field in x.Schema.Fields)
            {
                try
                {
                    if (!x.TryGetValue(field.Name, out var value))
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
                            property.SetValue(result, value);
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
    }
}