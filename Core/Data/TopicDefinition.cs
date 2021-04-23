using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using AspNetCore.Kafka.Automation.Attributes;

namespace AspNetCore.Kafka.Data
{
    public static class TopicDefinition
    {
        private static readonly ConcurrentDictionary<Type, Func<object, object>> KeyAccessorCache = new();
            
        public static MessageAttribute GetTopicDefinition<T>(this T source) => FromType(source?.GetType() ?? typeof(T));

        public static MessageAttribute FromType<T>() => FromType(typeof(T));

        public static MessageAttribute FromType(Type type)
        {
            if (type is null)
                throw new ArgumentNullException(nameof(type));

            var messageAttribute = type?
                .GetCustomAttributes(typeof(MessageAttribute), true)
                .Cast<MessageAttribute>()
                .FirstOrDefault();

            var messageKey = type
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .Where(x => x.GetCustomAttribute<MessageKey>() is not null)
                .Select(x => x.Name)
                .FirstOrDefault();

            if (messageAttribute?.Key is not null &&
                messageKey is not null &&
                !string.Equals(messageAttribute.Key, messageKey, StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException($"Ambiguous Key definition for message type '{type.Name}'");

            messageAttribute ??= new MessageAttribute();
            messageAttribute.Key ??= messageKey;

            return messageAttribute;
        }

        public static string GetMessageKey(this MessageAttribute message, object value)
        {
            if (value is null)
                return null;
            
            return message?.Key?.Split('.').Aggregate(value, ExtractPropertyKey)?.ToString();
        }

        public static string GetMessageKey<T>(this T source)
            => source.GetTopicDefinition().GetMessageKey(source);

        private static object ExtractPropertyKey(object instance, string propertyName)
        {
            if (instance is null)
                return null;
                
            var accessor = KeyAccessorCache.GetOrAdd(instance.GetType(), type =>
            {
                var property = type.GetProperty(propertyName,
                    BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);

                if (property is null)
                    throw new MissingMemberException($"Property key '{propertyName}' is missing on type '{type}'");

                var parameter = Expression.Parameter(typeof(object));
                var propertyExpression =
                    Expression.Convert(Expression.Property(Expression.Convert(parameter, type), property.Name),
                        typeof(object));
                
                return Expression
                    .Lambda<Func<object, object>>(propertyExpression, parameter)
                    .Compile();
            });

            return accessor(instance);
        }
    }
}