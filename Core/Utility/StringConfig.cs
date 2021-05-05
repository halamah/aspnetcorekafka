using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace AspNetCore.Kafka.Utility
{
    public class InlineConfigurationValues
    {
        public static InlineConfigurationValues Empty => new();
            
        public string Result { get; init; }

        public T GetResult<T>() => Result.ChangeType<T>();

        public Dictionary<string, string> Properties { get; init; } = new();

        public Dictionary<string, string[]> Functions { get; init; } = new();
    }
    
    public static class InlineConfiguration
    {
        static class Patterns
        {
            private const string Term = @"[\w-.]+";
            private const string Assign = @"\s*(:|=)\s*";
            private const string Separator = @"(\s*(,|;)\s*)?";
            private static readonly string Result = $@"(\s*=>\s*(?<res>{Term})\s*)?";
            private const RegexOptions Options = RegexOptions.IgnoreCase | RegexOptions.Compiled;
            
            public static readonly Regex Property = new($@"((?<pn>{Term}){Assign}(?<pv>{Term}))", Options);
            public static readonly Regex Function = new($@"((?<fn>{Term})\s*\((\s*((?<fv>{Term}){Separator})*)*\s*\))", Options);
            public static readonly Regex Config = new($@"^\s*(({Property}|{Function}){Separator})*{Result}{Separator}$", Options);
        }

        public static InlineConfigurationValues ReadInlineConfiguration(this string config)
        {
            if(string.IsNullOrWhiteSpace(config))
                return InlineConfigurationValues.Empty;

            if (!config.ValidateConfigString())
                throw new ArgumentException($"Invalid configuration [{config}]");

            return new InlineConfigurationValues
            {
                Result = config.ReadConfiguredResult(),
                Properties = config.ReadConfiguredProperties(),
                Functions = config.ReadConfiguredFunctions(),
            };
        }
        
        public static bool ValidateConfigString(this string config)
            => Patterns.Config.IsMatch(config);
        
        public static Dictionary<string, string> ReadConfiguredProperties(this string configString)
            => string.IsNullOrWhiteSpace(configString)
                ? new()
                : Patterns.Property.Matches(configString).ReadProperties();

        public static Dictionary<string, string[]> ReadConfiguredFunctions(this string configString)
            => string.IsNullOrWhiteSpace(configString)
                ? new()
                : Patterns.Function.Matches(configString).ReadFunctions();
        
        public static string ReadConfiguredResult(this string configString)
            => string.IsNullOrWhiteSpace(configString) 
                ? null
                : Patterns.Config.Matches(configString).ReadResult();

        private static Dictionary<string, string> ReadProperties(this MatchCollection match)
            => match.ToDictionary(
                x => x.Groups["pn"].Value.Trim(), 
                x => x.Groups["pv"].Value.Trim());

        private static Dictionary<string, string[]> ReadFunctions(this MatchCollection match)
            => match.ToDictionary(
                x => x.Groups["fn"].Value.Trim(),
                x => x.Groups["fv"].Captures.Select(c => c.Value.Trim()).ToArray());

        private static string ReadResult(this MatchCollection match) =>
            match.Single().Groups["res"].Value.Trim();

        public static T ChangeType<T>(this string value) => (T) ChangeType(value, typeof(T));
        
        public static object ChangeType(this string value, Type type)
        {
            type = Nullable.GetUnderlyingType(type) ?? type;

            if (value is null)
                return type.IsValueType ? Activator.CreateInstance(type) : null;

            return type.IsEnum
                ? Enum.Parse(type, value, true)
                : type switch
                {
                    _ when type == typeof(DateTimeOffset) => DateTimeOffset.Parse(value),
                    _ when type == typeof(DateTime) => DateTime.Parse(value),
                    _ when type == typeof(TimeSpan) => TimeSpan.Parse(value),
                    _ => Convert.ChangeType(value, type)
                };
        }
        
        public static bool IsNumericType(this Type type)
        {
            if (type == null)
                return false;

            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Byte:
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.SByte:
                case TypeCode.Single:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    return true;
                
                case TypeCode.Object:
                    if ( type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                        return IsNumericType(Nullable.GetUnderlyingType(type));
                    
                    return false;
            }
            return false;
        }
    }
}