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

        public bool TryGetProperty<T>(string name, out T value)
        {
            if (Properties.TryGetValue(name, out var x))
            {
                value = x.ChangeType<T>();
                return true;
            }

            value = default;
            return false;
        }

        public T GetPropertyOrDefault<T>(string name) =>
            Properties.TryGetValue(name, out var x) ? x.ChangeType<T>() : default;
        
        public T GetPropertyOr<T>(string name, T defaultValue) =>
            Properties.TryGetValue(name, out var x) ? x.ChangeType<T>() : defaultValue;

        public T GetResult<T>() => Result.ChangeType<T>();

        public Dictionary<string, string> Properties { get; init; } = new();

        public Dictionary<string, string[]> Functions { get; init; } = new();
    }
    
    public static class InlineConfiguration
    {
        static class Patterns
        {
            private const string Term = @"[\w-.]+";
            private const string Value = @"[\w-.:\+\s]+";
            private const string Assign = @"\s*(:|=)\s*";
            private const string Separator = @"(\s*(,|;)\s*)?";
            private static readonly string Result = $@"(\s*=>\s*(?<res>{Term})\s*)?";
            private const RegexOptions Options = RegexOptions.IgnoreCase | RegexOptions.Compiled;
            
            public static readonly Regex Property = new($@"((?<pn>{Term}){Assign}(?<pv>{Value}))", Options);
            public static readonly Regex Function = new($@"((?<fn>{Term})\s*\((\s*((?<fv>{Value}){Separator})*)*\s*\))", Options);
            public static readonly Regex Config = new($@"^\s*(({Property}|{Function}){Separator})*{Result}{Separator}$", Options);
        }

        public static InlineConfigurationValues ParseInlineConfiguration(this string config)
        {
            if(string.IsNullOrWhiteSpace(config))
                return InlineConfigurationValues.Empty;

            if (!config.ValidateConfigString())
                throw new ArgumentException($"Invalid configuration [{config}]");

            return new InlineConfigurationValues
            {
                Result = config.ParseConfiguredResult(),
                Properties = config.ParseConfiguredProperties(),
                Functions = config.ParseConfiguredFunctions(),
            };
        }
        
        public static bool ValidateConfigString(this string config)
            => Patterns.Config.IsMatch(config);
        
        public static Dictionary<string, string> ParseConfiguredProperties(this string configString)
            => string.IsNullOrWhiteSpace(configString)
                ? new()
                : Patterns.Property.Matches(configString).GetProperties();

        public static Dictionary<string, string[]> ParseConfiguredFunctions(this string configString)
            => string.IsNullOrWhiteSpace(configString)
                ? new()
                : Patterns.Function.Matches(configString).GetFunctions();
        
        public static string ParseConfiguredResult(this string configString)
            => string.IsNullOrWhiteSpace(configString) 
                ? null
                : Patterns.Config.Matches(configString).GetResult();

        private static Dictionary<string, string> GetProperties(this MatchCollection match)
            => match.ToDictionary(
                x => x.Groups["pn"].Value.Trim(), 
                x => x.Groups["pv"].Value.Trim());

        private static Dictionary<string, string[]> GetFunctions(this MatchCollection match)
            => match.ToDictionary(
                x => x.Groups["fn"].Value.Trim(),
                x => x.Groups["fv"].Captures.Select(c => c.Value.Trim()).ToArray());

        private static string GetResult(this MatchCollection match) =>
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