using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace AspNetCore.Kafka.Utility
{
    internal class ConfigurationString
    {
        static class Patterns
        {
            private const string Term = @"[\w-.]+";
            private const string PropertyValue = @"[^,;]+";
            private const string FunctionValue = @"[^,)]+";
            private const string Assign = @"\s*(:|=)\s*";
            private const string Separator = @"(\s*(,|;)\s*)?";
            private static readonly string Result = $@"(\s*=>\s*(?<res>{Term})\s*)?";
            private const RegexOptions Options = RegexOptions.IgnoreCase | RegexOptions.Compiled;
            
            public static readonly Regex Property = new($@"((?<pn>{Term}){Assign}(?<pv>{PropertyValue}))", Options);
            public static readonly Regex Function = new($@"((?<fn>{Term})\s*\((\s*((?<fv>{FunctionValue}){Separator})*)*\s*\))", Options);
            public static readonly Regex Config = new($@"^\s*(({Property}|{Function}){Separator})*{Result}{Separator}$", Options);
        }

        public static ConfigurationString Empty => new();
            
        public string Result { get; init; }

        public bool TryGetProperty<T>(string name, out T value)
        {
            if (Properties.TryGetValue(name, out var x))
            {
                value = ChangeType<T>(x);
                return true;
            }

            value = default;
            return false;
        }

        public T GetPropertyOrDefault<T>(string name) =>
            Properties.TryGetValue(name, out var x) ? ChangeType<T>(x) : default;
        
        public T GetPropertyOr<T>(string name, T defaultValue) =>
            Properties.TryGetValue(name, out var x) ? ChangeType<T>(x) : defaultValue;

        public T GetResult<T>() => ChangeType<T>(Result);

        public bool IsEmpty => !Properties.Any() && !Functions.Any();

        public Dictionary<string, string> Properties { get; init; } = new();

        public Dictionary<string, string[]> Functions { get; init; } = new();
        
        public static ConfigurationString Parse(string input)
        {
            if(string.IsNullOrWhiteSpace(input))
                return Empty;

            if (!Validate(input))
                throw new ArgumentException($"Invalid configuration [{input}]");

            return new ConfigurationString
            {
                Result = ParseConfiguredResult(input),
                Properties = ParseConfiguredProperties(input),
                Functions = ParseConfiguredFunctions(input),
            };
        }
        
        public static bool Validate(string input)
            => Patterns.Config.IsMatch(input);
        
        public static Dictionary<string, string> ParseConfiguredProperties(string input)
            => string.IsNullOrWhiteSpace(input)
                ? new()
                : GetProperties(Patterns.Property.Matches(input));

        public static Dictionary<string, string[]> ParseConfiguredFunctions(string input)
            => string.IsNullOrWhiteSpace(input)
                ? new()
                : GetFunctions(Patterns.Function.Matches(input));
        
        public static string ParseConfiguredResult(string input)
            => string.IsNullOrWhiteSpace(input) 
                ? null
                : GetResult(Patterns.Config.Matches(input));

        private static Dictionary<string, string> GetProperties(MatchCollection match)
            => match.ToDictionary(
                x => x.Groups["pn"].Value.Trim(), 
                x => x.Groups["pv"].Value.Trim());

        private static Dictionary<string, string[]> GetFunctions(MatchCollection match)
            => match.ToDictionary(
                x => x.Groups["fn"].Value.Trim(),
                x => x.Groups["fv"].Captures.Select(c => c.Value.Trim()).ToArray());

        private static string GetResult(MatchCollection match) =>
            match.Single().Groups["res"].Value.Trim();

        public static T ChangeType<T>(string value) => (T) ChangeType(value, typeof(T));
        
        public static object ChangeType(string value, Type type)
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
        
        public static bool IsNumericType(Type type)
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