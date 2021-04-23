using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace AspNetCore.Kafka.Utility
{
    public class InlineConfigurationValues
    {
        public static InlineConfigurationValues Empty => new();
            
        public string Result { get; init; } = null;

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
            
            public static readonly string Property = $@"((?<pn>{Term}){Assign}(?<pv>{Term}))";
            public static readonly string Function = $@"((?<fn>{Term})\s*\((\s*((?<fv>{Term}){Separator})*)*\s*\))";
            public static readonly string Config = $@"^\s*(({Property}|{Function}){Separator})*{Result}{Separator}$";
        }

        public static InlineConfigurationValues ReadInlineConfiguration(this string config)
        {
            if (!config.ValidateConfigString())
                return null;

            return new InlineConfigurationValues
            {
                Result = config.ReadConfiguredResult(),
                Properties = config.ReadConfiguredProperties(),
                Functions = config.ReadConfiguredFunctions(),
            };
        }
        
        public static bool ValidateConfigString(this string config)
            => Regex.IsMatch(config, Patterns.Config, RegexOptions.IgnoreCase);
        
        public static Dictionary<string, string> ReadConfiguredProperties(this string configString)
        {
            if (string.IsNullOrWhiteSpace(configString))
                return new();
            
            return Regex.Matches(configString, Patterns.Property, RegexOptions.IgnoreCase).ReadProperties();
        }

        public static Dictionary<string, string[]> ReadConfiguredFunctions(this string configString)
        {
            if (string.IsNullOrWhiteSpace(configString))
                return new();
            
            return Regex.Matches(configString, Patterns.Function, RegexOptions.IgnoreCase).ReadFunctions();
        }
        
        public static string ReadConfiguredResult(this string configString)
        {
            if (string.IsNullOrWhiteSpace(configString))
                return null;
            
            return Regex.Matches(configString, Patterns.Config, RegexOptions.IgnoreCase).ReadResult();
        }

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
        
        public static object ChangeType(this string value, Type type)
        {
            type = Nullable.GetUnderlyingType(type) ?? type;

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