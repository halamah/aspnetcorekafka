using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace AspNetCore.Kafka.Utility
{
    public class InlineConfigurationValues
    {
        public string Result { get; set; } = null;

        public Dictionary<string, string> Properties { get; set; } = new();

        public Dictionary<string, string[]> Functions { get; set; } = new();
    }
    
    public static class InlineConfiguration
    {
        private const string Term = @"[\w-.]+";
        
        private static readonly string Pattern = 
            $@"^\s*(((({Term})\s*(:|=)\s*{Term})|({Term}\s*\(({Term}\s*,?\s*)*\)))\s*[,;]?\s*)*(\s*=>\s*{Term}\s*)$";
        
        private static readonly string FunctionPattern 
            = $@"(?<name>{Term})\s*\((\s*(?<value>{Term})\s*,?\s*)*\)";
        
        private static readonly string PropertyPattern 
            = $@"(?<name>{Term})\s*(:|=)\s*(?<value>{Term})";
        
        private static readonly string ResultPattern 
            = $@".*=>\s*(?<result>{Term})\s*$";
        
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
            => Regex.IsMatch(config, Pattern, RegexOptions.IgnoreCase);
        
        public static Dictionary<string, string> ReadConfiguredProperties(this string configString)
        {
            if (string.IsNullOrWhiteSpace(configString))
                return new();
            
            var matches = Regex.Matches(configString, PropertyPattern, RegexOptions.IgnoreCase);
            
            return matches.ToDictionary(
                x => x.Groups["name"].Value.Trim(), 
                x => x.Groups["value"].Value.Trim());
        }

        public static Dictionary<string, string[]> ReadConfiguredFunctions(this string configString)
        {
            if (string.IsNullOrWhiteSpace(configString))
                return new();
            
            var matches = Regex.Matches(configString, FunctionPattern, RegexOptions.IgnoreCase);
            
            return matches.ToDictionary(
                x => x.Groups["name"].Value.Trim(),
                x => x.Groups["value"].Captures.Select(c => c.Value.Trim()).ToArray());
        }
        
        public static string ReadConfiguredResult(this string configString)
        {
            if (string.IsNullOrWhiteSpace(configString))
                return null;
            
            var matches = Regex.Matches(configString, ResultPattern, RegexOptions.IgnoreCase);
            
            if(!matches.Any())
                return null;
            
            return matches.Single().Groups["result"].Value.Trim();
        }
        
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