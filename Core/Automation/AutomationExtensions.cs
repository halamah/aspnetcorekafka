using System;
using System.Reflection;
using AspNetCore.Kafka.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.VisualBasic;
using Microsoft.VisualBasic.CompilerServices;

namespace AspNetCore.Kafka.Automation
{
    internal static class AutomationExtensions
    {
        public static object ResolveBlock(this MethodInfo method, IServiceProvider provider)
        {
            var blockInfo = method.GetCustomAttribute<MessageBlockAttribute>() ??
                            new MessageBlockAttribute(typeof(ActionMessageBlock));

            var argument = blockInfo.ArgumentType is not null
                ? provider.GetService(blockInfo.ArgumentType)
                  ?? Versioned.CallByName(
                      provider.GetRequiredService(typeof(IOptions<>).MakeGenericType(blockInfo.ArgumentType)), "Value",
                      CallType.Get)
                : blockInfo;

            argument ??= new InvalidOperationException($"Null options provided for {method}");

            try
            {
                return ActivatorUtilities.CreateInstance(provider, blockInfo.BlockType, argument);
            }
            catch (InvalidOperationException e)
            {
                return ActivatorUtilities.CreateInstance(provider, blockInfo.BlockType);
            }
        }
    }
}