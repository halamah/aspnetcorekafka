using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Automation
{
    public class ActionMessageBlock
    {
        public ActionMessageBlock(object arg)
        {
        }
        
        public Func<IMessage<T>, Task> Create<T>(Func<IMessage<T>, Task> next) => next;

        public static Delegate CreateDelegate(object target, MethodInfo methodInfo)
        {
            var types = methodInfo.GetParameters().Select(p => p.ParameterType).Concat(new[] { methodInfo.ReturnType });

            return methodInfo.IsStatic
                ? Delegate.CreateDelegate(Expression.GetFuncType(types.ToArray()), methodInfo) 
                : Delegate.CreateDelegate(Expression.GetFuncType(types.ToArray()), target, methodInfo.Name);
        }

        public static Type GetContractType(MethodInfo method)
        {
            return method
                .GetParameters()
                .SelectMany(x => x.ParameterType.GetInterfaces().Concat(new[] {x.ParameterType}))
                .Where(x => x.IsGenericType)
                .Select(x => new[] {x}.Concat(x.GetGenericArguments()))
                .SelectMany(x => x)
                .FirstOrDefault(x => x.GetGenericTypeDefinition() == typeof(IMessage<>))?
                .GetGenericArguments()
                .FirstOrDefault();
        }

        public override string ToString() => "No conversion";
    }
}