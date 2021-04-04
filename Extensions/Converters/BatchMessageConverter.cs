using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation;
using AspNetCore.Kafka.Options;
using Microsoft.Extensions.Options;
using Microsoft.VisualBasic;
using Microsoft.VisualBasic.CompilerServices;
using MoreLinq.Extensions;

namespace AspNetCore.Kafka.Extensions.Converters
{
    public class BatchMessageConverter : IMessageConverter
    {
        private readonly IPropagatorBlock<IMessage, IMessage[]> _batch;
            
        public BatchMessageConverter(
            object instance,
            MethodInfo method,
            IOptions<KafkaOptions> options,
            IMessageConverterArgument<object> argument = null)
        {
            var argumentType = method.GetParameters().FirstOrDefault()?.ParameterType;
            PayloadType = argumentType?.GetGenericArguments().FirstOrDefault();
            
            if (argumentType?.GetGenericTypeDefinition() != typeof(IEnumerable<>) ||
                PayloadType?.GetGenericTypeDefinition() != typeof(IMessage<>))
                throw new ArgumentException($"Unsupported handler type {method}");

            var sink = Delegate.CreateDelegate(
                typeof(Func<,>).MakeGenericType(argumentType, method.ReturnType), instance, method);

            var arg = argument?.Value ?? new
            {
                Size = 0,
                Timeout = 0,
            };

            var size = Versioned.CallByName(arg, "Size", CallType.Get) as int?;
            var timeout = Versioned.CallByName(arg, "Timeout", CallType.Get) as int?;

            size = size > 1 ? size.Value : throw new ArgumentException("Batch size must be greater than 1");
            timeout = timeout > 0 ? timeout.Value : int.MaxValue;

            var timeoutStr = timeout == int.MaxValue ? "Unset" : timeout.ToString();

            Info = $"Batch(Size: {size}, Timeout: {timeoutStr})";
            
            var manual = options.Value?.IsManualCommit() ?? false;

            object Cast(IEnumerable<IMessage> collection) =>
                typeof(Enumerable).GetMethod(nameof(Enumerable.Cast))!.MakeGenericMethod(PayloadType)
                    .Invoke(null, new object[] {collection});

            _batch = CreateBatchBlock<IMessage>(size.Value, timeout.Value, async batch =>
            {
                try
                {
                    await (sink.DynamicInvoke(Cast(batch)) as Task)!.ConfigureAwait(false);
                }
                finally
                {
                    if (manual)
                        batch.OrderByDescending(x => x.Offset).DistinctBy(x => x.Partition).ForEach(x => x.Commit(true));
                }
            });
        }

        private static IPropagatorBlock<T, T[]> CreateBatchBlock<T>(int size, int time, Func<T[], Task> handler, GroupingDataflowBlockOptions options = null)
        {
            options ??= new GroupingDataflowBlockOptions();
            
            var batch = new BatchBlock<T>(size, options);
            var timer = new Timer(_ => batch.TriggerBatch());
            var transform = new TransformBlock<T, T>(x => { timer.Change(time, Timeout.Infinite); return x; });
            
            transform.LinkTo(batch);
            batch.LinkTo(new ActionBlock<T[]>(handler));
            
            return DataflowBlock.Encapsulate(transform, batch);
        }

        public Task HandleAsync(IMessage message)
        {
            _batch.Post(message);
            return Task.CompletedTask;
        }

        public Type PayloadType { get; }

        public string Info { get; }
    }
}