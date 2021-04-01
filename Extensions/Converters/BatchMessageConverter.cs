using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation;
using AspNetCore.Kafka.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MoreLinq;

namespace AspNetCore.Kafka.Extensions.Converters
{
    internal class BatchMessageConverter : IMessageConverter
    {
        private readonly List<IMessage> _batch = new();
        private readonly Stopwatch _watch = new();
        private readonly Func<IEnumerable<IMessage>, object> _cast;
        private readonly ILogger _logger;
        private readonly bool _manual;

        private readonly long _capacity;
        private readonly long _latency;
        
        public Type PayloadType { get; }

        public Type TargetType { get; }

        public string Info
        {
            get
            {
                var capacity = _capacity == long.MaxValue ? "Unset" : _capacity.ToString();
                var latency = _latency == long.MaxValue ? "Unset" : _latency.ToString();
                
                return $"Batch(Capacity: {capacity}, Latency: {latency})";       
            }
        }

        public BatchMessageConverter(
            MethodInfo methodInfo,
            ILogger<BatchMessageConverter> logger, 
            IOptions<KafkaOptions> options,
            IMessageConverterArgument<object> argument = null)
        {
            TargetType = methodInfo?.GetParameters().FirstOrDefault()?.ParameterType;

            if (TargetType?.GetGenericTypeDefinition() != typeof(IEnumerable<>))
                throw new ArgumentException($"Unsupported handler type {methodInfo}");

            PayloadType = TargetType.GetGenericArguments().FirstOrDefault();
            
            if(PayloadType?.GetGenericTypeDefinition() != typeof(IMessage<>))
                throw new ArgumentException($"Unsupported handler type {methodInfo}");
            
            var method = typeof(Enumerable).GetMethod(nameof(Enumerable.Cast))!.MakeGenericMethod(PayloadType);

            var capacity = argument?.Value?.GetType().GetProperty("Capacity")?.GetValue(argument.Value) as long?;
            var latency = argument?.Value?.GetType().GetProperty("Latency")?.GetValue(argument.Value) as long?;

            _capacity = capacity > 0 ? capacity.Value : long.MaxValue;
            _latency = latency > 0 ? latency.Value : long.MaxValue;
            
            _manual = options.Value?.IsManualCommit() ?? false;
            _logger = logger;
            _cast = collection => method.Invoke(null, new object[] { collection });
        }

        public async Task HandleAsync(Delegate actualHandler, IMessage message)
        {
            try
            {
                if (!_watch.IsRunning)
                    _watch.Start();

                _batch.Add(message);

                if (_batch.Count < _capacity && _watch.ElapsedMilliseconds < _latency)
                    return;

                await (actualHandler.DynamicInvoke(_cast(_batch)) as Task)!.ConfigureAwait(false);

                if(_manual)
                    _batch.DistinctBy(x => x.Partition).ForEach(x => x.Commit());
                
                _batch.Clear();
                _watch.Restart();
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Batch consumer failure");
            }
            finally
            {
                message.SuppressCommit();
            }
        }
    }
}