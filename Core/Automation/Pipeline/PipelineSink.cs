using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Client;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Automation.Pipeline
{
    public class PipelineSink<TContract>
    {
        private readonly IMessagePipeline<TContract> _pipeline;
        private readonly CompletionSource _completions;

        private ITargetBlock<IMessage<TContract>> _input;

        public PipelineSink(IMessagePipeline<TContract> pipeline)
        {
            _pipeline = pipeline;
            _completions = new CompletionSource(pipeline.Consumer.Log);
        }

        public async Task<bool> SendAsync(IMessage<TContract> message)
        {
            _input ??= _pipeline.Build(_completions).Input;
            var result = await _input.SendAsync(message);
            
            if(!result)
                _pipeline.Consumer.Log.LogWarning("Pipeline message failed to queue {Topic} {Offset}", message.Topic, message.Offset);

            return result;
        }

        public async Task CompleteAsync()
        {
            await _completions.CompleteAsync().ConfigureAwait(false);
            _input = null;
        }
    }
}