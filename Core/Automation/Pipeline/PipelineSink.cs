using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Client;

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

        public Task SendAsync(IMessage<TContract> message)
        {
            _input ??= _pipeline.Build(_completions).Input;
            return _input.SendAsync(message);
        }

        public async Task CompleteAsync()
        {
            await _completions.CompleteAsync();
            _input = null;
        }
    }
}