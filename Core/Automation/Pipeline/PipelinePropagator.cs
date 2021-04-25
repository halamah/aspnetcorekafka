using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.Automation.Pipeline
{
    public class PipelinePropagator<TContract, TOutput>
    {
        public PipelinePropagator(IPropagatorBlock<IMessage<TContract>, TOutput> propagator)
        {
            Input = propagator;
            Output = propagator;
        }
        
        public PipelinePropagator(ITargetBlock<IMessage<TContract>> input, ISourceBlock<TOutput> output)
        {
            Input = input;
            Output = output;
        }

        public ITargetBlock<IMessage<TContract>> Input { get; }
        
        public ISourceBlock<TOutput> Output { get; }

        public PipelinePropagator<TContract, T> Link<T>(IPropagatorBlock<TOutput, T> next)
        {
            Output.LinkTo(next, new DataflowLinkOptions {PropagateCompletion = true});
            
            return new(Input, next);
        }
        
        public PipelinePropagator<TContract> Link(ITargetBlock<TOutput> next)
        {
            Output.LinkTo(next, new DataflowLinkOptions {PropagateCompletion = true});
            
            return new(Input, next);
        }
        
        public PipelinePropagator<TContract> Close() => new(Input, Output);
    }
    
    public class PipelinePropagator<TContract>
    {
        public PipelinePropagator(ITargetBlock<IMessage<TContract>> input, IDataflowBlock final)
        {
            Input = input;
            Output = final;
        }

        public ITargetBlock<IMessage<TContract>> Input { get; }
        
        public IDataflowBlock Output { get; }
    }
}