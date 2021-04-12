using System.Threading.Tasks.Dataflow;
using AspNetCore.Kafka.Abstractions;

namespace AspNetCore.Kafka.MessageBlocks
{
    public class CommitBlock : IMessageBlock
    {
        public IDataflowBlock CreateBlock<T>() => new TransformBlock<IMessageOffset, IMessageOffset>(x =>
            {
                x.Commit(true);
                return x;
            },
            new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = 1,
                EnsureOrdered = true
            });

        public override string ToString() => "Commit()";
    }
}