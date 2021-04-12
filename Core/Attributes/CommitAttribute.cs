using AspNetCore.Kafka.Data;
using AspNetCore.Kafka.MessageBlocks;

namespace AspNetCore.Kafka.Attributes
{
    public class CommitAttribute : MessageBlockAttribute
    {
        public CommitAttribute() : base(typeof(CommitBlock), null, BlockStage.Commit)
        {
        }
    }
}