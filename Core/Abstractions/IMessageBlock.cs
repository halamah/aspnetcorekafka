using System.Threading.Tasks.Dataflow;

namespace AspNetCore.Kafka.Abstractions
{
    public interface IMessageBlock
    {
        public IDataflowBlock CreateBlock<T>();
    }
}