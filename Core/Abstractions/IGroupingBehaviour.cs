namespace AspNetCore.Kafka.Abstractions
{
    public interface IGroupingBehaviour<T>
    {
        int SelectGroup(IMessage<T> msg);

        IGroupingBehaviourFactory<T> And { get; }
    }
}