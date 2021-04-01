namespace AspNetCore.Kafka.Automation
{
    public interface IMessageConverterArgument<out T>
    {
        T Value { get; }
    }
    
    public class MessageConverterArgument<T> : IMessageConverterArgument<T>
    {
        public MessageConverterArgument(T value)
        {
            Value = value;
        }
        
        public T Value { get; }
    }
}