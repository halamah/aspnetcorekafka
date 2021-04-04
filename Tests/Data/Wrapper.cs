namespace Tests.Data
{
    public class Wrapper<T>
    {
        public T Value { get; set; }

        public Wrapper(T value)
        {
            Value = value;
        }
    }
}