using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using NSubstitute;
using Xunit.Abstractions;

namespace Tests.Data
{
    public class Sink<T> where T : class, new()
    {
        private static int _id;
        
        public static IMessage<T> NewMessage
        {
            get
            {
                var id = _id++;
                
                var sink = Substitute.For<IMessage<T>>();

                sink.Value.Returns(new T());
                sink.Offset.Returns(id);
                sink.Partition.Returns(id % 2);
                sink.Key.Returns(id.ToString());
                sink.Topic.Returns(string.Empty);
                sink.Commit().Returns(x => true);

                return sink;
            }
        }
        
        public static ISink<T> Create(Action<object> handler = null)
        {
            var sink = Substitute.For<ISink<T>>();

            var totalMessages = new Wrapper<int>(0);

            sink.Batch(Arg.Any<IMessageEnumerable<T>>()).Returns(x =>
            {
                var batch = (IEnumerable<IMessage<T>>) x[0];
                totalMessages.Value += batch.Count();
                handler?.Invoke(batch);
                return Task.CompletedTask;
            });
            
            sink.Message(Arg.Any<IMessage<T>>()).Returns(x =>
            {
                totalMessages.Value++;
                handler?.Invoke(x[0]);
                return Task.CompletedTask;
            });
            
            sink.TotalMessages().Returns(x => totalMessages.Value);

            return sink;
        }
    }
    
    public interface ISink<in T>
    {
        Task Message(IMessage<T> x);
        
        Task Batch(IMessageEnumerable<T> x);
        
        long TotalMessages();
    }
}