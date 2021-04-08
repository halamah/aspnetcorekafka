using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Data;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit.Abstractions;

namespace Tests.Data
{
    public class SampleMessage
    {
        public int Id { get; set; }
    }
    
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
                sink.SuppressCommit().Returns(x => true);

                return sink;
            }
        }
        
        public static ISink<T> Create(ITestOutputHelper log, Action<object> handler = null)
        {
            var sink = Substitute.For<ISink<T>>();

            var totalMessages = new Wrapper<int>(0);

            sink.Batch(Arg.Any<IEnumerable<IMessage<T>>>()).Returns(x =>
            {
                var batch = (IEnumerable<IMessage<T>>) x[0];
                totalMessages.Value += batch.Count();
                log.WriteLine($"* Batch {batch.Count()}");
                handler?.Invoke(batch);
                return Task.CompletedTask;
            });
            
            sink.Message(Arg.Any<IMessage<T>>()).Returns(x =>
            {
                totalMessages.Value++;
                log.WriteLine($"* Message");
                handler?.Invoke(x[0]);
                return Task.CompletedTask;
            });
            
            sink.MessageMethodInfo.Returns(sink.GetType().GetMethod(nameof(ISink<T>.Message)));
            sink.BatchMethodInfo.Returns(sink.GetType().GetMethod(nameof(ISink<T>.Batch)));

            var messageDelegate = Delegate.CreateDelegate(typeof(Func<IMessage<SampleMessage>, Task>), sink,
                sink.MessageMethodInfo);
            var batchDelegate = Delegate.CreateDelegate(typeof(Func<IEnumerable<IMessage<SampleMessage>>, Task>), sink,
                sink.BatchMethodInfo);

            sink.MessageDelegate.Returns(messageDelegate);
            sink.BatchDelegate.Returns(batchDelegate);
            
            sink.TotalMessages().Returns(x => totalMessages.Value);

            return sink;
        }
    }
    
    public interface ISink<in T>
    {
        Task Message(IMessage<T> x);
        
        Task Batch(IEnumerable<IMessage<T>> x);
        
        MethodInfo MessageMethodInfo { get; } 
        
        MethodInfo BatchMethodInfo { get; } 
        
        Delegate MessageDelegate { get; }
        
        Delegate BatchDelegate { get; }

        long TotalMessages();
    }
}