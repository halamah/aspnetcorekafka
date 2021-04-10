using System;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Attributes;
using AspNetCore.Kafka.Automation;
using AspNetCore.Kafka.Extensions.Abstractions;
using AspNetCore.Kafka.Extensions.Attributes;
using Tests.Data;
using Xunit;

namespace Tests
{
    public class TypeTests
    {
        [Theory]
        [InlineData(typeof(MessageHandlerFromAnAttribute))]
        [InlineData(typeof(MessageHandlerFromAnInterface))]
        [InlineData(typeof(MessageHandlerFromGenericInterface))]
        [InlineData(typeof(MessageBatchHandlerFromAnAttribute))]
        [InlineData(typeof(MessageBatchHandlerFromAnInterface))]
        public void IsAMessageHandler(Type type)
        {
            Assert.True(type.IsMessageHandlerType());
            Assert.Contains(type.GetMethods(), m => m.IsMessageHandlerMethod());
        }
        
        [Theory]
        [InlineData(typeof(string))]
        [InlineData(typeof(int))]
        [InlineData(typeof(NotAMessageHandler))]
        public void IsNotAMessageHandler(Type type)
        {
            Assert.False(type.IsMessageHandlerType());
            Assert.DoesNotContain(type.GetMethods(), m => m.IsMessageHandlerMethod());
        }

        private class MessageHandlerFromGenericInterface : IMessageHandler<StubMessage>
        {
            public Task Handle(IMessage<StubMessage> message)
            {
                return Task.CompletedTask;
            }
        }

        public class NotAMessageHandler {}
        
        private class MessageHandlerFromAnInterface : IMessageHandler
        {
            [Message]
            public Task Handle(IMessage<StubMessage> message)
            {
                return Task.CompletedTask;
            }
        }

        [MessageHandler]
        private class MessageHandlerFromAnAttribute
        {
            [Message]
            public Task Handle(IMessage<StubMessage> message)
            {
                return Task.CompletedTask;
            }
        }

        [MessageHandler]
        private class MessageBatchHandlerFromAnAttribute
        {
            [Message]
            [MessageBatch]
            public Task Handle(IMessageEnumerable<StubMessage> batch)
            {
                return Task.CompletedTask;
            }
        }
        
        private class MessageBatchHandlerFromAnInterface : IMessageBatchHandler<StubMessage>
        {
            [MessageBatch]
            public Task Handle(IMessageEnumerable<StubMessage> messages)
            {
                return Task.CompletedTask;
            }
        }
    }
}