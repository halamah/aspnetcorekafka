using System;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Attributes;
using AspNetCore.Kafka.Automation;
using Xunit;

namespace Tests
{
    public class TypeTests
    {
        [Theory]
        [InlineData(typeof(MessageHandlerFromAnAttribute))]
        [InlineData(typeof(MessageHandlerFromAnInterface))]
        [InlineData(typeof(MessageHandlerFromGenericInterface))]
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

        public record StubMessage;

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
    }
}