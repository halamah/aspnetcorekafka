using System;
using System.Threading.Tasks;
using AspNetCore.Kafka.Abstractions;
using AspNetCore.Kafka.Automation;
using AspNetCore.Kafka.Automation.Attributes;
using FluentAssertions;
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
        public void IsMessageHandler(Type type)
        {
            type.IsMessageHandlerType().Should().BeTrue();
            type.GetMethods().Should().Contain(m => m.IsMessageHandlerMethod());
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
            public Task HandleAsync(StubMessage message) => Task.CompletedTask;
        }

        public class NotAMessageHandler {}
        
        private class MessageHandlerFromAnInterface : IMessageHandler
        {
            [Message]
            public Task Handle(IMessage<StubMessage> message) => Task.CompletedTask;
        }

        [MessageHandler]
        private class MessageHandlerFromAnAttribute
        {
            [Message]
            public Task Handle(IMessage<StubMessage> message) => Task.CompletedTask;
        }

        [MessageHandler]
        private class MessageBatchHandlerFromAnAttribute
        {
            [Message]
            [Batch]
            public Task Handle(IMessageEnumerable<StubMessage> batch) => Task.CompletedTask;
        }
        
        private class MessageBatchHandlerFromAnInterface : IMessageHandler<IMessageEnumerable<StubMessage>>
        {
            [Batch]
            public Task HandleAsync(IMessageEnumerable<StubMessage> messages) => Task.CompletedTask;
        }
    }
}