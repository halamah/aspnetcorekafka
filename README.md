# AspNetCore.Kafka

[Sample program](Samples/Sample/Program.cs)

A messaging infrastructure for Confluent.Kafka.

# Registration

```c#
services.AddKafka(Configuration);
```

# Message handlers

## Fluent subscription

Example 1 : Simple handler

```c#
  var subscription = _consumer.Subscribe("topic-name", x => LogAsync(x), new SourceOptions {
    // set topic format 
    Format = TopicFormat.Avro,
    // change consume offset to start from 
    Offset = new MessageOffset { ... },
  });
```

Example 2 : Pipeline

```c#
  var subscription = _consumer
    .Message<Notification>() // create pipeline
    .Buffer(100) // buffer messages
    .Partitioned(4) // parallelise the rest of pipeline per partitions (optionally limiting the maximum degree of parallelism)
    .Batch(100, TimeSpan.FromSeconds(5)) // batch messages
    .Action(x => LogAsync(x)) // handler
    .Commit() // commit offsets when handler finished
    .Subscribe(); // perform actual subscription
```

Example 3 : Observable

When using an SubscribeObservable extension with empty pipeline - a 1 message buffer is inserted.

```c#
 // to get topic and options from contract declaration
  var subscription1 = _consumer.Message<Notification>().AsObservable();
  
  // in-line topic and options
  var subscription2 = _consumer.Message<Notification>().SubscribeObservable(topic, options);
```

## Message contract declaration

```c#
[Message(Topic = "event.currency.rate-{env}", Format = TopicFormat.Avro)]
public class RateNotification
{
    public string Currency { get; set; }
    public decimal Rate { get; set; }
}
```

## Change consumption offset

Changing consume offset to start on can be set in fluent pipeline, 
message Offset attributes or via configuration.

Example 1 : Fluent

```c#
  var subscription = _consumer.Subscribe<RateNotification>(new SourceOptions { 
    Offset = new MessageOffset {
      // relative offset
      Offset = TopicOffset.Begin,
      Bias = -1000,
      // or specific date
      DateOffset = DateTimeOffset.UtcNow - TimeSpan.FromDays(1),
    },
  });
```

Example 2 : Message Offset attribute

```c#
[MessageHandler]
public class RateNotificationMessageHandler
{
    [Message]
    // start consume from end minus 1000 (for each partition)
    [Offset(TopicOffset.End, -1000)]
    // or at specific date
    [Offset("2021-01-01T00:00:00Z")]
    public Task Handler(IMessage<RateNotification> message) { ... };
}
```

Example 3 : Configuration

appsetings.json:

```json
{
  "Kafka": {
    "Message": {
      "Default": "offset: begin, bias: -100, dateOffset: 2021-01-01",
      "MessageName": "offset: begin, bias: -100, dateOffset: 2021-01-01"
    }
  }
}
```

*Kafka:Message:Default* - offset config will be added by default for all message 
subscriptions overriding any values set in the code.

*Kafka:Message:MessageName* - offset config will be added to messages marked 
with [MessageConfig("MessageName")] attribute only overriding any values set in the code or Default
configuration above.

## Attribute based subscription

* Subscribe all Types marked with [MessageHandler] attribute.
* Message handler and specific subscription on a method marked with [Message] attribute.  

```c#
// Kafka message handler
[MessageHandler]
public class RateNotificationMessageHandler
{
    // with message wrapper
    [Message] public Task Handler(IMessage<RateNotification> message) { ... };
    
    // or handle payload directly
    [Message] public Task Handler(RateNotification message) { ... };
}
```

## Subscription over an interface

* Subscribe all Types implementing [IMessageHandler] interface.
* Message handler and specific subscription on a method marked with [Message] attribute.

```c#
// Kafka message handler
public class RateNotificationMessageHandler : IMessageHandler
{
    // class with proper DI support.

    // with message wrapper
    [Message] public Task Handler(IMessage<RateNotification> message) { ... }
    
    // or handle payload directly
    [Message] public Task Handler(RateNotification message) { ... };
}
```

## Subscription over an interface with specific message type

* Subscribe all Types implementing [IMessageHandler<T>] interface.
* Message handler and specific subscription on a [Handle] method that implements IMessageHandler<T>.

```c#
// with message wrapper
public class RateNotificationMessageHandler : IMessageHandler<IMessage<RateNotification>>
{
    public Task HandleAsync(IMessage<RateNotification> message) { ... }
}

// or handle payload directly
public class RateNotificationMessageHandler : IMessageHandler<RateNotification>
{
    public Task HandleAsync(RateNotification message) { ... }
}

// or when batching
public class RateNotificationMessageHandler : IMessageHandler<IMessageEnumerable<RateNotification>>
{
    public Task HandleAsync(IMessageEnumerable<RateNotification> messages) { ... }
}
```

## In-place topic details

```c#
// Kafka message handler
public class WithdrawNotificationMessageHandler : IMessageHandler
{
    // Inplace topic subscription definition and a backing consumption buffer
    [Message(Topic = "withdraw_event-{env}", Format = TopicFormat.Avro, Offset = TopicOffset.Begin))]
    public Task Handler(IMessage<WithdrawNotification> message)
    {
        Console.WriteLine($"Withdraw {message.Value.Amount} {message.Value.Currency}");
        return Task.CompletedTask;
    }
}
```

# Message blocks

Message blocks are TPL blocks to allow message processing pipelining.

## Batches, Buffer, Commit and Parallel execution per partition

The order of attributes doesn't matter - the actual pipeline is always get built this way: 

[Buffer] > [Parallel] > [Batch] > [Action] > [Commit]

Any of the following blocks could be omitted.

[Parallel] with DegreeOfParallelism set to greater than 1 - is to lower the actual degree of parallelization, 
otherwise it's set to [-1] and means the degree of parallelization equals to partitions count of the target topic.

```c#
public class RateNotificationHandler : IMessageHandler<IEnumerable<RateNotification>>
{
    // buffer messages
    [Buffer(Size = 100)]
    // parallelized execution per partition
    [Parallel(DegreeOfParallelism = 4)]
    // use constant values
    [Batch(Size = 190, Time = 5000)]
    //commit after handler finished
    [Commit]
    public Task HandleAsync(IEnumerable<RateNotification> messages)
    {
        Console.WriteLine($"Received batch with size {messages.Count}");
        return Task.CompletedTask;
    }
}
```

## Additional message consumption declarations

```c#
public class RateNotificationHandler : IMessageHandler<RateNotification>
{
    // set initial offset
    [Offset(TopicOffset.End, -1000)]
    // retry forever when failure
    [RetryOnFailure]
    // skip all failures
    [SkipFailure]
    public Task HandleAsync(RateNotification message) { ... }
}
```

## Configure all in appsettings.json

When using [MessageConfig] all the configuration along with blocks will be 
retrieved from message configuration in appsettings. 

```c#
public class RateNotificationHandler : IMessageHandler<RateNotification>
{
    // set initial offset
    [MessageConfig("MessageName")]
    public Task HandleAsync(RateNotification message) { ... }
}
```

Actual message consumption configuration:

```json
{
  "Kafka": {
    "Message": {
      "Default": "buffer(100)",
      "MessageName": "offset: end, buffer(100), parallel(), commit()"
    }
  }
}
```

*Kafka:Message:Default* - specified blocks will be added by default for all message
subscriptions overriding any values set in the code.

*Kafka:Message:MessageName* - specified blocks will be added to messages marked
with [MessageConfig("MessageName")] attribute only overriding any values set in the code or Default
configuration above.

# Interceptors

```c#

public class MyInterceptor : IMessageInterceptor
{
    public Task ConsumeAsync(ICommitable commitable, Exception exception);
    {
        if(commitable is IMessage message) {
            Console.WriteLine($"{message.Topic} processed. Exception: {exception}");
        }
        else {
            // otherwise it could be a batch reporting an exception occurred
        }
        return Task.CompletedTask;
    }
    
    public Task ProduceAsync(string topic, object key, object message, Exception exception)
    {
        Console.WriteLine($"{message.Topic} produced. Exception: {exception}");
        return Task.CompletedTask;
    }
}

services
    .AddKafka(Configuration)
    .AddInterceptor<MyInterceptor>();
```

# In-memory broker for Consumer/Producer mocking

The following setup will create a memory based broker and the actual 
IKafkaConsumer and IKafkaProducer are used as usual with complete support of all features.

An additional interface IKafkaMemoryBroker is available from DI container for produce/consume tracking or specific setup.

```c#
public void ConfigureServices(IServiceCollection services)
{
    services
        .AddKafka(_config)
        .UseInMemoryBroker();
}
```

# Metrics

Implemented as a MetricsInterceptor.

```c#
services
    .AddKafka(Configuration)
    .AddMetrics();
```

# Configuration

```json
{
  "Kafka": {
    "Group": "consumer-group-name",
    "Producer": {
      "linger.ms": 5,
      "socket.timeout.ms": 15000,
      "message.send.max.retries": 10,
      "message.timeout.ms": 200000
    },
    "Consumer": {
      "socket.timeout.ms": 15000,
      "enable.auto.commit": false
    },
    "Message": {
      "Default": "offset: stored",
      "Rate": "offset: end, buffer(100), parallel(), batch(100, 1000), commit()"
    }
  },
  "ConnectionStrings": {
    "Kafka": "192.168.0.1:9092,192.168.0.2:9092",
    "SchemaRegistry": "http://192.168.0.1:8084"
  }
}
```
