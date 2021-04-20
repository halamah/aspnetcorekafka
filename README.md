# AspNetCore.Kafka

[Sample program](Sample/Program.cs)

The following implementation covers:
  * An abstraction over Confluent.Kafka with a predefined TPL based pipeline blocks.
  * Subscribe in declarative way as well as a regular fluent style.
  * Buffering, batching, parallelization etc. - available out of the box.
  * Flexible configuration either explicitly in code or via appsettings.json.  
  * Intercept messages.
  * An In-memory broker provider for unit and integration testing.

# Registration

```c#
// Get Kafka bootstrap servers from ConnectionString:Kafka options
services.AddKafka(Configuration);
```

# Message handlers

To cover different scenarios - subscriptions can be declared in several ways:
* type marked with a [MessageHandler] attribute and any number of methods (subscriptions) marked with [Message] attribute.
* type that implements IMessageHandler interface and any number of methods (subscriptions) marked with [Message] attribute.
* type that implements [MessageHandler<T>] interface and a [HandleAsync(T)] method (subscription) implementation. For multiple subscriptions within a single type - that type should implement multiple interfaces.

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

Example 2 : Complex pipeline

```c#
  var subscription = _consumer
    .Pipeline<Notification>() // create pipeline
    .Buffer(100) // buffer messages
    .Partitioned(4) // parallelise the rest of pipeline per partitions (optionally limiting the maximum degree of parallelism)
    .Batch(100, TimeSpan.FromSeconds(5)) // batch messages
    .Action(x => LogAsync(x)) // handler
    .Commit() // commit offsets when handler finished
    .Subscribe("topic-name", new SourceOptions { ... }); // perform actual subscription
```

Example 3 : Observable

When using an AsObservable extension with empty pipeline - a 1 message buffer is inserted.

```c#
  var subscription = _consumer.Pipeline<Notification>().AsObservable();
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
    // class with proper DI support.

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
    // class with proper DI support.

    public Task HandleAsync(IMessage<RateNotification> message) { ... }
}

// or handle payload directly
public class RateNotificationMessageHandler : IMessageHandler<RateNotification>
{
    // class with proper DI support.

    public Task HandleAsync(RateNotification message) { ... }
}
```

## In-place topic details

```c#
// Kafka message handler
public class WithdrawNotificationMessageHandler : IMessageHandler
{
    // class with proper DI support.

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

## Batches, Buffer, Commit and parallelized execution per partition

The order of attributes doesn't matter - the actual pipeline is always get built this way: 

[Buffer] > [Parallel] > [Batch] > [Execute] > [Commit]

Any of the following blocks could be omitted.

[Parallel] with DegreeOfParallelism set to greater than 1 - is to lower the actual degree of parallelization, 
otherwise it's set to [-1] and means the degree of parallelization equals to partitions count of the target topic.

```c#
[MessageHandler]
public class RateNotificationHandler
{
    // required
    [Message]
    // buffer messages
    [Buffer(Size = 100)]
    // parallelized execution per partition
    [AsParallel(DegreeOfParallelism = 4)]
    // use constant values
    [Batch(Size = 190, Time = 5000)]
    //commit after handler finished
    [Commit]
    // Parameter of type IEnumerable<IMessage<RateNotification>> is also supported
    public Task Handler(IMessageEnumerable<RateNotification> messages)
    {
        Console.WriteLine($"Received batch with size {messages.Count}");
        return Task.CompletedTask;
    }
}
```

## Pipeline configuration

Any message processing pipeline can be configured in appsettings in the following way.

appsetings.json:

```json
{
  "Kafka": {
    "Message": {
      "Default": "buffer(100)",
      "MessageName": "offset: end, buffer(100), parallel(), batch(100, 1000), commit()"
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
    public Task ConsumeAsync(IMessage message, Exception exception);
    {
        Console.WriteLine($"{message.Topic} processed. Exception: {exception}");
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
    .AddInterceptor(new MyInterceptor())
    // or
    .AddInterceptor(x => new MyInterceptor())
    // or
    .AddInterceptor(typeof(MyInterceptor))
    // or
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
