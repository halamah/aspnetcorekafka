# AspNetCore.Kafka

[Sample program](Sample/Program.cs)

The following implementation covers:
* An abstraction over Confluent.Kafka with a predefined TPL based subscription blocks.
* Subscribe in declarative way as well as a regular fluent style.
* Intercept messages.
* Buffering, batching etc.
* An In-memory broker provider for unit and integration testing.

## Registration

```c#
// Get Kafka bootstrap servers from ConnectionString:Kafka options
services.AddKafka(Configuration);
```

## Message handlers

To cover different scenarios - subscriptions can be declared in several ways:
* type marked with a [MessageHandler] attribute and any number of methods (subscriptions) marked with [Message] attribute.
* type that implements IMessageHandler interface and any number of methods (subscriptions) marked with [Message] attribute.
* type that implements [MessageHandler<T>] interface and a [HandleAsync(T)] method (subscription) implementation. For multiple subscriptions within a single type - that type should implement multiple interfaces.

### Fluent subscription

Example 1

```c#
  var subscription = _consumer.Subscribe("topic-name", x => LogAsync(x), new SubscriptionOptions { 
    Format = TopicFormat.Avro, 
    Offset = TopicOffset.Begin,
    Bias = -1000,
    DateOffset = DateTimeOffset.UtcNow - TimeSpan.FromDays(1),
    RelativeOffsetMinutes = TimeSpan.FromDays(1)
  });
```

Example 2

```c#
  var pipeline = _consumer.Pipeline("topic-name", new SubscriptionOptions { 
    Format = TopicFormat.Avro, 
    Offset = TopicOffset.Begin,
    Bias = -1000,
    DateOffset = DateTimeOffset.UtcNow - TimeSpan.FromDays(1),
    RelativeOffsetMinutes = TimeSpan.FromDays(1)
  });
  
  pipeline.Buffer(100).Batch(100, TimeSpan.FromSeconds(5)).Execute(x => LogAsync(x)).Commit().Subscribe();
```

### Message contract declaration

```c#
[Message(Topic = "event.currency.rate-{env}", Format = TopicFormat.Avro)]
public class RateNotification
{
    public string Currency { get; set; }
    public decimal Rate { get; set; }
}
```

### Attribute based subscription

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
    
    // or handle concrete type
    [Message] public Task Handler(RateNotification message) { ... };
}
```

### Subscription over an interface

* Subscribe all Types implementing [IMessageHandler] interface.
* Message handler and specific subscription on a method marked with [Message] attribute.

```c#
// Kafka message handler
public class RateNotificationMessageHandler : IMessageHandler
{
    // class with proper DI support.

    // with message wrapper
    [Message] public Task Handler(IMessage<RateNotification> message) { ... }
    
    // or handle concrete type
    [Message] public Task Handler(RateNotification message) { ... };
}
```

### Subscription over an interface with specific message type

* Subscribe all Types implementing [IMessageHandler<T>] interface.
* Message handler and specific subscription on a [Handle] method that implements IMessageHandler<T>.

```c#
// with message wrapper
public class RateNotificationMessageHandler : IMessageHandler<IMessage<RateNotification>>
{
    // class with proper DI support.

    public Task HandleAsync(IMessage<RateNotification> message) { ... }
}

// or handle concrete type
public class RateNotificationMessageHandler : IMessageHandler<RateNotification>
{
    // class with proper DI support.

    public Task HandleAsync(RateNotification message) { ... }
}
```

### In-place topic details

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

## Message blocks

### Batches and/or Buffer and Commit

```c#
[MessageHandler]
public class RateNotificationHandler
{
    // required
    [Message]
    // buffer messages
    [Buffer(Size = 100)]
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

## Interceptors

```c#

public class MyInterceptor : IMessageInterceptor
{
    public Task ConsumeAsync(IMessage<object> message, Exception exception);
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

## In-memory broker for Consumer/Producer mocking

```c#
public void ConfigureServices(IServiceCollection services)
{
    services
        .AddKafka(_config)
        .UseInMemoryBroker();
}
```

## Metrics

Implemented as a MetricsInterceptor.

```c#
services
    .AddKafka(Configuration)
    .AddMetrics();
```

## Configuration

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
    }
  },
  "ConnectionStrings": {
    "Kafka": "192.168.0.1:9092,192.168.0.2:9092",
    "SchemaRegistry": "http://192.168.0.1:8084"
  }
}
```
