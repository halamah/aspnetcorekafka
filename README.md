# AspNetCore.Kafka

[Sample program](Sample/Program.cs)

The following implementation covers:
* An abstraction over Confluent.Kafka client to provide a bit more flexibility 
  and ready to use features in AspNetCore infrastructure.
* Subscribe in declarative way.
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

    [Message]
    public Task Handler(IMessage<RateNotification> message)
    {
        Console.WriteLine($"{message.Value.Currency} rate is {message.Value.Rate}");
        return Task.CompletedTask;
    }
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

    [Message]
    public Task Handler(IMessage<RateNotification> message)
    {
        Console.WriteLine($"{message.Value.Currency} rate is {message.Value.Rate}");
        return Task.CompletedTask;
    }
}
```

### Subscription over an interface with specific message type

* Subscribe all Types implementing [IMessageHandler<T>] interface.
* Message handler and specific subscription on a [Handle] method that implements IMessageHandler<T>.

```c#
// Kafka message handler
public class RateNotificationMessageHandler : IMessageHandler<RateNotification>
{
    // class with proper DI support.

    public Task HandleAsync(IMessage<RateNotification> message)
    {
        Console.WriteLine($"{message.Value.Currency} rate is {message.Value.Rate}");
        return Task.CompletedTask;
    }
}
```

### In-place topic details

```c#
// Kafka message handler
public class WithdrawNotificationMessageHandler : IMessageHandler
{
    // class with proper DI support.

    // Inplace topic subscription definition and a backing consumption buffer
    [Message(Topic = "withdraw_event-{env}", Format = TopicFormat.Avro, Offset = TopicOffset.Begin, Buffer = 100))]
    public Task Handler(IMessage<WithdrawNotification> message)
    {
        Console.WriteLine($"Withdraw {message.Value.Amount} {message.Value.Currency}");
        return Task.CompletedTask;
    }
}
```

## Message blocks

### Batches

```c#
public class MyBatchOptions : IMessageBatchOptions
{
    // Max size of the batch
    public int Size { get; set; }
    
    // Max period in milliseconds to populate batch before consuming
    public int Time { get; set; }
    
    // Whether to commit latest offset when batch handler completed (despite of it's succeedded or not)
    public bool Commit { get; set; }
}

[MessageHandler]
public class RateNotificationHandler
{
    // required
    [Message]
    // use constant values
    [MessageBatch(Size = 190, Time = 5000, Commit = true)]
    // or resolve from DI
    [MessageBatch(typeof(MyBatchOptions))]
    // Parameter of type IEnumerable<IMessage<RateNotification>> is also supported
    public Task Handler(IMessageEnumerable<RateNotification> messages)
    {
        Console.WriteLine($"Received batch with size {messages.Count}");
        return Task.CompletedTask;
    }
}
```

### Custom block sample

The following block will filter transaction events by transaction Amount property 
according to attribute value or use a value resolved from provided options. 

```c#
public interface IAmountFilterOptions 
{ 
    decimal Threshold { get; }
}

public class AmountFilterBlockOptions : IAmountFilterOptions 
{ 
    public decimal Threshold { get; set; }
}

public class AmountFilterBlock
{
    private readonly IAmountFilterOptions _options;
    
    public AmountFilterBlock(IAmountFilterOptions options, /* Other DI dependencies */)
        => _options = options;
    
    public Func<IMessage<T>, Task> Create<T>(Func<IMessage<T>, Task> next)
    {
        return async x => {
            if(x.Value.Amount > _options.Amount)
                await next(x);
        };
    }
}

public class AmountFilterAttribute : MessageBlockAttribute, IAmountFilterOptions
{
    public AmountFilterAttribute() : base(typeof(AmountFilterBlock)) { }
    
    public MessageBatchAttribute(Type argumentType) : base(typeof(AmountFilterBlock), argumentType)
    { }
    
    public decimal Threshold { get; set; }
}

[MessageHandler]
public class TransactionHandler
{
    [Message]
    [AmountFilter(Threshold = 100)]
    public Task Handler(IMessage<TransactionNotification> message)
    {
        Console.WriteLine($"Received transaction with amount {message.Value.Amount}");
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
