# AspNetCore.Kafka samples

## Registration

```c#
// Get Kafka bootstrap servers from ConnectionString:Kafka options
services.AddKafka(Configuration);
```

## Message handlers

```c#
// optional attribute
[Message(Topic = "event.currency.rate-{env}", Format = TopicFormat.Avro)]
public class RateNotification
{
    public string Currency { get; set; }
    public decimal Rate { get; set; }
}

...

// Kafka message handler
[Message]
public class RateNotificationMessageHandler
{
    // class with proper DI support.

    [Message]
    public Task Handler(IMessage<RateNotification> message)
    {
        Console.WriteLine($"{message.Currency} rate is {message.Rate}");
        return Task.CompletedTask;
    }
}

...

// Kafka message handler
[Message]
public class WithdrawNotificationMessageHandler
{
    // class with proper DI support.

    // Inplace topic subscription definition and a backing consumption buffer
    [Message(Topic = "withdraw_event-{env}", Format = TopicFormat.Avro, Offset = TopicOffset.Begin, Buffer = 100))]
    public Task Handler(IMessage<WithdrawNotification> message)
    {
        Console.WriteLine($"Withdraw {message.Amount} {message.Currency}");
        return Task.CompletedTask;
    }
}
```

## Message blocks

* [MessageBatch] - batch messages by size and time.

User defined message blocks supported via MessageBlockAttribute

```c#
public class MyBatchOptions
{
    // Max size of the batch
    public int Size { get; set; }
    
    // Max period in milliseconds to populate batch before consuming
    public int Time { get; set; }
}

public class RateNotificationMessageHandler
{
    // required
    [Message]
    // batching
    [MessageBlock(typeof(BatchMessageBlock), typeof(MyBatchOptions))]
    // or
    [MessageBatch(Size = 190, Time = 5000)]
    // or
    [MessageBatch(typeof(MyBatchOptions))]
    public Task Handler(IEnumerable<IMessage<RateNotification>> messages)
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
