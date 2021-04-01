# AspNetCore.Kafka samples

## Registration

```c#
// Get Kafka bootstrap servers from ConnectionString:Kafka options
services.AddKafka(Configuration);
```

## Message handlers

```c#
// optional attribute
[Message(Topic = "event.currency.rate-{env}", Format = TopicFormat.GenericRecord)]
public class RateNotification
{
    public string Currency { get; set; }
    public decimal Rate { get; set; }
}

// Attribute to mark as a Kafka message handler.
// Otherwise - class name must have a 'MessageHandler' suffix.
[Message]
public class RateNotificationMessageHandler
{
    // class with proper DI support.

    // Required attribute for actual subscription
    [Message(Topic = "event.currency.rate-{env}", Format = TopicFormat.Avro))]
    // or to get topic name from type definition attribute
    [Message]
    public Task Handler(IMessage<RateNotification> message)
    {
        Console.WriteLine($"{message.Currency} rate is {message.Rate}");
        return Task.CompletedTask;
    }
}
```

## Interceptors

```c#

public class MyInterceptor : IMessageInterceptor
{
    public Task ConsumeAsync(IMessage<object> payload, Exception exception);
    {
        Console.WriteLine($"{payload.Topic} processed. Exception: {exception}");
        return Task.CompletedTask;
    }
    
    public Task ProduceAsync(string topic, object key, object message, Exception exception)
    {
        Console.WriteLine($"{payload.Topic} produced. Exception: {exception}");
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

## Message converters

User defined converters.

A predefined batch converter supposed to be inherited to set batching parameters.

```c#
public class MyBatchOptions
{
    // Max size of the batch
    public long Capacity { get; set; }
    
    // Max period in milliseconds to populate batch before consuming
    public long Latency { get; set; }
}

public class RateNotificationMessageHandler
{
    [Message]
    // batching
    [MessageConverter(typeof(BatchMessageConverter), typeof(MyBatchOptions))]
    // or
    [MessageBatch(Capacity = 0, Latency = 15)]
    // or
    [MessageBatch(typeof(MyBatchOptions))]
    public Task Handler(IEnumerable<IMessage<RateNotification>> messages)
    {
        Console.WriteLine($"Received batch with size {messages.Count}");
        return Task.CompletedTask;
    }
}
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
    "Kafka": "192.168.0.1:9092,192.168.0.2:9092"
  }
}
```