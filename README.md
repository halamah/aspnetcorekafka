# AspNetCore.Kafka

A messaging infrastructure for Confluent.Kafka and AspNetCore.

# Features

* Declare your subscriptions
* Complex processing pipeline configuration (buffer, batch, parallel etc.)
* Shutdown/Re-balance process to correctly store all the offsets
* Interceptors
* Mock implementation for unit tests
* Flexible configuration in appsettings

# Registration

```c#
services.AddKafka(Configuration);
```

### Extended registration

```c#
services
    .AddKafka(Configuration)
        .ConfigureSerializer(new MySerializer())
        .Subscribe(x => x
            .AddTransient<PlayerLoginHandler>() // subscribe explicitly specified handler only
            .AddAssembly(ServiceLifetime.Transient)) // subscribe explicitly from assembly
        .AddInterceptor<Interceptor>()
        .AddMetrics();
```

This will register an IKafkaConsumer interface in AspNetCore DI container. But you won't need it when defining handlers with MessageHandler attribute or deriving from IMessageHandler interface.

# Handlers

If you configure Kafka to scan assembly for declared handlers i.e. `services.AddKafka(Configuration).Subscribe(x => x.AddAssembly(ServiceLifetime.Transient));`, the only thing you need to start consuming is the handler itself.

### Declare handler with attribute

```c#
[MessageHandler]
public class RateNotificationMessageHandler
{
    [Message(Topic = "my.topic")]
    public Task Handler(IMessage<RateNotification> message) { ... }
}
```

### Derive your handler from `IMessageHandler` interface

```c#
[MessageHandler]
public class RateNotificationMessageHandler : IMessageHandler
{
    [Message(Topic = "my.topic")]
    public Task Handler(IMessage<RateNotification> message) { ... }
}
```

### Derive your handler from `IMessageHandler<TContract>` interface

```c#
[MessageHandler]
public class RateNotificationMessageHandler : IMessageHandler<RateNotification>
{
    [Message(Topic = "my.topic")]
    public Task Handler(IMessage<RateNotification> message) { ... }
}
```

In example above TContract is an actual event type after deserialization. It's wrapped into IMessage to get access to additional message properties like offset etc.

If you don't need those additional message properties it's allowed to consume raw TContract, i.e.

### Derive your handler from `IMessageHandler<TContract>` interface without `IMessage` additonal properties

```c#
[MessageHandler]
public class RateNotificationMessageHandler : IMessageHandler<RateNotification>
{
    [Message(Topic = "my.topic")]
    public Task Handler(RateNotification message) { ... }
}
```

## Message contract declaration

Having a contract type in you handler it's possible to define a topic this type is tied to with `Message` attribute as below.
In this case you don't need to repeat a topic name neither in your consumer handler or when producing message for this specific type - topic name will be retrieved from Message declaration.

```c#
[Message(Topic = "topic.name-{env}", Format = TopicFormat.Avro)]
public record RateNotification(string Currency, decimal Rate);

// ...

public class RateNotificationMessageHandler : IMessageHandler<RateNotification>
{
    public Task Handler(RateNotification message) { ... }
}
```

### `IMessage` wrapper

This will give you a chance to access Kafka related message properties like partition, offset, group, key etc. 
Also the are two methods: `Commit()` and `Store()` to manually commit or store (see Kafka documentation for more details) current message offset. 
It's also possible to configure message processing pipeline (along with Kafka related consumer configuration) to do all the stuff automatically.

# Shutdown consumption

When consumption is shutting down or broker initiated a re-balance - it is important to process and commit/store offsets correctly. 
Having an async processing and an internal processing pipeline when a client receives a shutdown notification (basically when an application is shutting down) or a re-balance notification - 
it will wait for the entire consumed messages (consumed but not processed yet) to be completely processed and their offsets stored as per configuration and only then will allow shutdown or rebalance.

In other words when you shutdown, bounce, re-deploy your service or a broker is re-balancing - all messages are supposed to be processed before the initiated action, so you have all the offsets stored correctly.

# Fluent subscription

#### Simple handler

```c#
  var subscription = _consumer.Subscribe("topic.name", x => LogAsync(x), new SourceOptions {
    // set topic format 
    Format = TopicFormat.Avro,
    // change consume offset to start from 
    Offset = new MessageOffset { ... },
  });
```

#### Pipeline

```c#
  var subscription = _consumer
    .Message<Notification>() // create pipeline
    .Where(x => x is not null)
    .Buffer(100) // buffer messages
    .Parallel(4) // parallelise the rest of pipeline per partitions (optionally limiting the maximum degree of parallelism)
    .Batch(100, TimeSpan.FromSeconds(5)) // batch messages
    .Action(x => LogAsync(x)) // handler
    .Commit() // commit offsets when handler finished
    .Subscribe(); // actual subscription
```

## Change consumption offset

Changing consume offset to start from can be set in fluent pipeline,
message Offset attributes or via configuration.

#### Fluent

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

#### Message Offset attribute

```c#
[MessageHandler]
public class RateNotificationMessageHandler
{
    [Message]
    // start consume from end minus 1000 (per partition partition)
    [Offset(TopicOffset.End, -1000)]
    // or at specific date
    [Offset("2021-01-01T00:00:00Z")]
    public Task Handler(IMessage<RateNotification> message) { ... };
}
```

#### Configuration

appsetings.json:

```json
{
  "Kafka": {
    "Message": {
      "Default": "offset: begin, bias: -100, dateOffset: 2021-01-01",
      "MyMessage": "offset: begin, bias: -100, dateOffset: 2021-01-01"
    }
  }
}
```

**Kafka:Message:Default**:<br>
Offset config will be added by default for all message subscriptions.

**Kafka:Message:[MyMessage]**:<br>
Offset config will be added to messages marked
with [Message(Name = "MyMessage")] attribute only overriding any values set in Default
configuration above.

# Message processing pipeline

Message pipelines are based on TPL blocks.

## Batches, Buffer, Commit/Store and Parallel execution per partition

The order of attributes doesn't matter - the actual pipeline is always get built this way:

[Buffer] > [Parallel] > [Batch] > [Action] > [Commit] / [Store]

Any of the following blocks could be omitted.

[Parallel] with DegreeOfParallelism set to greater than 1 - is to lower the actual degree of parallelization,
otherwise it's set to [-1] and means the degree of parallelization equals to partitions count of the target topic.

```c#
public class RateNotificationHandler : IMessageHandler<IEnumerable<RateNotification>>
{
    // set initial offset
    [Offset(TopicOffset.End, -1000)]
    // buffer messages
    [Buffer(Size = 100)]
    // parallelized execution per partition
    [Parallel(DegreeOfParallelism = 4)]
    // use constant values
    [Batch(Size = 190, Time = 5000)]
    //commit or store offset after handler finished
    [Commit] / [Store]
    //retry 3 times with a 500ms delay when handler failed
    [Retry(3, 500)]
    public Task HandleAsync(IEnumerable<RateNotification> messages)
    {
        Console.WriteLine($"Received batch with size {messages.Count}");
        return Task.CompletedTask;
    }
}
```

## Configure all in appsettings.json

You could specify a message name to get all the configuration along with policies from
message configuration in appsettings.

```c#
public class RateNotificationHandler : IMessageHandler<RateNotification>
{
    // set initial offset
    [Message(Name = "MyMessage")]
    public Task HandleAsync(RateNotification message) { ... }
}
```

Actual message consumption configuration:

```json
{
  "Kafka": {
    "Message": {
      "Default": "buffer(100), retry(3)",
      "MyMessage": "offset: end, buffer(100), parallel(), commit()"
    }
  }
}
```

*Kafka:Message:Default* - specified blocks will be added by default for all message
subscriptions overriding any values set in the code.

*Kafka:Message:MyMessage* - properties ans policies will be added to messages marked
with [Message(Name = "MyMessage")] attribute overriding any values set by Default configuration above.

# Producing messages

```c#
IKafkaProducer producer;
```

Message producing is available using message declaration [Message] 
attribute (to get topic name and format) as well as setting it inline while actual
message producing.

```c#
[Message(Topic = "topic.name", Format = TopicFormat.Avro)]
public class RateNotification { ... }

...

// using message declaration
producer.ProduceAsync(new RateNotification());

// using inline topic definition 
producer.ProduceAsync("topic.name", new RateNotification());
```

# Keys for produced messages

Keys could be set in several ways:

#### Explicit keys
```c#
producer.ProduceAsync("topic.name", new RateNotification(), "keyId");
```

#### Key property name in message declaration:
```c#
[Message(Topic = "topic.name", Key = "KeyProperty")]
public class RateNotification
{ 
    public int KeyProperty { get; set; }
}

...


producer.ProduceAsync("topic.name", new RateNotification());
```

#### [MessageKey] attribute (preferred):
```c#
[Message(Topic = "topic.name")]
public class RateNotification
{ 
    [MessageKey]
    public string KeyProperty { get; set; }
}

...


producer.ProduceAsync("topic.name", new RateNotification());
```

# Interceptors

```c#

public class MyInterceptor : IMessageInterceptor
{
    public Task ConsumeAsync(KafkaInterception interception) => MeterAsync(interception, "Consume");

    public Task ProduceAsync(KafkaInterception interception) => MeterAsync(interception, "Produce");
    
    private Task MeterAsync(KafkaInterception interception, string name)
    {
        ...
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

...

class SampleTest
{
    [Fact]
    Task TestSomething()
    {
      IKafkaMemoryBroker kafkaBroker = <resolve from DI>
      
      // perform actions
      
      // assert nothing produced
      kafkaBroker.Topics.SelectMany(x => x.Produced).Should.BeEmpty();
    }
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

#### Message configuration properties

<table>
<tr><td>Name</td><td>Attribute</td><td>Value</td><td>Description</td></tr>
<tr><td>State</td><td>[MessageState]</td><td>Enabled/Disabled</td><td>Set message subscription state</td></tr>
<tr><td>Offset</td><td>[Offset]</td><td>[begin,end,stored] <br> 2020-01-01 <br> (end, -100)</td><td>Set message offset</td></tr>
<tr><td>Bias</td><td>[Offset]</td><td>e.g. -100</td><td>Set message offset bias. Offset is defaulted to End</td></tr>
<tr><td>Batch</td><td>[Batch]</td><td>e.g. (10, 1000)</td><td>Group incoming message into batches with count 10. If less than 10 wait 1000ms and then send message for further processing.</td></tr>
<tr><td>Buffer</td><td>[Buffer]</td><td>e.g. 100</td><td>Buffer incoming message during processing</td></tr>
<tr><td>Commit</td><td>[Commit]</td><td>-</td><td>Commit current message offset after message being processed</td></tr>
<tr><td>Store</td><td>[Store]</td><td>-</td><td>Store current message offset after message being processed</td></tr>
<tr><td>Parallel</td><td>[Parallel]</td><td>e.g. 2</td><td>2 - is a degree of paralelisation. Split pipeline into multiple sub-pipelines to process messages in parallel.</td></tr>
</table>

"Consume/Producer" json objects contain a set of Key/Values pairs that will eventually map to Kafka Consumer/Producer configuration, 
that is to say you can configure any additional client configuration in here, i.e. authentication mechanism.

### Sample

```json
{
  "Kafka": {
    "group.id": "consumer-group-name",
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
      "Rate": "state: enabled, offset: end, buffer(100), parallel(), batch(100, 1000), commit()"
    }
  },
  "ConnectionStrings": {
    "Kafka": "192.168.0.1:9092,192.168.0.2:9092",
    "SchemaRegistry": "http://192.168.0.1:8084"
  }
}
```
