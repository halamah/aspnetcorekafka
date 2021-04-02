using AspNetCore.Kafka.Avro;
using Avro.Generic;
using Confluent.Kafka;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace AspNetCore.Kafka.Client.Consumer
{
    public class MessageParser<TKey, TValue>
    {
        private static readonly JsonSerializerSettings JsonSerializerSettings = new()
        {
            DateTimeZoneHandling = DateTimeZoneHandling.Utc,
            ContractResolver = new CamelCasePropertyNamesContractResolver(),
        };
        
        public TContract Parse<TContract>(ConsumeResult<TKey, TValue> message) where TContract : class
        {
            if (message.Message.Value == null)
                return null;

            if (typeof(TValue) == typeof(TContract))
                return message.Message.Value as TContract;

            if (message.Message.Value is GenericRecord x)
                return x.ToObject<TContract>();

            var json = message.Message.Value.ToString(); 

            return json is not null 
                ? JsonConvert.DeserializeObject<TContract>(json, JsonSerializerSettings)
                : null;
        }
    }
}