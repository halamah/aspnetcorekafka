using System;
using System.Collections.Generic;
using System.Linq;
using AspNetCore.Kafka.Options;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace AspNetCore.Kafka.Client.Consumer
{
    public static class PartitionsAssigner
    {
        public static IEnumerable<TopicPartitionOffset> Handler<TKey, TValue>(ILogger logger,
            SubscriptionConfiguration subscription,
            IConsumer<TKey, TValue> consumer,
            List<TopicPartition> partitions)
        {
            logger.LogInformation("Assignment for {Subscription}({Partitions}): {Assignment}",
                string.Join(", ", consumer.Subscription),
                string.Join(", ", partitions.Select(x => x.Partition.Value)),
                string.Join(", ", consumer.Assignment));

            var offset = subscription.Options?.Offset?.Offset ?? TopicOffset.Stored;
            var bias = subscription.Options?.Offset?.Bias ?? 0;
            var date = subscription.Options?.Offset?.DateOffset;
            
            Offset Bias(TopicPartition partition)
            {
                var range = new Lazy<WatermarkOffsets>(() =>
                    consumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(5)));

                var current = new Lazy<Offset>(() => consumer
                    .Committed(new[] {partition}, TimeSpan.FromSeconds(5))
                    .Select(x => x.Offset)
                    .Concat(new[] {Offset.Unset})
                    .First()
                    .Otherwise(range.Value.Low));

                return offset switch
                {
                    TopicOffset.Begin => bias == 0
                        ? Offset.Beginning
                        : Math.Clamp(range.Value.Low + bias, range.Value.Low, range.Value.High),

                    TopicOffset.End => bias == 0
                        ? Offset.End
                        : Math.Clamp(range.Value.High + bias, range.Value.Low, range.Value.High),

                    TopicOffset.Stored => bias == 0
                        ? current.Value
                        : Math.Clamp(current.Value + bias, range.Value.Low, range.Value.High),

                    _ => throw new ArgumentOutOfRangeException(nameof(offset))
                };
            }

            var offsets = date == null
                ? partitions.Select(partition => new TopicPartitionOffset(partition, Bias(partition))).ToList()
                : consumer.OffsetsForTimes(
                    partitions.Select(x => new TopicPartitionTimestamp(x, new Timestamp(date.Value))),
                    TimeSpan.FromSeconds(5));

            logger.LogInformation("Partition offsets assigned {Offsets}",
                string.Join(",", offsets.Select(x => x.Offset.Value)));

            return offsets;
        }
    }
}