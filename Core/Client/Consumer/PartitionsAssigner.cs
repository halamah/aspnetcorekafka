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
        public static IEnumerable<TopicPartitionOffset> Handler<TKey, TValue>(
            ILogger logger,
            TopicOffset offset,
            long bias,
            IConsumer<TKey, TValue> consumer,
            List<TopicPartition> partitions)
        {
            logger.LogInformation("Assignment for {Subscription}({Partitions}): {Assignment}",
                string.Join(", ", consumer.Subscription),
                string.Join(", ", partitions.Select(x => x.Partition.Value)),
                string.Join(", ", consumer.Assignment));

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

            var offsets = partitions
                .Select(partition => new TopicPartitionOffset(partition, Bias(partition)))
                .ToList();

            logger.LogInformation("Partition offsets assigned {Offsets}",
                string.Join(",", offsets.Select(x => x.Offset.Value)));

            return offsets;
        }
    }
}