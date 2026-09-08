### Monitoring consumer lag

To support Kafka tools that monitor consumer lag, Kafka sources commit offsets
once the messages up through that offset have been durably recorded in
Materialize's storage layer.

However, rather than relying on committed offsets, Materialize suggests using
our native [progress monitoring](#monitoring-source-progress), which contains
more up-to-date information.

{{< note >}}
Some Kafka monitoring tools may indicate that Materialize's consumer groups have
no active members. This is **not a cause for concern**.

Materialize does not participate in the consumer group protocol nor does it
recover on restart by reading the committed offsets. The committed offsets are
provided solely for the benefit of Kafka monitoring tools.
{{< /note >}}

Committed offsets are associated with a consumer group specific to the source.
The ID of the consumer group consists of the prefix configured with the [`GROUP
ID PREFIX` option](#syntax) followed by a Materialize-generated
suffix.

You should not make assumptions about the number of consumer groups that
Materialize will use to consume from a given source. The only guarantee is that
the ID of each consumer group will begin with the configured prefix.

The consumer group ID prefix for each Kafka source in the system is available in
the `group_id_prefix` column of the [`mz_kafka_sources`] table. To look up the
`group_id_prefix` for a source by name, use:

```mzsql
SELECT group_id_prefix
FROM mz_internal.mz_kafka_sources ks
JOIN mz_sources s ON s.id = ks.id
WHERE s.name = '<src_name>'
```
