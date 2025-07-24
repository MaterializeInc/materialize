---
title: "ALTER SINK"
description: "`ALTER SINK` allows cutting a sink over to a new upstream relation without causing disruption to downstream consumers."
menu:
  main:
    parent: 'commands'
---

`ALTER SINK` allows cutting a sink over to a new upstream relation without
causing disruption to downstream consumers. This is useful in the context
of [blue/green deployments](/manage/dbt/blue-green-deployments/).

## Syntax

{{< diagram "alter-sink.svg" >}}

## Details

To alter the upstream relation a sink depends on while ensuring continuity in
data processing, Materialize must pick a consistent cutover timestamp. When you
execute an `ALTER SINK` command, the resulting output will contain all the
updates that happened before the cutover timestamp for the old relation, as
well as all the updates that happened after the cutover timestamp for the new
relation.

{{< note >}}
To select a consistent timestamp, Materialize must wait for the previous
definition of the sink to emit results up until the oldest timestamp at which
the contents of the new upstream relation are known. Attempting to `ALTER` an
unhealthy sink that can't make progress will result in the command timing out.
{{</ note >}}

A sink cannot be created directly on a catalog object. As a workaround you can
create a materialized view on a catalog object and create a sink on the
materialized view.

### Valid schema changes

For `ALTER SINK` to be successful, the newly specified relation must lead to a
valid sink definition with the same conditions as the original `CREATE SINK`
statement.

When using the Avro format with a schema registry, the generated Avro
schema for the new relation must be compatible with the previously published
schema. If that's not the case, the `ALTER SINK` command will succeed, but the
subsequent execution of the sink will result in errors and will not be able to
make progress.

To monitor the status of a sink after an `ALTER SINK` command, navigate to the
respective object page in the [Materialize console](https://console.materialize.com/),
or query the [`mz_internal.mz_sink_statuses`](/sql/system-catalog/mz_internal/#mz_sink_statuses)
system catalog view.

### Cutover scenarios

Because Materialize emits updates from the newly specified relation **only** if
they happen after the cutover timestamp, you might observe different scenarios
in the output topic. Depending on the contents and state of the old and new
relations at the time the `ALTER SINK` command is executed, some common
scenarios are:

**Scenario 1: Topic contains stale value for a key**

Since cutting over a sink to a new upstream relation using `ALTER SINK` does not
emit a snapshot of the new relation, all keys will appear to have the old value
for the key in the previous relation until an update happens to them. At that
point, the current value will be published to the topic.

Consumers of the topic must be prepared to handle an old value for a key, for
example by filling in additional columns with default values. Alternatively,
forcing an update to all the keys after `ALTER SINK` will force the sink to
re-emit all the updates.

**Scenario 2: Topic is missing a key that exists in the new relation**

As a consequence of not re-emitting a snapshot after `ALTER SINK`, if additional
keys exist in the new relation that are not present in the old one, these will
not be visible in the topic after the cutover. The keys will remain absent
until an update happens to them, at which point Materialize will emit a record
to the topic containing the new value.

To avoid this, ensure that both the old and the new relations have identical
keyspaces.

**Scenario 3: Topic contains a key that does not exist in the new relation**

Materialize does not compare the contents of the old relation with the new
relation when cutting a sink over. This means that, if the old relation
contains additional keys that are not present in the new one, these records
will remain in the topic without a corresponding tombstone record. This may
cause readers to assume that certain keys exist when they don't.

To avoid this, ensure that both the old and the new relations have identical
keyspaces.

## Examples

To alter a sink originally created to use `matview_1` as the upstream relation,
and start sinking the contents to `matview_2` instead:

```mzsql
CREATE SINK avro_sink
  FROM matview_1
  INTO KAFKA CONNECTION kafka_connection (TOPIC 'test_avro_topic')
  KEY (key_col)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE UPSERT;
```

```mzsql
ALTER SINK foo SET FROM matview_2;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-sink.md" >}}

## See also

- [`CREATE SINK`](/sql/create-sink/)
- [`SHOW SINKS`](/sql/show-sinks)
