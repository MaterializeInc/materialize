---
title: "ALTER SINK"
description: "`ALTER SINK` changes certain characteristics of a sink."
menu:
  main:
    parent: 'commands'
---

`ALTER SINK` changes certain characteristics of a sink.

## Syntax

{{< diagram "alter-sink.svg" >}}

## Context

Altering the `FROM` item of a sink results in Materialize picking a consistent
cutover timestamp. The resulting topic will contain all the updates of the old
item that happened before the cutover timestamp and all the updates of the new
item that happened after the cutover timestamp. Care must be taken to ensure
that the old and the new items make sense when stitched together. See the
anomalies section for more details on what can be observed.

**Note**: In order to select a consistent timestamp Materialize waits for the
previous definition of the sink to make enough progress such that there is
overlap between the available timestamps of the desired new item. Attempting to
`ALTER` an unhealthy sink that can't make progress will result to the command
timing out.

### Valid schema changes

Materialize allows altering a sink to a new item provided that the new item
leads to a valid sink definition with the same rules as the `CREATE SINK`
statement.

When using the Avro format with a schema registry care must be taken such that
the generated Avro schema of the new item is compatible with the previously
published schema. When that is not the case the `ALTER SINK` statement will
succeed but the subsequent execution of the sink will result in errors and will
be unable to make progress.

### Possible anomalies

Depending on the contents and state of the old and new items a few different
anomalies can be observed in the output topic as a result of altering a sink.
These anomalies occur because `ALTER SINK` causes Materialize to emit updates
from the new item only if they happen after the cutover timestamp.

**Anomaly 1: Topic contains stale value for a key**

As a consequence of not re-emitting a snapshot after `ALTER SINK`, all keys of
the collection will appear to have the value they had in the old collection
until an update happens to them. At that point the current value will be
published to the topic.

It is important that readers of the topic are prepared to handle an old value
for a key, for example by filling in additional columns with default values.
Alternatively, forcing an update to all the keys after `ALTER SINK` will force
the sink to re-emit all the updates.

**Anomaly 2: Topic is missing a key that exists in the new item**

As a consequence of not re-emitting a snapshot after `ALTER SINK`, if
additional keys exist in the new item that are not present in the old item they
will not be visible in the topic after altering it. The keys will remain absent
until they are updated, at which point Materialize will emit a record in the
topic containing the new value.

This anomaly can be avoided by ensuring both the old and the new items have
identical keyspaces.

**Anomaly 3: Topic contains a key that does not exist in the new item**

Materialize does not compare the contents of the old item with the new item
when altering a sink. This means that if the old item contains additional keys
that are not present in the new item, their entries will remain in the topic
without a corresponding tombstone record. This may cause readers to assume that
certain keys exist when they don't.

This anomaly can be avoided by ensuring both the old and the new items have
identical keyspaces.


## Examples

To alter a sink so that it starts sinking the contents of `new_from`:

```sql
ALTER SINK foo SET FROM new_from;
```

## Privileges

The privileges required to execute this statement are:

- Ownership of the sink being altered.
- `SELECT` privileges on the new item being written out to an external system.
- `CREATE` privileges on the containing cluster.
- `USAGE` privileges on all connections and secrets used in the sink definition.
- `USAGE` privileges on the schemas that all connections and secrets in the statement are contained in.

## See also

- [`CREATE SINK`](/sql/create-sink/)
- [`SHOW SINKS`](/sql/show-sinks)
