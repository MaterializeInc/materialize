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

{{% include-example file="examples/alter_sink" example="syntax" %}}

## Details

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
respective object page in the [Materialize console](/console/),
or query the [`mz_internal.mz_sink_statuses`](/sql/system-catalog/mz_internal/#mz_sink_statuses)
system catalog view.

### Cutover timestamp

To alter the upstream relation a sink depends on while ensuring continuity in
data processing, Materialize must pick a consistent cutover timestamp. When you
execute an `ALTER SINK` command, the resulting output will contain:
- all updates that happened before the cutover timestamp for the old
relation, and
- all updates that happened after the cutover timestamp for the new
relation.

{{< note >}}
To select a consistent timestamp, Materialize must wait for the previous
definition of the sink to emit results up until the oldest timestamp at which
the contents of the new upstream relation are known. Attempting to `ALTER` an
unhealthy sink that can't make progress will result in the command timing out.
{{</ note >}}

### Cutover scenarios and workarounds

Because Materialize emits updates from the new relation **only** if
they occur after the cutover timestamp, the following scenarios may occur:

#### Scenario 1: Topic contains stale value for a key

Since cutting over a sink to a new upstream relation using `ALTER SINK` does not
emit a snapshot of the new relation, all keys will appear to have the old value
for the key in the previous relation until an update happens to them. At that
point, the current value will be published to the topic.

Consumers of the topic must be prepared to handle an old value for a key, for
example by filling in additional columns with default values.

**Workarounds**:

- Use an intermediary, temporary view to handle the cutover scenario difference.
See [Example: Handle cutover scenarios](#handle-cutover-scenarios).

- Alternatively, forcing an update to all the keys after `ALTER SINK` will force
the sink to re-emit all the updates.

#### Scenario 2: Topic is missing a key that exists in the new relation

As a consequence of not re-emitting a snapshot after `ALTER SINK`, if additional
keys exist in the new relation that are not present in the old one, these will
not be visible in the topic after the cutover. The keys will remain absent until
an update occurs for the keys, at which point Materialize will emit a record to
the topic containing the new value.

**Workarounds**:

- Use an intermediary, temporary view to handle the cutover scenario difference.
See [Example: Handle cutover scenarios](#handle-cutover-scenarios).

- Alternatively, ensure that both the old and the new relations have identical
keyspaces to avoid the scenario.

#### Scenario 3: Topic contains a key that does not exist in the new relation

Materialize does not compare the contents of the old relation with the new
relation when cutting a sink over. This means that, if the old relation
contains additional keys that are not present in the new one, these records
will remain in the topic without a corresponding tombstone record. This may
cause readers to assume that certain keys exist when they don't.

**Workarounds**:

- Use an intermediary, temporary view to handle the cutover scenario difference.
See [Example: Handle cutover scenarios](#handle-cutover-scenarios).

- Alternatively, ensure that both the old and the new relations have identical
keyspaces to avoid the scenario.

### Catalog objects

A sink cannot be created directly on a [catalog object](/sql/system-catalog/).
As a workaround, you can create a materialized view on a catalog object and
create a sink on the materialized view.

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-sink.md" >}}

## Examples

### Alter sink

The following example alters a sink originally created from `matview_old` to use
`matview_new` instead.

{{% include-example file="examples/alter_sink"
example="alter-sink-create-original-sink" %}}

{{% include-example file="examples/alter_sink" example="alter-sink-simple" %}}

### Handle cutover scenarios

{{% include-example file="examples/alter_sink"
example="handle-cutover-scenarios-intro" %}}

1. {{< include-example file="examples/alter_sink"
example="handle-cutover-scenarios-step-1-intro" >}}

   {{% include-example file="examples/alter_sink"
example="handle-cutover-scenarios-create-transition-mv" %}}

1. {{< include-example file="examples/alter_sink"
example="handle-cutover-scenarios-step-2-intro" >}}

   {{% include-example file="examples/alter_sink"
example="handle-cutover-scenarios-alter-sink-to-transition" %}}

1. {{< include-example file="examples/alter_sink"
example="handle-cutover-scenarios-step-3-intro" >}}

   {{% include-example file="examples/alter_sink"
example="handle-cutover-scenarios-update-switch" %}}

1. {{< include-example file="examples/alter_sink"
example="handle-cutover-scenarios-step-4-intro" >}}

   {{% include-example file="examples/alter_sink"
example="handle-cutover-scenarios-alter-sink-to-new-mv" %}}

1. {{< include-example file="examples/alter_sink"
example="handle-cutover-scenarios-step-5-intro" >}}

   {{% include-example file="examples/alter_sink"
example="handle-cutover-scenarios-drop-intermediary-objects" %}}

## See also

- [`CREATE SINK`](/sql/create-sink/)
- [`SHOW SINKS`](/sql/show-sinks)
