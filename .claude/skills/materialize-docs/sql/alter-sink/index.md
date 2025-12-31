---
audience: developer
canonical_url: https://materialize.com/docs/sql/alter-sink/
complexity: advanced
description: '`ALTER SINK` allows cutting a sink over to a new upstream relation without
  causing disruption to downstream consumers.'
doc_type: reference
keywords:
- only
- 'Note:'
- ALTER SINK
- Workarounds
product_area: Sinks
status: stable
title: ALTER SINK
---

# ALTER SINK

## Purpose
`ALTER SINK` allows cutting a sink over to a new upstream relation without causing disruption to downstream consumers.

If you need to understand the syntax and options for this command, you're in the right place.


`ALTER SINK` allows cutting a sink over to a new upstream relation without causing disruption to downstream consumers.


Use `ALTER SINK` to:
- Change the relation you want to sink from. This is useful in the context of
[blue/green deployments](/manage/dbt/blue-green-deployments/).
- Rename a sink.
- Change owner of a sink.

## Syntax

This section covers syntax.

#### Change sink from relation

### Change sink from relation

To change the relation you want to sink from:

<!-- Syntax example: examples/alter_sink / syntax-set-from -->

#### Rename

### Rename

To rename a sink:

<!-- Syntax example: examples/alter_sink / syntax-rename -->

#### Change owner

### Change owner

To change the owner of a sink:

<!-- Syntax example: examples/alter_sink / syntax-change-owner -->

## Details

This section covers details.

### Changing sink from relation

#### Valid schema changes

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

#### Cutover timestamp

To alter the upstream relation a sink depends on while ensuring continuity in
data processing, Materialize must pick a consistent cutover timestamp. When you
execute an `ALTER SINK` command, the resulting output will contain:
- all updates that happened before the cutover timestamp for the old
relation, and
- all updates that happened after the cutover timestamp for the new
relation.

> **Note:** 
To select a consistent timestamp, Materialize must wait for the previous
definition of the sink to emit results up until the oldest timestamp at which
the contents of the new upstream relation are known. Attempting to `ALTER` an
unhealthy sink that can't make progress will result in the command timing out.


#### Cutover scenarios and workarounds

Because Materialize emits updates from the new relation **only** if
they occur after the cutover timestamp, the following scenarios may occur:

##### Scenario 1: Topic contains stale value for a key

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

##### Scenario 2: Topic is missing a key that exists in the new relation

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

##### Scenario 3: Topic contains a key that does not exist in the new relation

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

- Ownership of the sink being altered.
- In addition,
  - To change the sink from relation:
    - `SELECT` privileges on the new relation being written out to an external system.
    - `CREATE` privileges on the cluster maintaining the sink.
    - `USAGE` privileges on all connections and secrets used in the sink definition.
    - `USAGE` privileges on the schemas that all connections and secrets in the
      statement are contained in.
  - To change owners:
    - Role membership in `new_owner`.
    - `CREATE` privileges on the containing schema if the sink is namespaced
  by a schema.


## Examples

This section covers examples.

### Alter sink

The following example alters a sink originally created from `matview_old` to use
`matview_new` instead.

<!-- Unresolved shortcode: {{% include-example file="examples/alter_sink"
exa... -->

<!-- Unresolved shortcode: {{% include-example file="examples/alter_sink" exa... -->

### Handle cutover scenarios

<!-- Unresolved shortcode: {{% include-example file="examples/alter_sink"
exa... -->

1. <!-- Example: examples/alter_sink / handle-cutover-scenarios-step-1-intro -->

   <!-- Unresolved shortcode: {{% include-example file="examples/alter_sink"
exa... -->

1. <!-- Example: examples/alter_sink / handle-cutover-scenarios-step-2-intro -->

   <!-- Unresolved shortcode: {{% include-example file="examples/alter_sink"
exa... -->

1. <!-- Example: examples/alter_sink / handle-cutover-scenarios-step-3-intro -->

   <!-- Unresolved shortcode: {{% include-example file="examples/alter_sink"
exa... -->

1. <!-- Example: examples/alter_sink / handle-cutover-scenarios-step-4-intro -->

   <!-- Unresolved shortcode: {{% include-example file="examples/alter_sink"
exa... -->

1. <!-- Example: examples/alter_sink / handle-cutover-scenarios-step-5-intro -->

   <!-- Unresolved shortcode: {{% include-example file="examples/alter_sink"
exa... -->

## See also

- [`CREATE SINK`](/sql/create-sink/)
- [`SHOW SINKS`](/sql/show-sinks)