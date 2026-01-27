# ALTER SINK
`ALTER SINK` allows cutting a sink over to a new upstream relation without causing disruption to downstream consumers.
Use `ALTER SINK` to:
- Change the relation you want to sink from. This is useful in the context of
[blue/green deployments](/manage/dbt/blue-green-deployments/).
- Rename a sink.
- Change owner of a sink.

## Syntax


**Change sink from relation:**

### Change sink from relation

To change the relation you want to sink from:



```mzsql
ALTER SINK <name> SET FROM <relation_name>;

```

| Syntax element | Description |
| --- | --- |
| `<name>`  | The name of the sink you want to change.  |
| `<relation_name>`  | The name of the relation you want to sink from.  |



**Rename:**

### Rename

To rename a sink:



```mzsql
ALTER SINK <name> RENAME TO <new_name>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The current name of the sink.  |
| `<new_name>` | The new name of the sink.  |
See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).





**Change owner:**

### Change owner

To change the owner of a sink:



```mzsql
ALTER SINK <name> OWNER TO <new_owner_role>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the sink you want to change ownership of.  |
| `<new_owner_role>` | The new owner of the sink.  |
To change the owner, you must be a current owner as well as have membership
in the `<new_owner_role>`.




## Details

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

> **Note:** To select a consistent timestamp, Materialize must wait for the previous
> definition of the sink to emit results up until the oldest timestamp at which
> the contents of the new upstream relation are known. Attempting to `ALTER` an
> unhealthy sink that can't make progress will result in the command timing out.


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

### Alter sink

The following example alters a sink originally created from `matview_old` to use
`matview_new` instead.

That is, assume you have a Kafka sink `avro_sink` created from `matview_old`
(See [`CREATE SINK`:Kafka/Redpanda](/sql/create-sink/kafka/) for more
information):
```mzsql
CREATE SINK avro_sink
  FROM matview_old
  INTO KAFKA CONNECTION kafka_connection (TOPIC 'test_avro_topic')
  KEY (key_col)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE UPSERT
;

```


To have the sink read from `matview_new` instead of `matview_old`, you can
use `ALTER SINK` to change the `FROM <relation>`:

{{< note >}}
`matview_new` must be compatible with the previously published
schema. Otherwise, the `ALTER SINK` command will succeed, but the
subsequent execution of the sink will result in errors and will not be able
to make progress. See [Valid schema changes](#valid-schema-changes) for
details.
{{< /note >}}
```mzsql
ALTER SINK avro_sink
  SET FROM matview_new
;

```
{{< tip >}}

Because Materialize emits updates from the newly specified relation **only** if
they happen after the cutover timestamp, you might observe the following
scenarios:
- [Topic contains stale value for a
  key](#scenario-1-topic-contains-stale-value-for-a-key)
- [Topic is missing a key that exists in the new relation](#scenario-2-topic-is-missing-a-key-that-exists-in-the-new-relation)
- [Topic contains a key that does not exist in the new relation](#scenario-3-topic-contains-a-key-that-does-not-exist-in-the-new-relation)

For workaround, see [Example: Handle cutover scenarios](#handle-cutover-scenarios)
{{< /tip >}}


### Handle cutover scenarios

Because Materialize emits updates from the newly specified relation **only** if
they happen after the cutover timestamp, you might observe the following
scenarios:
- [Topic contains stale value for a
  key](#scenario-1-topic-contains-stale-value-for-a-key)
- [Topic is missing a key that exists in the new relation](#scenario-2-topic-is-missing-a-key-that-exists-in-the-new-relation)
- [Topic contains a key that does not exist in the new relation](#scenario-3-topic-contains-a-key-that-does-not-exist-in-the-new-relation)

To handle these scenarios, you can first alter sink to an intermediary
materialized view. The intermediary materialized view uses a temporary table
`switch` that switches the view's contents from old relation content to new
relation content. At the time of the switch, Materialize emits the diff of
the changes. Then, after the sink upper has advanced beyond the time of the
switch, you can `ALTER SINK` to the new relation (and remove the temporary
intermediary materialized view and table).


1. For example, create a table `switch` and a temporary materialized view
`transition` that contains either:
- the `matview_old` content if `switch.value` is `false`.
- the `matview_new` content if `switch.value` is `true`.

At first, the `switch.value` is `false`, so the `transition` materialized view contains the `matview_old` content.


   <no value>```mzsql
   CREATE TABLE switch (value bool);
   INSERT INTO switch VALUES (false); -- controls whether we want the new or the old materialized view.

   CREATE MATERIALIZED VIEW transition AS
   (SELECT matview_old.* FROM matview_old JOIN switch ON switch.value = false)
   UNION ALL
   (SELECT matview_new.* FROM matview_new JOIN switch ON switch.value = true)
   ;

   ```

1. `ALTER SINK` to use `transition`, which currently contains `matview_old` content:


   <no value>```mzsql
   ALTER SINK avro_sink SET FROM transition;

   ```

1. Update `switch.value` to `true`, which causes the `transition` materialized view to contain `matview_new` content:


   <no value>```mzsql
   UPDATE switch SET value = true;

   ```

1. Wait for the sink's upper frontier
([`mz_frontiers`](/sql/system-catalog/mz_internal/#mz_frontiers)) to advance
beyond the time of the switch update. Once advanced, alter sink to use
`matview_new`:


   <no value>```mzsql
   -- After sink upper has advanced beyond the time of the switch UPDATE.
   ALTER SINK avro_sink SET FROM matview_new;

   ```

1. Drop the `transition` materialized view and the `switch` table:


   <no value>```mzsql
   DROP MATERIALIZED VIEW transition;
   DROP TABLE switch;

   ```

## See also

- [`CREATE SINK`](/sql/create-sink/)
- [`SHOW SINKS`](/sql/show-sinks)
