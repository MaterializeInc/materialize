<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)

</div>

# ALTER SINK

`ALTER SINK` allows cutting a sink over to a new upstream relation
without causing disruption to downstream consumers. This is useful in
the context of [blue/green
deployments](/docs/manage/dbt/blue-green-deployments/).

## Syntax

<div class="highlight">

``` chroma
ALTER SINK <sink_name> SET FROM <relation_name>;
```

</div>

## Details

### Valid schema changes

For `ALTER SINK` to be successful, the newly specified relation must
lead to a valid sink definition with the same conditions as the original
`CREATE SINK` statement.

When using the Avro format with a schema registry, the generated Avro
schema for the new relation must be compatible with the previously
published schema. If that’s not the case, the `ALTER SINK` command will
succeed, but the subsequent execution of the sink will result in errors
and will not be able to make progress.

To monitor the status of a sink after an `ALTER SINK` command, navigate
to the respective object page in the [Materialize
console](/docs/console/), or query the
[`mz_internal.mz_sink_statuses`](/docs/sql/system-catalog/mz_internal/#mz_sink_statuses)
system catalog view.

### Cutover timestamp

To alter the upstream relation a sink depends on while ensuring
continuity in data processing, Materialize must pick a consistent
cutover timestamp. When you execute an `ALTER SINK` command, the
resulting output will contain:

- all updates that happened before the cutover timestamp for the old
  relation, and
- all updates that happened after the cutover timestamp for the new
  relation.

<div class="note">

**NOTE:** To select a consistent timestamp, Materialize must wait for
the previous definition of the sink to emit results up until the oldest
timestamp at which the contents of the new upstream relation are known.
Attempting to `ALTER` an unhealthy sink that can’t make progress will
result in the command timing out.

</div>

### Cutover scenarios and workarounds

Because Materialize emits updates from the new relation **only** if they
occur after the cutover timestamp, the following scenarios may occur:

#### Scenario 1: Topic contains stale value for a key

Since cutting over a sink to a new upstream relation using `ALTER SINK`
does not emit a snapshot of the new relation, all keys will appear to
have the old value for the key in the previous relation until an update
happens to them. At that point, the current value will be published to
the topic.

Consumers of the topic must be prepared to handle an old value for a
key, for example by filling in additional columns with default values.

**Workarounds**:

- Use an intermediary, temporary view to handle the cutover scenario
  difference. See [Example: Handle cutover
  scenarios](#handle-cutover-scenarios).

- Alternatively, forcing an update to all the keys after `ALTER SINK`
  will force the sink to re-emit all the updates.

#### Scenario 2: Topic is missing a key that exists in the new relation

As a consequence of not re-emitting a snapshot after `ALTER SINK`, if
additional keys exist in the new relation that are not present in the
old one, these will not be visible in the topic after the cutover. The
keys will remain absent until an update occurs for the keys, at which
point Materialize will emit a record to the topic containing the new
value.

**Workarounds**:

- Use an intermediary, temporary view to handle the cutover scenario
  difference. See [Example: Handle cutover
  scenarios](#handle-cutover-scenarios).

- Alternatively, ensure that both the old and the new relations have
  identical keyspaces to avoid the scenario.

#### Scenario 3: Topic contains a key that does not exist in the new relation

Materialize does not compare the contents of the old relation with the
new relation when cutting a sink over. This means that, if the old
relation contains additional keys that are not present in the new one,
these records will remain in the topic without a corresponding tombstone
record. This may cause readers to assume that certain keys exist when
they don’t.

**Workarounds**:

- Use an intermediary, temporary view to handle the cutover scenario
  difference. See [Example: Handle cutover
  scenarios](#handle-cutover-scenarios).

- Alternatively, ensure that both the old and the new relations have
  identical keyspaces to avoid the scenario.

### Catalog objects

A sink cannot be created directly on a [catalog
object](/docs/sql/system-catalog/). As a workaround, you can create a
materialized view on a catalog object and create a sink on the
materialized view.

## Privileges

The privileges required to execute this statement are:

- Ownership of the sink being altered.
- `SELECT` privileges on the new relation being written out to an
  external system.
- `CREATE` privileges on the cluster maintaining the sink.
- `USAGE` privileges on all connections and secrets used in the sink
  definition.
- `USAGE` privileges on the schemas that all connections and secrets in
  the statement are contained in.

## Examples

### Alter sink

The following example alters a sink originally created from
`matview_old` to use `matview_new` instead.

That is, assume you have a Kafka sink `avro_sink` created from
`matview_old` (See
[`CREATE SINK`:Kafka/Redpanda](/docs/sql/create-sink/kafka/) for more
information):

<div class="highlight">

``` chroma
CREATE SINK avro_sink
  FROM matview_old
  INTO KAFKA CONNECTION kafka_connection (TOPIC 'test_avro_topic')
  KEY (key_col)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE UPSERT
;
```

</div>

To have the sink read from `matview_new` instead of `matview_old`, you
can use `ALTER SINK` to change the `FROM <relation>`:

<div class="note">

**NOTE:** `matview_new` must be compatible with the previously published
schema. Otherwise, the `ALTER SINK` command will succeed, but the
subsequent execution of the sink will result in errors and will not be
able to make progress. See [Valid schema changes](#valid-schema-changes)
for details.

</div>

<div class="highlight">

``` chroma
ALTER SINK avro_sink
  SET FROM matview_new
;
```

</div>

<div class="tip">

**💡 Tip:**

Because Materialize emits updates from the newly specified relation
**only** if they happen after the cutover timestamp, you might observe
the following scenarios:

- [Topic contains stale value for a
  key](#scenario-1-topic-contains-stale-value-for-a-key)
- [Topic is missing a key that exists in the new
  relation](#scenario-2-topic-is-missing-a-key-that-exists-in-the-new-relation)
- [Topic contains a key that does not exist in the new
  relation](#scenario-3-topic-contains-a-key-that-does-not-exist-in-the-new-relation)

For workaround, see [Example: Handle cutover
scenarios](#handle-cutover-scenarios)

</div>

### Handle cutover scenarios

Because Materialize emits updates from the newly specified relation
**only** if they happen after the cutover timestamp, you might observe
the following scenarios:

- [Topic contains stale value for a
  key](#scenario-1-topic-contains-stale-value-for-a-key)
- [Topic is missing a key that exists in the new
  relation](#scenario-2-topic-is-missing-a-key-that-exists-in-the-new-relation)
- [Topic contains a key that does not exist in the new
  relation](#scenario-3-topic-contains-a-key-that-does-not-exist-in-the-new-relation)

To handle these scenarios, you can first alter sink to an intermediary
materialized view. The intermediary materialized view uses a temporary
table `switch` that switches the view’s contents from old relation
content to new relation content. At the time of the switch, Materialize
emits the diff of the changes. Then, after the sink upper has advanced
beyond the time of the switch, you can `ALTER SINK` to the new relation
(and remove the temporary intermediary materialized view and table).

1.  For example, create a table `switch` and a temporary materialized
    view `transition` that contains either:

    - the `matview_old` content if `switch.value` is `false`.
    - the `matview_new` content if `switch.value` is `true`.

    At first, the `switch.value` is `false`, so the `transition`
    materialized view contains the `matview_old` content.

    <div class="highlight">

    ``` chroma
    CREATE TABLE switch (value bool);
    INSERT INTO switch VALUES (false); -- controls whether we want the new or the old materialized view.

    CREATE MATERIALIZED VIEW transition AS
    (SELECT matview_old.* FROM matview_old JOIN switch ON switch.value = false)
    UNION ALL
    (SELECT matview_new.* FROM matview_new JOIN switch ON switch.value = true)
    ;
    ```

    </div>

2.  `ALTER SINK` to use `transition`, which currently contains
    `matview_old` content:
    <div class="highlight">

    ``` chroma
    ALTER SINK avro_sink SET FROM transition;
    ```

    </div>

3.  Update `switch.value` to `true`, which causes the `transition`
    materialized view to contain `matview_new` content:
    <div class="highlight">

    ``` chroma
    UPDATE switch SET value = true;
    ```

    </div>

4.  Wait for the sink’s upper frontier
    ([`mz_frontiers`](/docs/sql/system-catalog/mz_internal/#mz_frontiers))
    to advance beyond the time of the switch update. Once advanced,
    alter sink to use `matview_new`:
    <div class="highlight">

    ``` chroma
    -- After sink upper has advanced beyond the time of the switch UPDATE.
    ALTER SINK avro_sink SET FROM matview_new;
    ```

    </div>

5.  Drop the `transition` materialized view and the `switch` table:
    <div class="highlight">

    ``` chroma
    DROP MATERIALIZED VIEW transition;
    DROP TABLE switch;
    ```

    </div>

## See also

- [`CREATE SINK`](/docs/sql/create-sink/)
- [`SHOW SINKS`](/docs/sql/show-sinks)

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/alter-sink.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
