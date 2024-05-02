# sql+storage: deprecate concept of subsources

-   Associated:
    -   [storage/sources: make (sub)sources a flavor of tables #20208
        ](https://github.com/MaterializeInc/materialize/issues/20208)
    -   [sql/storage: consider if TEXT COLUMNS, IGNORE COLUMNS belongs on
        subsources
        #26774](https://github.com/MaterializeInc/materialize/issues/26774)

## The Problem

There are two distinct notions of `SUBSOURCE`s in MZ, both of which can be
folded neatly into other concepts.

-   Ingestion export subsources should be tables (as should the current primary
    source's collection)
-   Progress subsources should become the relation returned when querying sources

Doing both of these lets us remove the concept of subsources from Materialize
entirely. It, by definition, also lets us get rid of the notion of "progress
subsources," which is a concept that users often find confusing.

## Success Criteria

-   Users no longer need to use the keyword `SUBSOURCE` to interact with
    Materialize in any capacity; and we do this without deprecating any
    functionality whatsoever.

    Note that we're also going to remove the term "subsource" from any system
    catalog table names or rows.

## Solution Proposal

### SOURCEs should output to TABLES

In the current version of Materialize, we have an impedance mismatch between
single-output and multi-output sources.

-   Single-ouput sources write their data directly to the `SOURCE` itself, i.e.
    users query the `SOURCE` object to read the data.
-   Multi-output sources write their data to `SUBSOURCES`. The primary source to
    which they belong is an empty, unused (though still queryable) collection.

I propose that the output of all sources, both single- and multi-output, should
be to `TABLE`s (albeit special, read-only tables). Users already have an
intuition around how `TABLE`s work, and understanding "read-only" tables is much
simpler than doubling down on subsources––a concept we invented.

Additionally, we have plans to let users perform arbitrary DML statements on
these special tables, at which point they will behave like tables that just
occasionally receive writes from upstream objects.

#### Why not just rename `SUBSOURCE` to `TABLE`?

This leaves the impedance mismatch in place where users have to `SELECT` from
the source if they're using e.g. Kafka, but have to find the right `TABLE` to
`SELECT` from for PG sources.

### SOURCES should contain their progress information

The other type of `SUBSOURCE` Materialize supports is known as the
remap/progress collection. While most users don't access this data, it's an
important debugging tool.

I propose that the progress collection should be moved "into" the source, i.e.
querying a source should return the what-is-today-the-progress-collection's
data.

### New API

The API to support this is similar to the existing `CREATE SOURCE` API. However,
`ALTER SOURCE...ADD SUBSOURCE` will disappear and be replaced with a flavor of
`CREATE TABLE`.

(h/t @benesch for this)

---

Create a Kafka source that reads from a topic named `t`:

```sql
CREATE SOURCE kafka_src
FROM KAFKA CONNECTION kafka_conn (TOPIC 't')
FOR ALL TABLES
```

`kafka_src` is the progress relation for the source, and a table named `t` is
automatically created in the same schema as `kafka_src`.

Note that we could also probably make `FOR ALL TABLES` implied for Kafka
sources.

---

Create a Kafka source that reads from a topic named `t` with a custom table
name:

```sql
CREATE SOURCE kafka_src
FROM KAFKA CONNECTION kafka_conn (TOPIC 't')
FOR TABLES (t AS my_t)
```

`kafka_src` is again the progress relation for the source. The data is in a
table named `my_t`. You could also imagine more complex scenarios where you can
output the same topic with multiple schemas. What that ends up looking like is
under discussion.

---

Create a PostgreSQL source:

```sql
CREATE SOURCE postgres_src
FROM POSTGRES CONNECTION postgres_conn (PUBLICATION p)
FOR ALL TABLES
```

`postgres_src` is the progress relation; there is no separate
`postgres_src_progress` relation created. Otherwise this command works the same
way that it does today.

---

Add a new table to a PostgreSQL source:

```sql
CREATE TABLE my_new_table FROM SOURCE postgres_src (upstream_schema.upstream_table);
```

This would replace the `ALTER SOURCE ... ADD {SUBSOURCE|TABLE}` command.

---

Add a new table to a Kafka source:

```sql
CREATE TABLE kafka_copy FROM SOURCE kafka_src ("t");
```

At first this looks weird. Why would you ever want two copies of a Kafka source?
It turns out this lets you incorporate upstream schema changes without losing
reclocking information! This would get us reusable reclocking
(https://github.com/MaterializeInc/materialize/issues/17021), without needing to
introduce a new `SOURCE PROGRESS` concept.

Note that support for this is blocked on at least
https://github.com/MaterializeInc/materialize/issues/24843, and also additional
work to support multi-output sources in Kafka https://github.com/MaterializeInc/materialize/issues/26994.

#### System tables

-   `mz_tables` should grow an `managing_source_id` column, whose type is a
    nullable string (the value being the source's `GlobalId` if it exists).

-   The output of `SHOW TABLES` should include a new column called `managed_by`
    returns the name of the source that this table is managed by using whatever
    level of qualification we use in `SHOW SUBSOURCES` today. It will be `NULL`
    for all "regular" tables.

-   `SHOW SUBSOURCES` gets deprecated.

-   `SHOW TABLES` gets a new clause akin to `ON`, which is `MANAGED BY` and lets
    you see which tables a given source manages.

#### TEXT + IGNORE COLUMNS

These options can remain on `CREATE SOURCE` statements, and the `ADD SUBSOURCE`
variants of them will move to instead be `CREATE TABLE` variants. We could also
probably squeeze #26774 in here without much effort.

### Migration

#### Kafka, single-output load generators

-   Current source - ID, persist shard becomes read-only `TABLE` - Durable statement becomes `CREATE TABLE curr_source_name FROM SOURCE
curr_progress_name ( topic_name )`
-   Progress collection
    -   ID, persist shard becomes `SOURCE`
    -   Durable statement becomes `CREATE SOURCE curr_progress_name FROM...`

#### PG, MySQL, multi-output load generators

-   Current source
    -   ID, persist shard becomes regular `TABLE` no longer associated
        with the source whatsoever. Users can continue depending on this, but they
        should drop it after the release at their leisure
    -   Durable statement becomes `CREATE TABLE curr_source_name ()`
-   Subsources
    -   ID, persist shard becomes read-only `TABLE`
    -   Durable statement becomes `CREATE TABLE curr_source_name FROM SOURCE curr_progress_name (external_reference )`
    -   `TEXT COLUMNS`, `IGNORE COLUMNS` move from "current source" to `CREATE TABLE`
-   Progress collection
    -   ID, persist shard becomes `SOURCE`
    -   Durable statement becomes `CREATE SOURCE curr_progress_name FROM....`

## Alternatives

The most realistic alternative to this proposal is to leave things as they are.

## Open questions

-   Terraform and DBT: Marta, Seth, and Bobby say that we can simply roll forward
    with a breaking change and teach users what the new version ought to do. Is
    that right?
-   Do we want to mark the object type for "read-only" tables as something other
    than "table?" Seems like it would be nice to make clear to someone exploring
    an environment which tables support inserts vs. which don't.

    @benesch suggest:

    > I think it depends the interface! I'd suggest we use "table" for anything
    > programmatic—i.e., these tables should be in mz_tables, be labeled as
    > objects in mz_objects, that sort of thing. But then the commands for humans
    > (SHOW TABLES,SHOW OBJECTS) probably want to very prominently display
    > read-only vs not. And the console of course can use a prominent badge or
    > tooltip or whatever makes sense.

-   How do we want to manage Kafka topic names as identifiers? @benesch
    discusses
    [here](https://github.com/MaterializeInc/materialize/pull/26881#discussion_r1590498651)
    and
    [here](https://github.com/MaterializeInc/materialize/pull/26881#discussion_r1590499189).
    Conclusion to follow.

## Follow-up work

-   [Multi-output Kafka sources](https://github.com/MaterializeInc/materialize/issues/26994)
