# Configurable error handling

Associated issues:

- [#22430]

## Product brief

See [Bad Data Kills Sources (Programmable Errors) in Notion][product-brief].

## Solution Proposal

### Synopsis

We propose to introduce new table reference modifiers that control the
behavior of errors in queries:

```sql
SELECT ... FROM
    -- Suppress any errors present in the `x` table.
    IGNORE ERRORS x,
    -- Instead of returning the data in `y`, return the errors that were
    -- produced while maintaining `y`.
    ONLY ERRORS y,
    --
    z
WHERE ...
```

### Syntax

We'll extend the SQL grammar to allow the following phrases at the start of
a [table factor]:

  * `IGNORE ERRORS`
  * `ONLY ERRORS`

The grammar is based on the `LATERAL` modifier for subqueries[^1], as in:

```sql
SELECT ... FROM x, LATERAL (SELECT x.a)
```

Syntatically, the `IGNORE ERRORS` and `ONLY ERRORS` modifiers may appear before
any table factor. For example, all of the following will be syntatically
valid SQL queries:

```sql
SELECT ... FROM IGNORE ERRORS x;
SELECT ... FROM IGNORE ERRORS (SELECT 1);
SELECT ... FROM IGNORE ERRORS (x JOIN y);
```

#### Syntax: alternatives

There were two major alternatives to the syntax discussed.

The first alternative used a syntax that looked like a function call:

```
SELECT ... FROM IGNORE_ERRORS(x);
```

PostgreSQL notably does not have a facility for user-defined functions that take
relational fragments as input. Supporting this syntax seemed likely to cause
problematic parsing ambiguities, particularly when the argument to
`IGNORE_ERRORS` was a complex relational fragment (e.g., `IGNORE_ERRORS(x JOIN
y)`) rather than just a simple name.

The second alternative placed the `IGNORE ERRORS` clause at the end of the table
fragment:

```
SELECT ... FROM x IGNORE ERRORS;
SELECT ... FROM (SELECT 1) IGNORE ERRORS;
SELECT ... FROM (x JOIN y) IGNORE ERRORS;
```

This syntax was deemed more confusing than the chosen syntax, as `IGNORE ERRORS`
appears next to the optional alias.

```sql
-- Aliases `x` as `ignore` and does not ignore errors.
SELECT ... FROM x IGNORE;
-- Does not alias `x` and ignore errors.
SELECT ... FROM x IGNORE ERRORS;
-- Aliases `x` as `no` and ignores errors.
SELECT ... FROM x NO IGNORE ERRORS;
```

#### Syntax: open questions

* Did we choose the right syntax? The design still requires SQL council review.

### Semantics

Please read the [error handling section][error-handling-docs] of the
`compute::render` module as background. The terms *oks stream* and *errs stream*
will be used as defined in that module.

#### `IGNORE ERRORS`

The `IGNORE ERRORS <r>` clause indicates that any errors present in `r`'s errs
stream should not be considered when computing the query.

Note that the `r`'s oks stream may be meaningless in the presence of errors in
the errs stream. Materialize generally makes no guarantees about the correctness
of the oks stream when errors are present. However, for specific types of
computations (e.g., source ingestion, possibly MFP application), Materialize can
make more specific guarantees about the contents of the oks stream in the face
of errors. See [Guardrails](#guardrails) below for further discussion.

#### `ONLY ERRORS`

The `ONLY ERRORS <r>` clause references the errs stream associated with `r`,
rather than the data in `r` itself. Regardless of the structure of the oks
stream, the errs stream is always presented as a relation with two columns:

 Name      | Type
-----------|--------
 `message` | `text`
 `code`    | `text`


### Guardrails

#### `IGNORE ERRORS`

As mentioned above, the `IGNORE ERRORS` clause can have dangerous consequences
for correctness. Materialize makes no general claims about the contents of the
oks stream when errors are present in the errs stream.

However, for certain simple types of computations, we can offer better
guarantees. Specifically, for sources, we can guarantee that an error while
decoding a record will only cause that one record to be omitted from the oks
stream.

So, to start, we will permit applying the `IGNORE ERRORS` modifier only to
sources and subsources.

In the future, we can consider expanding the set of relations to which `IGNORE
ERRORS` can be applied. For example, applying `IGNORE ERRORS` to a SQL query
which contains only map, filter, and project nodes in its `HirScalarExpr` likely
has sensible semantics under `IGNORE ERRORS`: the computation proceeds row by
row, and any errors while applying the computation to a row omits only that one
row from the output.

#### `ONLY ERRORS`

There are two important guardrails to apply to `ONLY ERRORS`:

1. We will permit applying the `ONLY ERRORS` modifier only to persist-backed
   objects: tables, sources, and materialized views. This relieves some pressure
   on the compute layer, which already does not assume that the errors in a
   persist shard have any particular relationship to their source.

2. We will clearly document that error messages and codes are **not stable**. We
   reserve the right to reword error messages, change error codes, add new
   errors, or eliminate errors in future releases of Materialize.

   Note also that `ONLY ERRORS` doesn't fundamentally add new backwards
   compatibility surface area. Error messages, descriptions, hints, and codes
   are already returned via the SQL interface, and users may already be
   erroneously relying on seeing particular error messages and codes from
   Materialize in particular situations.

### Implementation

#### `IGNORE ERRORS`

[@maddyblue] has a prototype implementation of `IGNORE ERRORS` in [#27137].
The core of the implementation is the addition of a new `ignore_errors`
field to `MirRelationExpr::Get`:

```diff
 MirRelationExpr::Get {
     /// A global or local identifier naming the collection.
     id: Id,

     // ...

+    /// Whether to ignore errors.
+    ignore_errors: bool,
 },
```

When rendered, if `ignore_errors` is true, the referenced collection's errs
stream is simply not wired up to the downstream error stream.

**Open questions:**

  * Is this implementation acceptable to the cluster team?
  * Will the existence of the new field cause correctness issues if existing
    references to `MirRelationExpr` fail to check the field?

#### `ONLY ERRORS`

There is not yet a prototype for `ONLY ERRORS`.

One candidate proposal is to introduce a `FlavoredGid` type, which couples a
global ID with a specification of whether the oks stream or the errs stream is
of interest.

```diff
+enum GidFlavor {
+    Oks,
+    Errs,
+}
+
+struct FlavoredGid {
+    id: GlobalId,
+    flavor: GidFlavor,
+}
+
 pub enum Id {
     Local(LocalId),
-    Global(GlobalId),
+    Global(FlavoredGid),
 }
```

This would be an unfortunately massive change. We'd need to audit every use of
`GlobalId` across the codebase to assess whether it should remain a `GlobalId`
or become a `FlavoredGid`.

## Alternatives

### Kafka source-specific dead letter queue

Only the Kafka source is particularly prone to bad data. With PostgreSQL and
MySQL sources, it is almost always a bug (either in in the upstream system or in
Materialize) if data fails to decode, as the upstream systems enforce schemas.
But Kafka does not guarantee schema enforcement, so it is common for bad data to
slip into a Kafka topic.

Because Kafka sources are the most prone to bad data, we could instead choose to
pursue a Kafka source-specific solution. For example, imagine that we introduced
a dead letter queue (DLQ) for Kafka sources. Concretely, we could add a `DEAD
LETTERS` option (strawman name) to `CREATE SOURCE` that specifies the name of a
relation in which to emit undecodable messages:

```
CREATE SOURCE ksrc TO KAFKA CONNECTION kconn ... WITH (
    DEAD LETTERS = kbad
)
```

The `kbad` relation would have a structure like:

 Name         | Type
--------------|--------
 `key`        | `bytea`
 `value`      | `bytea`
 `error`      | `text`
 `error_code` | `text`


There are a few considerations to note.

First, we expect to add other source types in the future that are prone to bad
data. Imagine S3 sources or Kinesis sources. We'd need to be comfortable
extending the DLQ concept to these new sources—which seems admittedly
straightforward.

Second, the source DLQ does not generalize to handling decoding errors that
occur outside of the source decoding pipeline. For example, imagine a JSON
source that is further parsed into columns by a downstream materialized view:

```sql
CREATE SOURCE my_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'samsa')
  FORMAT JSON;

CREATE MATERIALIZED VIEW my_view AS
  SELECT
    (data->>'userid')::int AS userid,
  FROM my_source;
```

Bad data can cause both the initial JSON parsing to fail, or the cast to `int`
in `my_view` to fail. A source DLQ does not help with the second type of error.
We'd need to additionally pursue a solution for gracefully handling
invalid function calls, like those described in [#6367].

**Open questions:**

  * Would this alternative be preferable to the proposed design?
  * Is this alternative compatible with introducing `IGNORE ERRORS` and
    `ONLY ERRORS` in the future?
  * Can a dead letter queue be added to an existing source?

[^1]: Refer to the PostgreSQL documentation for [details on the behavior of `LATERAL`][lateral-docs].

[product-brief]: https://www.notion.so/materialize/Bad-Data-Kills-Sources-Programmable-Errors-09a530ed8af2407f822678f630b2c333
[lateral-docs]: https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-LATERAL
[table factor]: https://github.com/MaterializeInc/materialize/blob/2c7ca4fb6b2a63134f6c123557bc09c1565524b7/src/sql-parser/src/parser.rs#L6991
[error-handling-docs]: https://github.com/MaterializeInc/materialize/blob/2c7ca4fb6b2a63134f6c123557bc09c1565524b7/src/compute/src/render.rs#L12-L101
[@maddyblue]: https://github.com/maddyblue

[#6367]: https://github.com/MaterializeInc/materialize/issues/6367
[#22430]: https://github.com/MaterializeInc/materialize/issues/22430
[#27137]: https://github.com/MaterializeInc/materialize/issues/27137
