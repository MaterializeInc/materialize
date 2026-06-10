# Configurable error handling

Associated issues:

- [#22430]

## Product brief

See [Bad Data Kills Sources (Programmable Errors) in Notion][product-brief].

## Solution Proposal

### Motivation

Only the Kafka source is particularly prone to bad data. With PostgreSQL and
MySQL sources, it is almost always a bug (either in the upstream system or in
Materialize) if data fails to decode, as the upstream systems enforce schemas.
But Kafka does not guarantee schema enforcement, so it is common for bad data to
slip into a Kafka topic.

Because Kafka sources are the most prone to bad data, we propose to pursue a
Kafka source-specific solution for handling source decoding errors.

### User experience

We propose implementing two features to provide users an understanding of the errors
encountered by their sources. These features will only apply to Kafka sources for now:

- A [dead letter queue](#dead-letter-queue) (DLQ) to capture *all* decoding errors encountered by
  the source. This feature is conceptually similar to those in other streaming-type
  systems (e.g. [kafka connect](https://www.confluent.io/blog/kafka-connect-deep-dive-error-handling-dead-letter-queues/),
  [AWS SNS](https://docs.aws.amazon.com/sns/latest/dg/sns-configure-dead-letter-queue.html),
  [Flink](https://github.com/confluentinc/flink-cookbook/blob/master/kafka-dead-letter/README.md))
  and can capture any type of decoding errors from all types of Kafka sources.
- [Inline errors](#inline-errors-for-upsert-sources) to expose value-decoding errors on a per-key
  basis in Kafka sources using the [Upsert envelope](https://materialize.com/docs/sql/create-source/#upsert-envelope),
  to ensure correctness and expose upsert values that are potentially stale.


#### Dead letter queue

Intuitively, the [DLQ](dlq) provides a running log of all decoding errors encountered
by the source. Materialize users can monitor the count of errors in the DLQ and
set up alerts whenever the count increases. Users can filter on `mz_timestamp`
and/or `timestamp` to eliminate old errors that are no longer relevant. (Using
the former column filters by ingestion time, while filtering on the latter
column filters by the time the message was produced to Kafka.)

Concretely, we propose to add a `REDIRECT ERRORS` option to `CREATE SOURCE` that
specifies the name of a DLQ table in which to emit information about undecodable
messages:

```sql
CREATE SOURCE src TO KAFKA CONNECTION kconn ... WITH (
    REDIRECT ERRORS = dlq
)
```

The specified DLQ table (`dlq` in the example above) must not exist before the
`CREATE SOURCE` command is executed. Materialize will automatically create the
DLQ table with the following Kafka-specific structure.

 Name           | Type           | Nullable | Description
----------------|----------------|----------|------------
 `mz_timestamp` | `mz_timestamp` | No       | The logical timestamp that the message was reclocked to.
 `error`        | `text`         | No       | The text of the decoding error.
 `error_code`   | `text`         | No       | The code of the decoding error.
 `partition`    | `integer`      | No       | The partition of the Kafka message that failed to decode.
 `offset`       | `bigint`       | No       | The offset of the Kafka message that failed to decode.
 `timestamp`    | `timestamp`    | No       | The timestamp of the Kafka message that failed to decode.
 `key`          | `bytea`        | Yes      | The key bytes of the Kafka message that failed to decode.
 `value`        | `bytea`        | Yes      | The value bytes of the Kafka message that failed to decode.

The DLQ table works like a subsource. It can be queried via `SELECT`, subscribed
to via `SUBSCRIBE`, and used as the upstream relation for a Kafka sink.

The DLQ table is append only. Allowing users to specify a retention policy on
the DLQ table is left to future work. Users cannot issue `INSERT`, `UPDATE`, or
`DELETE` commands against the DLQ[^2].

When a DLQ table is attached to a source, no errors are emitted to the source's
*errs stream* (see the [error handling section][error-handling-docs] of the
`compute::render` module).

**Open questions:**

  * Is there a better name for the option than `REDIRECT ERRORS`?
  * What type gets reported in the system catalog for DLQ tables? Is it really a
    table? Or is it a source or a subsource?
    - 6/18 Update: Product preference is to use table over subsource, but this will
    be decided based on implementation complexity.
  * Is it useful to report the `key` and the `value` in the DLQ relation? The
    raw bytes will not be human readable for binary formats like Avro, and even
    for textual formats like JSON you'll need to cast the values to `text`
    to make them human readable.
  * Can the `REDIRECT ERRORS` option be added to an existing source (i.e., via
    `ALTER SOURCE`)? If so, what is the behavior? Does it pick up for all new
    messages?
    - 6/18 Update: This will not be allowed since it is impossible to retract
    a message from the errors stream without breaking correctness guarantees.
  * Should the `REDIRECT ERRORS` option be the default for new Kafka sources?
  * Does the `mz_timestamp` column make sense? Is there something better
    to call this column? Do the semantics make sense? (Including an
    `ingested_at` wall clock timestamp would be nice, but that wouldn't
    be a deterministic function of reclocking.)

#### Inline errors (for Upsert sources)

While the DLQ is useful for alerting users to data quality issues, it doesn't
help users answer the question "does my upsert source currently have any keys
whose most recent value failed to decode?"

To solve this problem, we propose the addition of an `INLINE ERRORS` option
to `ENVELOPE UPSERT`:

```sql
CREATE SOURCE src TO KAFKA CONNECTION kconn ...
ENVELOPE UPSERT (
    VALUE DECODING ERRORS = ({INLINE | PROPAGATE}, ...)
)
```

This option requires one or more of `PROPAGATE` and `INLINE` to be specified.

The default behavior, which matches today's behavior, is `PROPAGATE`. Value
decoding errors are propagated to the DLQ table or the source's *err stream*,
whichever is active.

If `PROPAGATE` is not specified, value decoding errors are essentially *not*
treated as errors. They are neither forwarded to the DLQ table
nor the source *errs stream*.

When `INLINE` is specified, the source's relation will contain two
top-level columns, `error` and `value` and will not contain top-level columns for
fields in decoded values. The column named `error` is nullable with a
type of `record(description: text, code: text)`. If the most recent value for a
key has been successfully decoded, this column will be `NULL`. If the most recent
value for a key was not succesfully decoded, this column will contain details
about the error. In this case the value is forced into the nullable column
named `value` with a type reflecting the configured value. Format—flattening of
record-type values into top-level columns does not occur.

When both `INLINE` and `PROPAGATE` are specified, the errors are both reported
inline in the source and propagated to the DLQ table or the source's *errs
stream*, whichever is active.

Even when using `VALUE DECODING ERRORS = INLINE`, users need to monitor the DLQ
table or the source's *errs stream* for errors, as errors while decoding the key
still get sent to the DLQ. (There is no good way to represent key decoding
errors inline without breaking upsert semantics.)

The `INLINE` behavior allows users to discover upsert sources that presently
have bad data by querying each source and checking `count(error) > 0`. They can
discover the affected keys by running `SELECT key ... WHERE error IS NOT NULL`.

**Open questions:**

  * Does the separation of the `REDIRECT ERRORS` and the `VALUE DECODING ERRORS`
    options make sense?
    - 6/18 Update: We are considering just implementing the `VALUE DECODING ERRORS`
    approach to start, as this provides correctness guarantees and solves the immediate
    need of our customers who primarily encounter value-decoding errors in Kafka
    upsert sources.
    - 7/8 Update: We have implemented `VALUE DECODING ERRORS = INLINE` as an option
    on `ENVELOPE UPSERT` sources.

#### Limitations

The above options do not generalize to handling decoding errors that occur
outside of the source decoding pipeline. For example, imagine a JSON source that
is further parsed into columns by a downstream materialized view:

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
in `my_view` to fail. The `REDIRECT ERRORS` and `VALUE DECODING ERRORS` options
do not help with the second type of error. We'll likely need to additionally
pursue a solution for gracefully handling invalid function calls, like those
described in [#6367].

### Implementation

##### Update 6/26

Inline errors have been implemented in https://github.com/MaterializeInc/materialize/pull/27802
albeit just with the `INLINE` option, since the `PROPAGATE` option doesn't make
sense without the DLQ.


#### Inline errors

When `VALUE DECODING ERRORS = INLINE` is set:

[`UpsertStyle`](/src/storage-types/src/sources/envelope.rs#L142) will be extended with a new
`ValueErrInline` enum value to indicate the inline style should be used.
```
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum UpsertStyle {
    /// `ENVELOPE UPSERT`, where the key shape depends on the independent
    /// `KeyEnvelope`
    Default(KeyEnvelope),
    /// `ENVELOPE DEBEZIUM UPSERT`
    Debezium { after_idx: usize },
    /// `ENVELOPE UPSERT`, where any decoded value will get packed into a ScalarType::Record
    ///  named `value`, and any decode errors will get serialized into a ScalarType::Record
    /// named `error`. The error will be propagated to the error stream if `propagate_errors`
    /// is set. The key shape depends on the independent `KeyEnvelope`.
    ValueErrInline {
      key_envelope: KeyEnvelope,
      propagate_errors: bool,
    },
}
```
and this will be set in [`plan_create_source`](/src/sql/src/plan/statement/ddl.rs#L605) based
on the value of the `VALUE DECODING ERRORS` option. If `PROPAGATE` is also included in the
option value, `propagate_errors` will be set to true.

[`UnplannedSourceEnvelope::desc`](/src/storage-types/src/sources/envelope.rs#L79) will be
updated to handle the new `UpsertStyle::ValueErrInline` value, with the same logic as
`UpsertStyle::Default` to determine the column-key and key_desc but returning a
`ScalarType::Record` in a `value` column rather than merging the value desc into the
top-level desc, and including thenew `error` column in the returned `RelationDesc`.

The source rendering [`upsert_commands`](/src/storage/src/render/sources.rs#L520) method
will be updated to handle the new `UpsertStyle::ValueErrInline` style. If it receives a
`DecodeError` row it will serialize the error into a `record(description: text, code: text)`
and include that in the `error` column, and if it receives a valid value Row it will insert
the value row into a `record` datum for the `value` column.

If `propagate_errors` is set to `true`, it will continue to produce an additional row with the
`UpsertValueError` error. This will require switching the `map` collection operator to a `flat_map`.

At this point the downstream upsert operators should not require any additional changes, as they
will continue to operate on the same
`Collection<G, (UpsertKey, Option<Result<Row, UpsertError>>, FromTime), Diff>` input collection
received from `upsert_commands` as before.

#### Dead-letter queue (DLQ)

TODO if we decide to proceed with this implementation in the future.

**Open questions:**

  * How do we ensure consistency? The DLQ shard and data shard need to be
    updated in lockstep.

  * When using `VALUE DECODING ERRORS = INLINE`, do we correctly handle
    retractions across versions if the error text changes? (How do we handle
    this today?)

### Future extensions

We expect to add other source types in the future that are prone to bad data.
Imagine S3 sources or Kinesis sources. We'll need to be comfortable extending
the DLQ concept to these new sources. This seems admittedly straightforward,
though each new source type will require its own specific DLQ relation
structure.

## Rejected alternative

> [!CAUTION]
> This section describes a design that was rejected due to concerns over its
> implications for correctness.

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

[^1]: Refer to the PostgreSQL documentation for [details on the behavior of `LATERAL`][lateral-docs].
[^2]: Unless Materialize allowed "writing at" sources ([#15903](https://github.com/MaterializeInc/database-issues/issues/4579)).

[product-brief]: https://www.notion.so/materialize/Bad-Data-Kills-Sources-Programmable-Errors-09a530ed8af2407f822678f630b2c333
[lateral-docs]: https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-LATERAL
[table factor]: https://github.com/MaterializeInc/materialize/blob/2c7ca4fb6b2a63134f6c123557bc09c1565524b7/src/sql-parser/src/parser.rs#L6991
[error-handling-docs]: https://github.com/MaterializeInc/materialize/blob/2c7ca4fb6b2a63134f6c123557bc09c1565524b7/src/compute/src/render.rs#L12-L101
[@maddyblue]: https://github.com/maddyblue
[dlq]: https://aws.amazon.com/what-is/dead-letter-queue/

[#6367]: https://github.com/MaterializeInc/database-issues/issues/1968
[#22430]: https://github.com/MaterializeInc/database-issues/issues/6771
[#27137]: https://github.com/MaterializeInc/materialize/pulls/27137
