# Self Describing Persist Batches

- Associated:
  - [write Schema-ified data blobs #24830](https://github.com/MaterializeInc/database-issues/issues/7411)
  - [persist: schema evolution](https://github.com/MaterializeInc/database-issues/issues/4818)
  - [Table support for push sources](https://github.com/MaterializeInc/database-issues/issues/6896)
  - [[dnm] columnar: Write columnar encodings as part of a Batch](https://github.com/MaterializeInc/materialize/pull/26120)
  - [[dnm] columnar: Array support](https://github.com/MaterializeInc/materialize/pull/25848)

## The Problem

We want Persist to have an understanding of the data it contains. The main
motivating factor for this is to support schema migrations, concretely,
being able to run `ALTER TABLE ... ADD COLUMN ...` or
`ALTER TABLE ... DROP COLUMN ...`.

Today Persist writes Parquet files with the following schema:

```
{
  "k": bytes,
  "v": bytes,
  "t": i64,
  "d": i64,
}
```

Where `"k"` is a `Row` (technically `SourceData`) encoded as Protobuf (via the
[`Codec`](https://github.com/MaterializeInc/materialize/blob/d0aa5b7d0b47e55cf4e211e507116a41cb7f8680/src/persist-types/src/lib.rs#L39)
trait) and `"v"` is always the unit type `()`, which gets encoded as an empty
byte array. The root of the problem is `Row` is not self describing, we need
some extra information (i.e. a `RelationDesc`) to know what columns the
`Datum`s in a `Row` map to. Consider the following scenario:

```sql
CREATE TABLE t1 ('a' text, 'b' int);
INSERT INTO t1 VALUES ('hello', 5);

ALTER TABLE t1 DROP COLUMN 'b';
ALTER TABLE t1 ADD COLUMN 'b' int DEFAULT 50;

DELETE FROM t1 WHERE 'b' = 5;
```

To properly handle the `DELETE` we need enough information in the persisted
batch to realize that the `Row('hello', 5)` initially inserted should __not__
be deleted because the column with the value `5` corresponds to the previous
column named `b`.

## Success Criteria

* Batches within Persist are self-describing.
* Unblock work for the following projects:
  * Evolving the schema of a Persist shard (e.g. adding columns to tables).
  * User defined sort order of data, i.e. `PARTITION BY`.
  * Only fetch the columns from a shard that are needed, i.e. projection
    pushdown.
  * Make `persist_source` faster [#25901](https://github.com/MaterializeInc/database-issues/issues/7726).

## Out of Scope

* Detailed logic for migrating the schema of a batch.
  * The following design doc will touch upon how this migration could work, but
    is a large enough issue to warrant its own design doc.
* Design or implementation of any "unblocked" features.

## Solution Proposal

Require that data written to Persist (e.g. `Row`s) be transformed into
[Apache Arrow](https://arrow.apache.org/) data types, then use this columnar
format to write [Apache Parquet](https://parquet.apache.org/) to S3.

### Apache Arrow and Parquet

Arrow is a relatively new in-memory columnar data format. It is designed for
efficient (e.g. SIMD vectorized) analytical operations and to be a standard
that all "data libraries" can interface with. Persist currently collects
statistics for filter-pushdown using Arrow.

Parquet is columnar format designed for efficient storage based off of the
[Dremel paper from Google](https://research.google/pubs/dremel-interactive-analysis-of-web-scale-datasets-2/)
and built as a [collaboration between Twitter and Cloudera for Hadoop](https://blog.twitter.com/engineering/en_us/a/2013/announcing-parquet-10-columnar-storage-for-hadoop).
It's goal is space-efficient storage of data that allows fetching individual
columns from a collection of rows. Crucially it is also self describing and
supports additional arbitrary metadata.

Parquet also has a number of nice features that we won't use immediately but
could enable fairly easily:

* Tracks the size and location of columns within the Parquet file, designed for
  projection pushdown.
* Can collect statistics on the data in the Parquet file, which would
  supplement our own filter pushdown.
* Can store arbitrary metadata for a file. This could be a way we spill our own
  collected stats to S3.
* Dictionary encoding out of the box. For columns with repeated values (e.g.
  time or diff) could result in space savings.
* Compression. Individual columns can be compressed with Snappy, gzip, Brotli,
  or LZ4.

> **Note:** Arrow and Parquet are pretty tightly coupled, mapping from one format to
the other is already handled by any open-source library we would use. For the
most part our codebase will interact with Arrow, only the lowest layer of
Persist having to know about Parquet.

### Columnar Encodings

Currently we durably persist `Datum`s using the [`ProtoDatum`](https://github.com/MaterializeInc/materialize/blob/d0aa5b7d0b47e55cf4e211e507116a41cb7f8680/src/repr/src/row.proto#L32-L69)
protobuf message. Arrow's data types are not as rich as protobufs, so we need
to figure out how exactly we would represent `Datum`s in an Arrow format. These
decisions are not set in stone though, we describe below how migrations to
other Arrow data types would be possible, but it's less work if we get this
right from the start.

<table>
<tr>
<td> Scalar Type </td><td> Rust Representation </td><td> Arrow Array </td><td> Notes </td>
</tr>

<tr>
<td> Numeric </td>
<td>

```rust
struct Decimal<const N: usize> {
  digits: u32,
  exponent: i32,
  bits: u8,
  lsu: [u16; N],
}
```

</td>
<td>

```
StructArray<{
  lossy: f64,
  actual: Bytes,
}>
```

</td>
<td>

Encoded as two values, a lossy `f64` for sorting and filtering, then a
serialized representation of `Decimal` as opaque bytes.

See <https://speleotrove.com/decimal/dnnumb.html> for an explanation as to what
the fields in the Rust type represent.

> **Note:** Arrow does have [`Decimal`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Decimal128)
types, but we opt not to use them because they can't represent the full range
of values that can be represented by `Numeric`. Specifically, the `Decimal`
types are fixed-point and the largest variant, [`Decimal256`], has a maximum
precision of 76 digits. `Numeric` is floating-point and has a maximum precision
of 39 digits, which means we would need a fixed-point number capable of storing
78 digits which Arrow doesn't have.

</td>

<tr>
<td> Date </td>
<td>

```rust
struct Date {
  // Days since the Posgres Epoch.
  days: i32,
}
```

</td>
<td>

`PrimitiveArray<i32>`

</td>
<td>

Directly encode the number of days since the UNIX Epoch (1970-01-01).

> **Alternative:** We could encode this as number of days since the Postgres
Epoch so it would be a direct representation of the Rust type, but I'm leaning
towards encoding as days since the UNIX epoch for consistency with `Timestamp`
which does is also relative to the UNIX epoch. The max value supported for
`Date` in Postgres is the year 5,874,897 AD which can be represented with
either offset.

</td>

<tr>
<td> Time </td>
<td>

```rust
struct NaiveTime {
  secs: u32,
  frac: u32,
}
```

</td>
<td>

`FixedSizeBinary[8]`

</td>
<td>

Represented as the `secs` field and `frac` field encoded in that order as
big-endian.

> **Alternative:** We could represent this as number of nanoseconds since
midnight which is a bit more general but is a more costly at runtime for
encoding. Ideally Persist encoding is a fast as possible so I'm leaning towards
the more direct-from-Rust approach.

> Note: We only need 47 bits to represent this total range, leaving 19 bits
unused. In the future if we support the `TIMETZ` type we could probably also
represent that in a `u64`, using these extra bits to store the timezone.

</td>

<tr>
<td> Timestamp </td>
<td>

```rust
struct NaiveDateTime {
  date: NaiveDate {
    // (year << 13) | day
    ymdf: i32,
  },
  time: NaiveTime,
}
```

</td>
<td>

`PrimitiveArray<i64>`

</td>
<td>

`chrono` (our underlying date time library) uses a more memory efficient
encoding of date by squeezing both year and day into a single `i32`, combined
with a `NaiveTime` this ends up being 12 bytes.


We can repesent this same range of time as the number of microseconds since
the UNIX epoch in an `i64`. Postgres does something very similar, the only
difference is it uses an offset of 2000-01-01.

</td>

<tr>
<td> TimestampTz </td>
<td>

```rust
struct DateTime<Tz: TimeZone> {
    datetime: NaiveDateTime,
    // purely type info
    offset: Tz::Offset,
}
```

</td>
<td>

`PrimitiveArray<i64>`

</td>
<td>

Just like Timestamp, we'll encode this as the number of microseconds since
the UNIX epoch. We don't actually need to store any timezone information,
instead we convert to the session timezone when loaded. This is how Postgres
works.

</td>

<tr>
<td> Interval </td>
<td>

```rust
struct Interval {
  months: i32,
  days: i32,
  micros: i64,
}
```

</td>
<td>

`FixedSizeBinary[16]`

</td>
<td>

Represented by encoding the `months`, `days`, and `micros` fields encoded as
big endian.

> **Alternative:** The smallest possible representation for interval would be
11 bytes, or 12 if we don't want to do bit swizzling. But other than space
savings I don't believe there is a benefit to this approach. In fact it would
incur some computational overhead to encode and there are no benefits from a
SIMD perspective either.

> **Alternative:** We could represent `Interval`s in a `StructArray` but we
don't expose the internal details of `Interval` so this wouldn't aid in
filtering or pushdown. The only benefit of structuring an interval would be for
space reduction if we enable dictionary encoding.

</td>

<tr>
<td> Jsonb </td>
<td>

```rust
// JsonbRef<'a>(Datum<'a>)
enum Value {
  Null,
  Boolean(bool),
  Number(f64),
  String(String),
  Array(Vec<Value>),
  Map(BTree<String, Value>),
}
```

</td>
<td>

`BinaryArray`

</td>
<td>

Serialize JSON with the existing protobuf types, i.e. ProtoDatum, and store
this binary blob.

> **Structured Data:** An option is to structure the JSON data using an Arrow
Union type. What is nice about this approach is it would allow us to do some
form of projection pushdown on the JSON data. The main issue though is Arrow
does not really support recursive data types. In fact, it is impossible to
statically define the above `Value` enum in Arrow. The only option is to
dynamically generate a DataType/schema given a column of values, see [1] for an
example of this approach. I don't believe dynamically generating the schema is
a good option because it is relatively complex, and we would end up with
arbitrarily deep schemas based on user provided data. The arbitrarily deep
schemas particularly concerns me because it would have unpredictable
performance.

> **Alternative:** An alternative to fully structing the data is structuing it
with a depth limit. For example, structuring up-to X levels deep, and then
binary encoding the rest. This gets us predictable performance with the ability
to do limited pushdown, at the cost of code complexity. This is probably the
best approach in the long term, but in my opinion the additional technical
complexity makes it out-of-scope for the initial implementation.

> **Alternative:** Instead of serializing the JSON data with protobuf, we could
use a different serialization format like [BSON](https://bsonspec.org/). This
approach is nice because it gets us a path to entirely eliminating `ProtoDatum`
(🔥) but I am slightly leaning away from this given Protobuf is already used so
heavily in our codebase. If we do use a different serialization format we'll
need to be careful about how we encode numeric data, currently our JSON `Datum`
uses `ProtoNumeric` which has very high precision.

I am leaning away from this approach because we already use protobuf internally
so it's well understood, and there are a few tricks we can use to improve
deserialization to greatly improve our performance, e.g. zero-copy strings,
lazy deserialization, and skipping fields we don't care about.

[1] https://gist.github.com/ParkMyCar/594f647a1bc5a146bb54ca46e6e95680

</td>

<tr>
<td> UUID </td>
<td>

```rust
extern crate uuid;

uuid::Uuid([u8; 16])
```

</td>
<td>

`FixedSizeBinary[16]`

</td>
<td>

Encode the bytes from the `Uuid` directly into a fixed size buffer.

</td>

<tr>
<td> Array </td>
<td>

```rust
struct Array {
  elements: DatumList,
  dims: ArrayDimensions,
}
```

</td>
<td>

```
ArrayDimensions: StructArray<{
  lower_bound: i64,
  length: u64,
}>

Array: StructArray<{
  elements: VariableListArray<T>,
  dimensions: VariableListArray<ArrayDimensions>,
}>
```

</td>
<td>

Store all arrays (including multidimensional) linearly in Row-major order, with
their metadata structured.

Arrays are a bit tricky, their shape must be rectangular but all of the values
in a column don't need to have the same shape, and users can specify a logical
lower bound other than 1. For example, the following is valid:

```sql
CREATE TABLE t1 (a int[]);
INSERT INTO t1 VALUES (ARRAY[1]), (ARRAY[ARRAY[2], ARRAY[3]]);
```

Even though column `a` is defined as a single dimension `int[]`, it's valid to
insert a multi-dimensional array. This is because arrays in Postgres are all a
single type, in other words, `int[]` and `int[][][]` are the same type.

> **Alternative:** We could binary encode the `ArrayDimensions` data but the
Arrow types aren't too complex, so it's not clear that this would definitely be
a better solution.

</td>

<tr>
<td> List </td>
<td>

```rust
// DatumList<'a>.
Vec<T>
```

</td>
<td>

`VariableSizeList<T>`

</td>
<td>

A list of values.

> **Note:** Unlike `Array`, all the values in a column of `List`s must have the
same number of dimensions. Also internally [Arrow represents nested lists](https://arrow.apache.org/docs/format/Columnar.html#list-layout)
in a Row-major format.

</td>

<tr>
<td> Record </td>
<td>

```rust
Vec<(ColumnName, ColumnType)>
```

</td>
<td>

`StructArray`

</td>
<td>

All Record types have the same schema, so at creation time we can define the
schema of the column. This is different than JSON where all values can have a
different schema/shape.

</td>

<tr>
<td> Map </td>
<td>

```rust
// DatumMap<'a>.
HashMap<String, T>
```

</td>
<td>

`MapArray`

</td>
<td>

The Arrow spec does not include the concept of a Map but the `arrow2` and
`arrow-rs` crates have a `MapArray` type that is a list of tuples.

> **Alternative:** We could encode maps to some binary format, e.g. proto, and
store them as a binary blob. While this might be simpler it prevents us from
being able to push down optimizations into the map.

</td>

<tr>
<td> MzTimestamp </td>
<td>

```rust
struct MzTimestamp(u64);
```

</td>
<td>

`PrimitiveArray<u64>`

</td>
<td>

Number of milliseconds since the UNIX epoch.

</td>

<tr>
<td> Range </td>
<td>

```rust
struct Range<T> {
  lower: RangeBound<T> {
    inclusize: bool,
    bound: Option<T>,
  },
  upper: RangeBound<T>,
}
```

</td>
<td>

```
RangeBound: StructArray<{
  inclusive: bool,
  bound: T,
}>
```

```
Range: StructArray<{
  lower: RangeBound,
  upper: RangeBound,
}>
```

</td>
<td>

Structure the data as it is in Rust.

Ranges seem pretty interesting and powerful, so Persist having an understanding
of the data seems worthwhile for the long term. They could also be entirely
unused (I'm not sure) in which case the complexity of encoding these in a
structured way might not be worth it.

> **Alternative:** Encode a Range into a binary format and store it as a blob.

</td>

<tr>
<td> MzAclItem </td>
<td>

```rust
struct MzAclItem {
  // String
  grantee: RoleId,
  // String
  grantor: RoleId,
  // u64
  acl_mode: AclMode,
}
```

</td>
<td>

```
StructArray<{
  grantee: String,
  grantor: String,
  acl_mode: u64,
}>
```

</td>
<td>

Structure the data as it is in Rust.

> **Alternative:** Encode an MzAclItem into a binary format and store it as a blob.

</td>

<tr>
<td> AclItem </td>
<td>

```rust
struct AclItem {
  // u32
  grantee: Oid,
  // u32
  grantor: Oid,
  // u64
  acl_mode: AclMode,
}
```

</td>
<td>

```
StructArray<{
  grantee: u32,
  grantor: u32,
  acl_mode: u64,
}>
```

</td>
<td>

Structure the data as it is in Rust.

> **Alternative:** It would be relatively easy to stitch together the three
values that make up an `AclItem` into a `FixedSizeBinary<16>`, it should even
sort the same as its Rust counterpart.

</td>

<tr>
<td> Int2Vector </td>
<td>

```rust
Vec<i16>
```

</td>
<td>

`VariableSizeList<i16>`

</td>
<td>

Structure the data as it is in Rust.

</td>

</table>

### Extension Types

Arrow has the concept of [Extension Types](https://arrow.apache.org/docs/format/Columnar.html#extension-types)
which do not change the physical layout of a column, but allow you to identify
the kind of data stored in a column by tagging it with metadata. At the time
of writing there are only two accepted [canonical extension types](https://arrow.apache.org/docs/format/CanonicalExtensions.html#official-list)
but we are free to create our own.

As part of our mapping from `Datum` to Arrow column we could include an
extension type in the column's metadata, specifically:
```
'ARROW:extension:name': 'materialize.persist.<version>.<datum_name>'
```
To start `<version>` will just be `1`, but could be used in the future to
evolve how we represent `Datum`s in Arrow. And we include the 'persist'
namespacing to make sure we don't collide with any other Materialize Arrow
formats, e.g. for `COPY TO ...`.

> **Note:** The [guidance](https://arrow.apache.org/docs/format/Columnar.html#extension-types)
provided by the Arrow Project is to name extension types with a prefix to
prevent collisions with other applications. I don't think this really matters
for Persist, but I don't see a reason not to follow this pattern.

### Schemas and Column Naming

When writing Arrow columns as Parquet you need to include a schema, which
requires each column to be named. Instead of using the externally visible
column names, we could name the Arrow fields with their column index. For
example, a table with two columns 'foo' and 'bar' would be persisted with
column names '0' and '1'. This is a bit forward looking, but aims to support
two goals:

1. Dropping and adding columns of the same name. We would use an
   auto-incrementing tag number instead of column index, but would allow us to
   distinguish between these two states.
2. Users that consider column names to be sensitive. Definitely not a blocker
   but redacting possibly sensitive information at the lowest levels seems like
   a good win.

Like Extension Types, schemas can also be tagged with metadata. We already
write some [limited metadata](https://github.com/MaterializeInc/materialize/blob/7135e53182c77e2c2fe31fe2aa700adc37ac134d/src/persist/src/indexed/columnar/parquet.rs#L54-L59)
to our Parquet files in the form of a [protobuf message](https://github.com/MaterializeInc/materialize/blob/7135e53182c77e2c2fe31fe2aa700adc37ac134d/src/persist/src/persist.proto#L26-L35)
I propose we extend this message to include include a "version" which initially
will just be `1` and can be incremented at its own pace. The goal of this
"batch version" is it gives us a way to entirely ignore the new columnar data
if necessary. For example, say we incorrectly encode the new structured format
and should always ignore the bad data on decode, bumping the version number
gives us an easy way to do that.

### Migration

#### Introducing Columnar/Structured Data

Today data is encoded with the `Codec` trait and written to S3 with the
following Parquet schema:

```
{
  "k": bytes,
  "v": bytes,
  "t": i64,
  "d": i64,
}
```

To migrate to the new structured data I propose we extend the above schema to:

```
{
  "k": bytes,
  "v": bytes,
  "t": i64,
  "d": i64,
  "k_s": {
    // ... nested structured data
  },
  "v_s": {
    // ... nested structured data
  },
}
```

In other words, we dual encode blobs with both `Codec` and Arrow and write them
to a single Parquet file. This has the following benefits:

1. Slow rollout/shadow migration. If we write both formats we can let this
   change bake for as long as we would like, validating that `Codec` and Arrow
   decode to the same `Row`s. Reading only Arrow data could be turned on or off
   via a LaunchDarkly flag.
2. Incremental progress. If we write both formats then we could read back Arrow
   data but sort and consolidate with the `Codec` format. Currently decoding
   protobuf (i.e. `ProtoDatum`) is computationally expensive, getting an
   incremental win of improving decode time would be nice.
3. Doesn't increase the number of `PUT` or `GET` requests to `S3`.
4. Doesn't require any change to state stored in Consensus.
5. (Forward looking) When we switch to reading only Arrow data we can leverage
   Parquet's projection pushdown to ignore the old `'k'` and `'v'` columns.

The current max blob size is 128MiB, we have two options for how we want to
handle the "max" size when we write both `Codec` data and Arrow at the same
time.

1. Ignore Arrow when calculating the max. This increases the size of S3 objects
   which may impact performance for things that have been tuned to assume
   128MiB sized objets, e.g. compute back pressure.
2. Include Arrow when calculating the max. This reduces the amount of rows that
   can get stored in a single blob, thus increasing the number of Parts
   per-Run. This increases the number of S3 `GET`s and `PUT`s which could
   impact cost, and requires `ConsolidatingIter` to handle a larger number of
   Parts.

Neither option is great, but I am leanning towards [1] since it puts the
pressure/complexity on S3 and the network more than Persist internals. To make
sure this solution is stable we can add Prometheus metrics tracking the size of
blobs we read from S3 and monitor them during rollout to staging and canaries.

### Consolidation and Compaction

To start we can write both the `Codec` and Arrow versions of the data, and do
compaction based on the bytes from `Codec`. Eventually we'll need to
consolidate the Arrow data directly which requires sorting the data. Sorting
an Arrow `StructArray` is possible (e.g. the `pyarrow` library [supports it](https://arrow.apache.org/docs/python/generated/pyarrow.StructArray.html#pyarrow.StructArray.sort))
but neither the `arrow-rs` or `arrow2` crates support it. I filed an
[issue with `arrow-rs`](https://github.com/apache/arrow-rs/issues/5559) but
realistically this is something we'll probably need to implement. Once data is
sorted consolidating an Arrow array is relatively easy and fast! To delete a
value all we need to do is unset the bit in the
[validity bitmap](https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps)
for that value.

#### Across Multiple Versions of Data

There are three different scenarios we need to handle consolidation for:

1. Evolving the way we encode `Datum`s to Arrow Arrays.
2. Batch A with `Codec` data, Batch B with Arrow Arrays.
3. Two batches across different versions of a table (e.g. after `ALTER TABLE .. ADD COLUMN ...`)

For all of these scenarios I believe we can handle consolidation by first
decoding to the Rust type (e.g. `Row`) and consolidating based on that. It will
be relatively inefficient, but none of these scenarios should happen too often.

> **Note:** In the current state of the world we'll need to support compacting
`Codec` and Arrow data forever since we can't guarantee all batches written
with `Codec` have been compacted yet. The ability to make this guarantee is
something we could add to [Compaction 2.0](https://github.com/MaterializeInc/database-issues/issues/4809).

> **Note:** While technically out-of-scope for this design doc, we can
implement efficient consolidation for case 3 (table evolution) without decoding
to the Rust type. For columns that have been dropped we can drop the
corresponding Arrow Array or for new columns with a default value we can add a
[run-end encoded array](https://arrow.apache.org/docs/format/Columnar.html#run-end-encoded-layout),
both should take `O(1)` time.

## Minimal Viable Prototype

While not a working prototype I have three draft PRs that validate the ideas
described in this design doc.

* [[dnm] columnar: Write columnar encodings as part of a Batch](https://github.com/MaterializeInc/materialize/pull/26120)
  * Explores the migration strategy of writing `'k_s'` and `'v_s'` with our
    current Parquet schema.
* [[dnm] columnar: Array support](https://github.com/MaterializeInc/materialize/pull/25848)
  * Adds support for Array types in our current columnar framework.
* [[dnm] columnar: UUID support](https://github.com/MaterializeInc/materialize/pull/25853)
  * Adds support for UUID types in our current columnar framework.

### Ordering of Work

A goal of this project is to break up the work to provide incremental value.
This makes code easier to review, allows changes to bake for longer periods of
time, and we can pause on work if something else arrises.

An approximate ordering of work would be:

1. Implement a "V0" Arrow encoding for non-primitive types that is the encoded
   `ProtoDatum` representation and collects no stats. This unblocks writing
   structured columnar data, and asynchronously we can nail down the right
   Arrow encoding formats.
2. Begin writing structured columnar data to S3 for staging and prod canaries,
   in the migration format. Allows us to begin a "shadow migration" and
   validate the new format.
3. Serve reads from the new structured columnar data. For `Datum`s that have
   had non-V0 Arrow encodings implemented, this allows us to avoid decoding
   `ProtoDatum` in the read path, which currently is relatively expensive.
   Crucially this is possible without supporting consolidation via Arrow
   because we don't yet consolidate on read.
4. Implement the `Datum` to Arrow encodings and migrate from the "V0" encoding
   introduced in step 1. They can be implemented independent of one another,
   and would allow us to begin collecting stats for filter pushdown for the new
   type and further optimize our reads.
5. Support consolidating with Arrow data, including consolidating `Codec` with
   Arrow. Allows us to stop writing `Codec` for new batches, saving on S3
   costs.

After all 4 of these things are completed, we would be able to stop encoding
with `ProtoDatum` entirely.

## Forward Looking

The following changes are out of scope for this project, but our goal is to
unblock these features with the described work.

* `ALTER TABLE ... [ADD | DROP] COLUMN ...`

  * Batches within Persist being self describing, and including a way to
    uniquely tag columns are the two features currently missing that are
    required for adding columns to tables.

* User specified sorting of data, `PARTITION BY`

  * Persist understanding the structure of data in a Batch allows us to sort
    on columns within that batch.

* Projection Pushdown

  * Parquet has builtin support for projection pushdown. It appends metadata
    about the file which describes where each column chunk is located.

## Alternatives

### Why Parquet?

I found serialization formats generally fell into two categories:

1. Mainly used for inter-process communication, e.g. Protobuf, JSON,
   FlatBuffers, etc.
2. Designed for storing tabular data, e.g. jsonl, CSV, Apache Avro, Apache ORC,
   etc.

While we could store the formats in group 1 (e.g. today we store a list of
protobuf encoded objects) they lack the features of group 2 which work well for
large amounts of data, e.g. compression or dictionary encoding.

Out of the formats in group 2, it's generally accepted that columnar formatted
data is better for analytical workloads. That elimates row based formats like
Apache Avro, CSV, and jsonl.

The only serialization format, other than Apache Parquet, I could find that was
designed for large tabular data and columnar formatted was Apache ORC. I didn't
do too much research into ORC, nor think it's the right fit for us, because it
doesn't have much support in the open source community. There is only a partial
[Rust implementation](https://crates.io/crates/orc-rust) which has little
usage.

#### What about our own format?

I briefly explored the idea of inventing our own serialization format. Because
we're serializing a specific kind of data structure with a specific access
pattern there are probably some optimizations we could make for the format. But
in my opinion these possible benefits are not worth the time and maintenance
required to invent our own format, considering where we are today.

### Why Arrow?

There are not many in-memory columnar formats other than Arrow. I did find
[lance](https://github.com/lancedb/lance) which is a farily popular columnar
format written in Rust, but it's geared towards Machine Learning and LLMs. In
fact, when I looked a bit deeper parts of `lance` seemed to use Arrow directly?
Given this and the different goals it doesn't seem like it would be a good fit.

#### Can we use Parquet directly?

Currently we only use Arrow Arrays to collect stats on the data we're writing,
so we're not really benefiting from Arrow's high performance compute kernels.
It is possible to go from `Datum`s straight to Parquet types, but when
exploring this I found Parquet to be a much lower level interface than desired,
and Arrow offered the appropriate abstraction that made mapping `Datum`s
relatively easy.

Arrow also allows for faster random access than Parquet. While not used today,
it's a nice property to have.

### Migration Format

There is one other alternative for how we migrate to the new columnar format,
which is write two blobs to S3 instead of one. We could continue to write blobs
as they are today and then begin writing a second blob that has only our
columnar data. This avoids the issues with blob size that the proposed solution
has, but otherwise has a number of its own issues:

1. Increases `GET`s and `PUT`s to S3, increasing cost.
2. Requires changing our stored state in Consensus. Each `Part` would not have
   two blobs associated, and we need to keep track of these at some layer.
3. Introduces the possibility of inconsistencies. While not likely, it would be
   possible for one blob to exist while the other doesn't.

## Open questions

None currently
