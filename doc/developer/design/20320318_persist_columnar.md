# Persist Columnar

- Associated:
  - [write Schema-ified data blobs #24830](https://github.com/MaterializeInc/materialize/issues/24830)
  - [persist: schema evolution](https://github.com/MaterializeInc/materialize/issues/16625)
  - [Table support for push sources](https://github.com/MaterializeInc/materialize/issues/22836)
  - [[dnm] columnar: Write columnar encodings as part of a Batch](https://github.com/MaterializeInc/materialize/pull/26120)
  - [[dnm] columnar: Array support](https://github.com/MaterializeInc/materialize/pull/25848)

<!--
The goal of a design document is to thoroughly discover problems and
examine potential solutions before moving into the delivery phase of
a project. In order to be ready to share, a design document must address
the questions in each of the following sections. Any additional content
is at the discretion of the author.

Note: Feel free to add or remove sections as needed. However, most design
docs should at least keep the suggested sections.
-->

## The Problem

<!--
What is the user problem we want to solve?

The answer to this question should link to at least one open GitHub
issue describing the problem.
-->

We want Persist to have an understanding of the data it contains. The main
motivating factor for this is to support schema migrations, concretely,
being able to run `ALTER TABLE ... ADD COLUMN ...`.

Today Persist writes Parquet files with the following schema:

```
{
  "k": bytes,
  "v": bytes,
  "t": i64,
  "d": i64,
}
```

Where `"k"` is a `Row` (technically `SourceData`) encoded as Protobuf and `"v"`
is always the unit type `()`, which gets encoded as an empty byte array. The
root of the problem is `Row` is not self describing, we need some extra
information (i.e. a `RelationDesc`) to know what columns the `Datum`s in a
`Row` map to. Consider the following scenario:

```sql
CREATE TABLE t1 ('foo' text, 'bar' text);
-- 1. insert some values
ALTER TABLE t1 DROP COLUMN 'foo';
ALTER TABLE t1 ADD COLUMN 'foo' text;
-- 2. insert some more values
```

## Success Criteria

<!--
What does a solution to this problem need to accomplish in order to
be successful?

The criteria should help us verify that a proposed solution would solve
our problem without naming a specific solution. Instead, focus on the
outcomes we hope result from this work. Feel free to list both qualitative
and quantitative measurements.
-->

## Out of Scope

<!--
What does a solution to this problem not need to address in order to be
successful?

It's important to be clear about what parts of a problem we won't be solving
and why. This leads to crisper designs, and it aids in focusing the reviewer.
-->

## Solution Proposal

<!--
What is your preferred solution, and why have you chosen it over the
alternatives? Start this section with a brief, high-level summary.

This is your opportunity to clearly communicate your chosen design. For any
design document, the appropriate level of technical details depends both on
the target reviewers and the nature of the design that is being proposed.
A good rule of thumb is that you should strive for the minimum level of
detail that fully communicates the proposal to your reviewers. If you're
unsure, reach out to your manager for help.

Remember to document any dependencies that may need to break or change as a
result of this work.
-->

### Columnar Encodings

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

</td>

<tr>
<td> Date </td>
<td> 

```rust
struct Date {
  days: i32,
}
``` 

</td>
<td>

`PrimitiveArray<i32>`

</td>
<td>

Directly encode the number of days.

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

`PrimitiveArray<u64>`

</td>
<td>

Represented as the number of microseconds since midnight.

> **Alternative:** Instead of representing this as the number of microseconds
since midnight we could stich together the two `u32`s from `NaiveTime`. This
would maybe save some CPU cycles during encoding and decoding, but probably not
enough to matter, and it locks us into a somewhat less flexible format.

> Note: We only need 37 bits to represent this total range, leaving 27 bits 
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
instead we convert to the session timezone when loaded. This is how both 
Postgres works.

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
little endian. 

> **Alternative:** The smallest possible representation for interval would be
11 bytes, or 12 if we don't want to do bit swizzling. But other than space
savings I don't believe there is a benefit to this approach. In fact it would
incur some computational overhead to encode and there are no benefits from a
SIMD perspective either.

> **Alternative:** We could represent `Interval`s in a `StructArray` but we
don't expose the internal details of `Interval` so this wouldn't aid in
filtering or pushdown. The only benefit of structuring an interval would be for
space reduction in dictionary encoding, but this is offset by the overhead of
the struct itself.

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
to do limited pushdown, at the cost of code complexity. I am not a fan of this
approach because in my opinion the benefit of limited pushdown is not enough to
justify the complexity.

> **Alternative:** Instead of serializing the JSON data with protobuf, we could
use a higher performance serialization format like [FlatBuffers](https://github.com/google/flatbuffers).
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
// DatumList<'a>.
[T; N]
``` 

</td>
<td>

`FixedSizeList<T>[N]`

</td>
<td>

A column of type array requires all of its elements to be the same size.

> **Alternative:** We could use Arrow's Variable-size list, but it's less
memory efficient

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

Unlike `Array`s, `List`s are allowed to be ragged.

</td>

<tr>
<td> Record </td>
<td> 

```rust
Vec<(ColumnName, ColumnType)>
``` 

</td>
<td>

`BinaryArray`

</td>
<td>

Serialize JSON with the existing protobuf types, i.e. ProtoDatum, and store
this binary blob.

> **Alternative:** See "Notes" on JSON for alternative approaches. tl;dr Arrow
does not support recursive types very well.

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


## Minimal Viable Prototype

<!--
Build and share the minimal viable version of your project to validate the
design, value, and user experience. Depending on the project, your prototype
might look like:

- A Figma wireframe, or fuller prototype
- SQL syntax that isn't actually attached to anything on the backend
- A hacky but working live demo of a solution running on your laptop or in a
  staging environment

The best prototypes will be validated by Materialize team members as well
as prospects and customers. If you want help getting your prototype in front
of external folks, reach out to the Product team in #product.

This step is crucial for de-risking the design as early as possible and a
prototype is required in most cases. In _some_ cases it can be beneficial to
get eyes on the initial proposal without a prototype. If you think that
there is a good reason for skpiping or delaying the prototype, please
explicitly mention it in this section and provide details on why you you'd
like to skip or delay it.
-->

## Alternatives

<!--
What other solutions were considered, and why weren't they chosen?

This is your chance to demonstrate that you've fully discovered the problem.
Alternative solutions can come from many places, like: you or your Materialize
team members, our customers, our prospects, academic research, prior art, or
competitive research. One of our company values is to "do the reading" and
to "write things down." This is your opportunity to demonstrate both!
-->

## Open questions

<!--
What is left unaddressed by this design document that needs to be
closed out?

When a design document is authored and shared, there might still be
open questions that need to be explored. Through the design document
process, you are responsible for getting answers to these open
questions. All open questions should be answered by the time a design
document is merged.
-->
