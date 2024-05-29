# MIR Representation Types

- Associated:
  + https://github.com/MaterializeInc/materialize/issues/4171
  + https://github.com/MaterializeInc/materialize/pull/27029
  + https://github.com/MaterializeInc/materialize/pull/27109

MIR is typechecked using user-facing SQL types.
SQL types make distinctions between types (e.g., `CHAR(5)`, `VARCHAR(5)`, and `TEXT`) that will not be realized at runtime (all of these are represented using `Datum::String`).

This design doc proposes _representation types_---a new notion of type for MIR terms.
A representation type maps to a subset of constructors/variants of the `Datum` enum.

Making this change should help address issues in optimization, particularly index selection (see ["The Problem"](#the-problem)).
It will also simplify and clarify what MIR is doing.

## The Problem

[Postgres has several types for character data:](https://www.postgresql.org/docs/current/datatype-character.html)

- `char(n)`, `bpchar(n)` for fixed-length strings with a fixed-length representation (whitespace padded)
- `varchar(n)` for fixed-length strings with a variable-length representation
- `varchar` for arbitrary-length strings with a variable-length representation
- `bpchar` for artbirary-length strings with a variable-length representation where trailing whitespace is trimmed
- `text` for arbitrary-length strings with a variable-length representation

(There are also `"char"` and `name`, but they are very much internal to Postgres---let's ignore them for the moment.)

Materialize implements all five of these character sequence types using `Datum::String`.
Materialize only implements meaningful operations on the `text` type; to operate on other character sequence types, one must convert to `text`.
Conversion to `text` is a no-op at runtime, since we already have the string.
We freely insert implicit coercions to `text` in the early stages of query processing.
For correct error behavior, however, we need to carefully keep track of failures to meet length limits.

These implicit coercions---often of the form `varchar_to_text(#c)` for some column number `c`---get in the way during join planning.
Even if we have an index on column `c` as a `varchar`, we cannot use that index to plan a join on `varchar_to_text(#c)`: not only are the types different, the terms are different, too.
We know these implicit coercions interfere with joins; they may also interfere with other aspects of optimization.

## Success Criteria

1. Plan joins without implicit coercions between equivalent representation types getting in the way.
2. It should be possible for us to elide noop casts at the MIR level.
3. The catalog should reflect SQL-level types.
4. There is clear guidance for customers regarding the different character sequence types.
5. Notices help customers avoid pitfalls with character sequence types.

## Out of Scope

We will not rethink other components of MIR types, like nullability or unique keys.

In general, we may want a "smart index selector", which can appropriately handle mixed-type lookups, i.e., it should let us look up an `i32` in an `i64`-sized index without needing to build a separate `i32`-sized arrangement. Such an approach generalizes `eq-indx` (see ["Alternatives"](#alternatives)).

## Solution Proposal

Having MIR be typechecked in terms of representation types demands a few changes, which can be broken into three PRs:

1. Rename the existing `ScalarType` to `HirScalarType`. (We may want to rename `ColumnType` and `RelationType`, as well.)
2. Have MIR work with `MirScalarType` (see [MVP](#minimal-viable-prototype)).
   a. Introduce the `MirScalarType` datatype (see [MVP](#minimal-viable-prototype)).
   b. Change the optimizer to work with `MirScalarType` throughout.
   c. Change the adapter to record the `HirScalarType` of a query and update its nullability information using the `MirScalarType` at the end of optimization.
3. Elide noop casts, confirming that we plan the problematic joins better.

PR #2 will have a large diff.
On the plus side, it should be a largely mechanical change: there is a straightforward way to project a `HirScalarType` to a `MirScalarType`, and most uses of types in the optimizer will be parametric.

## Minimal Viable Prototype

The representation types will look like the following:

```rust
pub enum MirScalarType {
  Bool,
  Int16,
  Int32,
  Int64,
  UInt8, // also includes HirScalarType::PgLegacyChar
  UInt16,
  UInt32, // also includes HirScalarType::{Oid,RegClass,RegProc,RegType}
  UInt64,
  Float32,
  Float64,
  Numeric,
  Date,
  Time,
  Timestamp  { precision: Option<TimestampPrecision>, },
  TimestampTz { precision: Option<TimestampPrecision>, },
  MzTimestamp,
  Interval,
  Bytes,
  Jsonb,
  String, // also includes HirScalarType::{VarChar,Char,PgLegacyName}
  Uuid,
  Array(Box<MirScalarType>), // also includes HirScalarType::Int2Vector
  List { element_type: Box<MirScalarType> }, // also includes HirScalarType::Record
  Map { value_type: Box<MirScalarType> },
  Range { element_type: Box<MirScalarType> },
  MzAclItem,
  AclItem,
}

pub struct MirColumnType {
    pub scalar_type: MirScalarType,
    pub nullable: bool,
}
```

Since LIR is effectively untyped, there's no need for us to define `LirScalarType` or the like. We already use `MirScalarExpr` inside of `Mfp`s in LIR.

We'll need to define a projection from `HirScalarType` to `MirScalarType` (see [`ScalarType::is_instance_of()`](https://github.com/MaterializeInc/materialize/blob/e4578164fb28a204b43c58ab8ff6c1d3e3b4427f/src/repr/src/scalar.rs#L947) for the correspondence).

In `EXPLAIN PLAN`s, we will report `MirScalarType`s with `r`-prefixed names, where `r` is short for "representation", e.g., `rstring` is the `MirScalarType` corresponding to `text`, `varchar`, etc.

## Alternatives

We considered several alternatives:

- (`eq-type`) Change the MIR typechecker to conflate `varchar` and `text` types (and drop calls to `varchar_to_text`).
- (`eq-indx`) Change index selection to conflate `varchar` and `text` types (and leave calls to `varchar_to_text`).
- (`remixvc`) Automatically turn add `varchar_to_text` to all indices on a `varchar`.
- (`remix++`) Automatically wrap every use of a `varchar` with `varchar_to_text`.

The `eq-type` and `eq-indx` approach came up in early discussions.
These are fundamentally hacks, with the upside of being quick fixes.
The downside is the addition of a tricky corner case, some risk (what if keeping the types separate matters somewhere we've forgotten about?), and some technical debt/mess-making.

The `remixvc` and `remix++` approaches address the problem at the user/adapter side.
Such rewriting feels a bit risky/annoying/confusing, and I'm not sure how it would interact with sources.

## Open questions

It's possible that (as Nikhil worries) somewhere in the pgwire or WS layers calls `.typ()` on a `MirRelationExpr` and expects a `HirScalarType` but will now find a `MirScalarType`.
I don't know those layers well, but we could derisk some of the work of PR #2 by checking for that in advance.
(In any case, the Rust type checker will be eager to tell us about it.)
