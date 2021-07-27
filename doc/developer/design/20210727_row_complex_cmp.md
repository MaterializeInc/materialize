# Complex `repr::Row` comparisons

## Summary

The current derived implementation of `repr::Row`'s equality operation hinders
our ability to support complex types (e.g. `numeric`, `char`, and eventually
collated strings) with fully compatible PostgreSQL semantics. The current
implementation relies solely on byte-by-byte equality checks, which is very
fast, but doesn't allow for complex types that require any other kind of
equality operations.

This design doc proposes a change to `repr::Row`'s equality that will allow us
to support more complex data types with full support for Postgres' semantics.

Note that we use "equality" here, but these changes would also require changing
the implementations of all comparators, i.e. `Eq`, `PartialEq`, `Ord` and
`PartialOrd`.

## Goals

- Implement a new comparison operator for `repr::Row` that supports arbitrary
comparison functions, e.g. supporting equality for a `Datum::Char` whose
semantics require a more complex operation than a byte-by-byte comparison for
fully compatible PostgreSQL support.
- Retain performance parity with the current implementation when complex
  comparisons are not required.

## Non-Goals

- Retain performance parity with the current implementation for complex comparisons.

## Description

- Track all
  [tags](https://github.com/MaterializeInc/materialize/blob/1907a5681c2737100ddbc78cfd6b194bf165299a/src/repr/src/row.rs#L192)
  that correspond to `Datum`s that require complex equality operations, say
  `COMPLEX_EQ_TAGS`.
-  Add a field to `repr::Row`, `complex_eq`, that is set to `true` whenever
   constructing a row that contains at least one element of `COMPLEX_EQ_TAGS`.

When performing comparisons:
1. Check both rows' `complex_eq`:
    - If xor true, return false (two rows cannot be equal if one contains a
      complex eq tag and the other does not).
    - If both are false, use the rows' binary equality
    - If both are true, go to the next step
1. Iterate over both rows' tags.
    - For tags in `COMPLEX_EQ_TAGS`, unpack the data into `Datums`, and then
      perform equality on the `Datum`. This requires a new implementation of
      `Datum`'s comparisons operator, which would actually house the custom
      logic.
    - For all other tags, perform a byte-by-byte comparison of the data seen in
      the area delineated by the tag, i.e. ~use the current approach.

This approach would provide us flexibility for custom/complex equality, while
minimizing the impact to the operations' speed in the complex case.

## Alternatives

### Do nothing

The main alternative is to _not_ introduce a change here, and require that:
- The data stored in a row is the version that produces the byte-by-byte equality you
desire.
- The data pulled from a row into some other part of the system either live with
  the mutated data, or find some way of "resaturating" any lost or modified data.

However, I have tried this approach twice recently with some compromises, as
well as considered a third instance where the current approach is wholly
insufficient.

#### Numerics
`numeric` equality ignores values' trailing zeroes, e.g.

```
SELECT 1.23000000 = 1.23;
 ?column?
----------
 t
```

We achieved this with the `rust-dec` refactor of the `numeric` type by
truncating all trailing zeroes from the datum before packing it into the row.
However, this has the cosmetic annoyance of permanently throwing away trailing
zeroes.

Because this was a lower-order concern and didn't affect any computations, we
decided to live with the side effects. However, in an ideal world, we wouldn't
be forced to do this.

#### Char

`char` equality ignores values' trailing whitespace, e.g.

```
SELECT 'a     '::char(5) = 'a'::char(3);
 ?column?
----------
 t
```

In working on the new `char` type, this was mostly achievable by storing the
truncated character string in the row, and then resaturating the blank padding
whenever we brought a `Datum` into some other part of the system.

However, this approach is insufficient for supporting Postgres' array
semantics for our list type.

For example this is possible in PostgreSQL:

```sql
SELECT ARRAY['a'::char(2), 'a'::char(3), 'a'::char(4)];
```
```
        array
---------------------
 {"a ","a  ","a   "}
```

An equivalent expression is not possible Materialize––at least not with the same
result. For example, this would be the most likely output:

```sql
SELECT LIST['a'::char(2), 'a'::char(3), 'a'::char(4)];
```
```
        array
---------------------
 {a,a,a}
```

Note that the trailing whitespaces are missing. This is because when the list
is getting unpacked, it believes that all of its elements are simply `Char`
without any typmod applied, so it doesn't have any sense of how to resaturate
the truncated blank padding when e.g. printing them.

It's important to note that permanently losing the blank padding data can change
the semantics of other operations. The trailing whitespaces are very important
for `char`, which is more properly referred to as `bpchar`, as in blank-padded
characters, i.e. the feature is so important it's in the type name. The
semantics of functions that take `char` data (such as the `(char,text)`) version
of `LIKE`) rely on the whitespace's presence.

The only meaningful way to support this would be to track each values typmod in
the type itself at the `ScalarType` level. That seems very fussy, brittle, and
would require some dramatic refactoring to the list type. I mention it only
because I thought about it, but believe it's unreasonable.

In short, it is exceedingly difficult to support differently typmoded values
with the current equality implementation, but much simpler with the proposed
approach; we could simply store the blank-padded string itself without worrying
about concocting the typmod itself.

#### Collated strings

While we don't currently support collated strings, we might want to. However,
supporting them **requires** a change to row equality.

Strings in a collation store two vallues: one for the bytes to print, and
another that express their ordering. A simple byte-by-byte comparison is
insufficient to support collation; collations expressly provide different forms
of comparison.

### Store equality bits separately

Add a field to `repr::Row`, like `complex_eq_bits: Option<SmallVec>`, which
stores a version of the row with values that support byte-by-byte comparisons in
cases where it's necessary.

However, adding >=24 bytes to every row would, in @benesch's words, "hurt."

A slight variation on this theme would be to store the binary comparison bytes
in the row "behind" the "literal" datum, and access them in a similar manner as
the proposal (i.e. iterating over tags when you know there are special bytes,
and doing binary equality otherwise). This is likely faster than the main
proposal, but increases each row's memory usage by essentially duplicating some
columns' data.

### "Bonus bytes"/composite row

@benesch and @mjibson both mentioned storing some "bonus bytes" that do "magical
things" at the end of a row; pinging them to get some more details here.

## Open questions
- How much slower are the proposed approaches than the current approach for rows with complex types?
