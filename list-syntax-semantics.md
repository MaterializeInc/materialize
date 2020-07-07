List subscript syntax + semantics

# Index

## Syntax

`[<i64 expr>]`

## Semantics

Returns the _i_th element of the list (i.e. is 1-indexed), or returns _NULL_ if _i_ exceeds the list's length.

### Reasoning

- Indexes are 1-indexed to match Postgres arrays.
- Returning _NULL_ rather than an "index out of range" error makes lists more flexible, given that we support jagged internal lists.

  The motivating use case for this decision is joining/filtering values on an index expression; rather than simply erroring if you've "over-indexed", we can return _NULL_, allowing for much greater expressivity.

  This is a lame example, but with a little more imagination, one can come up with something more plausible.

  ```
  SELECT *
  FROM applicants
  WHERE preferred_places_to_work[2] = 'Materialize';
  ```

  This also mirrors Postgres' behavior, which means that it's at-least-somewhat ergonomic.

- Our indexing operation should _not_ allow infinite indexing into a list; instead, we should naturally do type resolution, e.g. indexing into a `LIST INT INT` should return a `LIST INT`, and attempting to perform three successive indexing operations should return an error because you cannot index into an `INT`.

# First-dimensional Slice

## Syntax

`[<i64 expr>?:<i64 expr>?]`

This syntax includes support for all Postgres-style slice subscripts, i.e. `[:j]`, `[i:]`, `[:]`.

## Semantics

Takes a slice of the outermost list from the _i_th to the _j_th position, inclusive (also 1-indexed, so `[1:2]` takes the first two elements), or returns _NULL_ if _i_ exceeds the list's length. If _j_ exceeds the list's length, the slice contains all of the elements after _i_.

### Reasoning

- This operator only accesses the outermost list. Repeatedly calling it will only continue taking slices of the outermost list. This differs from Postgres' `ARRAY` syntax that uses repeated invocations of this operator to perform multi-dimensional slices.

  The thinking behind this is that the PostgreSQL syntax is unintuitive, and given that our `LIST` implementation differs from PG's `ARRAY` type in how it handles inner lists (i.e. supports jagged list), we're both totally free to design a new interface and _should_ to avoid signalling that its semantics are identical.

# Multi-dimensional slices (experiemental)

There's some jargon here I picked up:

- **Axis** refers to a specific dimensions of a list, e.g. along the second axis refers to the "second dimension" of the list.
- **Rank** refers to the number of axes in a list.

## Syntax

`[<i64 expr>?:<i64 expr>?(, <i64 expr>?:<i64 expr>?)*]`

e.g. `[1:2, :, :3]`

Note that `[:]` is actually a nop, with the exception of advancing the axis counter.

This syntax is inspired by `numpy`'s [multi-dimensional slicing](http://ilan.schnell-web.net/prog/slicing/).

## Semantics

Takes a slice of the list along the _n_th axis, where _n_ is the range expression's position in the comma-separated list, from _i_ to _j_, or returns _NULL_ if _i_ exceeds the list's length. If _j_ exceeds the list's length, the slice contains all of the elements after _i_.

Attempting to take a slice along an axis greater than the list's rank is an error, e.g. `(INT LIST)[1:1, 1:1]` errors because you are attempting to access the second axis of a one-rank list.

### Reasoning
Our list implementation applies the same ergonomics to slicing as it does to indexing:

- We "strongly type" the list's base type and do not allow slices of infinite depth, just as we do not allow infinite indexing.

- Trying to start a slice past the last element of a list returns _NULL_ in the same way that accessing an element of an index beyond its last element does.

  ```sql
  SELECT (LIST[1])[2:2];
   ?column?
  ----------
   NULL
  ```

  Note that this greatly differs from Postgres' behavior, which _always_ returns at least an empty list, although they do it "[for historical reasons](https://www.postgresql.org/docs/9.1/arrays.html)." Given that `LIST` isn't bound to Postgres' history, we can do something consistent.

  Because `LIST` supports "jagged" inner `LIST`s, this restriction is applied recursively to each inner list, e.g.

  ```sql
  SELECT (LIST[[1, 2], [3]])[1:2, 2:2];
  ?column?
  ------------
   {{2},NULL}

  SELECT (LIST[[1], []])[1:2, 1:1];
    ?column?
  ------------
   {{1},NULL}
  ```

  This preserves the tree-like semantics of jagged `LIST LIST...`s:
  - You can follow any non-_NULL_ edge to retrieve an appropriate interior slice. As a convenience, if all of `LIST`'s elements are _NULL_, we convert the list itself to a _NULL_ value because it contains no meaningful values.
  - It maintains the expected length and relative ordering of each lower-axis slice.

- This feature is experimental because its semantics don't seem to have much prior art; it's a mixture of `numpy`'s syntax, Postgres' `ARRAY` behavior, and some independent thought.
