# Style guide

Following are the coding conventions that Materialize developers should strive
to adhere to.

## SQL style

The PostgreSQL manual adheres to a consistent style for its SQL statements, and
we strive to do the same.

The basic idea is to capitalize keywords, but lowercase identifiers. What
counts as a keyword and what counts as an identifier is often surprising,
especially when the identifier refers to a built-in function or type.

For example, here is a properly formatted `CREATE TABLE` statement:

```sql
CREATE TABLE distributors (
    id integer PRIMARY KEY,
    name varchar(40)
)
```

`CREATE` and `TABLE` are obviously both keywords, and are capitalized as such.
Perhaps surprisingly, `integer` is a type name—an identifier—and therefore is
not capitalized. The same is true for `varchar`. But `PRIMARY` and `KEY` *are*
keywords, and are again capitalized.

The formatting of SQL statements is generally straightforward. For example:

```sql
SELECT
    sum(l_extendedprice * l_discount) AS revenue,
    EXTRACT(DAY FROM l_shipdate)
FROM
    lineitem
WHERE
    l_quantity < 24
    AND l_shipdate >= DATE '1994-01-01'
    AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
    AND l_discount BETWEEN 0.06 - 0.01 AND 0.07
```

A few tokens require special attention.

Aggregate functions, like `sum`, are not capitalized. In general, functions are
never capitalized. `EXTRACT`, however, *is* capitalized, because `EXTRACT` is a
function-like operator that has special syntax—notice how it its argument is
`DAY FROM l_shipdate`, where `DAY` and `FROM` are keywords. If it were a
normal function, it would be formatted as `extract('day', l_shipdate)`.

Type constructors like `DATE '1994-01-01` and `INTERVAL '1' YEAR` *do*
capitalize the type name, because in this case the type name *is* a keyword.
These date-time related keywords are strange corner of SQL's syntax. There
is no `INTEGER '7'` syntax, for example.

The keywords `TRUE`, `FALSE`, and `NULL` are always capitalized, even though
they are often rendered in output as lowercase.

Function calls, including type constructors that take arguments, should not have
a space between the name of the function and the open parenthesis, but
keywords followed by an open parenthesis should have a space in between.
Symbolic operators, like `=` and `+`, should always be surrounded by spaces,
and commas should always be followed by a space. Following are several examples
of queries that obey the spacing rules

```sql
CREATE TABLE t (a varchar(40));
CREATE SOURCE test FROM KAFKA BROKER 'localhost:9092' TOPIC 'top' WITH (config_param = 'yes') FORMAT BYTES;
SELECT coalesce(1, NULL, 2);
SELECT CAST (1 AS text); -- note the space after CAST
SELECT (1 + 2) - (7 * 4);
```

and several queries that don't:

```sql
CREATE TABLE t (a varchar (40));
CREATE SOURCE test FROM KAFKA BROKER 'localhost:9092' TOPIC 'top' WITH(tail=true);
SELECT coalesce (1, NULL,2);
SELECT CAST(1 AS text);
SELECT 1+2;
```

There are no rules about how to break a SQL query across multiple lines, and how
to indent the resulting pieces, so just do whatever you think looks best.

These rules are admittedly somewhat obtuse, and even the PostgreSQL manual is
not entirely consistent on the finer points, so don't worry about being exact.
But please at least capitalize the obvious keywords, like `SELECT`, `INSERT`,
`FROM`, `WHERE`, `AS`, `AND`, etc.

## Rust style

[Clippy] and [rustfmt] are enforced via CI, and they are quite opinionated on
many matters of style.

Consider also familiarizing yourself with the [Rust API guidelines][rust-api],
which summarize many lessons learned by the Rust team as they designed the
standard library. Not all of these guidelines generalize well to application
code, so use your judgement.<sup>1</sup>

<small><sup>1</sup> For example, the `C-EXAMPLE` guideline suggests that
all public items (modules, traits, structs, enums, functions, methods,
macros, and type definitions) should have rustdoc examples. This is a great
standard for libraries, but total overkill for a fast-moving startup like
Materialize.</small>

### Tokio-specific

You should use the `spawn` and `spawn_blocking` functions in the [`ore::task`]
module as opposed to the native. There is also a [`RuntimeExt`] trait that adds
`spawn_named` and `spawn_blocking_named` to [`Arc<Runtime>`] and [`Handle`] from `tokio`.
They are named different as to avoid clashing with inherent methods.

These functions require a closure that produces a _name_ for the task, to improve the use of
[`tokio-console`]

[Clippy]: https://github.com/rust-lang/rust-clippy
[rustfmt]: https://github.com/rust-lang/rustfmt
[rust-api]: https://rust-lang.github.io/api-guidelines/
[`ore::task`]: https://github.com/MaterializeInc/materialize/blob/a9f51618c950f3a1c22ce4e6096e7d1d8babe6d5/src/ore/src/task.rs
[`RuntimeExt`]: https://github.com/MaterializeInc/materialize/blob/a9f51618c950f3a1c22ce4e6096e7d1d8babe6d5/src/ore/src/task.rs#L94
[`Handle`]: https://docs.rs/tokio/latest/tokio/runtime/struct.Handle.html
[`Arc<Runtime>`]: https://docs.rs/tokio/latest/tokio/runtime/struct.Runtime.html
[`tokio-console`]: /doc/developer/guide-tokio-console.md
