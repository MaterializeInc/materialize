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
CREATE CONNECTION kafka_conn TO KAFKA (BROKER 'localhost:9092');
CREATE SOURCE test FROM KAFKA CONNECTION kafka_conn (TOPIC 'top') WITH (config_param = 'yes') FORMAT BYTES;
SELECT coalesce(1, NULL, 2);
SELECT CAST (1 AS text); -- note the space after CAST
SELECT (1 + 2) - (7 * 4);
```

and several queries that don't:

```sql
CREATE TABLE t (a varchar (40));
CREATE CONNECTION kafka_conn TO KAFKA (BROKER 'localhost:9092');
CREATE SOURCE test FROM KAFKA CONNECTION kafka_conn )TOPIC 'top') WITH (tail=true);
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

### Error message style
_Portions of this section are copied from the PostgreSQL documentation and subject to the PostgreSQL license, a copy of which is available in the LICENSE file at the root of this repository._

For error message style, we strive to adhere to the [guidelines](https://www.postgresql.org/docs/current/error-style-guide.html) in the PostgreSQL manual, and use a format that consists of a **required** _primary_ message followed by **optional** _detail_ and _hint_ messages.

- **Primary message**: short, factual, and avoids reference to implementation details. Lowercase the first letter and do not end with any punctuation.

- **Detail message**: factual, use if needed to keep the primary message short or to mention implementation details. Use complete sentences, each of which starts with a capitalized letter and ends with a period. Separate sentences with two spaces.

- **Hint message**: suggestions about what to do to fix the problem. This should be actionable, and can provide a shortlink to direct the user to more information. Use complete sentences, each of which starts with a capitalized letter and ends with a period. Separate sentences with two spaces.

```
Primary: could not parse column "bar.foo"
Detail: Column "bar.foo" contains an unsupported type.
Hint: Try using CAST in the CREATE SOURCE statement or excluding this column from replication.
```

To use a shortlink in a Hint, add the shortlink to this [file](https://github.com/MaterializeInc/materialize/blob/3efb2c933e4e6c8e694afb1e794c7d8dae368e7d/ci/deploy/website.sh#L29) in your PR to add/modify the hint message. Be mindful that we are committed to the upkeep of the referenced link.

Some key principles to highlight are:
* Do not put any formatting into message texts, such as newlines.
* Use quotes to delimit file names, user-supplied identifiers, and other variables that might contain words.
* Use double quotes when quoting is appropriate.
* Use past tense if an attempt to do something failed, but could perhaps succeed next time (perhaps after fixing some problem). Use present tense if the failure is certainly permanent.
* When citing the name of an object, state what kind of object it is. Print the type in all lowercase.
* Avoid mentioning called function names. Instead say what the code was trying to do.
* Tricky words to avoid: unable, bad, illegal, unknown.
* Avoid contractions and spell out words in full.
* Word choices to be mindful of: find vs. exists, may vs. can vs. might.

### System catalog style
We adhere to standards for our system catalog relations (tables, views), which includes both the stable `mz_catalog` relations and the unstable `mz_internal` relations.

Modeling standards:
* Normalize the schema. If you’re adding a table that adds detail to rows in an existing table, refer to those rows by ID, and don’t duplicate columns that already exist. E.g., the `mz_kafka_sources` table does not include the name of the source, since that information is available in the `mz_sources` table.
  * Remember, Materialize is good at joins! We can always add syntax sugar via a `SHOW` command to spare users from typing out the joins for common queries.

Naming standards:
* Catalog relation names should be consistent with the user-facing naming and messaging in our docs. The names should not reference internal-only concepts when possible.
* Avoid all but the most common abbreviations. Say `position` instead of `pos`. Say `return` instead of `ret`. Say `definition` instead of `def`.
  * We allow three abbreviations at present: `id`, `oid`, and `ip`.
* Use `kebab-case` for enum values. E.g., the `type` of a Confluent Schema Registry connection is `confluent-schema-registry` and the `type` of a materialized view is `materialized-view`. Only use hyphens to separate multiple words. Don’t introduce hyphens for CamelCased proper nouns. For example, the “AWS PrivateLink” connection is represented as `aws-privatelink`.
* Name timestamp fields with an `_at` suffix, e.g., `occurred_at`.
* Do not name boolean fields with an `is_` prefix. E.g., say `indexed`, not `is_indexed`.
* For relation names, pluralize the final noun, unless the final noun in the name is a collective noun. Most relations in the catalog will be plural, e.g. `mz_sources` and `mz_connections`.
  * Examples of collective final nouns are `_usage`, `_history`, and `_utilization`.
  * A good example of these standards in practice is the set of relations `mz_sources`, `mz_source_statuses`, and `mz_source_status_history`.

### Function Style

We adhere to standards for our SQL functions.

Naming standards:
* If the function exists in PostgreSQL, then use the PostgreSQL function and argument names.
* If the function is specific to Materialize internals, then the name should be prefixed with `mz_`. For example, `mz_now()`. 



## Log message style

We use the [`tracing` crate](https://docs.rs/tracing/latest/tracing/)'s log
macros. Observe the following guidelines:

  * Use sentence fragment style (no initial capitalization and no terminating
    period). Say "beginning operation", not "Beginning operation."

  * Prefer structured fields to string formatting when it may be valuable to
    search by the value of said field, e.g.:

    ```
    fn handle_persist_thingy(shard_id: ShardId) {
        // Preferred.
        info!(shard_id = ?the_shard, "handling persist thingy");
        // Acceptable.
        info!("handling persist thingy for shard {}", shard_id);
    }
    ```

    (We're still working on deploying the log aggregator that will allow for
    searching by structured fields.)

Messages at each level must meet the indicated standard:

* **`ERROR`**: Reports an error that certainly indicates data corruption, data
  loss, unavailability, or an invariant violation that results in undefined
  behavior.

  Use *judiciously*. These errors are reported to Sentry. The goal is to
  eventually have every occurrence of an `error!` log message in production to
  automatically start an incident and page the on-call engineer.

  Consider instead using `panic!` or `unreachable!`. The scenarios in which a
  correctness invariant is violated but it is safe to proceed without crashing
  are quite rare.

  Examples:

  * A referenced collection is not present in the storage controller.
  * A source ingested the deletion of a record that does not exist, causing a
    "negative multiplicity."

* **`WARN`**: Either:

    * Like an `ERROR`, but where there is some uncertainty about the error
      condition.
    * A recoverable error that is still unexpected.

  These warnings are not reported to Sentry and do not require urgent attention
  on our end, but they should be the first events looked at when a problem has
  been identified.

  Any changes to the occurrence of `WARN`-level log events that are noticed
  during release qualification should be investigated before proceeding with the
  release.

  * A cancellation request was received for an unknown peek.
  * A Kafka source cannot connect to any of its brokers.

* **`INFO`**: Reports normal system status changes. Messages at this level are
  likely to be of interest to the support or engineering team during an
  investigation of a problem, but do not otherwise require attention.

  Examples:

  * A process has started listening for incoming connections.
  * A view was created.
  * A view was dropped.

* **`DEBUG`**: Like `INFO`, but for events where the likelihood of usefulness
  is too low for the event to be emitted by default.

  The idea is that when you are debugging a problem in a particular crate or
  module, you can temporarily enable `DEBUG`-level logs for that specific
  crate or module in the affected environment.

  Examples:

  * An HTTP request was routed through a proxy specified by the `http_proxy`
    environment variable.
  * An S3 object downloaded by an S3 source had an invalid `Content-Encoding`
    header that was ignored, but the object was nonetheless decoded
    successfully.

* **`TRACE`**: Like `DEBUG`, but for events that meet an even lower standard of
  relevance or importance.

  Enabling `TRACE` logs can generate multiple gigabytes of log messages per
  hour. Use extreme caution when enabling this log level. You should generally
  only enable this log level when developing locally, and only for a single
  module at a time.

  Examples:

  * A Kafka source consumed a message.
  * A SQL client issued a command.


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

### Test-specific

You should prefer to panic in tests rather than returning an error. Tests that return errors
do not produce useful backtraces and instead just point to the line in libtest that asserts
that the test didn't return an error. Panics will produce useful backtraces that include
the line where the panic occurred. This is especially useful for debugging flaky tests in CI.

[Clippy]: https://github.com/rust-lang/rust-clippy
[rustfmt]: https://github.com/rust-lang/rustfmt
[rust-api]: https://rust-lang.github.io/api-guidelines/
[`ore::task`]: https://github.com/MaterializeInc/materialize/blob/a9f51618c950f3a1c22ce4e6096e7d1d8babe6d5/src/ore/src/task.rs
[`RuntimeExt`]: https://github.com/MaterializeInc/materialize/blob/a9f51618c950f3a1c22ce4e6096e7d1d8babe6d5/src/ore/src/task.rs#L94
[`Handle`]: https://docs.rs/tokio/latest/tokio/runtime/struct.Handle.html
[`Arc<Runtime>`]: https://docs.rs/tokio/latest/tokio/runtime/struct.Runtime.html
[`tokio-console`]: /doc/developer/guide-tokio-console.md
