---
title: "Release Notes"
description: "What's new in this version of Materialize"
menu: "main"
weight: 500
---

This page details changes between versions of Materialize, including:

- New features
- Major bug fixes
- Substantial API changes

For information about available versions, see our [Versions page](../versions).

{{< comment >}}
# How to write a good release note

Every release note should be phrased in the imperative mood, like a Git
commit message. They should complete the sentence, "This release will...".

Good release notes:

  - [This release will...] Require the `-w` / `--threads` command-line option.
  - [This release will...] In the event of a crash, print the stack trace.

Misbehaved release notes:

  - Users must now specify the `-w` / `-threads` command line option.
  - Materialize will print a stack trace if it crashes.
  - Instead of limiting SQL statements to 8KiB, limit them to 1024KiB instead.

Link to at least one page where users can learn more about either the change or
the area which the change was made. Notes about new features can be concise if
the new feature has comprehensive documentation. Notes about changes to features
must be more detailed, as the note is likely the only documentation of the
change in behavior.

Strive for some variety of verbs. "Support new feature" gets boring as a release
note.

Use relative links (../path/to/doc), not absolute links
(https://materialize.io/docs/path/to/doc).

Wrap your release notes at the 80 character mark.
{{< /comment >}}

<span id="0.2.3"></span>
## 0.2.2 &rarr; 0.2.3 (Unreleased)

- Support [TLS encryption](../cli/#tls-encryption) for SQL and HTTP connections.

- Automatically refresh
  [AWS credentials for Kinesis sources](../sql/create-source/json-kinesis/#with-options)
  when credentials are sourced from an IAM instance or container profile.

<span id="0.2.2"></span>
## 0.2.1 &rarr; 0.2.2

- Introduce an "upsert" envelope for sources that follow the Kafka key–value
  convention for representing inserts, upserts, and deletes. See the [Upsert
  envelope](../sql/create-source/avro-kafka/#upsert-envelope-details) section of
  the `CREATE SOURCE` docs for details.

- Enable connections to Kafka brokers using either
  [SSL authentication](../sql/create-source/avro-kafka/#ssl-encrypted-kafka-details)
  or [Kerberos authentication](../sql/create-source/avro-kafka/#kerberized-kafka-details). This includes support for SSL authentication with
  Confluent Schema Registries.

- Introduce the [`AS OF`](../sql/tail/#as-of) and
  [`WITH SNAPSHOT`](../sql/tail/#with-snapshot) options for `TAIL` to provide
  more control over what data `TAIL` will produce.

- Improve reliability of Kinesis sources by rate-limiting Kinesis API calls
  ([#2807]).

- Improve startup speed for Kafka sources with many partitions by fetching from
  partitions evenly, rather than processing partitions sequentially, one after
  the next ([#2936]).

- Add two [`WITH` options](../sql/create-source/avro-kafka/#with-options)
  to Kafka sources:
  - The `group_id_prefix` option affords some control over the consumer group
    ID Materialize uses when consuming from Kafka.
  - The `statistics_interval_ms` controls how often the underlying Kafka library
    reports statistics to the logs.

- Improve reliability and performance of Kafka sinks with a smarter buffering
  and flushing policy ([#2855]) and a faster Avro encoding implementation
  ([#2907]).

- Support decoding enum ([#2923]) and union ([#2943]) values in Avro-formatted
  sources.

- Produce runtime errors when some numeric operations overflow, rather than
  silently wrapping around ([#2896]).

- Humanize the output of [`SHOW CREATE VIEW`] by avoiding quotes around
  identifiers that do not require them ([#2667]).

- Add the [`generate_series`](../sql/functions/#table) table function ([#2857]).

- Fix several bugs in the query optimizer that could cause crashes or incorrect
  query plans ([#2731], [#2724]).

- Correct output for `LEFT JOIN`s when the same join key appears multiple times
  in the relation on the left-hand side of the join ([#2724]).

- Disallow trailing commas in `SELECT` lists, so that `SELECT a, b, FROM table`
  results in a syntax error outright, rather than parsing as
  `SELECT a, b, "from" AS table`, which would result in a confusing error about
  the unknown column `"from"` ([#2893]).

[#2590]: https://github.com/MaterializeInc/materialize/pull/2590
[#2667]: https://github.com/MaterializeInc/materialize/pull/2667
[#2724]: https://github.com/MaterializeInc/materialize/issues/2724
[#2727]: https://github.com/MaterializeInc/materialize/pull/2727
[#2731]: https://github.com/MaterializeInc/materialize/pull/2731
[#2807]: https://github.com/MaterializeInc/materialize/pull/2807
[#2855]: https://github.com/MaterializeInc/materialize/pull/2855
[#2857]: https://github.com/MaterializeInc/materialize/pull/2857
[#2893]: https://github.com/MaterializeInc/materialize/pull/2893
[#2896]: https://github.com/MaterializeInc/materialize/pull/2896
[#2907]: https://github.com/MaterializeInc/materialize/pull/2907
[#2923]: https://github.com/MaterializeInc/materialize/pull/2923
[#2943]: https://github.com/MaterializeInc/materialize/pull/2943
[#2936]: https://github.com/MaterializeInc/materialize/pull/2936

<span id="0.2.1"></span>
## 0.2.0 &rarr; 0.2.1

- Allow query parameters (`$1`, `$2`, etc) to appear in
  [`EXPLAIN`](../sql/explain) statements.

- Avoid crashing if queries are executed without a value for each parameter in
  the query.

- Support runtime errors in dataflows. Views that encounter an error (e.g.,
  division by zero) while executing will report that error when queried.
  Previously, the error would be silenced, and the erroring expression would be
  replaced with `NULL`.

- Permit filtering the output of several `SHOW` commands with a `WHERE` or
  `LIKE` clause:

  - [SHOW DATABASES](../sql/show-databases)
  - [SHOW INDEXES](../sql/show-index)
  - [SHOW COLUMNS](../sql/show-index)

- Support reading from Kinesis streams with multiple shards. For details, about
  Kinesis sources, see [CREATE SOURCE: JSON over Kinesis](../sql/create-source/json-kinesis).

<span id="v0.2.0"></span>
## 0.1.3 &rarr; v0.2.0

- Require the `-w` / `--threads` command-line option. Consult the [CLI
  documentation](../cli/#worker-threads) to determine the correct value for your
  deployment.

- Introduce the [`--listen-addr`](../cli/#listen-address) command-line option to
  control the address and port that `materialized` binds to.

- Make formatting and parsing for [`real`](../sql/types/float) and
  [`double precision`](../sql/types/float) numbers more
  consistent with PostgreSQL. The strings `NaN`, and `[+-]Infinity` are
  accepted as input, to select the special not-a-number and infinity states,
  respectively,  of floating-point numbers.

- Allow [CSV-formatted sources](../sql/create-source/csv-source/#csv-format-details)
  to include a header row (`CREATE SOURCE ... FORMAT CSV WITH HEADER`).

- Provide the option to name columns in sources (e.g. [`CREATE SOURCE foo
  (col_foo, col_bar)...`](../sql/create-source/csv-source/#creating-a-source-from-a-dynamic-csv)).

- Improve conformance of the Avro parser, enabling support for
  a wider variety of Avro schemas in [Avro sources](../sql/create-source/avro-kafka).

- Introduce [Avro Object Container File (OCF) sinks](../sql/create-sink/#avro-ocf-sinks).

- Make [sink](../sql/create-sink/) output more correct and consistent by
  writing to a new Kafka topic or file on every restart.

- Add the [`jsonb_agg()`](../sql/functions/#aggregate-func) aggregate function.

- Support [casts](../sql/functions/cast/) for `time`->`text`,`time`->`interval`, `interval`->`time`.

- Improve the usability of the [`EXPLAIN` statement](../sql/explain):

  - Change the output format of to make large plans more readable by avoiding
    nesting.

  - Add `EXPLAIN ... FOR VIEW ...` to display the plan for an existing
    view.

  - Add `EXPLAIN <stage> PLAN FOR ...` to display the plan at various
    stages of the planning process.

<span id="v0.1.3"></span>
## 0.1.2 &rarr; 0.1.3

- Support [Amazon Kinesis Data Stream sources](../sql/create-source/kinesis-source/).

- Support the number functions `round(x: N)` and `round(x: N, y: N)`, which
  round `x` to the `y`th digit after the decimal. (Default 0).

- Support addition and subtraction between [`interval`]s.

- Support the [string concatenation operator, `||`](../sql/functions/#string).

- In the event of a crash, print the stack trace to the log file, if logging to
  a file is enabled, as well as the standard error stream.

<span id="v0.1.2"></span>
## 0.1.1 &rarr; 0.1.2

- Change [`SHOW CREATE SOURCE`] to render the full SQL statement used to create
  the source, in the style of [`SHOW CREATE VIEW`], rather than displaying a URL
  that partially describes the source. The URL was a vestigial format used in
  [`CREATE SOURCE`] statements before v0.1.0.

- Raise the maximum SQL statement length from approximately 8KiB to
  approximately 64MiB.

- Support casts from [`text`] to [`date`], [`timestamp`], [`timestamptz`], and
  [`interval`].

- Support the `IF NOT EXISTS` clause in [`CREATE VIEW`] and
  [`CREATE MATERIALIZED VIEW`].

- Attempt to automatically increase the nofile rlimit to acceptable levels, as
  creating multiple Kafka sources can quickly exhaust the default nofile rlimit
  on some platforms.

- Improve CSV parsing speed by 5-6x.

[`CREATE SOURCE`]: ../sql/create-source
[`SHOW CREATE SOURCE`]: ../sql/show-create-source
[`SHOW CREATE VIEW`]: ../sql/show-create-view
[`CREATE MATERIALIZED VIEW`]: ../sql/create-materialized-view
[`CREATE VIEW`]: ../sql/create-view
[`text`]: ../sql/types/text
[`date`]: ../sql/types/date
[`timestamp`]: ../sql/types/timestamp
[`timestamptz`]: ../sql/types/timestamptz
[`interval`]: ../sql/types/interval

<span id="v0.1.1"></span>
## 0.1.0 &rarr; 0.1.1

* Specifying the message name in a Protobuf-formatted source no longer requires
  a leading period.

- **Indexes on sources**: You can now create and drop indexes on sources, which
  lets you automatically store all of a source's data in an index. Previously,
  you would have to create a source, and then create a materialized view that
  selected all of the source's content.

<span id="v0.1.1"></span>
## _NULL_ &rarr; 0.1.0

- [What is Materialize?](../overview/what-is-materialize/)
- [Architecture overview](../overview/architecture/)
