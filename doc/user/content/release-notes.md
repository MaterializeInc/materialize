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

For information about available versions, see our [Versions page](/versions).

{{< comment >}}
# How to write a good release note

Every release note should be phrased in the imperative mood, like a Git
commit message. They should complete the sentence, "This release will...".

Good release notes:

  - [This release will...] Require the `-w` / `--workers` command-line option.
  - [This release will...] In the event of a crash, print the stack trace.

Misbehaved release notes:

  - Users must now specify the `-w` / `-threads` command line option.
  - Materialize will print a stack trace if it crashes.
  - Instead of limiting SQL statements to 8KiB, limit them to 1024KiB instead.

Link to at least one page where users can learn more about either the change or
the area which the change was made. Notes about new features can be concise if
the new feature has comprehensive documentation. Notes about changes to features
must be more detailed, as the note is likely the only documentation of the
change in behavior. Consider linking to a GitHub issue or pull request via the
`gh` shortcode if there is no good section of the documentation to link to.

Strive for some variety of verbs. "Support new feature" gets boring as a release
note.

Use relative links (/path/to/doc), not absolute links
(https://materialize.io/docs/path/to/doc).

Wrap your release notes at the 80 character mark.
{{< /comment >}}

<span id="v0.4.1"></span>
## v0.4.1 (Unreleased)

- Permit broker addresses in [Kafka sources](/sql/create-source/avro-kafka/) and
  [Kafka sinks](/sql/create-sink/) to use IP addresses in addition to hostnames.

- Expose [monitoring metrics](/ops/monitoring) for per-thread CPU usage {{% gh 3733 %}}.

- Ensure correlated subqueries in CASE statement branches that are not taken
  do not trigger errors {{% gh 3736 %}}.

- Reduce memory overhead of the built-in logging views {{% gh 3752 %}}.

- Improve performance of `TopK` operator {{% gh 3758 %}}.

- Handle Snappy-encoded Avro OCF files.
- Support casts from [`boolean`](/sql/types/boolean) to [`int`](/sql/types/int).

<span id="v0.4.0"></span>
## v0.4.0

- Rename the `--threads` command-line option to [`--workers`](/cli/#worker-threads),
  since it controls only the number of dataflow workers that Materialize will
  start, not the total number of threads that Materialize may use. The short
  form of this option, `-w`, remains unchanged.
  **Backwards-incompatible change.**

- Add the `--experimental` command-line option to enable a new [experimental
  mode](/cli/#experimental-mode), which grants access to experimental features
  at the risk of compromising stability and backwards compatibility. Forthcoming
  features that require experimental mode will be marked as such in their
  documentation.

- Support [SASL PLAIN authentication for Kafka sources](/sql/create-source/avro-kafka/#connecting-to-a-kafka-broker-using-sasl-plain-authentication).
  Notably, this allows Materialize to connect to Kafka clusters hosted by
  Confluent Cloud.

- Do not require [Kafka Avro sources](/sql/create-source/avro-kafka/) that use
  `ENVELOPE NONE` or `ENVELOPE DEBEZIUM` to have key schemas whose fields are a
  subset of the value schema {{% gh 3677 %}}.

- Teach Kafka sinks to emit Debezium style [consistency
  metadata](/sql/create-sink/#consistency-metadata) if the new `consistency`
  option is enabled. The consistency metadata is emitted to a Kafka topic
  alongside the data topic; the combination of these two topics is considered
  the Materialize change data capture (CDC) format.

- Introduce the [`AS OF`](/sql/create-sink/#as-of) and
  [`WITH SNAPSHOT`](/sql/create-sink/#with-snapshot-or-without-snapshot) options
  for `CREATE SINK` to provide more control over what data the sink will
  produce.

- Change the default [`TAIL` snapshot behavior](/sql/tail/#with-snapshot-or-without-snapshot)
  from `WITHOUT SNAPSHOT` to `WITH SNAPSHOT`. **Backwards-incompatible change.**

- Actively shut down [Kafka sinks](https://materialize.io/docs/sql/create-sink/#kafka-sinks)
  that encounter an unrecoverable error, rather than attempting to produce data
  until the sink is dropped {{% gh 3419 %}}.

- Improve the performance, stability, and standards compliance of Avro encoding
  and decoding {{% gh 3397 3557 3568 3579 3583 3584 3585 %}}.

- Support [record types](/sql/types/record), which permit the representation of
  nested data in SQL. Avro sources also gain support for decoding nested
  records, which were previously disallowed, into this new SQL record type.

- Allow dropping databases with cross-schema dependencies {{% gh 3558 %}}.

- Avoid crashing if [`date_trunc('week', ...)`](/sql/functions/#time-func) is
  called on a date that is in the first week of a month {{% gh 3651 %}}.

- Ensure the built-in `mz_avro_ocf_sinks`, `mz_catalog_names`, and
  `mz_kafka_sinks` views always reflect the latest state of the system
  {{% gh 3682 %}}. Previously these views could contain stale data that did not
  reflect the results of recent `CREATE` or `DROP` statements.

- Introduce several new SQL statements:

  - [`ALTER RENAME`](/sql/alter-rename) renames an index, sink, source, or view.

  - [`SHOW CREATE INDEX`](/sql/show-create-index/) displays information about
    an index.

  - [`EXPLAIN <statement>`](/sql/explain) is shorthand for
    `EXPLAIN OPTIMIZED PLAN FOR <statement>`.

  - `SHOW TRANSACTION ISOLATION LEVEL` displays a dummy transaction isolation
    level, `serializable`, in order to satisfy various PostgreSQL tools that
    depend upon this statement {{% gh 800 %}}.

- Adjust the semantics of several SQL expressions to match PostgreSQL's
  semantics:

  - Consider `NULL < ANY(...)` to be false and `NULL < ALL (...)` to be true
    when the right-hand side is the empty set {{% gh 3319 %}}.
    **Backwards-incompatible change.**

  - Change the meaning of ordinal references in a `GROUP BY` clause, as in
    `SELECT ... GROUP BY 1`, to refer to columns in the target list, rather than
    columns in the input set of tables {{% gh 3686 %}}.
    **Backwards-incompatible change.**

  - When casting from `numeric` or `float` to `int`, round to the nearest
    integer rather than discarding the fractional component {{% gh 3700 %}}.
    **Backwards-incompatible change.**

  - Allow expressions in `GROUP BY` to refer to output columns, not just input
    columns, to match PostgreSQL. In the case of ambiguity, the input column
    takes precedence {{% gh 1673 %}}.

  - Permit expressions in `ORDER BY` to refer to input columns that are not
    selected for output, as in `SELECT rel.a FROM rel ORDER BY rel.b`
    {{% gh 3645 %}}.

<span id="v0.3.1"></span>
## v0.3.1

- Improve the ingestion speed of Kafka sources with multiple partitions by
  sharding responsibility for each partition across the available worker
  threads {{% gh 3190 %}}.

- Improve JSON decoding performance when casting a `text` column to `json`, as
  in `SELECT text_col::json` {{% gh 3195 %}}.

- Simplify converting non-materialized views into materialized views with
  [`CREATE DEFAULT INDEX ON foo`](/sql/create-index). This creates the same
  [index](/overview/api-components/#indexes) on a view that would have been
  created if you had used [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view).

- Permit control over the timestamp selection logic on a per-Kafka-source basis
  via three new [`WITH` options](https://materialize.io/docs/sql/create-source/avro-kafka/#with-options):
    - `timestamp_frequency_ms`
    - `max_timestamp_batch_size`
    - `topic_metadata_refresh_interval_ms`

- Support assigning aliases for column names when referecing a relation
  in a `SELECT` query, as in:

  ```sql
  SELECT col1_alias, col2_alias FROM rel AS rel_alias (col1_alias, col2_alias)
  ```

- Add the [`abs`](/sql/functions/#numbers-func) function for the
  [`numeric`](/sql/types/numeric/) type.

- Improve the [string function](/sql/functions/#string-func) suite:
  - Add the trim family of functions to trim characters from the start and/or
    end of strings. The new functions are `btrim`, `ltrim`, `rtrim`, and `trim`.
  - Add the SQL standard length functions `char_length`, `octet_length`, and
    `bit_length`.
  - Improve the `length` function's PostgreSQL compatibility by accepting
    `bytea` as the first argument, rather than `text`, when getting the length
    of encoded bytes.

- Enhance compatibility with PostgreSQL string literals:
  - Allow the [`TYPE 'string'` syntax](/sql/functions/cast#signatures) to
    explicitly specify the type of a string literal. This syntax is equivalent
    to `CAST('string' AS TYPE)` and `'string'::TYPE`.
  - Support [escape string literals](/sql/types/text/#escape) of the form
    `E'hello\nworld'`, which permit C-style escapes for several special
    characters.
  - Automatically coerce string literals to the appropriate type, as required
    by their usage in calls to functions and operators {{% gh 481 %}}.

- Produce runtime errors in several new situations:
  - When multiplication operations overflow {{% gh 3354 %}}. Previously
    multiplication overflow would result in silent wraparound.
  - When casting from string to any other data type {{% gh 3156 %}}. Previously
    failed casts would return `NULL`.

- Fix several misplanned queries:
  - Ensure `CASE` statements do not trigger errors from unselected
    branches {{% gh 3395 %}}.
  - Prevent the optimizer from crashing on some queries involving the
    the `date_trunc` function {{% gh 3403 %}}.
  - Handle joins nested with non-default associativity correctly
    {{% gh 3427 %}}.

- Fix several bugs related to negative intervals:
  - Ensure the `EXTRACT` function-like operator returns a negative result when
    its input is negative {{% gh 2800 %}}.
  - Do not distinguish negative and positive zero {{% gh 2812 %}}.

- Expose [monitoring metrics](/monitoring/) for Kafka sinks {{% gh 3336 %}}.

<span id="v0.3.0"></span>
## v0.3.0

- Support [temporary views](/sql/create-view/#temporary-views).

- Improve the reliability and performance of Kafka sources, especially when the
  underlying Kafka topic has many partitions and data is not evenly distributed
  across the partitions.

- Infer primary keys based on the key schema for [Kafka Avro sources that use
  the Debezium envelope](/sql/create-source/avro-kafka/#debezium-envelope-details)
  to facilitate query optimization. This corrects a regression
  in v0.2.2.

  The new [`ignore_source_keys` option](/sql/create-source/avro-kafka/#with-options)
  can be set to `true` to explicitly disable this behavior.

- In [Avro sources that use the Debezium envelope](/sql/create-source/avro-kafka/#debezium-envelope-details),
  automatically filter out duplicate records generated by Debezium's MySQL
  connector.

  This release does not include support for deduplicating records generated by
  other Debezium connectors (e.g., PostgreSQL).

- Automatically refresh
  [AWS credentials for Kinesis sources](/sql/create-source/json-kinesis/#with-options)
  when credentials are sourced from an IAM instance or container profile
  {{% gh 2928 %}}.

- Support [TLS encryption](/cli/#tls-encryption) for SQL and HTTP connections.


- Improve compatibility with the [pg8000](https://pypi.org/project/pg8000/)
  Python driver, and likely other drivers, by including the number of rows
  returned by a `SELECT` statement in the SQL protocol command tag
  {{% gh 2987 %}}.

- Correct plans for `OUTER` joins that appear within subqueries, which could
  previously cause Materialize to crash {{% gh 3048 %}}.

- Prevent a small memory leak when a [TAIL](/sql/tail) command is uncleanly
  terminated {{% gh 2996 %}}.

- Adjust the precedence of several SQL operators to match PostgreSQL
  {{% gh 3087 %}}.

- Add a new command-line option, [`-vv`](/cli/#command-line-flags), that prints
  some build information in addition to the version.

<span id="v0.2.2"></span>
## v0.2.2

- Introduce an "upsert" envelope for sources that follow the Kafka key–value
  convention for representing inserts, upserts, and deletes. See the [Upsert
  envelope](/sql/create-source/avro-kafka/#upsert-envelope-details) section of
  the `CREATE SOURCE` docs for details.

- Enable connections to Kafka brokers using either
  [SSL authentication](/sql/create-source/avro-kafka/#ssl-encrypted-kafka-details)
  or [Kerberos authentication](/sql/create-source/avro-kafka/#kerberized-kafka-details).
  This includes support for SSL authentication with Confluent Schema Registries.

- Introduce the [`AS OF`](/sql/tail/#as-of) and
  [`WITH SNAPSHOT`](/sql/tail/#WITH SNAPSHOT or WITHOUT SNAPSHOT) options for `TAIL` to provide
  more control over what data `TAIL` will produce.

- Improve reliability of Kinesis sources by rate-limiting Kinesis API calls.
  {{% gh 2807 %}}

- Improve startup speed for Kafka sources with many partitions by fetching from
  partitions evenly, rather than processing partitions sequentially, one after
  the next. {{% gh 2936 %}}

- Add two [`WITH` options](/sql/create-source/avro-kafka/#with-options)
  to Kafka sources:
  - The `group_id_prefix` option affords some control over the consumer group
    ID Materialize uses when consuming from Kafka.
  - The `statistics_interval_ms` controls how often the underlying Kafka library
    reports statistics to the logs.

- Improve reliability and performance of Kafka sinks with a smarter buffering
  and flushing policy {{% gh 2855 %}} and a faster Avro encoding implementation
  {{% gh 2907 %}}.

- Support decoding enum {{% gh 2923 %}} and union {{% gh 2943 %}} values in Avro-formatted
  sources.

- Produce runtime errors when some numeric operations overflow, rather than
  silently wrapping around. {{% gh 2896 %}}

- Humanize the output of [`SHOW CREATE VIEW`] by avoiding quotes around
  identifiers that do not require them. {{% gh 2667 %}}

- Add the [`generate_series`](/sql/functions/#table-func) table function. {{% gh 2857 %}}

- Fix several bugs in the query optimizer that could cause crashes or incorrect
  query plans. {{% gh 2731 2724 %}}

- Correct output for `LEFT JOIN`s when the same join key appears multiple times
  in the relation on the left-hand side of the join. {{% gh 2724 %}}

- Disallow trailing commas in `SELECT` lists, so that `SELECT a, b, FROM table`
  results in a syntax error outright, rather than parsing as
  `SELECT a, b, "from" AS table`, which would result in a confusing error about
  the unknown column `"from"`. {{% gh 2893 %}}

<span id="v0.2.1"></span>
## v0.2.1

- Allow query parameters (`$1`, `$2`, etc) to appear in
  [`EXPLAIN`](/sql/explain) statements.

- Avoid crashing if queries are executed without a value for each parameter in
  the query.

- Support runtime errors in dataflows. Views that encounter an error (e.g.,
  division by zero) while executing will report that error when queried.
  Previously, the error would be silenced, and the erroring expression would be
  replaced with `NULL`.

- Permit filtering the output of several `SHOW` commands with a `WHERE` or
  `LIKE` clause:

  - [SHOW DATABASES](/sql/show-databases)
  - [SHOW INDEXES](/sql/show-index)
  - [SHOW COLUMNS](/sql/show-index)

- Support reading from Kinesis streams with multiple shards. For details, about
  Kinesis sources, see [CREATE SOURCE: JSON over Kinesis](/sql/create-source/json-kinesis).

<span id="v0.2.0"></span>
## v0.2.0

- Require the `-w` / `--threads` command-line option. Consult the [CLI
  documentation](/cli/#worker-threads) to determine the correct value for your
  deployment.

- Introduce the [`--listen-addr`](/cli/#listen-address) command-line option to
  control the address and port that `materialized` binds to.

- Make formatting and parsing for [`real`](/sql/types/float) and
  [`double precision`](/sql/types/float) numbers more
  consistent with PostgreSQL. The strings `NaN`, and `[+-]Infinity` are
  accepted as input, to select the special not-a-number and infinity states,
  respectively,  of floating-point numbers.

- Allow [CSV-formatted sources](/sql/create-source/csv-file/#csv-format-details)
  to include a header row (`CREATE SOURCE ... FORMAT CSV WITH HEADER`).

- Provide the option to name columns in sources (e.g. [`CREATE SOURCE foo
  (col_foo, col_bar)...`](/sql/create-source/csv-file/#creating-a-source-from-a-dynamic-csv)).

- Improve conformance of the Avro parser, enabling support for
  a wider variety of Avro schemas in [Avro sources](/sql/create-source/avro-kafka).

- Introduce [Avro Object Container File (OCF) sinks](/sql/create-sink/#avro-ocf-sinks).

- Make [sink](/sql/create-sink/) output more correct and consistent by
  writing to a new Kafka topic or file on every restart.

- Add the [`jsonb_agg()`](/sql/functions/#aggregate-func) aggregate function.

- Support [casts](/sql/functions/cast/) for `time`->`text`,`time`->`interval`, `interval`->`time`.

- Improve the usability of the [`EXPLAIN` statement](/sql/explain):

  - Change the output format of to make large plans more readable by avoiding
    nesting.

  - Add `EXPLAIN ... FOR VIEW ...` to display the plan for an existing
    view.

  - Add `EXPLAIN <stage> PLAN FOR ...` to display the plan at various
    stages of the planning process.

<span id="v0.1.3"></span>
## v0.1.3

- Support [Amazon Kinesis Data Stream sources](/sql/create-source/json-kinesis/).

- Support the number functions `round(x: N)` and `round(x: N, y: N)`, which
  round `x` to the `y`th digit after the decimal. (Default 0).

- Support addition and subtraction between [`interval`]s.

- Support the [string concatenation operator, `||`](/sql/functions/#string).

- In the event of a crash, print the stack trace to the log file, if logging to
  a file is enabled, as well as the standard error stream.

<span id="v0.1.2"></span>
## v0.1.2

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

[`CREATE SOURCE`]: /sql/create-source
[`SHOW CREATE SOURCE`]: /sql/show-create-source
[`SHOW CREATE VIEW`]: /sql/show-create-view
[`CREATE MATERIALIZED VIEW`]: /sql/create-materialized-view
[`CREATE VIEW`]: /sql/create-view
[`text`]: /sql/types/text
[`date`]: /sql/types/date
[`timestamp`]: /sql/types/timestamp
[`timestamptz`]: /sql/types/timestamptz
[`interval`]: /sql/types/interval

<span id="v0.1.1"></span>
## v0.1.1

* Specifying the message name in a Protobuf-formatted source no longer requires
  a leading period.

- **Indexes on sources**: You can now create and drop indexes on sources, which
  lets you automatically store all of a source's data in an index. Previously,
  you would have to create a source, and then create a materialized view that
  selected all of the source's content.

<span id="v0.1.0"></span>
## v0.1.0

- [What is Materialize?](/overview/what-is-materialize/)
- [Architecture overview](/overview/architecture/)
