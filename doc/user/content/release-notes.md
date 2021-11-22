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
# What changes require a release note?

Any behavior change to a stable, user-visible API requires a release note.
Roughly speaking, Materialize's stable APIs are:

  * The syntax and semantics of all documented SQL statements.
  * The observable behavior of any source or sink.
  * The behavior of all documented command-line flags.

For details, see the [backwards compatibility policy](/versions/#Backwards-compatibility).

Notably, changes to experimental or unstable APIs should *not* have release
notes. The point of having experimental and unstable APIs is to decrease the
engineering burden when those features are changed. Instead, write a release
note introducing the feature for the release in which the feature is
de-experimentalized.

Examples of changes that require release notes:

  * The addition of a new, documented SQL function.
  * The stabilization of a new source type.
  * A bug fix that fixes a panic in any component.
  * A bug fix that changes the output format of a particular data type in a
    sink.

Examples of changes that do not require release notes:

  * **An improvement to the build system.** The build system is not user
    visible.
  * **The addition of a feature to testdrive.** Similarly, our test frameworks
    are not user visible.
  * **An upgrade of an internal Rust dependency.** Most dependency upgrades
    do not result in user-visible changes. (If they do, document the change
    itself, not the fact that the dependency was upgraded. Users care about
    the visible behavior of Materialize, not its implementation!)

Performance improvements are a borderline case. A small performance improvement
does not need a release note, but a large performance improvement may warrant
one if it results in a noticeable improvement for a large class of users or
unlocks new use cases for Materialize. Examples of performance improvements
that warrant a release note include:

  * Improving the speed of Avro decoding by 2x
  * Converting an O(n<sup>2</sup>) algorithm in the optimizer to an O(n)
    algorithm so that queries with several dozen `UNION` operations can be
    planned in a reasonable amount of time

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
(https://materialize.com/docs/path/to/doc).

Wrap your release notes at the 80 character mark.

## Internal note order

Put breaking changes before other release notes, and mark them with
`**Breaking change.**` at the start.

List new features before bug fixes.

{{< /comment >}}

{{% version-header v0.10.1 %}}

- Support `generate_series` for [`timestamp`] data.

{{% version-header v0.10.0 %}}

- Allow ingesting avro schemas whose top node is not a record type.

{{% version-header v0.9.13 %}}

- **Breaking change**: Disallow creating views (materialized or not) with
  columns of the same name. This change brings views' structure into closer
  alignment with tables.

  When creating views whose `SELECT` statements return multiple columns with the
  same identifier, you can use the optional column renaming syntax to provide
  unique identifiers for all columns. For example:

  ```sql
  CREATE MATERIALIZED VIEW m (col_a, col_b) AS
    SELECT a AS col, b AS col FROM t;
  ```

  For more details, see [`CREATE MATERIALIZED VIEW`] and [`CREATE VIEW`].

- Add support for `LIST(<subquery>)` constructor.

- Fix a crash or incorrect results when a join consumes data from a distinct
  operation. {{% gh 9027 %}}

- Support protobuf sources that contain imported messages and enums.

- Correctly handle protobuf messages where the value is omitted instead of dropping them.

- Disallow calls to functions with using `DISTINCT *` as their arguments, e.g.
  `COUNT(DISTINCT *)`.

- Disallow `DISTINCT` expressions on 0-column objects, such as tables with no
  columns.

{{% version-header v0.9.12 %}}

- **Breaking change**: Disallow ambiguous table references in queries. For
  example:

  ```sql
  SELECT * FROM a, a;
  SELECT * FROM a, b AS a;
  SELECT * FROM a, generate_series(1,3) AS a;
  ```

  These queries previously worked, but will now throw the error:

  ```
  table name "a" specified more than once
  ```

  However, you can always work around this limitation using aliases, e.g.

  ```sql
  SELECT * FROM a, a AS b;
  ```

  {{% gh 4756 %}}

- Deduplicate columns in [arrangements](/overview/arrangements) that are
  shared between keys and values. This can result in memory savings of up to a
  factor of two for arrangements indexed by the whole row.

- Add the [`date_bin`](/sql/functions/date-bin) function, which is similar to
  `date_trunc` but supports "truncating" to arbitrary intervals.

- Add support for the CSV format in [`COPY FROM`].

- Fix incorrect results for a certain class of degenerate join queries.

- Fix a bug in `pg_catalog.pg_attribute` that would incorrectly omit
  certain rows based on the underlying column type.

- Add the `type_oid` column to [`mz_columns`](/sql/system-catalog#mz_columns).

- When using [`COPY FROM`], allow extra data after the end of copy marker
  (`\.`), but discard it. Previously, MZ would error in this case.

- Replace the `mz_kafka_broker_rtt` and `mz_kafka_consumer_partitions`
  system catalog tables with a new table,
  [`mz_kafka_source_statistics`](/sql/system-catalog#mz_kafka_source_statistics),
  containing raw statistics from the underlying librdkafka library.

- Fix a bug that caused a panic when using a query containing
  `STRING_AGG`, `JSON_AGG`, `JSON_OBJECT_AGG`, `LIST_AGG`, or `ARRAY_AGG`
  on data sets containing exactly one record
  when that condition is known at optimization time.

- Support the `row_number` window function.

- Support PgJDBC's `getColumns()` API.

{{% version-header v0.9.11 %}}

- Disallow `UPDATE` and `DELETE` operations on tables when boot in
  `--disable-user-indexes` mode.

- Support the `READ ONLY` transaction mode.

- Support `SET` in transactions, as well as `SET LOCAL`. This unblocks
a problem with PostgreSQL JDBC 42.3.0.

{{% version-header v0.9.10 %}}

- Evaluate TopK operators on constant inputs at query compile time.

- Add the [`session_user`](/sql/functions/#system-information-func) system
  information function.

{{% version-header v0.9.9 %}}

- **Breaking change.** Fix a bug that inadvertently let users create `char
  list` columns and custom types. This type is not meant to be supported.
- Beta support for [Redpanda sources](/third-party/redpanda/).

- Let users express `JOIN`-like `DELETE`s with `DELETE...USING`.

- Optimize some `(table).field1.field2` expressions to only generate the
  columns from `table` that are accessed subsequently. This avoids performance
  issues when extracting a single field from a table expression with several
  columns, for example records generated from [`ROW`](/sql/types/record).
  {{% gh 8596 %}}

- Fix a bug that inadvertently let users create an [`array`] with elements of
  type [`list`] or [`map`], which crashes Materialize. {{% gh 8672 %}}

- Format dates before AD 1 with the BC notation instead of using negative dates.

- Fix some sources of crashes and incorrect results from optimizing queries
  involving constants {{% gh 8713 8717 %}}.

- Support alternative `SUBSTRING(<string> [FROM <int>]? [FOR <int>]?)` syntax.

{{% version-header v0.9.8 %}}

- Throw errors on floating point arithmetic overflow and underflow.

{{% version-header v0.9.7 %}}

- Support the `IS TRUE`, `IS FALSE`, `IS UNKNOWN` operators (and their `NOT`
  variations). {{% gh 8455 %}}
- Add support for retention settings on Kafka sinks.

- Support explicit `DROP DATABASE ... (CASCADE | RESTRICT)` statements.  The
  default behavior remains CASCADE.

- Fix a bug that prevented some users from creating Protobuf-formatted
  sources. {{% gh 8528 %}}

{{% version-header v0.9.6 %}}

- Correctly handle TOASTed columns when using PostgreSQL sources. {{% gh 8371 %}}

- Return control of canceled sessions (`ctrl + c`) while `SELECT` statements
  await results. Previously, this could cause the session to never terminate.

- Fix a bug that could prevent Materialize from booting when importing views
  into dataflows whose indexes had not yet been converted to dataflows
  themselves. {{% gh 8021 %}}

{{% version-header v0.9.5 %}}

- Accept case insensitive timezones to be compatible with PostgreSQL.

- Add support for [bitwise operators on integers](/sql/functions/#numbers).

- Persist the `mz_metrics` and `mz_metric_histogram` system tables and rehydrate
  the previous contents on restart. This is a small test of the system that will
  power upcoming persistence features. Users are free to opt out of this test
  by starting `materialized` with the `--disable-persistent-system-tables-test` flag.

{{% version-header v0.9.4 %}}

- Improve the performance of
  [built-in sources and views](/cli/#introspection-sources) in the
  [system catalog](/sql/system-catalog), which should result in lower latency
  spikes and increased throughput when issuing many small queries, and reduce
  overall memory consumption. Additionally, the content of the views is now
  consistent at the introspection interval boundaries. Prior to this release,
  some views would reveal more details about ephemeral dataflows and operators.

- Fix a bug that caused a panic when computing the `max` of `int2` values.

- Ignore the trailing newline character of POSIX compliant files instead of
  decoding it as an empty byte row. {{% gh 8142 %}}

- Support `ORDER BY` in [aggregate functions](/sql/functions/#aggregate-func).

- When issuing `COMMIT` or `ROLLBACK` commands outside of an explicit
  transaction, always return a warning. Previously, the warning could be suppressed.

- Fix a bug in the `CREATE SINK` syntax by updating the optional `CONSISTENCY`
  clause.

{{% version-header v0.9.3 %}}

- Fix a bug that prevented creating Avro sinks on old versions of Confluent Platform

- Fix a bug that prevented upgrading to 0.9.2 if the catalog referenced CSV file sources with headers.

- Support the `USING CONFLUENT SCHEMA REGISTRY` schema option for
  Protobuf-formatted sources.

{{% version-header v0.9.2 %}}

- The metrics scraping interval to populate
  the [`mz_metrics`](/sql/system-catalog#mz_metrics) table and its variants is
  now independent of the introspection interval. It is controlled by the
  flag [--metrics-scraping-interval](/cli/#prometheus-metrics).

- Allow users to specify the names of columns that must be present in CSV
  objects with headers {{% gh 7507 %}}, and support CSV with headers in S3. {{%
  gh 7913 %}}

- Add the [`ALTER INDEX <name> SET ENABLED` syntax](/sql/alter-index) to
  aid troubleshooting and recovery of Materialize instances. {{% gh 8079 %}}

- Improve PostgreSQL catalog compatibility. {{% gh 7962 %}} {{% gh 8032 %}}

{{% version-header v0.9.1 %}}

- Change the type of the [`mz_metrics`](/sql/system-catalog#mz_metrics).`time`
  column from [`timestamp`] to [`timestamp with time zone`] to better reflect
  that the timestamp is in UTC.

- Add the `array_agg` function.

- Add the `list_agg` function.

- Add the [`string_agg`](/sql/functions/string_agg) function.

- Add the [`generate_series(start, stop, step)`](/sql/functions/#table-func) table function. {{% gh 7953 %}}

{{% version-header v0.9.0 %}}

- **Breaking change.** Reject [Protobuf sources] whose schemas contain
  unsigned integer types (`uint32`, `uint64`, `fixed32`, and `fixed64`).
  Materialize previously converted these types to
  [`numeric`](/sql/types/numeric).

  A future version of Materialize is likely to support unsigned integers
  natively, at which point the aforementioned Protobuf types will be converted
  to the appropriate Materialize types.

- **Breaking change.** The `HTTP_PROXY` variable is no longer respected. Use
  `http_proxy` instead.

- Respect the [`no_proxy` environment variable](/cli/#http-proxies) to exclude
  certain hosts from the configured HTTP/HTTPS proxy, if any.

- Add [`reuse_topic`](/sql/create-sink/#enabling-topic-reuse-after-restart-exactly-once-sinks) as
  a beta feature for Kafka Sinks. This allows re-using the output topic across
  restarts of Materialize.

- Add support for JSON-encoded Kafka sinks.

{{% version-header v0.8.3 %}}

- The `MZ_LOG` environment variable is no longer recognized. Setting the log
  level can be done using the `--log-filter` command line parameter or the
  `MZ_LOG_FILTER` environment variable.

- Refactor the [`numeric`](/sql/types/numeric) type's backing implementation.
  With this change comes more PostgreSQL-like semantics for unscaled values, as
  well as bug fixes. {{% gh 7312 %}}

- Add support for including Kafka keys in dataflows via new `INCLUDE KEY`
  syntax. {{% gh 6661 %}}

- The `FORMAT` argument to `UPSERT` to specify Kafka key formats has been
  deprecated, use the new `KEY FORMAT .. VALUE FORMAT ..` syntax
  ([documentation](/sql/create-source/avro-kafka)). The `UPSERT FORMAT ..`
  syntax will be removed in a future release.

- Add `WITH` options to read from environment variables for SSL and SASL
  passwords. {{% gh 7467 %}}

{{% version-header v0.8.2 %}}

- Stabilized [postgres sources](/sql/create-source/postgres) (no longer require
  `--experimental`)

- **Breaking change.** `HOST` keyword when creating
  [postgres sources](/sql/create-source/postgres/#syntax) has been renamed to
  `CONNECTION`.

- Record the initial high watermark offset on the broker for Kafka sources. This
  enables clients to observe the progress of initial data loading. The table
  `mz_kafka_consumer_partitions` has an additional column `initial_high_offset`
  containing the first reported `hi_offset` from the broker for each partition.

- Add `left` to the [string function](/sql/functions#string-func) suite.

{{% version-header v0.8.1 %}}

- Add [timelines](/sql/timelines) to all sources to prevent
  joining data whose time is not comparable. This only affects new
  [CDC](/connect/materialize-cdc) and Debezium consistency topic sources
  by default.

- Add the [`isolation_level`](/sql/create-source/avro-kafka/#with-options)
  `WITH` option to Kafka sources to allow changing read behavior of
  transactionally written messages.

- Add the [`kafka_time_offset`](/sql/create-source/avro-kafka/#with-options)
  `WITH` option for Kafka sources, which allows to set `start_offset` based on
  Kafka timestamps.

- Add the [`timestamp_frequency_ms`] `WITH` option to Kinesis, S3, and file sources.

- **Breaking change.** The `timezone(String, Time)` function can no
  longer be used in views.

- Debezium sinks
  emit [`collection_data`](/sql/create-sink/#consistency-metadata) attributes in
  their consistency topic.

- **Breaking change.** Renamed the `timestamp`, `diff`, and `progressed`
  columns in [`TAIL`](/sql/tail) to `mz_timestamp`, `mz_diff`, and
  `mz_progressed`.

- Add the [`current_role`](/sql/functions/#system-information-func) system
  information function.

- Support manually declaring a [`(non-enforced) primary key`](/sql/create-source/avro-kafka/#key-constraint-details)
  on sources.

- S3 sources retry failed requests to list buckets and download objects.

{{% version-header v0.8.0 %}}

- Add the [`COPY FROM`] statement, which allows populating a
  table via the PostgreSQL [`COPY` protocol][pg-copy].

- Stabilize the [`ARRAY`](/sql/types/array/#construction) constructor.

- Support casting single-dimensional [arrays](/sql/types/array/) from [`text`].

- Support the `#>` and `#>>` [`jsonb`](/sql/types/jsonb/) operators.

- **Breaking change.** Sort `NULL`s last, to match the default sort order in
  PostgreSQL.

- **Breaking change.** Rename `consistency` parameter to `consistency_topic`
  for both Kafka sources and sinks. Additionally, change `consistency_topic` on
  sinks to be a string that specifies a topic name instead of a boolean. This
  harmonizes the parameter behavior between sources and sinks.

{{% version-header v0.7.3 %}}

- Add the [`pow`](/sql/functions/#numbers-func) function as an alias for the
  [`power`](/sql/functions/#numbers-func) function.

- Add a new metric, `mz_log_message_total` that counts the number of log
  messages emitted per severity.

- Return the number of days between two dates (an integer) when subtracting one
  date from the other. Previously, the interval between the two dates would be
  returned. The new behavior matches the behavior in PostgreSQL.

- **Breaking change.** Change the default for the `enable_auto_commit` option
  on [Kafka sources](/sql/create-source/avro-kafka) to `false`.

- Support [equality operators](/sql/functions/#boolean) on
  [array data](/sql/types/array).

- Stabilized temporal filters (no longer require `--experimental`)

{{% version-header v0.7.2 %}}

- Introduce the concept of [volatility](/overview/volatility) to describe
  sources that do not provide reliability guarantees that
  Materialize relies on. The new volatility information is surfaced via
  [`SHOW SOURCES`](/sql/show-sources), [`SHOW VIEWS`](/sql/show-views),
  and [`SHOW SINKS`](/sql/show-sinks).

- Add [PubNub sources](/sql/create-source/text-pubnub).

- Add [`S3` sources](/sql/create-source/text-s3).

- Add a [`--log-filter` command-line option](/cli/#logging) and a
  `MZ_LOG_FILTER` environment variable that control which log messages to emit.

  This behavior was previously available via the undocumented `MZ_LOG`
  environment variable, which will be removed in a future release.

- Record Kafka Consumer metrics in the `mz_kafka_consumer_partitions` system
  table. Enabled by default for all Kafka sources.

- Add the [`jsonb_object_agg`](/sql/functions/jsonb_object_agg) function to
  aggregate rows into a JSON object.

- Permit the [`jsonb`](/sql/types/jsonb) type to store all 64-bit integers
  {{% gh 5919 %}}.
  Previously integers in the following ranges were rejected:

    * [-2<sup>64</sup>, -(2^<sup>53</sup>-1)]
    * [2<sup>53</sup> - 1, 2^<sup>64</sup>-1].

- Add the [`pg_postmaster_start_time`](/sql/functions#postgresql-compatibility-func)
  function, which reports the time at which the server started.

- Add the [`mz_workers`](/sql/functions#postgresql-compatibility-func)
  function, which reports the number of workers in use by the server.

- Add the [`mz_uptime`](/sql/functions#system-information-func)
  function, which reports the duration for which the server has been running.

- Add the [`repeat`](/sql/functions#string-func) function, which repeats a
  string N times.

- Avoid panicking when planning SQL queries of the form
  `SELECT DISTINCT ... ORDER BY <expr>` where `expr` is not a simple column
  reference {{% gh 6021 %}}.

- Support Kafka log compaction on Debezium topics via the [`DEBEZIUM
  UPSERT`](/sql/create-source/avro-kafka/#debezium-envelope-details) source envelope.

{{% version-header v0.7.1 %}}

- **Breaking change.** Change the default
  [`--logical-compaction-window`](/cli/#compaction-window) from 60 seconds to
  1 millisecond.

- **Breaking change.** Remove `CREATE SINK ... AS OF`, which did not have
  sensible behavior after Materialize restarted. The intent is to reintroduce
  this feature with a more formal model of `AS OF` timestamps. {{% gh 3467 %}}

- Add the [`cbrt` function](/sql/functions/#numbers-func) for computing the
  cube root of a [`double precision`](/sql/types/float).

  Thanks to external contributor [@andrioni](https://github.com/andrioni).

- Add the [`encode` and `decode` functions](/sql/functions/encode/) to convert
  binary data to and from several textual representations.

  Thanks to external contributor [@Posnet](https://github.com/Posnet).

- Add many of the basic
  [trigonometric functions](/sql/functions/#trigonometric-func).

  Thanks again to external contributor [@andrioni](https://github.com/andrioni).

- Add [`DROP TYPE`](/sql/drop-type) and [`SHOW TYPES`](/sql/show-types) commands.

- Multipartition Kafka sinks with consistency enabled will create single-partition
  consistency topics.

- Kafka sinks are now written via an idempotent producer to avoid duplicate or out
  of order messages.

- **Breaking change.** Change the behavior of the
  [`round` function](/sql/functions/#numbers-func) when applied to a `real` or
  `double precision` argument to round ties to the nearest even number,
  rather than away from zero. When applied to `numeric`, ties are rounded away
  from zero, as before.

  The new behavior matches PostgreSQL.

- Restore the `-D` command-line option as the short form of the
  [`--data-directory`](/cli/#data-directory) option.

- Allow setting [index parameters](/sql/alter-index/#available-parameters) when
  creating an index via the new `WITH` clause to [`CREATE INDEX`]. In older
  versions, setting these parameters required a separate call to [`ALTER
  INDEX`](/sql/alter-index).

- Fix a bug that prevented upgrading deployments from v0.6.1 or earlier to v0.7.0 if they
  contained:
  -  Views whose embdedded queries contain functions whose arguments are functions {{% gh 5802 %}}.
  -  Sinks using `WITH SNAPSHOT AS OF` {{% gh 5808 %}}.

- Reduce memory usage and increase processing speed in materialized views
  involving sources with the "upsert" envelope. {{% gh 5509 %}}.

  Users of the [memory usage visualization](/ops/monitoring#memory-usage-visualization)
  will see that the operator "UpsertArrange" has changed to "Upsert", and that
  the "Upsert" operator no longer shows any records. Actually, the "Upsert"
  operator still has a memory footprint proportional to the number of unique
  keys in the source.

- Add the basic exponentiation, power and [logarithm functions](/sql/functions/#numbers-func).

- Add `position` to the [string function](/sql/functions#string-func) suite.

- Add `right` to the [string function](/sql/functions#string-func) suite.

{{% version-header v0.7.0 %}}

- **Known issue.** You cannot upgrade deployments created with versions v0.6.1 or
  earlier to v0.7.0 if they contain:

  -  Views whose embdedded queries contain functions whose arguments are functions {{% gh 5802 %}}.
  -  Sinks using `WITH SNAPSHOT AS OF...` {{% gh 5808 %}}.

  If you encounter this issue, you can:

  - Use a previous version of Materialize to drop the view or sink before upgrading.
  - Skip upgrading to v0.7.0, and instead upgrade to v0.7.1 which contains fixes
    for these bugs.

  The next release (v0.7.1) contains fixes for these bugs.

- **Known issue.** The `-D` command-line option, shorthand for the
  `--data-directory` option, was inadvertently removed.

  It will be restored in the next release.

- **Breaking change.** Require a valid user name when [connecting to
  Materialize](/connect/cli#connection-details). Previously, Materialize did not
  support the concept of [roles](/sql/create-role), so it accepted all user
  names.

  Materialize instances have a user named `materialize` installed, unless you
  drop this user with [`DROP USER`](/sql/drop-user). You can add additional
  users with [`CREATE ROLE`](/sql/create-role).

- Allow setting most [command-line flags](/cli#command-line-flags) via
  environment variables.

- Fix a bug that would cause `DROP` statements targeting multiple objects to fail
  when those objects had dependent objects in common {{% gh 5316 %}}.

- Prevent a bug that would allow `CREATE OR REPLACE` statements to create dependencies
  on objects that were about to be dropped {{% gh 5272 %}}.

- Remove deprecated `MZ_THREADS` alias for `MZ_WORKERS`.

- Support equality operations on `uuid` data, which enables joins on `uuid`
  columns {{% gh 5540 %}}.
- Add the [`current_user`](/sql/functions/#system-information-func) system
  information function.

- Add the [`CREATE ROLE`](/sql/create-role),
  [`CREATE USER`](/sql/create-user), [`DROP ROLE`](/sql/drop-role), and
  [`DROP USER`](/sql/drop-user) statements to manage roles in a Materialize
  instance. These roles do not yet serve any purpose, but they will enable
  authentication in a later release.

- Functions can now be resolved as schema-qualified objects, e.g. `SELECT pg_catalog.abs(-1);`.

- Support [multi-partition](/sql/create-sink/#with-options) Kafka sinks {{% gh 5537 %}}.

- Support [gzip-compressed](/sql/create-source/text-file/#compression) file sources {{% gh 5392 %}}.

{{% version-header v0.6.1 %}}

- **Backwards-incompatible change.** Validate `WITH` clauses in [`CREATE
  SOURCE`](/sql/create-source) and [`CREATE SINK`](/sql/create-sink) statements.
  Previously Materialize would ignore any invalid options in these statement's
  `WITH` clauses.

  Upgrading to v0.6.1 will therefore fail if any of the sources or sinks within
  have invalid `WITH` options. If this occurs, drop these invalid sources or
  sinks using v0.6.0 and recreate them with valid `WITH` options.

- **Backwards-incompatible change.** Change the default value of the `timeout`
  option to [`FETCH`](/sql/fetch) from `0s` to `None`. The old default caused
  `FETCH` to return immediately even if no rows were available. The new default
  causes `FETCH` to wait for at least one row to be available.

  To maintain the old behavior, explicitly set the timeout to `0s`, as in:

  ```sql
  FETCH ... WITH (timeout = '0s')
  ```

- **Backwards-incompatible change.** Consider the following keywords to be fully
  reserved in SQL statements: `WITH`, `SELECT`, `WHERE`, `GROUP`, `HAVING`,
  `ORDER`, `LIMIT`, `OFFSET`, `FETCH`, `OPTION`, `UNION`, `EXCEPT`, `INTERSECT`.
  Previously only the `FROM` keyword was considered fully reserved.

  You can no longer use these keywords as bare identifiers anywhere in a SQL
  statement, except following an `AS` keyword in a table or column alias. They
  can continue to be used as identifiers if escaped. See the [Keyword
  collision](/sql/identifiers#keyword-collision) documentation for details.

- **Backwards-incompatible change.** Change the return type of
  [`sum`](/sql/functions/#aggregate-func) over [`bigint`](/sql/types/integer)s
  from `bigint` to [`numeric`](/sql/types/numeric). This avoids the possibility
  of overflow when summing many large numbers {{% gh 5218 %}}.

  We expect the breakage from this change to be minimal, as the semantics
  of `bigint` and `numeric` are nearly identical.

- Speed up parsing of [`real`](/sql/types/float) and
  [`numeric`](/sql/types/numeric) values by approximately 2x and 100x,
  respectively {{% gh 5341 5343 %}}.

- Ensure the first batch of updates in a [source](/sql/create-source) without
  consistency information is stamped with the current wall clock time, rather
  than timestamp `1` {{% gh 5201 %}}.

- When Materialize consumes a message from a [Kafka source](/sql/create-source/avro-kafka),
  commit that message's offset back to Kafka {{% gh 5324 %}}. This allows
  Kafka-related tools to monitor Materialize's consumer lag.

- Add the [`SHOW OBJECTS`](/sql/show-objects) SQL statement to display all
  objects in a database, regardless of their type.

- Improve the PostgreSQL compatibility of several date and time-related
  features:

  - Correct `date_trunc`'s rounding behavior when truncating by
    decade, century, or millenium {{% gh 5056 %}}.

    Thanks to external contributor [@zRedShift](https://github.com/zRedShift).

  - Allow specifying units of `microseconds`, `milliseconds`, `month`,
    `quarter`, `decade`, `century`, or `millenium` when applying the `EXTRACT`
    function to an [`interval`](/sql/types/interval) {{% gh 5107 %}}. Previously
    these units were only supported with the [`timestamp`](/sql/types/timestamp)
    and [`timestamptz`](/sql/types/timestamptz) types.

    Thanks again to external contributor
    [@zRedShift](https://github.com/zRedShift).

  - Support multiplying and dividing [`interval`](/sql/types/interval)s by
    numbers {{% gh 5107 %}}.

    Thanks once more to external contributor
    [@zRedShift](https://github.com/zRedShift).

  - Handle parsing [`timestamp`](/sql/types/timestamp) and [`timestamptz`](/sql/types/timestamptz)
    from additional compact formats like `700203` {{% gh 4889 %}}.

  - Support conversion of [`timestamp`](/sql/types/timestamp) and [`timestamptz`](/sql/types/timestamptz) to other time zones with [`AT TIME ZONE`](/sql/functions/#date-and-time-func) and [`timezone`](/sql/functions/#date-and-time-func) functions.

- Add the `upper` and `lower` [string functions](/sql/functions#string-func),
  which convert any alphabetic characters in a string to uppercase and
  lowercase, respectively.

- Permit specifying `ALL` as a row count to [`FETCH`](/sql/fetch) to indicate
  that there is no limit on the number of rows you wish to fetch.

- Support the `ISNULL` operator as an alias for the `IS NULL` operator, which
  tests whether its argument is `NULL` {{% gh 5048 %}}.

- Support the [`ILIKE` operator](/sql/functions#boolean), which is the
  case-insensitive version of the [`LIKE` operator](/sql/functions#boolean) for
  pattern matching on a string.

- Permit the `USING` clause of a [join](/sql/join) to reference columns with
  different types on the left and right-hand side of the join if there is
  an [implicit cast](/sql/types#casts) between the types {{% gh 5276 %}}.

- Use SQL standard type names in error messages, rather than Materialize's
  internal type names {{% gh 5175 %}}.

- Fix two bugs involving [common-table expressions (CTEs)](/sql/select#common-table-expressions-ctes):

  - Allow CTEs in `CREATE VIEW` {{% gh 5111 %}}.

  - Allow reuse of CTE names in nested subqueries {{% gh 5222 %}}. Reuse of
    CTE names within a given query is still prohibited.

- Fix a bug that caused incorrect results when multiple aggregations of a
  certain type appeared in the same `SELECT` query {{% gh 5304 %}}.

- Add the advanced [`--timely-progress-mode` and `--differential-idle-merge-effort` command-line arguments](/cli/#dataflow-tuning) to tune dataflow performance. These arguments replace existing undocumented environment variables.

{{% version-header v0.6.0 %}}

- Support specifying default values for table columns via the new
  [`DEFAULT` column option](/sql/create-table#syntax) in `CREATE TABLE`.
  Thanks to external contributor [@petrosagg](https://github.com/petrosagg).

- Add a `timeout` option to [`FETCH`](/sql/fetch/) to facilitate using `FETCH`
  to poll a [`TAIL`](/sql/tail) operation for new records.

- Add several new SQL functions:

  - The [`digest`](/sql/functions#cryptography-func) and
    [`hmac`](/sql/functions#cryptography-func) cryptography functions
    compute message digests and authentication codes, respectively. These
    functions are based on the [`pgcrypto`] PostgreSQL extension.

  - The [`version`](/sql/functions#postgresql-compatibility-func) and
    [`mz_version`](/sql/functions/#system-information-func) functions report
    PostgreSQL-specific and Materialize-specific version information,
    respectively.

  - The [`current_schema`](/sql/functions#postgresql-compatibility-func)
    function reports the name of the SQL schema that appears first in the
    search path.

- Fix a bug that would cause invalid data to be returned when requesting
  binary-formatted values with [`FETCH`](/sql/fetch/) {{% gh 4976 %}}.

- Fix a bug when using `COPY` with `TAIL` that could cause some drivers to
  fail if the `TAIL` was idle for at least one second {{% gh 4976 %}}.

- Avoid panicking if a record in a regex-formatted source fails to decode
  as UTF-8 {{% gh 5008 %}}.

- Allow [query hints](/sql/select#query-hints) in `SELECT` statements.

{{% version-header v0.5.3 %}}

- Add support for SQL cursors via the new [`DECLARE`](/sql/declare),
  [`FETCH`](/sql/fetch), and [`CLOSE`](/sql/close) statements. Cursors
  facilitate fetching partial results from a query and are therefore
  particularly useful in conjuction with [`TAIL`](/sql/tail#tailing-with-fetch).

  **Known issue.** Requesting binary-formatted values with [`FETCH`](/sql/fetch)
  does not work correctly. This bug will be fixed in the next release.

- Support [common-table expressions (CTEs)](/sql/select#common-table-expressions-ctes)
  in `SELECT` statements.

- Add a [`map`](/sql/types/map) type to represent unordered key-value pairs.
  Avro map values in [Avro-formatted sources](/sql/create-source/avro-kafka)
  will be decoded into the new `map` type.

- Fix a regression in the SQL parser, introduced in v0.5.2, in which nested
  field accesses, e.g.

  ```sql
  SELECT ((col).field1).field2
  ```

  would fail to parse {{% gh 4827 %}}.

- Fix a bug that caused the [`real`]/[`real`] types to be incorrectly
  interpreted as [`double precision`] {{% gh 4918 %}}.

{{% version-header v0.5.2 %}}

- Provide the [`list`](/sql/types/list/) type, which is an ordered sequences of
  homogenously typed elements; they're nestable, too! The type was previously
  available in v0.5.1, but this release lets you create [`list`s from
  `text`](/sql/types/list/#text-to-list-casts), making their creation more
  accessible.

- Support the [`pg_typeof`
  function](/sql/functions#postgresql-compatibility-func).

- Teach [`COPY TO`](/sql/copy-to) to support `FORMAT binary`.

- Support the [`DISCARD`](/sql/discard) SQL statement.

- Change [`TAIL`](/sql/tail) to:

  - Produce output ordered by timestamp.

  - Support timestamp progress with the `PROGRESSED` option.

  - **Backwards-incompatible change.** Use Materialize's standard `WITH` option
    syntax, meaning:

    - `WITH SNAPSHOT` is now `WITH (SNAPSHOT)`.

    - `WITHOUT SNAPSHOT` is now `WITH (SNAPSHOT = false)`.

- Report an error without crashing when a query contains unexpected UTF-8
  characters, e.g., `SELECT ’1’`. {{% gh 4755 %}}

- Suppress logging of warnings and errors to stderr when users supply the
  [`--log-file` command line flag](/cli/#command-line-flags) {{% gh 4777 %}}.

- When using the systemd service distributed in the APT package, write log
  messages to the systemd journal instead of a file in the `mzdata` directory
  {{% gh 4781 %}}.

- Ingest SQL Server-style Debezium data {{% gh 4762 %}}.

- Allow slightly more complicated [`INSERT`](/sql/insert) bodies, e.g. inserting
  `SELECT`ed literals {{% gh 4748 %}}.
  characters, e.g., `SELECT ’1’` {{% gh 4755 %}}.

{{% version-header v0.5.1 %}}

- **Known issue.** [`COPY TO`](/sql/copy-to) panics if executed via the
  ["simple query" protocol][pgwire-simple], which is notably used by the
  `psql` command-line client {{% gh 4742 %}}.

  A fix is available in the latest [unstable builds](/versions/#unstable-builds)
  and will ship in v0.5.2.

  Note that some PostgreSQL clients instead use the
  ["extended query" protocol][pgwire-extended] to issue `COPY TO` statements,
  or let you choose which protocol to use. If you are using one of these
  clients, you can safely issue `COPY TO` statements in v0.5.1.

- **Backwards-incompatible change.** Send the rows returned by the
  [`TAIL`](/sql/tail) statement to the client normally (i.e., as if the rows
  were returned by a [`SELECT`](/sql/select) statement) rather than via the
  PostgreSQL [`COPY` protocol][pg-copy]. The new format additionally moves the
  timestamp and diff information to dedicated `timestamp` and `diff` columns at
  the beginning of each row.

  To replicate the old behavior of sending `TAIL` results via the `COPY`
  protocol, explicitly wrap the `TAIL` statement in a [`COPY TO`](/sql/copy-to)
  statement:

  ```
  COPY (TAIL some_materialized_view) TO STDOUT
  ```

- Add the [`COPY TO`](/sql/copy-to) statement, which sends the results of
  the statement it wraps via the special PostgreSQL [`COPY` protocol][pg-copy].

- When creating a Kafka sink, permit specifying the columns to include in the
  key of each record via the new `KEY` connector option in [`CREATE
  SINK`](/sql/create-sink).

- Default to using a worker thread count equal to half of the machine's
  physical cores if the [`--workers`](/cli/#worker-threads) command-line
  option is not specified.

- Add the [`regexp_match`](/sql/functions#string-func) function to search a
  string for a match against a regular expression.

- Support [`SELECT DISTINCT ON (...)`](/sql/select/#syntax) to deduplicate the
  output of a query based on only the specified columns in each row.
  Prior to this release, the [`SELECT`](/sql/select) documentation incorrectly
  claimed support for this feature.

- Reduce memory usage in:
  - Queries involving `min` and `max` aggregations {{% gh 4523 %}}.
  - Indexes containing `text` or `bytea` data, especially when each
    individual string or byte array is short {{% gh 4646 %}}.

{{% version-header v0.5.0 %}}

- Support tables via the new [`CREATE TABLE`](/sql/create-table), [`DROP
  TABLE`](/sql/drop-table), [`INSERT`](/sql/insert) and [`SHOW CREATE
  TABLE`](/sql/show-create-table) statements. Tables are conceptually similar to
  a [source](/sql/create-source), but the data in a table is managed by
  Materialize, rather than by Kafka or a filesystem.

  Note that table data is currently ephemeral: data inserted into a table does
  not persist across restarts. We expect to add support for persistent table
  data in a future release.

- Generate a persistent, unique identifier associated with each cluster. This
  can be retrieved using the new [`mz_cluster_id`](/sql/functions#uuid-func) SQL
  function.

- Automatically check for new versions of Materialize on server startup. If a
  new version is available, a warning will be logged encouraging you to upgrade.

  This version check involves reporting the cluster ID and current version to a
  server operated by Materialize Inc. To disable telemetry of this sort, use the
  new [`--disable-telemetry` command-line option](/cli/).

- Add a web-based, interactive [memory usage visualization](/ops/monitoring#memory-usage-visualization) to aid in understanding and diagnosing
  unexpected memory consumption.

- Add the [`lpad`](/sql/functions/#string-func) function, which extends a
  string to a given length by prepending characters.

- Improve PostgreSQL compatibility:

  - Permit qualifying function names in SQL queries with the name of the schema
    and optionally the database to which the function belongs, as in
    `pg_catalog.abs(-1)` {{% gh 4293 %}}.

    Presently all built-in functions belong to the system `mz_catalog` or
    `pg_catalog` schemas.

  - Add an [`oid` type](/sql/types/oid) to represent PostgreSQL object IDs.

  - Add basic support for [array types](/sql/types/array), including the new
    [`array_to_string` function](/sql/functions#array-func).

  - Add the  `current_schemas`, `obj_description`, `pg_table_is_visible`, and
    `pg_encoding_to_char` [compatibility functions](/sql/functions#postgresql-compatibility-func).

  Together these changes enable the `\l`, `\d`, `\dv`, `\dt`, `\di` commands
  in the [psql terminal](/connect/cli).

- Correct a query optimization that could misplan queries that referenced the
  same relation multiple times with varying filters {{% gh 4361 %}}.

- Rename the output columns for `SHOW` statements to match the PostgreSQL
  convention of using all lowercase characters with words separated by
  underscores.

  For example, the `SHOW INDEX` command now returns a column named
  `seq_in_index` rather than `Seq_in_index`. This makes it possible to refer
  to the column without quoting when supplying a `WHERE` clause.

  The renamings are described in more detail in the documentation for each
  `SHOW` command that changed:

    - [`SHOW COLUMNS`](/sql/show-columns)
    - [`SHOW DATABASES`](/sql/show-databases)
    - [`SHOW INDEXES`](/sql/show-indexes)
    - [`SHOW SCHEMAS`](/sql/show-schemas)
    - [`SHOW SINKS`](/sql/show-sinks)
    - [`SHOW SOURCES`](/sql/show-sources)
    - [`SHOW TABLES`](/sql/show-tables)
    - [`SHOW VIEWS`](/sql/show-views)

- Expose metadata about the running Materialize instance in the new
  [system catalog](/sql/system-catalog), which contains various sources, tables,
  and views that can be queried via SQL.

- Rename the `global_id` column of the
  [`mz_avro_ocf_sinks`](/sql/system-catalog#mz_avro_ocf_sinks) and
  [`mz_kafka_sinks`](/sql/system-catalog#mz_kafka_sinks) tables
  to `sink_id`, for better consistency with the other system catalog tables.

- Support [Kafka sources](/sql/create-source/avro-kafka) on topics
  that use [Zstandard compression](https://facebook.github.io/zstd/)
  {{% gh 4342 %}}.

{{% version-header v0.4.3 %}}

- Permit adjusting the logical compaction window on a per-index basis via the
  [`logical_compaction_window`](/sql/alter-index/#available-parameters)
  parameter to the new [`ALTER INDEX`](/sql/alter-index) statement.

- Add the [`uuid`](/sql/types/uuid) type to efficiently represent
  universally-unique identifiers (UUIDs).

- Report the `integer_datetime` parameter as `on` to ensure that [PgJDBC]
  correctly decodes date and time values returned by prepared statements
  {{% gh 4117 %}}.

- Fix a bug in the query optimizer that could result in incorrect plans for
  queries involving `UNION` operators and literals {{% gh 4195 %}}.

{{% version-header v0.4.2 %}}

- Remove the `max_timestamp_batch_size` [`WITH`
  option](/sql/create-source/avro-kafka/#with-options) from sources. Materialize
  now automatically selects the optimal batch size. **Backwards-incompatible
  change.**

- Restore support for specifying multiple Kafka broker addresses in [Kafka
  sources](/sql/create-source/avro-kafka/) {{% gh 3986 %}}.

  This fixes a regression introduced in v0.4.1.

- Sort the output of [`SHOW COLUMNS`](/sql/show-columns/) by the order in which
  the columns are defined in the targeted source, table, or view. Prior versions
  did not guarantee any particular ordering.

- Improve memory utilization:

  - Reduce memory usage of [outer joins](/sql/join#join_type) when the join key
    consists only of simple column equalities {{% gh 4047 %}}.

  - Consume only a constant amount of memory when computing a
    [`min` or `max` aggregation](/sql/functions/#aggregate-func)
    on an [append-only source](/sql/create-source/avro-kafka/#append-only-envelope)
    {{% gh 3994 %}}.

- Always permit memory profiling via the `/prof` web UI, even if the
  `MALLOC_CONF` environment variable is not configured to enable profiling
  {% gh 4005 %}.

- Handle large `VALUES` expressions. Previously, `VALUES` expressions with more
  than several hundred entries would cause a stack overflow {{% gh 3995 %}}.

- Add the `mz_records_per_dataflow_global` [metric](/ops/monitoring) to expose
  the number of active records in each dataflow {{% gh 4036 %}}.

{{% version-header v0.4.1 %}}

- **Known regression.** Specifying multiple Kafka broker addresses in
  [Kafka sources](/sql/create-source/avro-kafka/), as in

  ```sql
  CREATE SOURCE ... FROM KAFKA BROKER 'host1:9092,host2:9092' ...
  ```

  is incorrectly prohibited in this version. This change was unintentional and
  is reverted in v0.5.0.

- Enhance internal monitoring tools:

  - Add a web UI at `/prof` for visualizing memory and CPU profiles of a running
    `materialized` process.

  - Expose [metrics](/ops/monitoring) for per-thread CPU usage {{% gh 3733 %}}.

  - Reduce memory overhead of built-in logging views {{% gh 3752 %}}.

- Improve robustness of several source types:

  - Permit broker addresses in [Kafka sources](/sql/create-source/avro-kafka/)
    and [Kafka sinks](/sql/create-sink/) to use IP addresses in addition to
    hostnames.

  - Handle Snappy-encoded [Avro OCF files](/sql/create-source/avro-file/).

  - In [Avro sources that use the Debezium envelope](/sql/create-source/avro-kafka/#debezium-envelope-details),
    automatically filter out duplicate records generated by Debezium's
    PostgreSQL connector.

    This brings support for the PostgreSQL connector on par with the support for
    the MySQL connector.

- Improve the performance of the `TopK` operator {{% gh 3758 %}}.

- Add several new SQL features:

  - Add support for [`LATERAL` subqueries](/sql/join#lateral-subqueries) in
    joins. `LATERAL` subqueries can be used to express [Top-K by group
    queries](/guides/top-k/)

  - Add the [regular expression matching operators](/sql/functions/#string) `~`,
    `~*`, `!~`, and `!~*`, which report whether a string does or does not match
    a regular expression.

  - Support casts from [`boolean`](/sql/types/boolean) to [`int`](/sql/types/int).

  - Add the [`split_part`](/sql/functions/#string-func) function, which splits a
    string on a delimiter and returns one of the resulting chunks.

  - Allow ordinal references in `GROUP BY` clauses to refer to items in the
    `SELECT` list that are formed from arbitrary expressions, as in:

    ```sql
    SELECT a + 1, sum(b) FROM ... GROUP BY 1
    ```

    Previously, Materialize only handled ordinal references to items that were
    simple column references, as in:

    ```sql
    SELECT a, sum(b) FROM ... GROUP BY 1
    ```

- Fix two PostgreSQL compatibility issues:

  - Change the text format of the [`timestamp with time zone`](/sql/types/timestamptz)
    type to match PostgreSQL {{% gh 3798 %}}.

  - Respect client-provided parameter types in prepared statements
    {{% gh 3625 %}}.


{{% version-header v0.4.0 %}}

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

- Support [SASL PLAIN authentication for Kafka sources](/sql/create-source/avro-kafka/#connecting-to-a-kafka-broker-using-sasl-authentication).
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

- Introduce the `AS OF` and
  [`WITH SNAPSHOT`](/sql/create-sink/#with-snapshot-or-without-snapshot) options
  for `CREATE SINK` to provide more control over what data the sink will
  produce.

- Change the default [`TAIL` snapshot behavior](/sql/tail/#snapshot)
  from `WITHOUT SNAPSHOT` to `WITH SNAPSHOT`. **Backwards-incompatible change.**

- Actively shut down [Kafka sinks](https://materialize.com/docs/sql/create-sink/#kafka-sinks)
  that encounter an unrecoverable error, rather than attempting to produce data
  until the sink is dropped {{% gh 3419 %}}.

- Improve the performance, stability, and standards compliance of Avro encoding
  and decoding {{% gh 3397 3557 3568 3579 3583 3584 3585 %}}.

- Support [record types](/sql/types/record), which permit the representation of
  nested data in SQL. Avro sources also gain support for decoding nested
  records, which were previously disallowed, into this new SQL record type.

- Allow dropping databases with cross-schema dependencies {{% gh 3558 %}}.

- Avoid crashing if [`date_trunc('week', ...)`](/sql/functions/#date-and-time-func) is
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

{{% version-header v0.3.1 %}}

- Improve the ingestion speed of Kafka sources with multiple partitions by
  sharding responsibility for each partition across the available worker
  threads {{% gh 3190 %}}.

- Improve JSON decoding performance when casting a `text` column to `json`, as
  in `SELECT text_col::json` {{% gh 3195 %}}.

- Simplify converting non-materialized views into materialized views with
  [`CREATE DEFAULT INDEX ON foo`](/sql/create-index). This creates the same
  [index](/overview/api-components/#indexes) on a view that would have been
  created if you had used [`CREATE MATERIALIZED VIEW`].

- Permit control over the timestamp selection logic on a per-Kafka-source basis
  via three new [`WITH` options](https://materialize.com/docs/sql/create-source/avro-kafka/#with-options):
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

{{% version-header v0.3.0 %}}

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

{{% version-header v0.2.2 %}}

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

{{% version-header v0.2.1 %}}

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

{{% version-header v0.2.0 %}}

- Require the `-w` / `--threads` command-line option. Consult the [CLI
  documentation](/cli/#worker-threads) to determine the correct value for your
  deployment.

- Introduce the [`--listen-addr`](/cli/#listen-address) command-line option to
  control the address and port that `materialized` binds to.

- Make formatting and parsing for [`real`](/sql/types/float) and
  [`double precision`](/sql/types/float) numbers more consistent with PostgreSQL. The
  strings `NaN`, and `[+-]Infinity` are accepted as input, to select the special
  not-a-number and infinity states, respectively,  of floating-point numbers.

- Allow [CSV-formatted sources](/sql/create-source/csv-file/#csv-format-details)
  to include a header row (`CREATE SOURCE ... FORMAT CSV WITH HEADER`).

- Provide the option to name columns in sources (e.g. [`CREATE SOURCE foo
  (col_foo, col_bar)...`](/sql/create-source/csv-file/#creating-a-source-from-a-dynamic-csv)).

- Support [offsets](/sql/create-source/) for partitions on Kafka sources {{% gh 2169 %}}.

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

{{% version-header v0.1.3 %}}

- Support [Amazon Kinesis Data Stream sources](/sql/create-source/json-kinesis/).

- Support the number functions `round(x: N)` and `round(x: N, y: N)`, which
  round `x` to the `y`th digit after the decimal. (Default 0).

- Support addition and subtraction between [`interval`]s.

- Support the [string concatenation operator, `||`](/sql/functions/#string).

- In the event of a crash, print the stack trace to the log file, if logging to
  a file is enabled, as well as the standard error stream.

{{% version-header v0.1.2 %}}

- Change [`SHOW CREATE SOURCE`] to render the full SQL statement used to create
  the source, in the style of [`SHOW CREATE VIEW`], rather than displaying a URL
  that partially describes the source. The URL was a vestigial format used in
  [`CREATE SOURCE`] statements before v0.1.0.

- Raise the maximum SQL statement length from approximately 8KiB to
  approximately 64MiB.

- Support casts from [`text`] to [`date`], [`timestamp`], [`timestamp with time zone`], and
  [`interval`].

- Support the `IF NOT EXISTS` clause in [`CREATE VIEW`] and
  [`CREATE MATERIALIZED VIEW`].

- Attempt to automatically increase the nofile rlimit to acceptable levels, as
  creating multiple Kafka sources can quickly exhaust the default nofile rlimit
  on some platforms.

- Improve CSV parsing speed by 5-6x.

{{% version-header v0.1.1 %}}

* Specifying the message name in a Protobuf-formatted source no longer requires
  a leading period.

- **Indexes on sources**: You can now create and drop indexes on sources, which
  lets you automatically store all of a source's data in an index. Previously,
  you would have to create a source, and then create a materialized view that
  selected all of the source's content.

{{% version-header v0.1.0 %}}

* [What is Materialize?](/overview/what-is-materialize/)
* [Architecture overview](/overview/architecture/)

[`array`]: /sql/types/array/
[`bytea`]: /sql/types/bytea
[`ALTER INDEX`]: /sql/alter-index
[`COPY FROM`]: /sql/copy-from
[`CREATE INDEX`]: /sql/create-index
[`CREATE MATERIALIZED VIEW`]: /sql/create-materialized-view
[`CREATE SOURCE`]: /sql/create-source
[`CREATE VIEW`]: /sql/create-view
[`date`]: /sql/types/date
[`double precision`]: /sql/types/float8
[`interval`]: /sql/types/interval
[`list`]: /sql/types/list/
[`map`]: /sql/types/map/
[`real`]: /sql/types/float4
[`pgcrypto`]: https://www.postgresql.org/docs/current/pgcrypto.html
[`SHOW CREATE SOURCE`]: /sql/show-create-source
[`SHOW CREATE VIEW`]: /sql/show-create-view
[`text`]: /sql/types/text
[`timestamp`]: /sql/types/timestamp
[`timestamp with time zone`]: /sql/types/timestamptz
[pg-copy]: https://www.postgresql.org/docs/current/sql-copy.html
[pgwire-simple]: https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.4
[pgwire-extended]: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
[PgJDBC]: https://jdbc.postgresql.org
