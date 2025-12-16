<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)
 /  [Reference](/docs/self-managed/v25.2/sql/)

</div>

# Identifiers

In Materialize, identifiers are used to refer to columns and database
objects like sources, views, and indexes.

## Naming restrictions

- The first character of an identifier must be an ASCII letter (`a`-`z`
  and `A`-`Z`), an underscore (`_`), or any non-ASCII character.

- The remaining characters of an identifier must be ASCII letters
  (`a`-`z` and `A`-`Z`), ASCII digits (`0`-`9`), underscores (`_`),
  dollar signs (`$`), or any non-ASCII characters.

You can circumvent any of the above rules by double-quoting the
identifier, e.g. `"123_source"` or `"fun_source_@"`. All characters
inside a quoted identifier are taken literally, except that
double-quotes must be escaped by writing two adjacent double-quotes, as
in `"includes""quote"`.

Additionally, the identifiers `"."` and `".."` are not permitted.

## Case sensitivity

Materialize performs case folding (the caseless comparison of text) for
identifiers, which means that identifiers are effectively
case-insensitive (`foo` is the same as `FOO` is the same as `fOo`). This
can cause issues when column names come from data sources which do
support case-sensitive names, such as Avro-formatted sources or CSV
headers.

To avoid conflicts, double-quote all field names (`"field_name"`) when
working with case-sensitive sources.

## Keyword collision

Materialize is very permissive with accepting SQL keywords as
identifiers (e.g. `offset`, `user`). If Materialize cannot use a keyword
as an identifier in a particular location, it throws a syntax error. You
can wrap the identifier in double quotes to force Materialize to
interpret the word as an identifier instead of a keyword.

For example, `SELECT offset` is invalid, because it looks like a
mistyping of `SELECT OFFSET <n>`. You can wrap the identifier in double
quotes, as in `SELECT "offset"`, to resolve the error.

We recommend that you avoid using keywords as identifiers whenever
possible, as the syntax errors that result are not always obvious.

The current keywords are listed below.

|                   |                   |                       |                |
|-------------------|-------------------|-----------------------|----------------|
| `ACCESS`          | `ACTION`          | `ADD`                 |                |
| `ADDED`           | `ADDRESS`         | `ADDRESSES`           | `AFTER`        |
| `AGGREGATE`       | `AGGREGATION`     | `ALIGNED`             | `ALL`          |
| `ALTER`           | `ANALYSE`         | `ANALYSIS`            | `ANALYZE`      |
| `AND`             | `ANY`             | `ARITY`               | `ARN`          |
| `ARRANGED`        | `ARRANGEMENT`     | `ARRAY`               | `AS`           |
| `ASC`             | `ASSERT`          | `ASSUME`              | `AT`           |
| `AUCTION`         | `AUTHORITY`       | `AVAILABILITY`        | `AVRO`         |
| `AWS`             | `BATCH`           | `BEGIN`               | `BETWEEN`      |
| `BIGINT`          | `BILLED`          | `BODY`                | `BOOLEAN`      |
| `BOTH`            | `BPCHAR`          | `BROKEN`              | `BROKER`       |
| `BROKERS`         | `BY`              | `BYTES`               | `CAPTURE`      |
| `CARDINALITY`     | `CASCADE`         | `CASE`                | `CAST`         |
| `CERTIFICATE`     | `CHAIN`           | `CHAINS`              | `CHAR`         |
| `CHARACTER`       | `CHARACTERISTICS` | `CHECK`               | `CLASS`        |
| `CLIENT`          | `CLOCK`           | `CLOSE`               | `CLUSTER`      |
| `CLUSTERS`        | `COALESCE`        | `COLLATE`             | `COLUMN`       |
| `COLUMNS`         | `COMMENT`         | `COMMIT`              | `COMMITTED`    |
| `COMPACTION`      | `COMPATIBILITY`   | `COMPRESSION`         | `COMPUTE`      |
| `COMPUTECTL`      | `CONFIG`          | `CONFLUENT`           | `CONNECTION`   |
| `CONNECTIONS`     | `CONSTRAINT`      | `CONTINUAL`           | `COPY`         |
| `COUNT`           | `COUNTER`         | `CPU`                 | `CREATE`       |
| `CREATECLUSTER`   | `CREATEDB`        | `CREATENETWORKPOLICY` | `CREATEROLE`   |
| `CREATION`        | `CROSS`           | `CSE`                 | `CSV`          |
| `CURRENT`         | `CURSOR`          | `DATABASE`            | `DATABASES`    |
| `DATUMS`          | `DAY`             | `DAYS`                | `DEALLOCATE`   |
| `DEBEZIUM`        | `DEBUG`           | `DEBUGGING`           | `DEC`          |
| `DECIMAL`         | `DECLARE`         | `DECODING`            | `DECORRELATED` |
| `DEFAULT`         | `DEFAULTS`        | `DELETE`              | `DELIMITED`    |
| `DELIMITER`       | `DELTA`           | `DESC`                | `DETAILS`      |
| `DIRECTION`       | `DISCARD`         | `DISK`                | `DISTINCT`     |
| `DOC`             | `DOT`             | `DOUBLE`              | `DROP`         |
| `EAGER`           | `ELEMENT`         | `ELSE`                | `ENABLE`       |
| `END`             | `ENDPOINT`        | `ENFORCED`            | `ENVELOPE`     |
| `EQUIVALENCES`    | `ERROR`           | `ERRORS`              | `ESCAPE`       |
| `ESTIMATE`        | `EVERY`           | `EXCEPT`              | `EXCLUDE`      |
| `EXECUTE`         | `EXISTS`          | `EXPECTED`            | `EXPLAIN`      |
| `EXPOSE`          | `EXPRESSIONS`     | `EXTERNAL`            | `EXTRACT`      |
| `FACTOR`          | `FALSE`           | `FAST`                | `FEATURES`     |
| `FETCH`           | `FIELDS`          | `FILE`                | `FILES`        |
| `FILTER`          | `FIRST`           | `FIXPOINT`            | `FLOAT`        |
| `FOLLOWING`       | `FOR`             | `FOREIGN`             | `FORMAT`       |
| `FORWARD`         | `FROM`            | `FULL`                | `FULLNAME`     |
| `FUNCTION`        | `FUSION`          | `GENERATOR`           | `GRANT`        |
| `GREATEST`        | `GROUP`           | `GROUPS`              | `HAVING`       |
| `HEADER`          | `HEADERS`         | `HINTS`               | `HISTORY`      |
| `HOLD`            | `HOST`            | `HOUR`                | `HOURS`        |
| `HUMANIZED`       | `HYDRATION`       | `ID`                  | `IDENTIFIERS`  |
| `IDS`             | `IF`              | `IGNORE`              | `ILIKE`        |
| `IMPLEMENTATIONS` | `IMPORTED`        | `IN`                  | `INCLUDE`      |
| `INDEX`           | `INDEXES`         | `INFO`                | `INHERIT`      |
| `INLINE`          | `INNER`           | `INPUT`               | `INSERT`       |
| `INSIGHTS`        | `INSPECT`         | `INSTANCE`            | `INT`          |
| `INTEGER`         | `INTERNAL`        | `INTERSECT`           | `INTERVAL`     |
| `INTO`            | `INTROSPECTION`   | `IS`                  | `ISNULL`       |
| `ISOLATION`       | `JOIN`            | `JOINS`               | `JSON`         |
| `KAFKA`           | `KEY`             | `KEYS`                | `LAST`         |
| `LATERAL`         | `LATEST`          | `LEADING`             | `LEAST`        |
| `LEFT`            | `LEGACY`          | `LETREC`              | `LEVEL`        |
| `LIKE`            | `LIMIT`           | `LINEAR`              | `LIST`         |
| `LOAD`            | `LOCAL`           | `LOCALLY`             | `LOG`          |
| `LOGICAL`         | `LOGIN`           | `LOWERING`            | `MANAGED`      |
| `MANUAL`          | `MAP`             | `MARKETING`           | `MATERIALIZE`  |
| `MATERIALIZED`    | `MAX`             | `MECHANISMS`          | `MEMBERSHIP`   |
| `MEMORY`          | `MESSAGE`         | `METADATA`            | `MINUTE`       |
| `MINUTES`         | `MODE`            | `MONTH`               | `MONTHS`       |
| `MUTUALLY`        | `MYSQL`           | `NAME`                | `NAMES`        |
| `NATURAL`         | `NEGATIVE`        | `NETWORK`             | `NEW`          |
| `NEXT`            | `NO`              | `NOCREATECLUSTER`     | `NOCREATEDB`   |
| `NOCREATEROLE`    | `NODE`            | `NOINHERIT`           | `NOLOGIN`      |
| `NON`             | `NONE`            | `NOSUPERUSER`         | `NOT`          |
| `NOTICE`          | `NOTICES`         | `NULL`                | `NULLIF`       |
| `NULLS`           | `OBJECTS`         | `OF`                  | `OFFSET`       |
| `ON`              | `ONLY`            | `OPERATOR`            | `OPTIMIZED`    |
| `OPTIMIZER`       | `OPTIONS`         | `OR`                  | `ORDER`        |
| `ORDINALITY`      | `OUTER`           | `OVER`                | `OWNED`        |
| `OWNER`           | `PARTITION`       | `PARTITIONS`          | `PASSWORD`     |
| `PATH`            | `PATTERN`         | `PHYSICAL`            | `PLAN`         |
| `PLANS`           | `POLICIES`        | `POLICY`              | `PORT`         |
| `POSITION`        | `POSTGRES`        | `PRECEDING`           | `PRECISION`    |
| `PREFIX`          | `PREPARE`         | `PRIMARY`             | `PRIORITIZE`   |
| `PRIVATELINK`     | `PRIVILEGES`      | `PROGRESS`            | `PROJECTION`   |
| `PROTOBUF`        | `PROTOCOL`        | `PUBLIC`              | `PUBLICATION`  |
| `PUSHDOWN`        | `QUALIFY`         | `QUERY`               | `QUOTE`        |
| `RAISE`           | `RANGE`           | `RATE`                | `RAW`          |
| `READ`            | `READY`           | `REAL`                | `REASSIGN`     |
| `RECURSION`       | `RECURSIVE`       | `REDACTED`            | `REDUCE`       |
| `REFERENCE`       | `REFERENCES`      | `REFRESH`             | `REGEX`        |
| `REGION`          | `REGISTRY`        | `RELATION`            | `RENAME`       |
| `REOPTIMIZE`      | `REPEATABLE`      | `REPLACE`             | `REPLAN`       |
| `REPLICA`         | `REPLICAS`        | `REPLICATION`         | `RESET`        |
| `RESPECT`         | `RESTRICT`        | `RETAIN`              | `RETURN`       |
| `RETURNING`       | `REVOKE`          | `RIGHT`               | `ROLE`         |
| `ROLES`           | `ROLLBACK`        | `ROTATE`              | `ROUNDS`       |
| `ROW`             | `ROWS`            | `RULES`               | `SASL`         |
| `SCALE`           | `SCHEDULE`        | `SCHEMA`              | `SCHEMAS`      |
| `SECOND`          | `SECONDS`         | `SECRET`              | `SECRETS`      |
| `SECURITY`        | `SEED`            | `SELECT`              | `SEQUENCES`    |
| `SERIALIZABLE`    | `SERVER`          | `SERVICE`             | `SESSION`      |
| `SET`             | `SHARD`           | `SHOW`                | `SINK`         |
| `SINKS`           | `SIZE`            | `SKEW`                | `SMALLINT`     |
| `SNAPSHOT`        | `SOME`            | `SOURCE`              | `SOURCES`      |
| `SQL`             | `SSH`             | `SSL`                 | `START`        |
| `STDIN`           | `STDOUT`          | `STORAGE`             | `STORAGECTL`   |
| `STRATEGY`        | `STRICT`          | `STRING`              | `STRONG`       |
| `SUBSCRIBE`       | `SUBSOURCE`       | `SUBSOURCES`          | `SUBSTRING`    |
| `SUBTREE`         | `SUPERUSER`       | `SWAP`                | `SYNTAX`       |
| `SYSTEM`          | `TABLE`           | `TABLES`              | `TAIL`         |
| `TASK`            | `TASKS`           | `TEMP`                | `TEMPORARY`    |
| `TEXT`            | `THEN`            | `TICK`                | `TIES`         |
| `TIME`            | `TIMEOUT`         | `TIMESTAMP`           | `TIMESTAMPTZ`  |
| `TIMING`          | `TO`              | `TOKEN`               | `TOPIC`        |
| `TPCH`            | `TRACE`           | `TRAILING`            | `TRANSACTION`  |
| `TRANSACTIONAL`   | `TRANSFORM`       | `TRIM`                | `TRUE`         |
| `TUNNEL`          | `TYPE`            | `TYPES`               | `UNBOUNDED`    |
| `UNCOMMITTED`     | `UNION`           | `UNIQUE`              | `UNKNOWN`      |
| `UNNEST`          | `UNTIL`           | `UP`                  | `UPDATE`       |
| `UPSERT`          | `URL`             | `USAGE`               | `USER`         |
| `USERNAME`        | `USERS`           | `USING`               | `VALIDATE`     |
| `VALUE`           | `VALUES`          | `VARCHAR`             | `VARIADIC`     |
| `VARYING`         | `VERBOSE`         | `VERSION`             | `VIEW`         |
| `VIEWS`           | `WAIT`            | `WARNING`             | `WEBHOOK`      |
| `WHEN`            | `WHERE`           | `WHILE`               | `WINDOW`       |
| `WIRE`            | `WITH`            | `WITHIN`              | `WITHOUT`      |
| `WORK`            | `WORKERS`         | `WORKLOAD`            | `WRITE`        |
| `YEAR`            | `YEARS`           | `YUGABYTE`            | `ZONE`         |
| `ZONES`           |                   |                       |                |

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/identifiers.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
