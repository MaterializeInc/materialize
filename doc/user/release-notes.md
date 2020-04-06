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
{{< /comment >}}

<span id="v0.2.0"></span>
## 0.1.3 &rarr; v0.2.0 (unreleased)

- Require the `-w` / `--threads` command-line option. Consult the
  [configuration documentation](../overview/configuration/#worker-threads)
  to determine the correct value for your deployment.

- Make formatting and parsing for [`real`](../sql/types/float) and
  [`double precision`](../sql/types/float) numbers more
  consistent with PostgreSQL. The strings `NaN`, and `[+-]Infinity` are
  accepted as input, to select the special not-a-number and infinity states,
  respectively,  of floating-point numbers.

- Allow [CSV-formatted sources](../sql/create-source/csv-source/#csv-format-details) to include a
  header row (`CREATE SOURCE ... FORMAT CSV WITH HEADER`).

- Provide users the option to name columns in sources (e.g. [`CREATE SOURCE foo
  (col_foo,
  col_bar)...`](../sql/create-source/csv-source/#creating-a-source-from-a-dynamic-csv))

- Introduce the [`--listen-addr`](../overview/configuration#listen-address)
  command-line option to control the address and port that `materialized` binds
  to.

- Improve conformance of the Avro parser, enabling support for
  a wider variety of Avro schemas in [Avro sources](../sql/create-source/avro).

- Add the [`jsonb_agg()`](../sql/functions/#aggregate-func) aggregate function.

- Support [casts](../sql/functions/cast/) for `time`->`text`,`time`->`interval`, `interval`->`time`.

- Change the output format of `EXPLAIN` to make large plans more readable.
- Support `EXPLAIN ... FOR VIEW view_name` to display the plan for an existing view.
- Support `EXPLAIN stage_name PLAN FOR ...` to display the plan at various stages of the planning process.

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
