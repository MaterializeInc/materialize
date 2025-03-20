---
title: "EXPLAIN TIMESTAMP"
description: "`EXPLAIN TIMESTAMP` displays the timestamps used for a `SELECT` statement."
menu:
  main:
    parent: commands
---

`EXPLAIN TIMESTAMP` displays the timestamps used for a `SELECT` statement -- valuable information to investigate query delays.

{{< warning >}}
`EXPLAIN` is not part of Materialize's stable interface and is not subject to
our backwards compatibility guarantee. The syntax and output of `EXPLAIN` may
change arbitrarily in future versions of Materialize.
{{< /warning >}}

## Syntax

{{< diagram "explain-timestamp.svg" >}}

### Output format

You can select between `JSON` and `TEXT` for the output format of `EXPLAIN TIMESTAMP`. Non-text
output is more machine-readable and can be parsed by common graph visualization libraries,
while formatted text is more human-readable.

Output type | Description
------|-----
**TEXT** | Format the explanation output as UTF-8 text.
**JSON** | Format the explanation output as a JSON object.

## Details

The explanation is divided in two parts:

1. Determinations for a timestamp
2. Sources frontiers

Having a _query timestamp_ outside the _[read, write)_ frontier values of a source can explain the presence of delays. While in the middle, the space of processed but not yet compacted data, allows building and returning a correct result immediately.

### Determinations for a timestamp

Queries in Materialize have a logical timestamp, known as _query timestamp_. It plays a critical role to return a correct result. Returning a correct result implies retrieving data with the same logical time from each source present in a query.

In this case, sources are objects providing data: materialized views, views, indexes, tables, or sources. Each will have a pair of logical timestamps frontiers, denoted as _sources frontiers_.

This section contains the following fields:

Field | Meaning | Example
---------|---------|---------
**query timestamp** | The query timestamp value |`1673612424151 (2023-01-13 12:20:24.151)`
**oracle read** | The value of the timeline's oracle timestamp, if used. | `1673612424151 (2023-01-13 12:20:24.151)`
**largest not in advance of upper** | The largest timestamp not in advance of upper. | `1673612424151 (2023-01-13 12:20:24.151)`
**since** | The maximum read frontier of all involved sources. | `[1673612423000 (2023-01-13 12:20:23.000)]`
**upper** | The minimum write frontier of all involved sources | `[1673612424152 (2023-01-13 12:20:24.152)]`
**can respond immediately** | Returns true when the **query timestamp** is greater or equal to **since** and lower than **upper** | `true`
**timeline** | The type of timeline the query's timestamp belongs | `Some(EpochMilliseconds)`

A timeline value of `None` means the query is known to be constant across all timestamps.

### Sources frontiers

Every source has a beginning _read frontier_ and an ending _write frontier_.
They stand for a source’s limits to return a correct result immediately:

* Read frontier: Indicates the minimum logical timestamp to return a correct result (advanced by _compaction_)
* Write frontier: Indicates the maximum timestamp to build a correct result without waiting for unprocessed data.

Each source has its own output section consisting of the following fields:

Field | Meaning | Example
---------|---------|---------
**source** | Source’s identifiers | `source materialize.public.raw_users (u2014, storage)`
**read frontier** | Minimum logical timestamp. |`[1673612423000 (2023-01-13 12:20:23.000)]`
**write frontier** | Maximum logical timestamp. | `[1673612424152 (2023-01-13 12:20:24.152)]`


## Examples

```mzsql
EXPLAIN TIMESTAMP FOR SELECT * FROM users;
```
```
                                 Timestamp
---------------------------------------------------------------------------
                 query timestamp: 1673618185152 (2023-01-13 13:56:25.152) +
           oracle read timestamp: 1673618185152 (2023-01-13 13:56:25.152) +
 largest not in advance of upper: 1673618185152 (2023-01-13 13:56:25.152) +
                           upper:[1673618185153 (2023-01-13 13:56:25.153)]+
                           since:[1673618184000 (2023-01-13 13:56:24.000)]+
         can respond immediately: true                                    +
                        timeline: Some(EpochMilliseconds)                 +
                                                                          +
 source materialize.public.raw_users (u2014, storage):                    +
                   read frontier:[1673618184000 (2023-01-13 13:56:24.000)]+
                  write frontier:[1673618185153 (2023-01-13 13:56:25.153)]+
```

<!-- We think of `since` as the "read frontier": times not later than or equal to
`since` cannot be correctly read. We think of `upper` as the "write frontier":
times later than or equal to `upper` may still be written to the TVC. -->
<!-- Who is the oracle? -->
<!--
We maintain a timestamp oracle that returns strictly increasing timestamps
Mentions that this is inspired/similar to Percolator.

Timestamp oracle is periodically bumped up to the current system clock
We never revert oracle if system clock goes backwards.

https://tikv.org/deep-dive/distributed-transaction/timestamp-oracle/
-->
<!-- Materialize's objects request timestamp to the oracle, a timestamp provider. The oracle's timestamp bumps up periodically to match the current system clock, and never goes backwards.

It relies on an oracle, a timestamp provider, to handle them correctly.

The oracle it is a timestamp provider. It bumps up periodically internal value to the current system clock, never going backwards.

Issuing a select statement in Materialize
When a select statement runs, Materialize will pick a timestamp between all the sources:

`max(max(read_frontiers), min(write_frontiers) - 1)` -->

<!-- /// Information used when determining the timestamp for a query.
#[derive(Serialize, Deserialize)]
pub struct TimestampDetermination<T> {
    /// The chosen timestamp context from `determine_timestamp`.
    pub timestamp_context: TimestampContext<T>,
    /// The largest timestamp not in advance of upper.
    pub largest_not_in_advance_of_upper: T,
}


*Query timestamp: The timestamp in a timeline at which the query makes the read
oracle read: The value of the timeline's oracle timestamp, if used.
largest not in advance of upper: The largest timestamp not in advance of upper.
upper: The write frontier of all involved sources.
since: The read frontier of all involved sources.
can respond immediately: True when the write frontier is greater than the query timestamp.
timeline: The type of timeline the query's timestamp belongs:
      /// EpochMilliseconds means the timestamp is the number of milliseconds since
      /// the Unix epoch.
      EpochMilliseconds,
      /// External means the timestamp comes from an external data source and we
      /// don't know what the number means. The attached String is the source's name,
      /// which will result in different sources being incomparable.
      External(String),
      /// User means the user has manually specified a timeline. The attached
      /// String is specified by the user, allowing them to decide sources that are
      /// joinable.
      User(String),

Each source contains two frontiers:
  Read: At which time
  Write:

                 query timestamp: 1673612424151 (2023-01-13 12:20:24.151) +
           oracle read timestamp: 1673612424151 (2023-01-13 12:20:24.151) +
 largest not in advance of upper: 1673612424151 (2023-01-13 12:20:24.151) +
                           upper:[1673612424152 (2023-01-13 12:20:24.152)]+
                           since:[1673612423000 (2023-01-13 12:20:23.000)]+


                                 Timestamp
---------------------------------------------------------------------------
                 query timestamp: 1673612424151 (2023-01-13 12:20:24.151) +
           oracle read timestamp: 1673612424151 (2023-01-13 12:20:24.151) +
 largest not in advance of upper: 1673612424151 (2023-01-13 12:20:24.151) +
                           upper:[1673612424152 (2023-01-13 12:20:24.152)]+
                           since:[1673612423000 (2023-01-13 12:20:23.000)]+
         can respond immediately: true                                    +
                        timeline: Some(EpochMilliseconds)                 +
                                                                          +
 source materialize.public.a (u2014, storage):                            +
                   read frontier:[1673612423000 (2023-01-13 12:20:23.000)]+
                  write frontier:[1673612424152 (2023-01-13 12:20:24.152)]+ -->

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schemas that all relations in the query are contained in.
