---
title: "SQL Shell"
description: "SQL Shell in the Materialize console"
disable_toc: true
menu:
  main:
    parent: console
    weight: 10
    identifier: console-sql-shell
aliases:
  - /console/sql-shell/
---

The Materialize Console provides a **SQL
Shell**, where you can issue your queries. Materialize follows the SQL standard
(SQL-92) implementation, and strives for compatibility with the PostgreSQL
dialect. If your query takes too long to complete, the SQL Shell provides
**Query Insights** listing some possible causes.

![Image of the Materialize Console SQL Shell](/images/console/console.png "Materialize Console SQL Shell")

The SQL Shell also includes:

- A top navigation panel, where you can select your cluster and database and
  schema.

- A [Quickstart](/get-started/quickstart/) tutorial. You can close the
  Quickstart by clicking the **Close Quickstart** button in the top-right
  corner.

## Query timing

Below each result, the SQL Shell shows how long the statement took, for
example `Returned in 148.0ms · 12.0ms to first row (served from an index)`:

- **Returned in** is the time until the statement's result arrived in your
  browser, including the network. It is measured from sending your input or,
  for the second and later statements of one input, from the previous result.
- The second number is measured by Materialize:
  - For a query, it ends when the first row is ready, so it does not include
    sending the rows to your browser. A query without rows shows the time to its
    empty result.
  - For a write outside an explicit transaction, it includes the commit. Inside
    `BEGIN ... COMMIT`, a write shows its time before the commit, and `COMMIT`
    shows the commit.
  - For other statements, it ends when the statement completes.

`SUBSCRIBE` results, and environments that do not report the second number,
show the round trip only.
