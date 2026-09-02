---
headless: true
---
<a name="partition-by"></a> *Optional.* The column(s) by which Materialize
should internally partition the table. The specified column(s) must be a prefix
of the upstream table's columns (i.e., a subset of one or more columns listed at
the start of the table's column definition list). See the
[partitioning guide](/transform-data/patterns/partition-by/) for restrictions on
valid values and other details. This option declares the ordering you expect and
is validated when the table is created, but it does not change how Materialize
stores the table.
