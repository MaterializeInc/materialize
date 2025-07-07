<a name="partition-by"></a> *Optional.* The column(s) by which Materialize
should internally partition the table. The specified column(s) must be a prefix
of the upstream table's columns (i.e., a subset of one or more columns listed at
the start of the table's column definition list). See the
[partitioning guide](/transform-data/patterns/partition-by/) for restrictions on
valid values and other details.
