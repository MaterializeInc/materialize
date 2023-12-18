# [LDBC SNB Business Intelligence benchmark](https://github.com/ldbc/ldbc_snb_bi/tree/main)

To run this benchmark, you will need:

  - inputs at an appropriate scale factor
  - parameters at that same scale factor

You can download pre-fab datasets and parameters from
<https://github.com/ldbc/ldbc_snb_bi/blob/main/snb-bi-pre-generated-data-sets.md>. (Note
that validation parameters exist only for scale factor 10.)

# What does the benchmark run?

LDBC BI runs a variety of analysis queries on a (generated)
social-network-like graph. After a bulk load, the queries are rerun
after several batches of updates. The benchmark specifies a protocol
for running "enough" queries on each batch of updates.

Each query of the benchmark has some parameters to make it somehow
specific to the data. For example, query 1 has a parameter `datetime`;
it calculates statistics on the number of messages for each 'message
length category' in the same year as `datetime` but before `datetime`
itself. The generated parameters offer a number of interesting
`datetime`s to select.

The [Umbra
implementation](https://github.com/ldbc/ldbc_snb_bi/tree/main/umbra)
does some pre-computation in manually materialized views. We translate
those to true materialized views.

# How do I run it?

The script `init.sql` does the initial bulk load. To get it to work,
you should [download scale factor
1](https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/bi-pre-audit/bi-sf1-composite-merged-fk.tar.zst)
and unzip it into `test/ldbc-bi/bi-sf1-composite-merged` and run `find
${UMBRA_CSV_DIR} -name "*.csv.gz" -print0 | parallel -q0 gunzip` to
unzip each individual CSV file.

You can then run `\i init.sql` from a psql session with
Materialize. It will take a few minutes to load the data and define
the materialized views.

Run `\i qXX.sql` to run query `XX`. Each query has appropriate `\set`
commands at the beginning to fill in a parameter value (the first one
in the parameter set, arbitrarily).

# Ideas, questions, and tasks

During the conversion process, each of the benchmark queries is run as
one-shot select. It may be more interesting to treat the queries as
materialized views; we would then want to not just track total time
querying in the benchmark, but also some measure of latency.

# Local changes

We've manually reordered `weights` in the `PathQ19` view of query 19 to
accommodate the way delta joins hydrate (they follow the join plan of
the first syntactic table, which happened to be a poor choice for this
query).

## TODO

- [ ] apply updates
  + do we want to measure "liveness" of the views as we run?
- [ ] fully automate locally/in staging
- [ ] load generator
