# RQG: grammar-based random query testing

Runs the [MaterializeInc/RQG](https://github.com/MaterializeInc/RQG) fork of
the Random Query Generator against Materialize. Depending on the workload,
generated queries are compared against Postgres, against another Materialize
version, or checked against invariants embedded in the grammar. This finds
query errors, panics, and (for the comparison workloads) correctness issues.

## Running

```shell
# All workloads that are not disabled
bin/mzcompose --find rqg run default

# One workload, with a fixed seed and shorter duration
bin/mzcompose --find rqg run default subqueries --seed=12345 --duration=300

# Compare against another Materialize version instead of Postgres
bin/mzcompose --find rqg run default wmr --other-tag=common-ancestor
```

Workloads are defined in `mzcompose.py`. Each pairs a grammar with a dataset
and a validator:

* `ResultsetComparatorSimplify` executes every query on both servers,
  compares the (sorted) result sets, and on a mismatch automatically shrinks
  the query to a minimal reproducer, printed between
  `RESULT COMPARISON ISSUE START/END` markers.
* `QueryProperties,RepeatableRead` (banking workload) checks invariant
  assertions embedded in the grammar as `/* RESULTSET_... */` comments and
  re-executes SELECTs inside transactions to check snapshot stability.

## Reproducing a failure

Every run prints its effective integer seed and a ready-to-run repro command
(`--- Reproduce with: ...`). Re-running with the same seed regenerates the
same query stream. The seed must reach gentest.pl as a real integer; the
harness hashes non-integer `--seed` values (such as `$BUILDKITE_JOB_ID`)
because Perl would otherwise silently numify the string and collapse the
seed space.

To shrink a reproducer further, `util/simplify-psql.pl` in the RQG repository
delta-debugs a SQL file against a running Materialize:

```shell
perl util/simplify-psql.pl --input-file=repro.sql --expected-output='internal error'
```

Note that `--sqltrace` is currently only implemented in RQG's MySQL executor,
so it has no effect here; use the seed for reproduction.

## Where grammars and datasets live

* Grammars and their datasets live in the
  [MaterializeInc/RQG](https://github.com/MaterializeInc/RQG) repository
  under `conf/mz/`, together with the RQG engine, pinned by commit in
  `Dockerfile`. Landing a change there means pushing to that repository and
  bumping the pin. For local iteration, mount your checkout with
  `RQG_CHECKOUT` (see below), which bypasses the pin.
* `grammars/` and `datasets/` in this directory (the left-join-stacks
  workload) are mounted into the rqg container at `/workdir` and can be
  changed like any other test file, with no image rebuild.

Dataset files are loaded with `psql -v ON_ERROR_STOP=1` into every
participating server, so they must be idempotent and valid in both the
Materialize and Postgres dialects (dialect-specific files can be marked
`Target.POSTGRES_ONLY` in `mzcompose.py`). After loading, the harness asserts
row-count parity across both servers, so a load that silently diverged fails
immediately instead of surfacing as a bogus result mismatch later.

Grammar rules that create or drop tables mid-run are a known trap: an init
rule executes while other workers are already issuing queries, and that race
produces spurious result differences (this is why the wmr and banking DDL
lives in dataset files). Also keep result comparison deterministic: total
ORDER BY when a query's row order matters (the comparator sorts rows, so
plain ORDER BY correctness is not checked), no float accumulation whose
result depends on evaluation order, and ORDER BY the aggregated expression
itself inside STRING_AGG and friends.

## Developing against a local RQG checkout

Set `RQG_CHECKOUT` to mount a local clone of MaterializeInc/RQG over the
pinned checkout in the image, e.g. to test changes to the RQG library or to
the `conf/mz/` grammars without rebuilding the image:

```shell
RQG_CHECKOUT=~/git/rqg bin/mzcompose --find rqg run default banking --duration=60
```

CI always runs the commit pinned in `Dockerfile`; after changing the RQG
repository, push there and update the pin.
