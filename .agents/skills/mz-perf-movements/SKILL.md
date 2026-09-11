---
name: mz-perf-movements
description: >
  Summarize performance movements in the nightly, release-qualification and
  spec-sheet pipelines. Trigger: "what moved in nightly", "did perf regress",
  "summarize the benchmark results", "any performance regressions this week",
  "did my optimization show up in CI", "compare spec sheet builds", or a pasted
  nightly / release-qualification / spec-sheet Buildkite URL asked about in
  performance rather than failure terms. For a red build use mz-debug-ci; for
  writing or changing a benchmark use mz-benchmark.
argument-hint: "[pipeline] [--since N builds] [--only regressions|improvements]"
---

Report which benchmark numbers moved, in which direction, and by how much.

This skill answers "what changed in performance", not "why is the build red".
A failing benchmark job is often a gate trip that this skill will show as a
large movement, but diagnosing a crashed or timed-out job belongs to
mz-debug-ci.

## The one thing to understand first

The three pipelines produce two different kinds of evidence, and conflating
them produces wrong answers.

**Self-comparing steps** run both sides themselves. The feature benchmark,
parallel benchmark and scalability benchmark each build the merge base or a
named ancestor, run it alongside the change, and print a comparison table.
For these, the movement is already computed in CI against a same-hardware,
same-run baseline. Trust it.

**Absolute-value steps** measure one build in isolation. The cluster spec
sheet uploads raw per-repetition CSV rows and nothing else. A movement only
exists once you compare against earlier builds yourself, which means the
baseline is cross-run and carries hardware and scheduling noise that the
self-comparing steps do not have. Treat a spec-sheet movement as weaker
evidence than a feature-benchmark movement of the same size.

## Running it

```bash
bin/pyactivate -m materialize.buildkite_insights.perf_movements.perf_movements
```

Useful flags:

| Flag | Effect |
|---|---|
| `--pipeline nightly` | Repeatable. Defaults to all three pipelines. |
| `--branch main` | Branch to read. Defaults to `main`. |
| `--threshold 5.0` | Minimum movement in percent to report. |
| `--only regressions` | Also `improvements`, or `all` (default). |
| `--max-builds 6` | Builds considered when reconstructing a spec-sheet baseline. |
| `--min-history 3` | Earlier observations a reconstructed baseline needs. |
| `--format json` | Machine-readable rows instead of the table. |
| `--urls` | List each contributing job URL after the table. |
| `--fetch always` | Bypass the local cache. Default `auto` reuses data up to 96 hours old. |

Output is one ranked table, deteriorations first, then improvements, each side
ordered by magnitude. A row marked `WORSE!` is one where the step itself
published a regression verdict; those are always reported regardless of
`--threshold`.

### Credentials

Fetching from Buildkite needs `BUILDKITE_TOKEN` (or `BUILDKITE_CI_API_KEY`)
with read access to builds and artifacts. Without one the tool exits 1 saying
so, before any request. An expired or under-scoped token instead surfaces as the
HTTP status of the first failing request.

If no token is configured but the Buildkite MCP server is available, fetch the
input through the MCP and feed it to the same parsers, so the numbers and the
ranking stay identical. The MCP saves a job log as a JSON document
(`{"entries": [{"c": ...}]}`) even though it announces plain text; pass that file
as-is, the tool recognizes both that shape and raw text.

```bash
# a self-comparing step's job log
bin/pyactivate -m materialize.buildkite_insights.perf_movements.perf_movements \
  --from-log /path/to/job.log --step-key feature-benchmark

# spec sheet, newest build against earlier ones
bin/pyactivate -m materialize.buildkite_insights.perf_movements.perf_movements \
  --from-csv latest.cluster.csv \
  --baseline-csv build45.cluster.csv --baseline-csv build44.cluster.csv \
  --min-history 2
```

## What each pipeline contributes

| Pipeline | Step keys | Kind | Metrics |
|---|---|---|---|
| nightly | `feature-benchmark` | self-comparing | `wallclock`, `memory_mz`, `memory_clusterd` |
| nightly | `parallel-benchmark` | self-comparing | `qps`, `queries`, `avg`, `min`, `max`, `std`, `p50` through `p99_999999` |
| nightly | `scalability-benchmark-dml-dql`, `-ddl`, `-connection` | self-comparing | `tps` per concurrency |
| release-qualification | `feature-benchmark-scale-plus-one` | self-comparing | as above, at `--scale=+1` |
| release-qualification | `long-parallel-benchmark` | self-comparing | as above |
| spec-sheet | `cluster-spec-sheet-cluster`, `-source-ingestion`, `-staging` | absolute | `time_ms`, `size_bytes`, `qps` |

`feature-benchmark` runs as 12 parallel shards and `parallel-benchmark` as 5;
every shard is read, since each covers different scenarios.

## Reading the numbers

**Direction is not the sign.** A parallel-benchmark `qps` rising is good and an
`avg` rising is bad; both print as a positive change. The tool resolves this
per metric, but a human reading a raw job log must not.

**Ungated statistics have no verdict.** The parallel benchmark prints `max`,
`min`, `p99` and the deeper percentiles with empty `THRESHOLD` and `REGRESSION?`
cells. They are real measurements and are reported, but CI never fails on them,
and `max` in particular is a single worst sample.

**`slope` is not reported.** The parallel benchmark's `slope` is a
millisecond-per-second drift, so its ratio against a baseline near zero carries
no meaning. `queries` is reported and counts as higher-is-better, being the
number of queries the load phase completed.

**The step's threshold is a build gate, not a noise floor.** Feature-benchmark
thresholds run 10% to 50%. A 6% wallclock movement passes CI and is still worth
knowing about, which is why `--threshold` is the caller's floor and the step's
threshold is shown as context only.

**Reruns overwrite.** The feature benchmark reruns scenarios that regressed, so
a scenario appears in several tables in one log. The last table it appears in
is the runner's settled result; that is the one reported.

**A single spec-sheet build is not a trend.** With `--min-history 3` a key needs
three earlier observations before it is reported at all, and the baseline is the
median of those builds, so one outlier build cannot manufacture a movement.

## Steps this skill does not cover

`limits`, `limits-instance-size`, `bounded-memory`, `bounded-memory-search`,
`orchestratord-rolling-upgrade-downtime` and `cargo-bench` publish neither a
per-metric comparison nor per-repetition CSV rows, so no movement can be derived
from Buildkite alone. The spec sheet's `.cluster_object_limits.csv` is also
excluded: its headline number is a maximum healthy object count derived from the
`healthy` and `failure_mode` columns, which is a different aggregation than the
median used for the other streams. Say so when asked about these rather than
substituting a number from elsewhere.

Richer history lives in the `test_analytics` database
(`raw.test_analytics`, tables `feature_benchmark_result`,
`cluster_spec_sheet_result` and siblings, schemas in
`misc/python/materialize/test_analytics/setup/tables/`). Reading it needs a
Materialize app password for that region, which is a separate credential from
any Buildkite or CI dashboard token.

## Verifying a change to the parsers

The parsers depend on print statements in
`misc/python/materialize/feature_benchmark/report.py`,
`test/parallel-benchmark/mzcompose.py` and
`misc/python/materialize/scalability/result/scalability_change.py`. When one of
those changes, the parser goes quiet rather than loud, so the fixtures are
copied verbatim from a real build:

```bash
bin/pytest misc/python/materialize/buildkite_insights/perf_movements/perf_movements_test.py
```

An empty report from a build you know produced benchmark output means a printer
changed. Re-capture the fixture from the live log before adjusting a regex.
