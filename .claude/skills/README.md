# Claude Code Skills for Materialize

This directory contains skills that teach Claude Code how to work effectively in
the Materialize codebase. Skills are triggered automatically based on context, or
can be invoked explicitly with `/<skill-name>`.

## Development Workflow

| Skill | Trigger | What it does |
|---|---|---|
| **mz-run** | Compiling, running, formatting, linting | How to build and run Materialize locally, including `cargo`, `bin/fmt`, `bin/lint`, log filters, and jemalloc setup |
| **mz-test** | Running or writing tests | Guides for every test framework: unit tests, sqllogictest, testdrive, pgtest, and mzcompose |
| **mz-commit** | Committing, creating PRs | Pre-commit checklist, PR title conventions, and git workflows |
| **mz-pr-review** | Reviewing code | Local code review against Materialize standards for tests, style, architecture, and polish |
| **mz-debug-ci** | CI failures on a PR | Investigates Buildkite failures using `gh` and `bk` CLI tools to identify root causes |

## Performance

| Skill | Trigger | What it does |
|---|---|---|
| **mz-benchmark** | Adding or debugging benchmarks | Three frameworks: Feature Benchmark (micro-benchmarks), Scalability Test (throughput under concurrency), and Parallel Benchmark (sustained performance) |
| **mz-profile** | Profiling, slow queries, high memory | CPU profiling with samply, memory profiling with heaptrack, and binary size analysis |
| **mz-query-tracing** | Tracing query execution time | Distributed tracing with OpenTelemetry/Tempo to understand where time goes in SQL statement execution |

## Test Frameworks

| Skill | Trigger | What it does |
|---|---|---|
| **mz-platform-checks** | Writing upgrade/restart checks | "Write once, run everywhere" framework for testing feature survival across restarts and upgrades |
| **mz-parallel-workload** | Concurrent stress testing | Runs random SQL actions concurrently to catch panics and unexpected errors |
| **mz-limits-test** | Stress-testing with many objects | Catches regressions (panics, stack overflows, OOMs) when scaling to large numbers of tables, views, sources, etc. |

## Architecture

| Skill | Trigger | What it does |
|---|---|---|
| **mz-adapter-guide** | Working on adapter, coordinator, pgwire | Correctness invariants and architectural notes for the adapter layer, timestamp oracle, peek paths, and related crates |
