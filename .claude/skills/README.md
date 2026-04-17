# Claude Code Skills for Materialize

This directory contains skills that teach Claude Code how to work effectively in
the Materialize codebase. Skills are triggered automatically based on their
SKILL.md description, or can be invoked explicitly with `/<skill-name>`.

> **Note:** This README is for humans browsing the repo. Claude Code does not
> read this file when deciding which skills to trigger -- it uses the `name` and
> `description` fields in each skill's SKILL.md frontmatter.

## Console

| Skill | Trigger | What it does |
|---|---|---|
| **mz-console-papercut** | Papercut issues, CNS-* references | Pulls a papercut from Linear, fixes it using existing codebase patterns, verifies in browser, and opens a PR |

## Development Workflow

| Skill | When to use | What it does |
|---|---|---|
| **mz-run** | Compiling, running locally, formatting, linting | Build and run Materialize locally (`cargo check`, `bin/environmentd`, `bin/fmt`, `bin/lint`, log filters, jemalloc) |
| **mz-test** | Running or writing tests, choosing a test framework | General testing guide: unit tests, sqllogictest, testdrive, pgtest, mzcompose, reproducing bugs. Points to dedicated skills for specific frameworks |
| **mz-commit** | Committing, creating PRs, pushing | Pre-commit checklist, PR title conventions, Cargo.lock discipline, git workflows |
| **mz-pr-review** | Reviewing code changes | Local code review against Materialize standards for tests, style, error messages, architecture, and polish |
| **mz-debug-ci** | CI failures, red builds, Buildkite issues | Investigates Buildkite failures using `gh` and `bk` CLI tools to identify root causes |

## Performance

| Skill | When to use | What it does |
|---|---|---|
| **mz-benchmark** | Adding or debugging benchmark scenarios | Three measurement frameworks: Feature Benchmark (micro-benchmarks), Scalability Test (throughput under concurrency), Parallel Benchmark (sustained latency) |
| **mz-profile** | Something is slow or using too much memory | CPU profiling with samply, memory profiling with heaptrack, binary size analysis |
| **mz-query-tracing** | Understanding where time goes in SQL execution | Distributed tracing with OpenTelemetry/Tempo to get a latency breakdown of any SQL statement |

## Specialized Test Frameworks

These skills provide deep guidance for specific test frameworks. For help
choosing which framework to use, start with **mz-test**.

| Skill | When to use | What it does |
|---|---|---|
| **mz-platform-checks** | Testing feature survival across restarts/upgrades | Write Check classes with initialize/manipulate/validate phases that run across upgrade and restart scenarios |
| **mz-parallel-workload** | Testing for panics under concurrent SQL | Extend the stress-testing framework that runs random SQL actions concurrently to catch panics and unexpected errors |
| **mz-limits-test** | Stress-testing with large numbers of objects | Add Generator subclasses that scale tables, views, sources, etc. to catch panics, stack overflows, and OOMs |

## Architecture

| Skill | When to use | What it does |
|---|---|---|
| **mz-adapter-guide** | Working on or asking about the adapter layer | Correctness invariants and architectural notes for the coordinator, pgwire, peek paths, timestamp oracle, and related crates |
