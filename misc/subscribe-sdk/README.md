# Materialize Subscribe SDK

Correct-by-construction clients for consuming Materialize
[`SUBSCRIBE`](https://materialize.com/docs/sql/subscribe/) and building external
sinks on top of it.

Consuming `SUBSCRIBE` durably is subtle, and the failure modes are invisible in
testing. These SDKs encode the protocol so callers cannot get it wrong.

> **Status: initial scaffolding.** This directory currently contains the tested
> protocol core and the client entry points for Rust and Python. It is expected
> to graduate to a standalone repository; it lives here under `misc/` as its own
> isolated workspace so relocating it is a move, not a rewrite. The full design
> lives in the Subscribe SDK design doc under `doc/developer/design/` (companion
> PR).

## Layout

| Path      | What                                                              |
| --------- | ----------------------------------------------------------------- |
| `rust/`   | `mz-subscribe` crate (its own Cargo workspace)                    |
| `python/` | `materialize-subscribe` package                                   |

Each language is a native implementation. They share one behavior, verified by
mirrored test suites, and (deliberately) produce **byte-identical resume
tokens**: a token minted by one SDK resumes correctly in the other.

## The protocol these encode

The durable-subscription protocol, and where each piece lives:

1. Subscribe `WITH (PROGRESS, SNAPSHOT = true)`. — the statement builder always
   requests `PROGRESS`.
2. Buffer updates until a progress message proves a timestamp is closed. — the
   batcher.
3. Apply the closed batch and persist its frontier atomically. — the consumer,
   using the batch's resume token.
4. On restart, resume `WITH (PROGRESS, SNAPSHOT = false) AS OF frontier - 1`. —
   the resume token owns the `- 1`; callers never compute a timestamp.
5. Keep `RETAIN HISTORY` on the subscribed object wider than downtime, and
   handle the compaction-horizon error otherwise. — a typed error.

Applying each batch and persisting its token in one transaction yields
exactly-once **state**: after any crash, resuming from the last token neither
drops nor duplicates data.

## Layers and cohorts

The client is three layers, each usable on its own.

1. The raw decoded stream (`subscribe_raw`): timestamped changes and progress
   markers, before any consistency policy. This is the composable substrate.
2. The consistency engine: buffer changes and release everything below a
   frontier, consolidated to the net effect within the released batch.
3. The closed unit handed to the consumer: a consistent batch for one view, or a
   consistent moment for a cohort of many.

A **cohort** subscribes to several views at once and releases them only up to
the *minimum* closed frontier across all of them, which is the latest instant
final for every view together. A downstream store therefore moves from one
cross-view-consistent state to the next, never a mix of a newer view with a
staler one. A single view is the cohort of one: the same engine, with one
member.

The transport drains each subscription continuously into a bounded client-side
buffer. Materialize buffers unread subscribe output without bound, so a consumer
that falls behind fails loud (a buffer-overflow error) rather than pushing that
cost onto the server. The client falls over, never the server.

A cohort carries a second bound. Consistency means a stalled member pins the
joint frontier, so its peers' changes would buffer without limit while waiting
for it. A lag budget caps the total buffered across the cohort and fails loud
(a cohort-lag error) when a laggard falls too far behind, rather than growing
memory unbounded.

## Building and testing

Rust:

```
cd rust
cargo test
cargo clippy --all-targets -- -D warnings
```

Python (from `python/`):

```
python -m venv .venv && . .venv/bin/activate
pip install -e '.[dev,client]'
pytest
mypy materialize_subscribe
ruff check .
```

The unit suites need no database; they exercise the protocol core against
synthetic streams. Live smoke tests are gated behind a connection string
(`MZ_SUBSCRIBE_TEST_DSN` for Rust; a running Materialize for Python) and are not
run by default.
