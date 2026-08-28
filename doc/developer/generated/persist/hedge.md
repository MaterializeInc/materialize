---
source: src/persist/src/hedge.rs
revision: 71750da8ad
---

# `persist::hedge`

A `Blob` decorator that hedges slow `get` requests.

## Overview

Established connections to blob stores occasionally die in ways that surface only after multiple seconds, well before any client timeout fires. A `get` riding such a connection stalls everything downstream while other connections on the same process serve the same store normally. `HedgedBlob` mitigates this: if the first `get` has not completed within a configurable delay, a second request races on a connection the first cannot have poisoned, and whichever completes first wins.

Only `get` is hedged. All other `Blob` methods (`list_keys_and_metadata`, `set`, `delete`, `restore`) are forwarded to the primary handle untouched, as they have side effects or are not latency-critical.

## Key Types

**`HedgeSibling`** — Describes the relationship of the hedge handle to the primary:
- `Isolated(Arc<dyn Blob>)` — a fully separate handle with its own connection state, kept warm by a background task
- `SharedWithPrimary` — the same handle instance; used for backends with no connection state to isolate
- `Unavailable` — opening the sibling failed; hedging is disabled for this process lifetime

**`HedgedBlob`** — The `Blob` decorator. Holds the primary handle, optional hedge handle, `ConfigSet`, `BlobHedgeMetrics`, a `HedgeBudget`, and an optional background warmer task.

**`HedgeBudget`** — Controls hedge amplification with two independent guards:
1. A concurrency cap (`BLOB_HEDGED_GET_MAX_CONCURRENT`) bounds the number of in-flight hedge requests and thereby the memory held by raced gets
2. A token bucket (rate controlled by `BLOB_HEDGED_GET_BUDGET_RATIO`) bounds the long-run fraction of gets that trigger a hedge, preventing hedging from dominating traffic during sustained slowness or when large gets legitimately exceed the delay

## Dynamic Configuration

| Config | Default | Description |
|---|---|---|
| `persist_blob_hedged_get_enabled` | `false` | Master enable/disable switch |
| `persist_blob_hedged_get_delay` | 2 s | Time before a hedge fires |
| `persist_blob_hedged_get_max_concurrent` | 2 | Concurrent hedge cap |
| `persist_blob_hedged_get_budget_ratio` | 0.01 | Long-run hedge fraction |
| `persist_blob_hedged_get_warm_interval` | 20 s | Warmer ping interval (0 = disable warming) |

## Race Semantics

The primary's outcome is authoritative. The hedge is opportunistic and invisible unless it wins. Error handling:
- A fast error on the primary before the hedge delay returns immediately without firing a hedge
- A fast error on the hedge leg falls back to awaiting the primary
- If the primary fails after the hedge has fired, the hedge is given a bounded grace window (equal to the hedge delay) before the primary's error is returned
- On a hedge error with the primary still pending, the concurrency slot is released early to avoid starving other gets

## Connection Warming

When the sibling is `Isolated`, a background task (`spawn_warmer`) issues periodic concurrent liveness gets (`BLOB_GET_LIVENESS_KEY`) to keep the sibling's connection pool warm. A cold hedge can stall up to a connect timeout during the correlated connection events it is designed to absorb. The warmer idles when hedging is disabled and shuts down when `HedgedBlob` is dropped.
