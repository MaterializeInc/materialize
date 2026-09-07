---
source: src/compute/src/compute_state/peek_offload.rs
revision: 82e054569f
---

# mz-compute::compute_state::peek_offload

Drives an index peek's walk away from the timely worker that owns it.

An offloaded walk steps its `IndexPeekScan` on the blocking pool. It returns to its async task only to write a batch to the peek stash or to deliver the final answer. The scan and the `WalkPermit` that admitted it travel together; whatever way the walk ends, including a panic, drops the two together.

## PeekPermits

`PeekPermits` bounds the walks that run concurrently across all workers sharing the instance. It wraps a `Semaphore` and a `Bound` (granted count and target) protected by one `Mutex`, so a resize computing its delta and a release deciding whether to return its permit see one consistent state.

`PeekPermits::new` initializes with one permit per worker. `resize` adjusts the bound to what the configured `INDEX_PEEK_PERMIT_FRACTION` fraction of workers asks for: raising it issues new permits immediately; lowering it takes free permits back and marks the remainder to be forgotten as held walks finish.

`WalkPermit` holds one `OwnedSemaphorePermit`. Its `Drop` calls `absorb_release`: if the bound has shrunk below the number of issued permits, the permit is forgotten rather than returned, converging the semaphore toward the lower target without interrupting running walks.

## OffloadedPeek

`OffloadedPeek::start` spawns the async walk task and returns immediately. The task:
1. Waits for a permit, watching for cancellation (a dropped result receiver).
2. Drives the scan on the blocking pool via `step_until_blocked` in a loop.
3. Writes full batches to the peek stash via `StashUpload::push` as the scan hands them over.
4. On scan completion, builds the `PeekResponse` and sends it through a `oneshot` channel.
5. Unparks the timely worker thread so the sweep picks up the answer.

`WalkState` holds the scan, the permit, and the result sender together so an aborted task cannot separate them.

## OffloadConfig

`OffloadConfig` holds `ConfigValHandle` handles on `INDEX_PEEK_YIELD_GRANULARITY`, `PEEK_RESPONSE_STASH_BATCH_MAX_RUNS`, `INDEX_PEEK_PERMIT_FRACTION`, and the row-iteration config. Handles are read at each slice boundary rather than once at hand-off, so a configuration change reaches a walk already under way.

`step_until_blocked` reads `yield_granularity` and `row_iteration_limit` at each iteration, checks for cancellation (closed result channel), and steps the scan until it ends, offers a batch, or the granularity is exhausted. A granularity of zero is clamped to one so the walk always makes forward progress.
