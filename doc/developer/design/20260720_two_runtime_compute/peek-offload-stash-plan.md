# Bringing the persist stash to the offloaded walk

## Why

E1 measured the offload cutting point-lookup tail latency 32-fold behind a 2.2 second scan.
That measurement required `enable_compute_peek_response_stash = false` for the environment.
With the stash on, which is how production runs, `should_offload_peek` declines every peek the stash could take, and the win is unreachable.

The gate is one branch.

```rust
fn should_offload_peek(&self, peek_stash_usable: bool) -> bool {
    if peek_stash_usable {
        return false;
    }
```

`peek_stash_usable` is a capability rather than a prediction.
It is `finishing.is_streamable(arity) && ENABLE_PEEK_RESPONSE_STASH && location_available`, with no reference to result size, so a peek returning three rows declines the offload over a divergence that will never happen.
The reachable domain today is peeks with an `ORDER BY` or a non-identity projection, which is not the traffic the offload was built for.

## What the two mechanisms actually do

They are usually described as alternatives.
They are not, and seeing that is what makes the fix small.

| | Walk runs on | Response path | Fixes |
|---|---|---|---|
| Peek response stash | the timely worker | persist, uploaded by an async task fed over a channel | response size |
| Index peek offload | a blocking task | inline, returned whole | head-of-line blocking |

The stash never moved the walk.
`pump_rows` keeps stepping the row iterator on the worker thread and pushes batches into `rows_tx`, and the comment on that field says exactly why: a trace cursor is not `Send`, so the iterator cannot be handed to the upload task.

That constraint is the one the offload has already solved.

## The two facts that make this cheap

Both are already in the tree, neither needs changing.

* `StashingPeek::start_upload` takes `Box<dyn Iterator<Item = Result<(Row, NonZeroI64), String>>>`.
  It is already independent of which trace produced the rows, because the maintenance path walks a `TraceBundle` and the interactive path a registry handle.
* `spawn_offloaded_walk` is bounded on `TraceCursor<OksTr>: Send` and `TraceStorage<OksTr>: Send`, and `OffloadSnapshot::Ready` carries owned cursors and storage.
  The offloaded walk's row source crosses threads by construction.

So on the offload path the reason `pump_rows` exists does not apply.
An offloaded walk can own the persist writer directly and stream into it as it walks, with no channel hop and no worker involvement at all.
Unifying these makes the stash simpler on the path that matters, not more complex.

## Plan

### Phase 1: let the offloaded walk stash

Pass the stash configuration into `spawn_offloaded_walk`: eligibility, threshold, persist location, and `batch_max_runs`.
The task walks its snapshot, accumulates as it does today, and on crossing the threshold opens the upload and streams the remainder into it.
`IndexOffloadPeek` then resolves to either a rows response or a stashed response, and `PendingPeek::IndexOffload` retires both the same way it retires a rows response now.

The mid-walk divergence that `PeekStatus::UsePeekStash` expresses as a return value becomes an ordinary branch inside the task.
Nothing has to be communicated back to the worker to make the decision, which is what forced the gate in the first place.

### Phase 2: delete the gate

`should_offload_peek` stops consulting `peek_stash_usable` and keeps only the in-flight cap.
The offload becomes reachable for ordinary streamable peeks, which is where the measured win lives.

Phases 1 and 2 land together behind `enable_index_peek_offload`, which is already off by default in production.

### Phase 3: demote the worker-pumped stash to a fallback

The inline stash still has to exist, because the offload declines in cases the stash does not.
Those are the in-flight cap being reached, `OffloadSnapshot::NotReady`, and any trace whose cursor cannot produce a `Send` snapshot.
This is a demotion rather than a deletion, and it should wait until E4 has been re-run, because the cap is what decides how often the fallback is taken.

## What to be careful about

**Compaction holds get longer.**
An offloaded walk pins its batches for the duration of the walk.
A stashing walk pins them for the walk plus the upload, which is seconds rather than milliseconds.
`index_peek_offload_max_inflight` now bounds a much longer-lived hold, and its interaction with the controller read holds and the capping in the multiplexer needs checking before the cap default is trusted.

**The finishing rule must not drift.**
`is_streamable` decides whether rows can be shipped as they are produced.
A non-streamable finishing has to accumulate and sort before anything is emitted, and the offloaded path must apply the same rule rather than assuming it can always stream.

**The size limit must stay identical.**
`max_result_size` is checked during the inline walk.
If the offloaded walk checks it differently, a peek that errors on one substrate succeeds on the other, which is a correctness difference visible to users.
The existing test asserting the offloaded walk returns exactly what the inline walk returns is the right place to extend.

**Backpressure gets simpler, not harder.**
Today the bounded `rows_tx` channel throttles the worker against the upload.
With walk and upload on one task it becomes an ordinary await, and the channel and its capacity constant can go.

## Alternative considered

Let the offloaded walk abandon on divergence and re-run inline.
Rejected.
It walks the data twice, and the restart lands on the timely worker exactly when the result is large, which is precisely the case where blocking the worker hurts most.
It would reintroduce the tail the offload exists to remove, at the worst moment.

## Validation

* Re-run E1 with the stash left on.
  The flat tail has to survive, since surviving a production configuration is the entire point of the work.
* Confirm engagement through `mz_index_peek_walks_total{substrate}` rather than inferring it from latency.
* Extend the offloaded-equals-inline walk test to cover a result large enough to cross the stash threshold, on both cursor sources.
* Re-run E4, because the cap now governs upload concurrency and compaction hold duration as well as walk concurrency.
