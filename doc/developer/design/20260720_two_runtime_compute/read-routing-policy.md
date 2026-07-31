# Read routing policy for the interactive runtime

## Status

Open design question. Blocks narrowing the interactive runtime's read routing (see
`design.md`, "Peek placement is static"). Fold the resolution into `design.md` and
delete this note rather than letting a second planning document accumulate.

## The question

Today every peek routes to the interactive runtime unconditionally
(`mz_compute_client::multiplex::Multiplexer::send`, the `Peek` arm). Which reads
*should* it serve?

## Why unconditional routing is the wrong default

An interactive peek at time `T` cannot be answered until the shared arrangement's
published `upper` passes `T`. Until then it parks in `ComputeState::pending_work` and
waits for the publisher's seal signal (`shared_trace::PublishArrangement::adopt`'s
`on_seal`, delivered through `ArrangementSharingRegistry::note_frontier`). Sealing is
driven by the maintenance runtime's stream frontier, so the interactive runtime removes
the *serving* cost from the maintenance step loop and does not remove the dependency on
maintenance sealing the read timestamp.

That splits reads into two populations with opposite outcomes.

* A read whose timestamp is already sealed is served immediately off the shared
  arrangement. This is the whole win, and it covers stale reads, serializable reads, and
  introspection read at a lagging timestamp.
* A read whose timestamp is not yet sealed waits either way, but waiting on the
  interactive runtime is slightly *worse* than waiting on maintenance. On maintenance the
  peek is retried by the every-step `process_peeks` poll and is answered in the same step
  that advances the frontier past `T`. On interactive the wake arrives only after the
  maintenance sink advances `upper` and releases the publication lock, then crosses a
  thread boundary through a `SyncActivator`, then re-walks the arrangement. Strict
  serializable reads, which take their timestamp at the write frontier, are in this
  population by construction.

So unconditional routing makes the default isolation level marginally worse in exchange
for making stale reads dramatically better, and it pays that cost on every read rather
than only on the ones that benefit.

Unconditional routing is also what forces the collateral losses. Peek stash, the
`mz_active_peeks` and `mz_peek_durations_histogram` introspection relations, and the
`index_peek_*` histograms are all implemented on the maintenance peek path only. They are
lost not because a second runtime exists but because *every* peek now avoids the path that
has them.

## What a routing signal needs

The decision needs one bit: is `peek.timestamp` already sealed for `peek.target`? Both
runtimes answer any peek correctly, so the bit is a hint. A wrong hint costs one extra
thread hop (routed to interactive, had to wait) or one lost isolation opportunity (routed
to maintenance, was already sealed). Neither is a correctness problem, which means the
policy can be approximate and can be tuned by measurement.

## Options

### P1: multiplexer-local, from the response stream it already sees

Every `ComputeResponse::Frontiers(id, frontiers)` already flows through
`Multiplexer::filter_response`. Track `write_frontier` per non-transient id there and
route an index peek to interactive only when the tracked frontier is beyond
`peek.timestamp`.

No protocol change, no controller change, and the data needed is already passing through
the component that makes the decision.

Caveats:

* The view is per process. Each process runs its own multiplexer, and only process 0 ever
  receives a `Peek` (commands other than `Hello` and `UpdateConfiguration` go to process 0
  and reach other processes through the intra-runtime command channel). So the hint is
  computed from process 0's frontier view while the peek is answered by every worker of
  every process. On a multi-process replica the hint can be wrong in either direction.
* The tracked frontier lags by whatever the worker reporting interval is, so it is
  conservative in the common case and optimistic right after a frontier advance.

### P2: controller hint on the `Peek` command

The controller aggregates every replica's frontiers and already chooses the peek
timestamp, so it holds the only globally correct view. Add a field to `Peek` that names
the preferred runtime, and have the multiplexer honor it.

More principled than P1 and it puts the policy where the timestamp choice already lives.
Costs a versioned protocol field, which needs a default that an older replica ignores
safely during a rolling upgrade.

P2 also generalizes past the sealed bit. The controller is where an isolation-first policy
for latency-SLA reads versus a placement-for-throughput policy for bulk reads would be
expressed.

### P3: interactive-first with bounce-back

Interactive attempts the read and hands it to maintenance instead of parking when the
arrangement is not sealed at `T`. Needs no frontier prediction at all, because it observes
the real answer.

The blocker is that no route exists. The multiplexer is a client fan-out, not a bus, and
the two runtimes have separate command channels and separate `ComputeState`s, so a bounce
means a new intra-process channel plus peek bookkeeping that straddles both runtimes.

Worth recording that P3 has a second-order benefit the others lack: it keeps real traffic
on the maintenance peek path, which directly counters the "the capability we lean on will
atrophy" argument in `design.md`.

### P4: policy by workload class or read SLA

Orthogonal to P1 through P3 and layers on top of any of them. Deferred.

## Recommendation

Adopt P1 first, because it is a change to one component with no protocol surface, and
measure how often its hint is wrong on a multi-process replica. Move to P2 if the
process-local view proves too weak. Hold P3 for the case where prediction turns out not to
be good enough, and note that it is the option that best preserves reversibility.

**Default to maintenance, not interactive.** Route to interactive only on a positive hint.
This keeps the maintenance peek path exercised by real traffic, which is what makes the
feature reversible per read rather than per replica, and it means peek stash and the peek
introspection relations keep working for the reads that use them.

Gate the policy on a dyncfg with three settings so the rollout is stepwise and the revert
is a config change rather than a deploy.

* `maintenance`: every peek to maintenance. Equivalent to the feature being off for reads,
  while the interactive runtime still exists and serves nothing.
* `hinted`: the policy above. The intended steady state.
* `interactive`: every peek to interactive. Today's behavior, useful for measurement.

## Also applies to the slow path

`CreateDataflow` routing uses the bounded-read predicate, not a frontier signal, so a
transient query dataflow whose `as_of` is not yet sealed sits on the interactive runtime
holding a shared import until maintenance seals it. The same hint applies, and the
multiplexer likewise already has the dataflow's `as_of` in hand. Decide both together, so
a peek and the query dataflow feeding it do not land on different sides of the policy.

## Open questions

* How stale is the tracked frontier in practice, and how often does P1's hint disagree with
  ground truth on a multi-process replica?
* If a hinted-interactive read turns out to produce a result above the peek stash
  threshold, the interactive path errors today rather than stashing. Either the stash works
  on both paths or the policy has to exclude stash-eligible reads, which is a per-peek
  property (`RowSetFinishing::is_streamable`) available at routing time.
* Should introspection reads be forced to interactive regardless of the hint? They are the
  motivating case, they are explicitly tolerated to be stale, and they are exactly the
  reads that must not queue behind a saturated maintenance runtime.
