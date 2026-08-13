# Read holds across two compute runtimes

Status: the diagnosis below stands and the mechanism is superseded. See
[broadcast-compaction.md](broadcast-compaction.md).

The invariant (I1), the split into I1a and I1b, the gap analysis and the requirement that
the direct dependency be the mechanism are all still current, and the newer document does
not restate them. What changed is the answer. Everything here reconstructs the ordering a
single command stream provided (I1b) by adding a hold protocol on top of the routing that
lost it. Sending compaction to both runtimes restores that ordering instead, which needs
no acquisition, no release and no reclaim, and makes the epoch boundary (G2) vacuous
rather than solving it.

Read the rest of this document for why a retroactive per-dataflow hold was tried and what
each attempt cost, which is what makes the simpler mechanism defensible rather than
merely simpler. Steps 0 through 4 of the sequence at the end were implemented and have
been deleted, so the code they describe no longer exists. Every hold command, module and
registry method named below is gone.

## The invariant

**I1.** For a dataflow `D` at `as_of X` importing collection `I`, the replica's
trace for `I` must satisfy `since <= X` for as long as `D` may read it.

Single-runtime this holds twice over. **I1a**: the controller keeps a read hold on
`I` covering every reader, so it never sends `AllowCompaction(I, F > X)`. **I1b**:
one totally ordered command stream means the replica renders `D` before it applies
any compaction that follows the create.

Splitting compute into a maintenance and an interactive runtime keeps I1a and
loses I1b: `CreateDataflow(D)` reaches only interactive, `AllowCompaction(I)` only
maintenance, and the two drain independently.

## What we require, and why the current answer is not it

The requirement is that **the direct dependency is the mechanism**. A read at `X`
must be protected by a hold derived from `D`'s import of `I`, not by the
controller's per-dataflow bookkeeping. I1a is a global floor and the controller's
job; it must not be what makes an individual read correct.

Today it is. Reader holds registered on the interactive side are funnelled into a
single `TraceAgent` on the maintenance side, and `TraceAgent::set_logical_compaction`
joins, so that agent only ratchets up. Once it has advanced past `X`, a reader at
`X` cannot be represented, and its read survives only because
`meet(publisher agent, TraceManager agent)` is still low, i.e. because of I1a.

## Gaps

**G1, per process.** Non-lifecycle commands reach process 0 only and each runtime
re-broadcasts to its own workers independently. Process 0's multiplexer state
therefore says nothing about process 1, so processes at index 1 and above have no
cross-runtime ordering at all. No amount of state in the multiplexer can fix this,
because the multiplexer does not exist on the path that orders process 1.

**G2, per connection.** The multiplexer's cap is per-connection state discarded on
`Hello`. A differently-nonced command is stashed rather than dropped, so commands
queued ahead of the `Hello` still execute. Replayed uncapped compaction, plus the
compactions reconciliation synthesizes locally and which never traverse the
multiplexer, can therefore land while interactive still has an old-epoch
`CreateDataflow` queued.

**G3, alias closure. DISSOLVED.** With a cap, `reexport` installing one publication
point under a second id meant a hold recorded against the first did not cap
`AllowCompaction` for the second. A real hold is keyed by arrangement rather than by id:
`ArrangementFlavor::Trace` installs a clone of the same `TraceBundle` under the second
id, so both ids' handles are agents on one `TraceBox` and share one publication point. A
hold on either pins the arrangement both name, and compaction of the other only advances
that other agent, which the box's meet absorbs.

**G4, retirement signal.** The cap retires when interactive reports a frontier for
the dataflow. Subscribe and copy-to collections are excluded from frontier
reporting, and from the drop path that would otherwise emit a terminal report, so
for those the cap would never retire and the imported collection's own deferred
drop would never flush.

**G5, representation.** Even ordered correctly, a reader's hold must be
*representable* in the trace. See the ratchet above.

## Design

### The asymmetry the design rests on

**Acquiring a hold must be ordered against compaction. Downgrading and releasing
one need not be.** Being late to downgrade only delays compaction, which is safe.
Being late to acquire means the compaction already happened, which is not
recoverable: times coalesced by a completed merge cannot be un-coalesced.

So acquisition goes on the command stream that orders compaction, and downgrade and
release stay local. This is also what keeps the change small: the intra-process
mechanism for downgrade already exists.

### Acquisition, on maintenance's stream

When the multiplexer sees `CreateDataflow(D, as_of = X, imports = Is)` routed to
interactive, it also emits to maintenance

    AcquireHolds { holder: E, ids: index_imports(D), as_of: X }

one per export `E` of `D`, because the drop that releases a hold is per export and a
dataflow's exports may drop at different times. Index imports only: a source import is
served from persist and carries its own read hold.

The ordering that matters is **entirely within maintenance's own stream**:
`AcquireHolds` precedes the later `AllowCompaction(I, F)` there, because the
multiplexer sees the create before the compaction and emits to maintenance in that
order. Maintenance's stream is ordered and is re-broadcast per runtime to every
process, so every process installs the hold before it applies that compaction.
That restores I1b **per process**, which closes G1. G3 needs nothing, see above.

Nothing here assumes an ordering between the two runtimes' protocols, and none
exists. In particular the order in which the multiplexer emits to interactive is
irrelevant: it could forward the create first. Interactive's stream does not enter
the argument at all, which is what makes the guarantee hold when interactive is
arbitrarily behind, or never processes the create.

Maintenance installs a real hold per id: a `TraceAgent` clone taken from its own
handle for that collection. Minting it there is what makes G5 tractable. The clone
inherits the frontier of the agent it is cloned from, and maintenance's own handle
sits at the controller's frontier, which by I1a is at or below every `as_of` the
controller may offer. So the clone starts low enough to be set to `X`, which is
exactly what the publisher's single ratcheting agent cannot do.

`TraceAgent` is `!Send`, so this can only happen on the maintenance worker. That
is not a limitation to work around, it is why the acquisition has to be a command
rather than something the interactive side does for itself.

### Interactive never waits, and maintenance lag cannot expose a read

Interactive registers its hold in the publication point's shared state under the
mutex and proceeds. It confirms nothing with maintenance, so there is no handshake
and no coupling of interactive's latency to maintenance's step rate. A design where
interactive blocked until the hold was confirmed would reintroduce exactly that
coupling, and is unnecessary because the ordering lives on maintenance's stream
rather than between the runtimes.

Maintenance being arbitrarily behind is also not an exposure. The trace's `since`
advances only when maintenance *applies* an `AllowCompaction`, which requires
maintenance to step, and `AcquireHolds` sits ahead of that compaction in the same
queue. A backlog delays both equally, so there is no interleaving in which
maintenance has compacted but not yet acquired. This is strictly better than a
response-triggered cap, which does couple to interactive's progress.

`handle_at`'s gate on the published `since` becomes belt-and-braces under this
design rather than load-bearing. That matters because the gate reads a value the
publisher operator republishes on its own activations, not one maintained by
command handling, so it can be stale-low after a compaction has been applied. The
hole exists today. It stops being reachable here, since the trace cannot have
compacted past `as_of` at all, but the gate is not the reason.

### Downgrade, intra-process

The acquired hold follows the publication point's reader registrations, floored at its
own `as_of`, re-evaluated on the maintenance runtime's maintenance tick. The floor is
what lets this work without attributing registrations to holders: the meet is at or
below every registration, so flooring it cannot carry a hold past its own reader. A
holder that lags therefore delays another's downgrade, which costs retained history and
not correctness.

Following is not optional. A hold frozen at its `as_of` for the dataflow's life is a
permanent pin, and an interactive `SUBSCRIBE` lives as long as its client.

### Release

`ReleaseHolds { holder }` goes to the runtime that *renders* the holder, so it is
ordered behind that holder's own create and drop there. The multiplexer emits it when
the holder's `AllowCompaction` reaches the empty frontier, after forwarding that drop.
The rendering runtime records it into the per-process sharing registry, and the owning
runtime reclaims from there on its maintenance tick.

That asymmetry against acquisition is load-bearing and the model forced it. A release on
the owning runtime's stream can overtake a create the rendering runtime has not
processed, so the owning runtime would apply acquire, release and compaction while the
dataflow was still queued.

Because the release is a command the controller's drop derives, and not a response, G4
dissolves: nothing depends on a frontier report that subscribe collections do not emit.

A release recorded before the matching acquisition is applied, which the two independent
streams allow, is consumed by that acquisition, which then installs nothing.

### Epoch

Not solved. Both replicas discard their command holds and their release records at
reconnection, and the controller replays the creates the multiplexer re-derives the
holds from. That is conservative rather than correct: reconciliation synthesizes
compactions locally that never traverse the multiplexer, so one can apply between the
discard and the re-derived acquisition. G2 stays open, and step 5 is where it is
addressed.

## What this deletes

The multiplexer's cap, `hold_floor`, `deferred_compaction`, `compaction_floor`,
`pending_compaction`, the retire-on-response trigger, and `reset`. Together with
them go the per-query `compaction_floor` leak, the `recv`-performs-`send` cancel
safety dependency, and the incomparable-antichain comparator. The multiplexer goes
back to being a router.

On the replica side, `compaction_target` and its zero-holds fallback go, because
`TraceBox` computes the meet natively across agents. The published `since` is still
needed for the import-time gate.

## Open questions for the model

These are not stylistic. Each has more than one defensible answer and the wrong
one reintroduces a gap.

1. **When may an unadopted acquired hold be released?** Settled: on the explicit
   release, which reaches the rendering runtime whether or not that runtime ever
   built the dataflow. Cancelled and refused creates are covered because the release
   is ordered after the create on that runtime's stream, so "the create will never be
   processed" and "the reader has finished" both present as "no registrations now".
2. **Is handing off from the acquired hold to the registration's hold safe, or
   should the acquired hold persist for the dataflow's life?** Settled: neither.
   Handing off is unsafe, because at handoff the publisher's agent may already sit
   above the registration's frontier and then nothing represents it. Persisting is a
   permanent pin. The hold instead follows the readers, floored at its `as_of`.
3. **Does the per-registration agent need a floor agent alongside it?** Moot. There
   is no per-registration agent and no handoff, so the acquired hold is the floor
   throughout, and the interval with no floor cannot arise.
4. **What must hold for `alias_closure` to be correct under a re-export that
   appears after the hold is acquired?**
5. **Does the design still hold when the interactive runtime spans processes and a
   given import is served pairwise per worker ordinal?**

## Model

TLA+, checked with TLC, in `protocol-holds/`. The model must be parameterised by
process count and must include the epoch boundary with stashed commands, since G1
and G2 are the two failures a single-process single-connection model cannot express.

Properties:

* **I1**, per process.
* **No regression** of compaction frontiers.
* **No permanent pin**: once no holder needs a collection, its compaction can
  proceed.
* **No cross-protocol ordering is assumed.** The two runtimes' queues must be
  allowed to drain in fully independent orders, including interactive processing a
  create arbitrarily late or never, and I1 must still hold. A model that
  accidentally couples them would validate a design that cannot be built.
* **Maintenance lag never exposes a read.** If maintenance on some process has
  applied a compaction for `I` beyond `X`, it has already applied an `AcquireHolds`
  covering `X`. This is the claim the equal-delay argument above rests on, and it is
  the kind of claim that has been wrong often enough in this work to be worth
  checking rather than asserting.

The existing Lean model in `protocol/` stays as it is. It certifies the
single-process core and runs in CI, and its habit of parameterising the step
relation by each retired behaviour is worth keeping. It is the wrong tool for this
question: it certifies a fix rather than searching for the next defect, and the
findings that produced this document were all interleavings nobody had imagined.

## Implementation sequence

Steps 0 to 4 are done (`9babb21b5b`, `cb496355e0`, `4198ec1e77`). Only step 5 remains.
The grouping was forced: step 1 without step 2 leaks a hold, and step 4 is only safe
once synthesis exists, so 3 and 4 landed together.

The mechanism is live rather than inert as of step 3, and `enable_two_runtime_compute`
defaults on in the CI system parameters, so every mzcompose test exercises it. The first
interactive dataflow to acquire holds in any session is a system-catalog query over the
introspection indexes, not a user query.

0. **Command vocabulary.** `ComputeCommand::AcquireHolds`/`ReleaseHolds`, boxed,
   plus the four exhaustive matches they touch. Nothing emits them and the compute
   side panics on receipt, so no behaviour changed. `reduce` treats them as
   unreachable because the multiplexer synthesizes them rather than the controller
   issuing them.
1. **Maintenance-side `AcquireHolds`.** Done. For each id, clone that collection's
   `TraceBundle` handles and pin them at `as_of`, keyed by holder. The clone base
   matters: `compute_state.traces` sits at the controller's frontier, which by I1a
   is at or below every `as_of` the controller may offer, so a clone of it can be
   set to `as_of`. A clone of the publisher's own agent cannot, because that agent
   has ratcheted (G5).

   Two things this needed that were not written down. The pin has to be *published*
   as well as taken: `since` is what a reader gates on, and a pin the publication
   point does not know about leaves `since` at the writer's frontier, so `handle_at`
   refuses the very reader the pin was for. `since` is therefore derived from the
   publisher-driven part and the recorded pins together, recomputed by whichever
   side moves, rather than assigned once per publisher activation. And the clone's
   *physical* hold, inherited from the base by `TraceAgent::clone`, is released at
   acquisition. Both setters join, so a physical hold kept here could never be
   lowered again and would pin batch granularity for the hold's whole life.
2. **Release.** Done, and not via `everRegistered`. The hold follows its reader and
   is dropped on an explicit `ReleaseHolds`, recorded by the rendering runtime into
   the per-process registry for the owning runtime to act on.

   Persisting the acquired hold at `as_of` for the dataflow's life, which is what
   step 1 alone amounts to, is a permanent pin: an interactive `SUBSCRIBE` lives as
   long as its client, so its index would never compact again. That is the same
   unbounded-growth failure as a reader hold nobody downgrades. So the acquired hold
   downgrades to the meet of the publication point's reader registrations, floored at
   its own `as_of`. The floor is what makes this work without attributing
   registrations to holders: the meet is at or below every registration, so flooring
   it cannot carry a hold past its own reader. A holder that lags therefore delays
   another's downgrade, which costs retained history and not correctness.

   This also answers open question 2 below: handing off to the registration and
   releasing the acquired hold is **unsafe**. At handoff the publisher's agent may
   sit above the registration's frontier, and then nothing represents it.

   `everRegistered` is not needed because the explicit release carries directly what
   that marker was inferring. It is ordered after the holder's own drop on the
   rendering runtime's stream, so by the time it is recorded the registrations are
   gone. A release recorded before the matching acquisition is applied, which the two
   independent streams allow, is consumed by that acquisition, which then installs
   nothing.
3. **Multiplexer synthesis.** Done. One `AcquireHolds` per export of an interactive
   dataflow, naming its *index* imports, emitted to maintenance before the create is
   forwarded to interactive. Per export rather than per dataflow because the drop that
   releases a hold is per export, and a dataflow's exports may drop at different
   times. Naming index imports rather than `import_ids()` because a source import has
   no trace to pin.

   **No alias closure is needed, and G3 dissolves.** That gap was an artifact of the
   cap being keyed by collection id. A `Trace` re-export installs a *clone of the same
   `TraceBundle`* under the second id (`render.rs`, `ArrangementFlavor::Trace`), so
   both ids' handles are agents on one `TraceBox`, and `reexport` shares the one
   publication point. A hold on either id therefore pins the arrangement both ids
   name, and `AllowCompaction` on the other only advances that other agent, which the
   box's meet absorbs. Holds are keyed by arrangement where the cap was keyed by id.
4. **Delete the cap.** Done. `hold_floor`, `deferred_compaction`, `compaction_floor`,
   `pending_compaction`, the retire-on-response trigger and `reset`. This is what
   pays down the debt: it removes the per-query `compaction_floor` leak, the
   `recv`-performs-`send` cancel-safety dependency, and `reset`'s epoch exposure,
   by removing the code that has them.
5. **Then model G2**, with epoch-scoped holder identity, and fold `CtlDrop` into
   `CtlCompact(export, empty)` so the export becomes a compactable id like any
   other. The multiplexer routes on the id and treats export and import
   differently, so a routing or ordering bug between them can only surface if both
   are the same command class in the model.

Known defects this sequence does not address, tracked separately: the subscribe
retirement signal (G4) is dissolved by step 2 rather than fixed, since nothing
depends on a response any more; `hold_floor`'s incomparable-antichain comparator
disappears with step 4; the publisher's single-agent ratchet (G5) is addressed by
step 1 only for holds acquired through the command, not for registrations the
interactive runtime makes on its own.

The epoch boundary is handled conservatively rather than correctly, pending step 5.
A replica drops its command holds and its release records at reconnection, since the
streams that would release them are being reset and the controller replays the creates
the multiplexer re-derives the holds from. Release records are cleared in that
direction on purpose: a record outliving its connection would be consumed by the next
connection's acquisition for the same holder, which would then install nothing and
leave that reader unprotected, whereas a record lost the other way only leaks a hold
until the following reconnection. This does not close G2, because reconciliation
synthesizes compactions locally that never traverse the multiplexer.
