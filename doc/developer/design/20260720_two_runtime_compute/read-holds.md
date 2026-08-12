# Read holds across two compute runtimes

Status: design, not implemented. The open questions in the last section must be
settled by the model before code is written.

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

**G3, alias closure.** `reexport` installs one publication point under a second
id. A hold recorded against the first does not cap `AllowCompaction` for the
second, so the shared `since` can be driven past a held `as_of` by a command the
multiplexer has no reason to cap.

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

    AcquireHolds { holder: D, ids: alias_closure(Is), as_of: X, nonce: N }

The ordering that matters is **entirely within maintenance's own stream**:
`AcquireHolds` precedes the later `AllowCompaction(I, F)` there, because the
multiplexer sees the create before the compaction and emits to maintenance in that
order. Maintenance's stream is ordered and is re-broadcast per runtime to every
process, so every process installs the hold before it applies that compaction.
That restores I1b **per process**, which closes G1. `alias_closure` closes G3.

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

### Downgrade and release, intra-process

An interactive import already writes its hold into the publication point's shared
state, and already follows the frontier it has acknowledged. The publisher, running
on the maintenance worker, forwards each registration's hold into that
registration's own agent rather than forwarding a single meet into a single agent.

Release is the same channel: when a registration disappears from the shared state,
the publisher drops the corresponding agent.

No command is needed for either, so G4 dissolves rather than being solved: nothing
depends on a response that subscribe collections do not emit.

### Epoch

Holds carry the connection nonce. Maintenance drops holds from an older nonce when
it processes the nonce change, in stream order, so there is no window of the G2
kind. The multiplexer keeps no state that has to survive a reconnection, which is
what made G2 possible.

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

1. **When may an unadopted acquired hold be released?** If interactive never
   renders the dataflow, because it was cancelled or reconciliation refused it, the
   acquired hold has no registration to hand off to. Candidates: release on the
   holder's drop reaching the multiplexer, which races a create still queued on
   interactive; release only once the publisher has observed a registration for the
   holder appear and then disappear, which leaks when the create never arrives;
   release on nonce change only, which leaks for the connection's lifetime.
2. **Is handing off from the acquired hold to the registration's hold safe, or
   should the acquired hold persist for the dataflow's life?** Handing off is
   cheaper but has a window; persisting is simpler but means the hold does not
   follow the reader's progress, which is what step 3 of the requirement asks for.
3. **Does the per-registration agent need a floor agent alongside it?** The
   acquired hold may be that floor. If it is released at handoff, is there an
   interval with no floor?
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

Step 0 is done (`9babb21b5b`). The rest is deliberately not split further, because
step 1 without step 2 leaks a hold and step 4 is only safe after 1 to 3.

0. **Command vocabulary.** `ComputeCommand::AcquireHolds`/`ReleaseHolds`, boxed,
   plus the four exhaustive matches they touch. Nothing emits them and the compute
   side panics on receipt, so no behaviour changed. `reduce` treats them as
   unreachable because the multiplexer synthesizes them rather than the controller
   issuing them.
1. **Maintenance-side `AcquireHolds`.** For each id, clone that collection's
   `TraceBundle` handles and pin them at `as_of`, keyed by holder. The clone base
   matters: `compute_state.traces` sits at the controller's frontier, which by I1a
   is at or below every `as_of` the controller may offer, so a clone of it can be
   set to `as_of`. A clone of the publisher's own agent cannot, because that agent
   has ratcheted (G5).
2. **Release without a command reaching maintenance.** The publisher reclaims a
   hold once the importing registration has existed and gone. Needs an
   `everRegistered` marker per holder, because "no registration" is otherwise
   ambiguous between "the create has not been processed yet" and "the reader is
   finished", and reclaiming in the first case is the defect the model found.
3. **Multiplexer synthesis**, over the alias closure of the imports (G3).
4. **Delete the cap.** `hold_floor`, `deferred_compaction`, `compaction_floor`,
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
