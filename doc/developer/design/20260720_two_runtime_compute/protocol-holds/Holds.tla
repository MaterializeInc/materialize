\* Copyright Materialize, Inc. and contributors. All rights reserved.
\*
\* Use of this software is governed by the Business Source License
\* included in the LICENSE file at the root of this repository.
\*
\* As of the Change Date specified in that file, in accordance with
\* the Business Source License, use of this software will be governed
\* by the Apache License, Version 2.0.

--------------------------- MODULE Holds ---------------------------
\* Read holds across two compute runtimes, checked per process.
\* See ../broadcast-compaction.md for the design and ../read-holds.md for the
\* invariant and the gap analysis.
\*
\* The model is about STREAM POSITIONS. A shared arrangement compacts only as fast as
\* the slowest runtime has applied, which is I1c, and that is the whole mechanism.
\* Nothing here is per dataflow: no acquisition, no release, no reclaim and no holder
\* identity, which is why the epoch boundary (G2) has nothing to go stale.
\*
\* `Mechanism` selects which design is in force, so an alternative is refuted rather
\* than merely absent. A model that can only express the shipped design cannot tell
\* you the design fixed anything.
\*   "routed"             `AllowCompaction` goes to the owning runtime only, as
\*                        routing by ownership does, and nothing else is added. The
\*                        raw defect the split introduced. Expected to violate I1.
\*   "broadcast"          `AllowCompaction` goes to both runtimes and nothing else is
\*                        added. Expected to violate I1: restoring the ordering
\*                        within each stream does not restore it between them.
\*   "broadcast-standing" as "broadcast", plus the rendering runtime holding every
\*                        shared collection at the last frontier IT has applied. What
\*                        the code does.
\*
\* G1 is why `Procs` is a set: commands reach process 0 only and each runtime
\* re-broadcasts to its own processes independently, so process 1 gets no
\* cross-runtime ordering. G2 (the epoch boundary) is NOT covered, see the README.

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Procs,          \* set of process ids, e.g. {0, 1}
    Times,          \* timestamps, e.g. 0..2
    Mechanism,      \* "routed", "broadcast", or "broadcast-standing"
    MaxEpochs,      \* how many connections to allow
    NoTime          \* sentinel for "no bound", a number above Times

ASSUME Mechanism \in {"routed", "broadcast", "broadcast-standing"}
\* Sets here must stay homogeneous: TLC canonicalizes a set value by sorting its
\* elements, so a set mixing a string sentinel with numbers fails to compare.
\* `NoTime` being ABOVE every time is load-bearing rather than incidental: it lets an
\* absent bound take part in a minimum without a special case, since a bound of
\* `NoTime` permits all compaction.
ASSUME NoTime \notin Times

\* Compaction reaching both runtimes is what puts a create and the compactions that
\* follow it back on one ordered stream for the runtime that renders it. That ordering
\* is what routing by ownership destroyed.
Broadcasts == Mechanism \in {"broadcast", "broadcast-standing"}

\* Broadcast ALONE, with no hold at all, is modelled deliberately so TLC decides
\* whether it suffices rather than leaving it argued. It does not: the runtimes still
\* drain independently, so the owning runtime can realize a compaction the rendering
\* runtime has not reached.
Standing == Mechanism = "broadcast-standing"

VARIABLES
    \* Controller state.
    ctlSince,       \* frontier the controller has released the collection to
    ctlHold,        \* controller's read hold for the dataflow, or NoTime
    dfAsOf,         \* the dataflow's as_of, or NoTime before it is created
    epoch,          \* current connection nonce
    \* Per-process, per-runtime command queues.
    maintQ,         \* [p \in Procs |-> Seq of owning-runtime commands]
    interQ,         \* [p \in Procs |-> Seq of rendering-runtime commands]
    \* Per-process replica state.
    applied,        \* [p \in Procs |-> compaction the owning runtime has applied]
    since,          \* [p \in Procs |-> published compaction frontier]
    readerHold,     \* [p \in Procs |-> the built import's own hold, or NoTime]
    standing,       \* [p \in Procs |-> frontier the rendering runtime has applied]
    dropped         \* [p \in Procs |-> the rendering runtime has applied the drop]

vars == <<ctlSince, ctlHold, dfAsOf, epoch, maintQ, interQ, applied, since,
          readerHold, standing, dropped>>

\* Commands. Each carries the epoch it was issued in, so a runtime can be given stale
\* commands after a Hello, which is G2.
Compact(t, e)  == [kind |-> "compact", time |-> t, epoch |-> e]
Create(t, e)   == [kind |-> "create",  time |-> t, epoch |-> e]
Drop(e)        == [kind |-> "drop",    time |-> 0, epoch |-> e]

\* Broadcast: the multiplexer hands one command to every process's queue for a
\* runtime. Delivery is per process, and each process then drains at its own rate,
\* which is what leaves process 1 unordered against process 0 (G1).
Broadcast(q, cmd) == [p \in Procs |-> Append(q[p], cmd)]

Min2(a, b) == IF a < b THEN a ELSE b

Init ==
    /\ ctlSince = 0
    /\ ctlHold = NoTime
    /\ dfAsOf = NoTime
    /\ epoch = 0
    /\ maintQ = [p \in Procs |-> <<>>]
    /\ interQ = [p \in Procs |-> <<>>]
    /\ applied = [p \in Procs |-> 0]
    /\ since = [p \in Procs |-> 0]
    /\ readerHold = [p \in Procs |-> NoTime]
    /\ standing = [p \in Procs |-> 0]
    /\ dropped = [p \in Procs |-> FALSE]

-----------------------------------------------------------------------------
\* Controller and multiplexer

\* The controller creates the interactive dataflow at t, taking a read hold there. It
\* may only choose a time it has not released past: this is I1a, the controller's own
\* discipline, and an assumption about the controller rather than something the
\* replica enforces.
\*
\* The multiplexer routes the create to the rendering runtime and adds nothing. That
\* is the point of the design: no command is synthesized here, so nothing depends on
\* this being the only place that observes both streams.
CtlCreate(t) ==
    /\ dfAsOf = NoTime
    /\ ctlSince <= t
    /\ ctlHold' = t
    /\ dfAsOf' = t
    /\ interQ' = Broadcast(interQ, Create(t, epoch))
    /\ dropped' = [p \in Procs |-> FALSE]
    /\ UNCHANGED <<ctlSince, epoch, maintQ, applied, since, readerHold, standing>>

\* The controller allows compaction to t. It never releases past its own hold (I1a).
\* The frontier is forwarded verbatim, never capped: capping it was an earlier
\* mechanism and it carries a regression hazard, since the command history derives a
\* dataflow's effective `as_of` from the last frontier seen per export.
\* NOTE: strictly increasing. A non-strict guard lets this fire forever, appending to
\* every queue, and the state space stops being finite.
CtlCompact(t) ==
    /\ ctlSince < t
    /\ ctlHold = NoTime \/ t <= ctlHold
    /\ ctlSince' = t
    /\ maintQ' = Broadcast(maintQ, Compact(t, epoch))
    \* The routing half of the mechanism: the same frontier also goes to the rendering
    \* runtime, so on that stream it sits BEHIND any create already queued there.
    /\ interQ' = IF Broadcasts
                 THEN Broadcast(interQ, Compact(t, epoch))
                 ELSE interQ
    /\ UNCHANGED <<ctlHold, dfAsOf, epoch, applied, since, readerHold, standing,
                   dropped>>

\* The controller finishes with the dataflow: it drops its own hold and sends the
\* dataflow's drop, which the multiplexer routes to the runtime that renders it.
\*
\* This action is where every mechanism without a standing hold fails. Dropping the
\* controller's hold is legitimate and immediate, as a cancelled peek does it, so the
\* controller may then compact past an `as_of` whose create is still queued on the
\* rendering runtime. From the controller's point of view the dataflow is gone.
CtlDrop ==
    /\ dfAsOf # NoTime
    \* Once, not repeatedly: without this the drop is appended forever.
    /\ ctlHold # NoTime
    /\ ctlHold' = NoTime
    /\ interQ' = Broadcast(interQ, Drop(epoch))
    /\ UNCHANGED <<ctlSince, dfAsOf, epoch, maintQ, applied, since, readerHold,
                   standing, dropped>>

\* A reconnection. The runtimes' queues do NOT drain, because a stale-nonce command is
\* stashed and still executes. That is G2.
\*
\* The replica keeps its standing holds across this boundary, deliberately: one is per
\* collection, carries no dataflow identity and only rises, so there is nothing to go
\* stale, and clearing it would only drop the bound until the replayed compactions
\* raised it again.
\*
\* NOT exercised by any config, which all set MaxEpochs = 0. See the README.
Hello ==
    /\ epoch < MaxEpochs
    /\ epoch' = epoch + 1
    /\ dfAsOf' = NoTime
    /\ ctlHold' = NoTime
    /\ UNCHANGED <<ctlSince, maintQ, interQ, applied, since, readerHold, standing,
                   dropped>>

-----------------------------------------------------------------------------
\* Replica, owning runtime

\* The frontier the published view may compact to on process p.
\*
\* The reader's own registration is a bound here, which it could NOT be under an
\* acquisition mechanism: a registration is forwarded through the publisher's single
\* agent, whose setter joins, so one below where that agent already sits cannot be
\* honoured. Bounding the agent by the standing hold is what removes that ratchet,
\* since the agent then never sits above a frontier the rendering runtime can still
\* present. So the registration became representable and the mechanism that existed to
\* escape the ratchet became unnecessary, both for the same reason.
\*
\* Under the refuting mechanisms this bound is more generous than the code could be,
\* which is deliberate: an alternative is worth refuting in its strongest form.
\*
\* `NoTime` is above every time, so an absent bound drops out of the minimum.
ReaderBound(p) == readerHold[p]
StandingBound(p) == IF Standing THEN standing[p] ELSE NoTime
Target(p) == Min2(applied[p], Min2(ReaderBound(p), StandingBound(p)))

\* The owning runtime on process p applies its next command.
\*
\* Applying a compaction and publishing one are separate. `applied` is this runtime's
\* stream position and only this action moves it, while `since` is what a reader gates
\* on and `MaintRefresh` recomputes it. Conflating them would model a publisher that
\* re-evaluates its bounds only when a command arrives, and would then miss that a
\* standing hold catching up releases compaction with no new command at all.
MaintStep(p) ==
    /\ Len(maintQ[p]) > 0
    /\ LET cmd == Head(maintQ[p]) IN
       /\ maintQ' = [maintQ EXCEPT ![p] = Tail(maintQ[p])]
       /\ applied' = [applied EXCEPT ![p] =
                          IF cmd.time > applied[p] THEN cmd.time ELSE applied[p]]
    /\ UNCHANGED <<ctlSince, ctlHold, dfAsOf, epoch, interQ, since, readerHold,
                   standing, dropped>>

\* The publisher recomputes the published `since` from its bounds, which it does on
\* every activation rather than on command arrival.
\*
\* Monotone, because `since` is derived from the publisher's own agent hold and an
\* agent's setter joins. That is also why every bound only ever rises: a bound that
\* could fall would be unrepresentable one activation later.
MaintRefresh(p) ==
    /\ Target(p) > since[p]
    /\ since' = [since EXCEPT ![p] = Target(p)]
    /\ UNCHANGED <<ctlSince, ctlHold, dfAsOf, epoch, maintQ, interQ, applied,
                   readerHold, standing, dropped>>

-----------------------------------------------------------------------------
\* Replica, rendering runtime

\* The rendering runtime on process p applies its next command. Building the dataflow
\* registers the import's own read hold in shared state, which needs no cooperation
\* from the owning runtime and so cannot be delayed by it.
InterStep(p) ==
    /\ Len(interQ[p]) > 0
    /\ LET cmd == Head(interQ[p]) IN
       /\ interQ' = [interQ EXCEPT ![p] = Tail(interQ[p])]
       /\ CASE cmd.kind = "create" ->
                    /\ readerHold' = [readerHold EXCEPT ![p] = cmd.time]
                    /\ UNCHANGED <<standing, dropped>>
            [] cmd.kind = "compact" ->
                    \* Only reachable under a broadcast mechanism. Applying the
                    \* frontier here is what advances this runtime's standing hold,
                    \* and it happens strictly after every command queued ahead of it,
                    \* which is where the ordering comes from.
                    /\ standing' = [standing EXCEPT ![p] =
                                        IF cmd.time > standing[p]
                                        THEN cmd.time ELSE standing[p]]
                    /\ UNCHANGED <<readerHold, dropped>>
            [] cmd.kind = "drop" ->
                    /\ readerHold' = [readerHold EXCEPT ![p] = NoTime]
                    /\ dropped' = [dropped EXCEPT ![p] = TRUE]
                    /\ UNCHANGED standing
    /\ UNCHANGED <<ctlSince, ctlHold, dfAsOf, epoch, maintQ, applied, since>>

\* The import makes progress and downgrades its own hold. Being late here only delays
\* compaction, which is the asymmetry the design rests on.
ReaderDowngrade(p, t) ==
    /\ readerHold[p] # NoTime
    /\ readerHold[p] < t
    /\ readerHold' = [readerHold EXCEPT ![p] = t]
    /\ UNCHANGED <<ctlSince, ctlHold, dfAsOf, epoch, maintQ, interQ, applied, since,
                   standing, dropped>>

-----------------------------------------------------------------------------

Next ==
    \/ \E t \in Times : CtlCreate(t)
    \/ \E t \in Times : CtlCompact(t)
    \/ CtlDrop
    \/ Hello
    \/ \E p \in Procs : MaintStep(p)
    \/ \E p \in Procs : MaintRefresh(p)
    \/ \E p \in Procs : InterStep(p)
    \/ \E p \in Procs, t \in Times : ReaderDowngrade(p, t)

Spec == Init /\ [][Next]_vars

\* What the implementation guarantees will keep happening. Only the liveness property
\* uses this; the invariants are checked against `Spec` and hold whatever any runtime
\* does or stops doing.
\*
\* Each runtime drains its command queue, because each worker's server loop does so
\* every iteration, and the publisher is activated as its dataflow runs. Draining is
\* not decoration here: under a standing hold the owning runtime's compaction is
\* bounded by the rendering runtime's position, so a rendering runtime that stopped
\* draining would stall compaction on every shared arrangement. That coupling is new,
\* and it is the design's price, so it is stated rather than left implicit.
Fairness ==
    /\ \A p \in Procs : WF_vars(MaintStep(p))
    /\ \A p \in Procs : WF_vars(MaintRefresh(p))
    /\ \A p \in Procs : WF_vars(InterStep(p))

FairSpec == Spec /\ Fairness

-----------------------------------------------------------------------------
\* Properties

TypeOK ==
    /\ ctlSince \in Times
    /\ ctlHold \in Times \cup {NoTime}
    /\ dfAsOf \in Times \cup {NoTime}
    /\ \A p \in Procs : applied[p] \in Times
    /\ \A p \in Procs : since[p] \in Times
    /\ \A p \in Procs : readerHold[p] \in Times \cup {NoTime}
    /\ \A p \in Procs : standing[p] \in Times
    /\ \A p \in Procs : dropped[p] \in BOOLEAN

\* I1, per process, in two windows.
\*
\* Before the dataflow is built here, it needs exactly its `as_of` and has made no
\* progress that would let it need less. Once built, it needs whatever its reader
\* currently holds, which moves as the reader downgrades. Stating only the first window
\* would flag a CORRECT downgrade as a violation, since after a downgrade the
\* collection may legitimately compact past the original `as_of`.
\*
\* Once the drop has been applied here there is no reader left to protect.
I1 ==
    \A p \in Procs :
        /\ (dfAsOf # NoTime /\ ~dropped[p] /\ readerHold[p] = NoTime)
                => since[p] <= dfAsOf
        /\ (readerHold[p] # NoTime) => since[p] <= readerHold[p]

\* I1c: the published frontier never passes the rendering runtime's stream position.
\*
\* The mechanism stated directly rather than through its consequence, and the same
\* thing the publisher's own `debug_assert` says. Checking both means a counterexample
\* distinguishes "the bound was not applied" from "the bound was applied and was not
\* enough".
I1c ==
    Standing => \A p \in Procs : since[p] <= standing[p]

\* Compaction frontiers never regress on an owning-runtime queue.
NoRegression ==
    \A p \in Procs :
        \A i \in 1..Len(maintQ[p]) :
            maintQ[p][i].kind = "compact" => maintQ[p][i].time >= since[p]

\* The bound is temporary. LIVENESS, checked against `FairSpec`.
\*
\* A standing hold that lags stalls compaction, and this says the stall ends: once the
\* rendering runtime drains the compaction it was given, the published frontier reaches
\* the one the owning runtime has applied. It cannot be an invariant, since the lagging
\* state is legal and expected.
\*
\* Worth checking because the failure it excludes is silent. A bound that never lifted
\* would keep every shared arrangement's full history and show up as memory growth
\* rather than as a wrong answer.
CompactionNotStalled ==
    \A p \in Procs :
        (since[p] < applied[p] /\ readerHold[p] = NoTime) ~> (since[p] = applied[p])

\* A backstop on the state space. The guards above already bound how often each
\* controller action can fire; this is here so that a future action without a progress
\* guard fails loudly instead of running forever.
QueuesBounded ==
    /\ \A p \in Procs : Len(maintQ[p]) =< 5
    /\ \A p \in Procs : Len(interQ[p]) =< 5

=============================================================================
