\* Copyright Materialize, Inc. and contributors. All rights reserved.
\*
\* Use of this software is governed by the Business Source License
\* included in the LICENSE file at the root of this repository.
\*
\* As of the Change Date specified in that file, in accordance with
\* the Business Source License, use of this software will be governed
\* by the Apache License, Version 2.0.

--------------------------- MODULE Holds ---------------------------
\* Read holds across two compute runtimes, checked per process. See ../read-holds.md.
\*
\* This models what the code does, not the design as first written. Three things
\* changed while building it and each is reflected here:
\*   - the acquired hold FOLLOWS its reader instead of sitting at the as_of for the
\*     dataflow's life, because a frozen hold is a permanent pin;
\*   - reclaim is driven by an explicit release the rendering runtime records, not
\*     by the publisher observing a registration appear and go away;
\*   - the drop and the release are two commands on the rendering runtime's stream,
\*     in that order, and conflating them would hide a mis-ordering between them.
\*
\* `Mechanism` selects which design is in force, so a retired one is refuted rather
\* than merely absent. A model that can only express the shipped design cannot tell
\* you the design fixed anything.
\*   "acquire"           AcquireHolds on the owning runtime's stream, release on the
\*                       rendering runtime's. What the code does.
\*   "release-on-maint"  as "acquire", but the release travels on the OWNING
\*                       runtime's stream. The asymmetry in the code exists to avoid
\*                       exactly this. Expected to violate I1.
\*   "cap"               the multiplexer caps AllowCompaction and retires the cap
\*                       when the rendering runtime reports a frontier. The
\*                       mechanism this work deleted. Expected to violate I1.
\*
\* G1 is why `Procs` is a set: commands reach process 0 only and each runtime
\* re-broadcasts to its own processes independently, so process 1 gets no
\* cross-runtime ordering. G2 (the epoch boundary) is NOT covered, see the README.

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Procs,          \* set of process ids, e.g. {0, 1}
    Times,          \* timestamps, e.g. 0..2
    Mechanism,      \* "acquire", "release-on-maint", or "cap"
    MaxEpochs,      \* how many connections to allow
    NoTime          \* sentinel for "no hold", a number outside Times

ASSUME Mechanism \in {"acquire", "release-on-maint", "cap"}
\* Sets here must stay homogeneous: TLC canonicalizes a set value by sorting its
\* elements, so a set mixing a string sentinel with numbers fails to compare.
ASSUME NoTime \notin Times

Acquires == Mechanism \in {"acquire", "release-on-maint"}

VARIABLES
    \* Controller state.
    ctlSince,       \* frontier the controller has released the collection to
    ctlHold,        \* controller's read hold for the dataflow, or NoTime
    dfAsOf,         \* the dataflow's as_of, or NoTime before it is created
    epoch,          \* current connection nonce
    \* Multiplexer state (per connection, process 0 only).
    muxHold,        \* "cap" only: recorded interactive hold, or NoTime
    muxFloor,       \* "cap" only: lowest frontier already forwarded
    muxHeld,        \* whether an acquisition was synthesized, so a release is too
    \* Per-process, per-runtime command queues.
    maintQ,         \* [p \in Procs |-> Seq of owning-runtime commands]
    interQ,         \* [p \in Procs |-> Seq of rendering-runtime commands]
    \* Per-process replica state.
    since,          \* [p \in Procs |-> applied compaction frontier]
    acquired,       \* [p \in Procs |-> command-acquired hold, or NoTime]
    readerHold,     \* [p \in Procs |-> the built import's own hold, or NoTime]
    dropped,        \* [p \in Procs |-> the rendering runtime has applied the drop]
    released,       \* [p \in Procs |-> the rendering runtime has applied the release]
    reported        \* has any process reported a frontier for the dataflow

vars == <<ctlSince, ctlHold, dfAsOf, epoch, muxHold, muxFloor, muxHeld,
          maintQ, interQ, since, acquired, readerHold, dropped, released,
          reported>>

\* Commands. Each carries the epoch it was issued in, so a runtime can be given
\* stale commands after a Hello, which is G2.
Compact(t, e)  == [kind |-> "compact", time |-> t, epoch |-> e]
Acquire(t, e)  == [kind |-> "acquire", time |-> t, epoch |-> e]
Release(e)     == [kind |-> "release", time |-> 0, epoch |-> e]
Create(t, e)   == [kind |-> "create",  time |-> t, epoch |-> e]
Drop(e)        == [kind |-> "drop",    time |-> 0, epoch |-> e]

\* Broadcast: the multiplexer hands one command to every process's queue for a
\* runtime. Delivery is per process, and each process then drains at its own
\* rate, which is what leaves process 1 unordered against process 0 (G1).
Broadcast(q, cmd) == [p \in Procs |-> Append(q[p], cmd)]

Init ==
    /\ ctlSince = 0
    /\ ctlHold = NoTime
    /\ dfAsOf = NoTime
    /\ epoch = 0
    /\ muxHold = NoTime
    /\ muxFloor = 0
    /\ muxHeld = FALSE
    /\ maintQ = [p \in Procs |-> <<>>]
    /\ interQ = [p \in Procs |-> <<>>]
    /\ since = [p \in Procs |-> 0]
    /\ acquired = [p \in Procs |-> NoTime]
    /\ readerHold = [p \in Procs |-> NoTime]
    /\ dropped = [p \in Procs |-> FALSE]
    /\ released = [p \in Procs |-> FALSE]
    /\ reported = FALSE

-----------------------------------------------------------------------------
\* Controller and multiplexer

\* The controller creates the interactive dataflow at t, taking a read hold there.
\* It may only choose a time it has not released past: this is I1a, the
\* controller's own discipline, and an assumption about the controller rather than
\* something the replica enforces.
\*
\* The multiplexer sees the create. Under an acquiring mechanism it emits the
\* acquisition to the owning runtime BEFORE forwarding the create, which is the
\* whole mechanism. The order it addresses the two runtimes in is what this action
\* models, and only the position within the owning runtime's queue matters.
CtlCreate(t) ==
    /\ dfAsOf = NoTime
    /\ ctlSince <= t
    /\ ctlHold' = t
    /\ dfAsOf' = t
    /\ IF Acquires
       THEN /\ maintQ' = Broadcast(maintQ, Acquire(t, epoch))
            /\ muxHold' = NoTime
            /\ muxHeld' = TRUE
       ELSE /\ maintQ' = maintQ
            /\ muxHold' = t
            /\ muxHeld' = FALSE
    /\ interQ' = Broadcast(interQ, Create(t, epoch))
    /\ dropped' = [p \in Procs |-> FALSE]
    /\ released' = [p \in Procs |-> FALSE]
    /\ UNCHANGED <<ctlSince, epoch, muxFloor, since, acquired, readerHold,
                   reported>>

\* The controller allows compaction to t. It never releases past its own hold
\* (I1a). Under "cap" the multiplexer caps what it forwards; otherwise the
\* frontier is forwarded verbatim, which is what the code does.
\* NOTE: strictly increasing. A non-strict guard lets this fire forever, appending
\* to every queue, and the state space stops being finite.
CtlCompact(t) ==
    /\ ctlSince < t
    /\ ctlHold = NoTime \/ t <= ctlHold
    /\ ctlSince' = t
    /\ LET capped == IF Mechanism = "cap" /\ muxHold # NoTime
                        /\ t > muxHold /\ muxHold >= muxFloor
                     THEN muxHold
                     ELSE t
       IN /\ maintQ' = Broadcast(maintQ, Compact(capped, epoch))
          /\ muxFloor' = capped
    /\ UNCHANGED <<ctlHold, dfAsOf, epoch, muxHold, muxHeld, interQ, since,
                   acquired, readerHold, dropped, released, reported>>

\* The controller finishes with the dataflow: it drops its own hold and sends the
\* dataflow's drop, which the multiplexer routes to the runtime that renders it.
\*
\* The release follows the drop on that same stream, so it is ordered behind both
\* the create and the drop. TLC refuted putting it on the owning runtime's stream
\* in nine steps: it can overtake a create the rendering runtime has not processed,
\* so the owning runtime applies acquire, release and compaction while the dataflow
\* is still queued, and the dataflow then renders against compacted data.
CtlDrop ==
    /\ dfAsOf # NoTime
    \* Once, not repeatedly: without this the drop is appended forever.
    /\ ctlHold # NoTime
    /\ ctlHold' = NoTime
    /\ IF Mechanism = "release-on-maint"
       THEN /\ interQ' = Broadcast(interQ, Drop(epoch))
            /\ maintQ' = IF muxHeld
                         THEN Broadcast(maintQ, Release(epoch))
                         ELSE maintQ
            /\ muxHold' = muxHold
       ELSE IF Acquires
       THEN /\ interQ' = IF muxHeld
                         THEN Broadcast(Broadcast(interQ, Drop(epoch)),
                                        Release(epoch))
                         ELSE Broadcast(interQ, Drop(epoch))
            /\ maintQ' = maintQ
            /\ muxHold' = muxHold
       ELSE /\ interQ' = Broadcast(interQ, Drop(epoch))
            /\ maintQ' = maintQ
            \* The cap retires on the rendering runtime's frontier report.
            /\ muxHold' = IF reported THEN NoTime ELSE muxHold
    /\ muxHeld' = FALSE
    /\ UNCHANGED <<ctlSince, dfAsOf, epoch, muxFloor, since, acquired,
                   readerHold, dropped, released, reported>>

\* A reconnection. The multiplexer's per-connection state goes; the runtimes'
\* queues do NOT, because a stale-nonce command is stashed and still executes.
\* That is G2.
\*
\* NOT exercised by `Holds.cfg`, which sets MaxEpochs = 0. Modelling a reconnection
\* faithfully needs the holder identity to be epoch-scoped, since a replayed
\* dataflow gets a fresh transient id and one holder shared across epochs conflates
\* two different dataflows. See the README.
Hello ==
    /\ epoch < MaxEpochs
    /\ epoch' = epoch + 1
    /\ muxHold' = NoTime
    /\ muxFloor' = 0
    /\ muxHeld' = FALSE
    /\ dfAsOf' = NoTime
    /\ ctlHold' = NoTime
    /\ UNCHANGED <<ctlSince, maintQ, interQ, since, acquired, readerHold,
                   dropped, released, reported>>

-----------------------------------------------------------------------------
\* Replica, owning runtime

\* The owning runtime on process p applies its next command. This is the only place
\* compaction is realized and the only place a hold is installed, so their order
\* here is exactly the order they had in the queue.
MaintStep(p) ==
    /\ Len(maintQ[p]) > 0
    /\ LET cmd == Head(maintQ[p]) IN
       /\ maintQ' = [maintQ EXCEPT ![p] = Tail(maintQ[p])]
       /\ CASE cmd.kind = "compact" ->
                    \* ONLY the command-acquired hold bounds this. The reader's own
                    \* registration deliberately does not: it is forwarded through
                    \* the publisher's single agent, whose setter joins, so a
                    \* registration below where that agent already sits cannot be
                    \* honoured. Treating the registration as a bound here would
                    \* assume away the very ratchet the acquired hold exists for.
                    LET bound == IF acquired[p] # NoTime THEN acquired[p]
                                 ELSE cmd.time
                    IN /\ since' = [since EXCEPT ![p] =
                                        IF cmd.time > since[p]
                                        THEN (IF bound < cmd.time
                                              THEN (IF bound > since[p]
                                                    THEN bound
                                                    ELSE since[p])
                                              ELSE cmd.time)
                                        ELSE since[p]]
                       /\ UNCHANGED acquired
            [] cmd.kind = "acquire" ->
                    /\ acquired' = [acquired EXCEPT ![p] = cmd.time]
                    /\ UNCHANGED since
            [] cmd.kind = "release" ->
                    \* Only reachable under "release-on-maint". The refuted design.
                    /\ acquired' = [acquired EXCEPT ![p] = NoTime]
                    /\ UNCHANGED since
    /\ UNCHANGED <<ctlSince, ctlHold, dfAsOf, epoch, muxHold, muxFloor, muxHeld,
                   interQ, readerHold, dropped, released, reported>>

\* The acquired hold follows the reader's own progress, floored at the frontier it
\* was acquired at. Run on the owning runtime's maintenance tick.
\*
\* The floor is what makes this sound without attributing registrations to holders:
\* the target is at or below every registration, so flooring it cannot carry the
\* hold past its own reader. `readerHold[p] # NoTime` is the "a registration
\* exists" precondition, and it is load-bearing: treating its absence as "the
\* reader finished" would release the hold in the window before the dataflow is
\* even built.
HoldDowngrade(p) ==
    /\ acquired[p] # NoTime
    /\ readerHold[p] # NoTime
    /\ acquired[p] < readerHold[p]
    /\ acquired' = [acquired EXCEPT ![p] = readerHold[p]]
    /\ UNCHANGED <<ctlSince, ctlHold, dfAsOf, epoch, muxHold, muxFloor, muxHeld,
                   maintQ, interQ, since, readerHold, dropped, released,
                   reported>>

\* The owning runtime reclaims the hold once the rendering runtime has recorded the
\* release. Intra-process through the per-process registry, no command.
\*
\* `released[p]` can only be set after that runtime applied the drop, which can
\* only happen after it applied the create. That ordering is what an earlier design
\* inferred from watching a registration appear and disappear.
HoldReclaim(p) ==
    /\ Acquires
    /\ acquired[p] # NoTime
    /\ released[p]
    /\ acquired' = [acquired EXCEPT ![p] = NoTime]
    /\ UNCHANGED <<ctlSince, ctlHold, dfAsOf, epoch, muxHold, muxFloor, muxHeld,
                   maintQ, interQ, since, readerHold, dropped, released,
                   reported>>

-----------------------------------------------------------------------------
\* Replica, rendering runtime

\* The rendering runtime on process p applies its next command. Building the
\* dataflow registers the import's own read hold in shared state, which needs no
\* cooperation from the owning runtime and so cannot be delayed by it.
InterStep(p) ==
    /\ Len(interQ[p]) > 0
    /\ LET cmd == Head(interQ[p]) IN
       /\ interQ' = [interQ EXCEPT ![p] = Tail(interQ[p])]
       /\ CASE cmd.kind = "create" ->
                    /\ readerHold' = [readerHold EXCEPT ![p] = cmd.time]
                    /\ UNCHANGED <<dropped, released>>
            [] cmd.kind = "drop" ->
                    \* Dropping the dataflow drops the import, and with it the
                    \* registration. Separate from the release below: they are two
                    \* commands in a defined order, and merging them would hide a
                    \* mis-ordering between them.
                    /\ readerHold' = [readerHold EXCEPT ![p] = NoTime]
                    /\ dropped' = [dropped EXCEPT ![p] = TRUE]
                    /\ UNCHANGED released
            [] cmd.kind = "release" ->
                    /\ released' = [released EXCEPT ![p] = TRUE]
                    /\ UNCHANGED <<readerHold, dropped>>
    /\ UNCHANGED <<ctlSince, ctlHold, dfAsOf, epoch, muxHold, muxFloor, muxHeld,
                   maintQ, since, acquired, reported>>

\* The rendering runtime reports a frontier for a dataflow it has built. This is
\* the signal the "cap" mechanism retires on. Subscribe collections never emit it,
\* which is one reason the shipped design does not depend on it.
InterReport(p) ==
    /\ readerHold[p] # NoTime
    /\ ~reported
    /\ reported' = TRUE
    /\ UNCHANGED <<ctlSince, ctlHold, dfAsOf, epoch, muxHold, muxFloor, muxHeld,
                   maintQ, interQ, since, acquired, readerHold, dropped,
                   released>>

\* The import makes progress and downgrades its own hold. Being late here only
\* delays compaction, which is the asymmetry the design rests on.
ReaderDowngrade(p, t) ==
    /\ readerHold[p] # NoTime
    /\ readerHold[p] < t
    /\ readerHold' = [readerHold EXCEPT ![p] = t]
    /\ UNCHANGED <<ctlSince, ctlHold, dfAsOf, epoch, muxHold, muxFloor, muxHeld,
                   maintQ, interQ, since, acquired, dropped, released, reported>>

-----------------------------------------------------------------------------

Next ==
    \/ \E t \in Times : CtlCreate(t)
    \/ \E t \in Times : CtlCompact(t)
    \/ CtlDrop
    \/ Hello
    \/ \E p \in Procs : MaintStep(p)
    \/ \E p \in Procs : InterStep(p)
    \/ \E p \in Procs : InterReport(p)
    \/ \E p \in Procs : HoldDowngrade(p)
    \/ \E p \in Procs : HoldReclaim(p)
    \/ \E p \in Procs, t \in Times : ReaderDowngrade(p, t)

Spec == Init /\ [][Next]_vars

\* What the implementation guarantees will keep happening. Only the liveness property
\* uses this; the invariants are checked against `Spec` and hold whatever any runtime
\* does or stops doing.
\*
\* Each runtime drains its command queue, because each worker's server loop does so
\* every iteration, and the owning runtime's maintenance tick keeps firing. Queue
\* draining is not decoration here: the release reaches the owning runtime only
\* through the rendering runtime applying a command, so without it TLC finds a
\* behaviour where the release sits queued forever and the hold is never reclaimed.
\* That is a genuine dependency of the design and stating it as fairness is what
\* records it.
Fairness ==
    /\ \A p \in Procs : WF_vars(MaintStep(p))
    /\ \A p \in Procs : WF_vars(InterStep(p))
    /\ \A p \in Procs : WF_vars(HoldDowngrade(p))
    /\ \A p \in Procs : WF_vars(HoldReclaim(p))

FairSpec == Spec /\ Fairness

-----------------------------------------------------------------------------
\* Properties

TypeOK ==
    /\ ctlSince \in Times
    /\ ctlHold \in Times \cup {NoTime}
    /\ dfAsOf \in Times \cup {NoTime}
    /\ muxHold \in Times \cup {NoTime}
    /\ muxFloor \in Times
    /\ muxHeld \in BOOLEAN
    /\ \A p \in Procs : since[p] \in Times
    /\ \A p \in Procs : acquired[p] \in Times \cup {NoTime}
    /\ \A p \in Procs : readerHold[p] \in Times \cup {NoTime}
    /\ \A p \in Procs : dropped[p] \in BOOLEAN
    /\ \A p \in Procs : released[p] \in BOOLEAN

\* I1, per process, in two windows.
\*
\* Before the dataflow is built here, it needs exactly its `as_of` and has made no
\* progress that would let it need less. Once built, it needs whatever its reader
\* currently holds, which moves as the reader downgrades. Stating only the first
\* window would flag a CORRECT downgrade as a violation, since after a downgrade
\* the collection may legitimately compact past the original `as_of`.
\*
\* Once the drop has been applied here there is no reader left to protect.
I1 ==
    \A p \in Procs :
        /\ (dfAsOf # NoTime /\ ~dropped[p] /\ readerHold[p] = NoTime)
                => since[p] <= dfAsOf
        /\ (readerHold[p] # NoTime) => since[p] <= readerHold[p]

\* The acquired hold never sits above the reader it protects.
\*
\* This is the safety half of "the hold follows its reader": the target is floored
\* at the acquisition frontier and capped by the reader's own hold, so an
\* over-eager downgrade shows up here rather than as a subtle I1 violation later.
HoldNeverPassesReader ==
    \A p \in Procs :
        (acquired[p] # NoTime /\ readerHold[p] # NoTime)
            => acquired[p] <= readerHold[p]

\* Compaction frontiers never regress on an owning-runtime queue.
NoRegression ==
    \A p \in Procs :
        \A i \in 1..Len(maintQ[p]) :
            maintQ[p][i].kind = "compact" => maintQ[p][i].time >= since[p]

\* Lag never exposes a read: if the owning runtime on p has compacted past the
\* dataflow's as_of while its reader there has not been built, it cannot still be
\* owing the acquisition that covers it. This is the claim the equal-delay argument
\* rests on, and it is the kind of claim that has been wrong often enough in this
\* work to be worth checking rather than asserting.
LagNeverExposes ==
    \A p \in Procs :
        (dfAsOf # NoTime /\ ~dropped[p] /\ readerHold[p] = NoTime
         /\ since[p] > dfAsOf)
            => ~(\E i \in 1..Len(maintQ[p]) : maintQ[p][i].kind = "acquire")

\* No permanent pin. LIVENESS, checked against `FairSpec`.
\*
\* A hold that sits below its reader eventually catches up. This is the property
\* that matters and it cannot be stated as an invariant: reclaim and downgrade are
\* separate steps, so there is always a state in which the release has been applied
\* and the hold is still there, and a safety version would flag that legal
\* intermediate state. Written as an invariant it demands an instantaneous reclaim,
\* which is what a first attempt here did.
\*
\* `NoTime` is above every element of `Times`, so a reclaimed hold satisfies the
\* consequent as well as a downgraded one. Both are ways of no longer pinning.
\*
\* Why it is worth checking at all: a hold frozen at the `as_of` it was acquired at
\* would pin its collection for as long as the reader lives, and an interactive
\* `SUBSCRIBE` lives as long as its client. That is the failure the shipped
\* downgrade exists to avoid.
NoPermanentPin ==
    \A p \in Procs :
        (acquired[p] # NoTime /\ readerHold[p] # NoTime
         /\ acquired[p] < readerHold[p])
            ~> (acquired[p] >= readerHold[p])

\* A backstop on the state space. The guards above already bound how often each
\* controller action can fire; this is here so that a future action without a
\* progress guard fails loudly instead of running forever.
QueuesBounded ==
    /\ \A p \in Procs : Len(maintQ[p]) =< 5
    /\ \A p \in Procs : Len(interQ[p]) =< 5

=============================================================================
