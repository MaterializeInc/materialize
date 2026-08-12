\* Copyright Materialize, Inc. and contributors. All rights reserved.
\*
\* Use of this software is governed by the Business Source License
\* included in the LICENSE file at the root of this repository.
\*
\* As of the Change Date specified in that file, in accordance with
\* the Business Source License, use of this software will be governed
\* by the Apache License, Version 2.0.

--------------------------- MODULE Holds ---------------------------
\* Read holds across two compute runtimes, checked per process and across an
\* epoch boundary. See ../read-holds.md.
\*
\* The design under test: when the multiplexer routes a `CreateDataflow` for an
\* interactive dataflow, it also emits `AcquireHolds` to MAINTENANCE's stream.
\* Maintenance's stream is ordered, so every process installs the hold before it
\* applies any compaction that follows. Nothing depends on an ordering between
\* the two runtimes' streams, and the model must not provide one.
\*
\* Two failures motivated this and neither is expressible in a single-process,
\* single-connection model:
\*   G1  commands reach process 0 only and each runtime re-broadcasts to its own
\*       processes independently, so process 1 gets no cross-runtime ordering.
\*   G2  the multiplexer's state is per-connection and discarded on `Hello`,
\*       while commands queued ahead of that `Hello` still execute.
\*
\* `Mechanism` selects which design is in force, so the retired one can be
\* refuted rather than merely absent:
\*   "acquire"  AcquireHolds on maintenance's stream (the proposal)
\*   "cap"      the multiplexer caps AllowCompaction and retires the cap when
\*              interactive reports a frontier (what the code does today)

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Procs,          \* set of process ids, e.g. {0, 1}
    Times,          \* timestamps, e.g. 0..3
    Mechanism,      \* "acquire" or "cap"
    MaxEpochs,      \* how many connections to allow
    NoTime          \* sentinel for "no hold", a number outside Times

ASSUME Mechanism \in {"acquire", "cap"}
\* Sets here must stay homogeneous: TLC canonicalizes a set value by sorting its
\* elements, so a set mixing a string sentinel with numbers fails to compare.
ASSUME NoTime \notin Times

Idx == "I"          \* the one maintained collection
Dfl == "D"          \* the one interactive dataflow

VARIABLES
    \* Controller state.
    ctlSince,       \* frontier the controller has released Idx to
    ctlHold,        \* controller's read hold for Dfl, or NoTime
    dfAsOf,         \* Dfl's as_of, or NoTime once never/no longer created
    epoch,          \* current connection nonce
    \* Multiplexer state (per connection, process 0 only).
    muxHold,        \* recorded interactive hold, or NoTime
    muxFloor,       \* lowest frontier already forwarded for Idx
    \* Per-process, per-runtime command queues.
    maintQ,         \* [p \in Procs |-> Seq of maintenance commands]
    interQ,         \* [p \in Procs |-> Seq of interactive commands]
    \* Per-process replica state.
    since,          \* [p \in Procs |-> applied compaction frontier for Idx]
    acquired,       \* [p \in Procs |-> hold installed by AcquireHolds, or NoTime]
    rendered,       \* [p \in Procs |-> interactive has built Dfl]
    readerHold,     \* [p \in Procs |-> hold the built import registered, or NoTime]
    everRegistered, \* [p \in Procs |-> the import has registered at least once]
    reported        \* has any process reported a frontier for Dfl

vars == <<ctlSince, ctlHold, dfAsOf, epoch, muxHold, muxFloor,
          maintQ, interQ, since, acquired, rendered, readerHold, everRegistered,
          reported>>

\* Commands. Each carries the epoch it was issued in, so a runtime can be given
\* stale commands after a Hello, which is G2.
Compact(t, e)  == [kind |-> "compact", time |-> t, epoch |-> e]
Acquire(t, e)  == [kind |-> "acquire", time |-> t, epoch |-> e]
Release(e)     == [kind |-> "release", time |-> 0, epoch |-> e]
Create(t, e)   == [kind |-> "create",  time |-> t, epoch |-> e]

\* Broadcast: the multiplexer hands one command to every process's queue for a
\* runtime. Delivery is per process, and each process then drains at its own
\* rate, which is what leaves process 1 unordered against process 0 (G1).
BroadcastMaint(q, cmd) == [p \in Procs |-> Append(q[p], cmd)]
BroadcastInter(q, cmd) == [p \in Procs |-> Append(q[p], cmd)]

Init ==
    /\ ctlSince = 0
    /\ ctlHold = NoTime
    /\ dfAsOf = NoTime
    /\ epoch = 0
    /\ muxHold = NoTime
    /\ muxFloor = 0
    /\ maintQ = [p \in Procs |-> <<>>]
    /\ interQ = [p \in Procs |-> <<>>]
    /\ since = [p \in Procs |-> 0]
    /\ acquired = [p \in Procs |-> NoTime]
    /\ rendered = [p \in Procs |-> FALSE]
    /\ readerHold = [p \in Procs |-> NoTime]
    /\ everRegistered = [p \in Procs |-> FALSE]
    /\ reported = FALSE

-----------------------------------------------------------------------------
\* Controller

\* The controller creates the interactive dataflow at t, taking a read hold
\* there. It may only choose a time it has not released past: this is I1a, the
\* controller's own discipline, and it is an assumption about the controller
\* rather than something the replica enforces.
CtlCreate(t) ==
    /\ dfAsOf = NoTime
    /\ ctlSince <= t
    /\ ctlHold' = t
    /\ dfAsOf' = t
    \* The multiplexer sees the create. Under "acquire" it also emits
    \* AcquireHolds to maintenance; under "cap" it records a local hold.
    /\ IF Mechanism = "acquire"
       THEN /\ maintQ' = BroadcastMaint(maintQ, Acquire(t, epoch))
            /\ muxHold' = NoTime
       ELSE /\ maintQ' = maintQ
            /\ muxHold' = t
    /\ interQ' = BroadcastInter(interQ, Create(t, epoch))
    /\ UNCHANGED <<ctlSince, epoch, muxFloor, since, acquired, rendered,
                   readerHold, everRegistered, reported>>

\* The controller allows compaction of Idx to t. It never releases past its own
\* hold (I1a). Under "cap" the multiplexer caps what it forwards.
\* NOTE: strictly increasing. A non-strict guard lets this fire forever, appending
\* to every maintenance queue, and the state space stops being finite.
CtlCompact(t) ==
    /\ ctlSince < t
    /\ ctlHold = NoTime \/ t <= ctlHold
    /\ ctlSince' = t
    /\ LET capped == IF Mechanism = "cap" /\ muxHold # NoTime
                        /\ t > muxHold /\ muxHold >= muxFloor
                     THEN muxHold
                     ELSE t
       IN /\ maintQ' = BroadcastMaint(maintQ, Compact(capped, epoch))
          /\ muxFloor' = capped
    /\ UNCHANGED <<ctlHold, dfAsOf, epoch, muxHold, interQ, since, acquired,
                   rendered, readerHold, everRegistered, reported>>

\* The controller finishes with the dataflow and drops its own hold.
\*
\* Under "acquire" the release goes on INTERACTIVE's stream, not maintenance's.
\* TLC refuted the other choice in nine steps: a release on maintenance's stream
\* can overtake a create that interactive has not processed yet, so maintenance
\* applies acquire, release and compaction while the dataflow is still queued and
\* will then render against compacted data. The release has to be ordered against
\* the create, and the create lives on interactive's stream, so the release must
\* too. Maintenance reclaims its hold by observing the registration go away, which
\* by construction can only happen after the create was processed.
CtlDrop ==
    /\ dfAsOf # NoTime
    \* Once, not repeatedly: without this the release is appended forever.
    /\ ctlHold # NoTime
    /\ ctlHold' = NoTime
    /\ IF Mechanism = "acquire"
       THEN /\ interQ' = BroadcastInter(interQ, Release(epoch))
            /\ muxHold' = muxHold
       ELSE /\ interQ' = interQ
            /\ muxHold' = IF reported THEN NoTime ELSE muxHold
    /\ UNCHANGED <<ctlSince, dfAsOf, epoch, muxFloor, maintQ, since,
                   acquired, rendered, readerHold, everRegistered, reported>>

\* A reconnection. The multiplexer's per-connection state goes; the runtimes'
\* queues do NOT, because a stale-nonce command is stashed and still executes.
\* That is G2.
\*
\* NOTE: this action resets only controller and multiplexer state. `rendered`,
\* `readerHold` and `everRegistered` are replica state and survive, because
\* reconciliation decides their fate later rather than at the `Hello` itself. An
\* earlier version reset them here and produced a counterexample that was an
\* artifact of that.
\*
\* This action is NOT exercised by `Holds.cfg`, which sets MaxEpochs = 0. Modelling
\* a reconnection faithfully needs the holder identity to be epoch-scoped, since a
\* replayed dataflow gets a fresh transient id, and one holder shared across epochs
\* conflates two different dataflows. See the README.
Hello ==
    /\ epoch < MaxEpochs
    /\ epoch' = epoch + 1
    /\ muxHold' = NoTime
    /\ muxFloor' = 0
    /\ dfAsOf' = NoTime
    /\ ctlHold' = NoTime
    /\ UNCHANGED <<ctlSince, maintQ, interQ, since, acquired, rendered,
                   readerHold, everRegistered, reported>>

-----------------------------------------------------------------------------
\* Replica

\* Maintenance on process p applies its next command. This is the only place
\* compaction is realized, and the only place a hold is installed, so their
\* order here is exactly the order they had in the queue.
MaintStep(p) ==
    /\ Len(maintQ[p]) > 0
    /\ LET cmd == Head(maintQ[p]) IN
       /\ maintQ' = [maintQ EXCEPT ![p] = Tail(maintQ[p])]
       /\ CASE cmd.kind = "compact" ->
                    \* An installed hold, or a reader's own downgraded hold,
                    \* bounds what can be applied. Under-compacting is safe.
                    LET bound == IF acquired[p] # NoTime
                                 THEN acquired[p]
                                 ELSE IF readerHold[p] # NoTime
                                      THEN readerHold[p]
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
    /\ UNCHANGED <<ctlSince, ctlHold, dfAsOf, epoch, muxHold, muxFloor,
                   interQ, rendered, readerHold, everRegistered, reported>>

\* Interactive on process p applies its next command. Rendering registers the
\* import's own read hold in shared state, which needs no cooperation from
\* maintenance and so cannot be delayed by it.
InterStep(p) ==
    /\ Len(interQ[p]) > 0
    /\ LET cmd == Head(interQ[p]) IN
       /\ interQ' = [interQ EXCEPT ![p] = Tail(interQ[p])]
       /\ CASE cmd.kind = "create" ->
                    /\ rendered' = [rendered EXCEPT ![p] = TRUE]
                    /\ readerHold' = [readerHold EXCEPT ![p] = cmd.time]
                    /\ everRegistered' = [everRegistered EXCEPT ![p] = TRUE]
            [] cmd.kind = "release" ->
                    /\ readerHold' = [readerHold EXCEPT ![p] = NoTime]
                    /\ UNCHANGED <<rendered, everRegistered>>
    /\ UNCHANGED <<ctlSince, ctlHold, dfAsOf, epoch, muxHold, muxFloor,
                   maintQ, since, acquired, reported>>

\* Interactive reports a frontier for a dataflow it has rendered. This is the
\* signal the "cap" mechanism retires on. Subscribe collections never emit it,
\* which is why the proposal does not depend on it.
InterReport(p) ==
    /\ rendered[p]
    /\ ~reported
    /\ reported' = TRUE
    /\ UNCHANGED <<ctlSince, ctlHold, dfAsOf, epoch, muxHold, muxFloor,
                   maintQ, interQ, since, acquired, rendered, readerHold,
                   everRegistered>>

\* The import makes progress and downgrades its own hold. Being late here only
\* delays compaction, which is the asymmetry the design rests on.
ReaderDowngrade(p, t) ==
    /\ readerHold[p] # NoTime
    /\ readerHold[p] < t
    /\ readerHold' = [readerHold EXCEPT ![p] = t]
    /\ UNCHANGED <<ctlSince, ctlHold, dfAsOf, epoch, muxHold, muxFloor,
                   maintQ, interQ, since, acquired, rendered, everRegistered,
                   reported>>

\* The publisher, on the maintenance worker, reclaims the acquired hold once the
\* import's own registration has existed and gone. Intra-process, no command.
\*
\* `everRegistered` is what makes this sound: without it, "no registration" is
\* ambiguous between "the create has not been processed yet" and "the reader is
\* finished", and reclaiming in the first case is the defect TLC found.
PublisherReclaim(p) ==
    /\ acquired[p] # NoTime
    /\ everRegistered[p]
    /\ readerHold[p] = NoTime
    /\ acquired' = [acquired EXCEPT ![p] = NoTime]
    /\ UNCHANGED <<ctlSince, ctlHold, dfAsOf, epoch, muxHold, muxFloor,
                   maintQ, interQ, since, rendered, readerHold, everRegistered,
                   reported>>

-----------------------------------------------------------------------------

Next ==
    \/ \E t \in Times : CtlCreate(t)
    \/ \E t \in Times : CtlCompact(t)
    \/ CtlDrop
    \/ Hello
    \/ \E p \in Procs : MaintStep(p)
    \/ \E p \in Procs : InterStep(p)
    \/ \E p \in Procs : InterReport(p)
    \/ \E p \in Procs, t \in Times : ReaderDowngrade(p, t)
    \/ \E p \in Procs : PublisherReclaim(p)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* Properties

TypeOK ==
    /\ ctlSince \in Times
    /\ ctlHold \in Times \cup {NoTime}
    /\ dfAsOf \in Times \cup {NoTime}
    /\ muxHold \in Times \cup {NoTime}
    /\ muxFloor \in Times
    /\ \A p \in Procs : since[p] \in Times
    /\ \A p \in Procs : acquired[p] \in Times \cup {NoTime}
    /\ \A p \in Procs : readerHold[p] \in Times \cup {NoTime}
    /\ \A p \in Procs : everRegistered[p] \in BOOLEAN

\* I1, per process: an index is never compacted past the as_of of a dataflow
\* that has been created and whose reader on this process has not finished.
\* "Has not finished" is "created and this process has not released its reader
\* hold", which covers the window before it has even rendered.
I1 ==
    \A p \in Procs :
        (dfAsOf # NoTime /\ (~rendered[p] \/ readerHold[p] # NoTime))
            => since[p] <= dfAsOf

\* Compaction frontiers never regress on a maintenance queue.
NoRegression ==
    \A p \in Procs :
        \A i \in 1..Len(maintQ[p]) :
            maintQ[p][i].kind = "compact" => maintQ[p][i].time >= since[p]

\* Maintenance lag never exposes a read: if maintenance on p has compacted past
\* the dataflow's as_of, it cannot still be owing the hold that covers it. This
\* is the claim the equal-delay argument rests on.
LagNeverExposes ==
    \A p \in Procs :
        (dfAsOf # NoTime /\ since[p] > dfAsOf)
            => ~(\E i \in 1..Len(maintQ[p]) : maintQ[p][i].kind = "acquire")

\* No permanent pin: with the dataflow gone and its holds released, nothing
\* keeps a process from compacting.
\* A backstop on the state space. The guards above already bound how often each
\* controller action can fire, so this should never bite; it is here so that a
\* future action without a progress guard fails loudly instead of running forever.
QueuesBounded ==
    /\ \A p \in Procs : Len(maintQ[p]) =< 4
    /\ \A p \in Procs : Len(interQ[p]) =< 4

NoPermanentPin ==
    \A p \in Procs :
        (dfAsOf = NoTime /\ readerHold[p] = NoTime) => acquired[p] = NoTime

=============================================================================
