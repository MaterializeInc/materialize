---------------------------- MODULE protocol ----------------------------
\* A model of the two-runtime compute command protocol, small enough to check
\* exhaustively and precise enough to have caught the bug it was written after.
\*
\* The property under test is I1 from design.md: an index's `since` never passes
\* the `as_of` of a dataflow that imports it, for that dataflow's lifetime.
\*
\* What is modelled: one index, one interactive dataflow, the controller's read
\* hold, two independent command streams, and the point at which each runtime
\* realizes a command. What is deliberately not modelled: data, frontiers as
\* antichains (a single time stands in), rendering cost, and failure.
\*
\* Run with TLC. `Spec` with `I1` as an invariant fails without capping and
\* holds with it, which is the whole point of the file.

EXTENDS Naturals, Sequences, TLC

CONSTANTS
    Times,          \* the totally ordered timestamps, e.g. 0..3
    Capping         \* TRUE models the multiplexer's hold floor, FALSE does not

VARIABLES
    ctlHold,        \* the controller's read hold on the index: a time, or NoHold
    maintQueue,     \* commands the maintenance runtime has not processed
    interQueue,     \* commands the interactive runtime has not processed
    since,          \* the maintenance replica's compaction frontier for the index
    rendered,       \* whether the interactive dataflow has rendered
    dfAsOf,         \* the as_of of the interactive dataflow, or NoDataflow
    muxHold         \* the multiplexer's recorded hold floor, or NoHold

vars == <<ctlHold, maintQueue, interQueue, since, rendered, dfAsOf, muxHold>>

NoHold == -1
NoDataflow == -1

\* Commands. `Create(t)` goes only to the interactive queue, `Compact(t)` only to
\* the maintenance queue. That asymmetry is the entire subject of the model.
Create(t) == [kind |-> "create", time |-> t]
Compact(t) == [kind |-> "compact", time |-> t]

Init ==
    /\ ctlHold = NoHold
    /\ maintQueue = <<>>
    /\ interQueue = <<>>
    /\ since = 0
    /\ rendered = FALSE
    /\ dfAsOf = NoDataflow
    /\ muxHold = NoHold

\* The controller creates an interactive dataflow at time t. It may only choose a
\* time the index can still serve, and it takes a read hold there. The multiplexer
\* records the hold BEFORE the command reaches interactive, which is the ordering
\* the fix relies on.
CtlCreate(t) ==
    /\ dfAsOf = NoDataflow
    /\ t >= since
    /\ ctlHold' = t
    /\ dfAsOf' = t
    /\ muxHold' = IF Capping THEN t ELSE NoHold
    /\ interQueue' = Append(interQueue, Create(t))
    /\ UNCHANGED <<maintQueue, since, rendered>>

\* The controller releases its hold and allows compaction to t. It only does this
\* once its own view of the dataflow is finished, which is the point: the
\* interactive runtime may not have started.
CtlCompact(t) ==
    /\ t > since
    /\ ctlHold = NoHold \/ t <= ctlHold
    /\ LET capped == IF Capping /\ muxHold # NoHold /\ t > muxHold
                     THEN muxHold
                     ELSE t
       IN maintQueue' = Append(maintQueue, Compact(capped))
    /\ UNCHANGED <<ctlHold, interQueue, since, rendered, dfAsOf, muxHold>>

\* The controller decides the dataflow is done. In the real system this is an
\* AllowCompaction to the empty frontier on the dataflow's export.
CtlDrop ==
    /\ dfAsOf # NoDataflow
    /\ ctlHold' = NoHold
    /\ muxHold' = NoHold
    /\ UNCHANGED <<maintQueue, interQueue, since, rendered, dfAsOf>>

\* Maintenance processes its next command. This is where compaction is realized.
MaintStep ==
    /\ Len(maintQueue) > 0
    /\ LET cmd == Head(maintQueue) IN
       /\ since' = IF cmd.time > since THEN cmd.time ELSE since
       /\ maintQueue' = Tail(maintQueue)
    /\ UNCHANGED <<ctlHold, interQueue, rendered, dfAsOf, muxHold>>

\* Interactive processes its next command. Rendering is where the reader's hold
\* becomes real on the replica, and it can be arbitrarily later than the create.
InterStep ==
    /\ Len(interQueue) > 0
    /\ LET cmd == Head(interQueue) IN
       /\ rendered' = IF cmd.kind = "create" THEN TRUE ELSE rendered
       /\ interQueue' = Tail(interQueue)
    /\ UNCHANGED <<ctlHold, maintQueue, since, dfAsOf, muxHold>>

Next ==
    \/ \E t \in Times : CtlCreate(t)
    \/ \E t \in Times : CtlCompact(t)
    \/ CtlDrop
    \/ MaintStep
    \/ InterStep

Spec == Init /\ [][Next]_vars /\ WF_vars(MaintStep) /\ WF_vars(InterStep)

------------------------------------------------------------------------

\* I1: the index is never compacted past the as_of of a dataflow that has been
\* created and not yet finished reading. "Not yet finished" is modelled as "not
\* yet rendered", the window the real bug lives in.
I1 ==
    (dfAsOf # NoDataflow /\ ~rendered) => since <= dfAsOf

\* Liveness: capping must not pin the index forever. Once the dataflow is done,
\* compaction can still make progress.
NoPermanentPin ==
    []<>(dfAsOf = NoDataflow => muxHold = NoHold)

============================================================================
