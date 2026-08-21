# Where a peek's walk runs

**Status: parked, deliberately.** The problem is real and M1's evidence is the strongest in
this directory. What is not settled is which remedy to keep, and that is answerable by
experiment on fixtures that already exist. Implementing every candidate, merging them, and
then learning which one was needed is the expensive order.

Nothing here is gated on the interactive runtime, and the interactive runtime does not need
any of it. See [design.md](design.md) for the mechanism decomposition these solutions are
scored against, and CPU-217 for this work's tracking issue.

## Two axes, not one

An earlier draft of this proposed a single `compute_peek_substrate` parameter enumerating
placements, on the argument that a walk runs in exactly one place so the choice is
mutually exclusive. **That argument is wrong.** Where the walk runs and whether it yields
are independent:

|  | run to completion | yields |
|---|---|---|
| **on the serving worker** | today's default | S1, cooperative slicing, PR #38040 |
| **off the worker** | S5, a blocking task | coherent, unmeasured, unimplemented |

A walk on a blocking task can perfectly well have yield points. It would gain a place to
observe a cancellation request, which is S2's prerequisite, and a bound on how long one
walk pins the batches its cursor covers. So the fourth cell is not a nonsense state that a
config should forbid, it is a candidate nobody has tried.

The single-selector shape would have made that cell unreachable, which is the failure mode
of collapsing two questions into one name. design.md already had this right, listing S1 and
S5 as separate solutions. Any future parameter follows the axes: placement and preemption
are separate settings, or one setting over the cross, but not one setting over placement
alone.

## What is measured, per cell

| Cell | M1, peek behind peek | M2, peek behind an activation | M8, freshness |
|---|---|---|---|
| on-worker, run to completion | baseline: E1 max 5783.7 ms, E11 58 of 261 slow | baseline: E12 p90 129.5 ms | baseline: E13 2340 ms peak |
| on-worker, yielding (S1) | **predicted only** | not measured | E13 365 ms peak, light write load |
| off-worker (S5) | E1 max 180.4 ms flat, E11 0 of 261, E8b 29151.7 to 152.4 ms | **worse**: E12 p90 148.2 ms | E13 about 101 ms |
| off-worker, yielding | nothing | nothing | nothing |

Two entries in that table decide the question and neither is filled in.

**S1 on M1 is predicted, not measured.** It is the presumed default and the row with the
most cells empty. Everything the decision rests on is an argument about its quantum.

**Yielding does not rescue S5 on M2.** Worth stating because the new cell invites the
hope. E12's regression is dispatch timing: the snapshot is taken in `process_peeks`, which
runs only after `step_or_park` returns, and retirement costs another step. That cost is
paid before the walk starts, so giving the walk yield points cannot recover it. S9,
size- or residency-aware routing, is the candidate that addresses it.

## The experiment that decides

The cheap decisive question is whether S1 alone matches S5 on the three fixtures that
carry S5's case. All three exist and are described in the project document "Interactive read isolation:
experimental evaluation":

* **E1**, a point lookup behind three concurrent scans at walk costs of 23, 190 and 2170 ms.
* **E11**, the skewed point lookup, a hot key holding millions of values under open-loop
  arrivals.
* **E8b**, a lookup on a resident index behind swap-resident walks at matched swap depth.

Decision rule, registered before running: **if S1 matches S5 within noise on all three,
S5 has no remaining justification and should be deleted rather than merged.** If S5 wins on
E8b alone, its case is the unattributed swap-walk duration (M11) and nothing else, which is
a much narrower claim than the one it was built on.

This needs PR #38040 and the existing fixtures. It does not need the interactive runtime,
which is why parking costs nothing.

## Why this is parked rather than dropped

The evidence that *something* is needed is not in doubt. E11 is a field-reported shape, and
E8b turned a 29.2 second victim latency into 152 ms. What is in doubt is which of four cells
to keep, and there are two reasons not to answer that by building:

The candidates are substitutes, not complements. S1 and S5 both create a preemption point
for the same walk, so shipping both means carrying two mechanisms where measurement is
expected to retire one. S5 already has no axis on which it is uniquely best: E2 showed it
is not needed for the peek-tail win once the walk moves, E12 measured it behind doing
nothing, and S1 reaches M8 as well.

And a merged mechanism is harder to delete than an unmerged one. S5 is about 700 lines
including `local_snapshot.rs`, `PendingPeek::IndexOffload` and its metric. Landing that to
discover S1 subsumes it means removing it afterwards, from a tree where something may
already depend on it.

## Consequence for the interactive-runtime work

S5's code currently rides on the interactive-runtime branch, where it is a conditional on
top of an inline walk rather than load-bearing: interactive peeks resolve through
`shared_index_peek_response`, and the offload branch is entered only when the parameter
selects it. So it can come out without touching how the interactive runtime serves reads.

Removing it costs the branch E1 and E11 as supporting evidence, which is correct: E2 showed
those results belong to the walk substrate rather than to the second runtime, and the
interactive runtime's own case is E7 and E9. A branch that keeps S5 is a branch arguing for
two things at once, and one of them is the one we just agreed to decide by experiment.
