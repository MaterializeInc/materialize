-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

import Veil

/-
A model of the BatchedSubscribe cutover protocol in `../20261021_batched_subscribe.md`: every
replica renders at its as-of with the snapshot and sends contiguous batches without learning
anything back, and the controller keeps an accepted upper `U`, drops batches below it, and trims a
batch that straddles it.

To run it, download Veil from `https://github.com/verse-lab/veil`, place this file at
`Examples/Materialize/Cutover.lean`, and run `lake build Examples.Materialize.Cutover`. No
Materialize build or CI job checks this file, so it can rot against the protocol it models.

What is checked:

* `fwd_content`, that every forwarded batch carries the content of its effective description:
  the snapshot content for the subscribe's first description when the subscribe has a snapshot,
  the delta content otherwise.
* `fwd_bounded`, `fwd_unique`, `fwd_disjoint`, and `fwd_chain`, that the forwarded batches form
  one gapless, non-overlapping chain of descriptions from the start to `U`.
* `can_progress`, a proxy for liveness: the next batch the controller processes from a live
  replica starts at or before `U`, so it is either dropped or accepted, and never blocks. Veil
  checks safety, so this states that progress stays possible, not that it happens.

What is modeled about read holds: the subscribe collection's since starts at the as-of and
advances only to a time strictly before `U`, which for discrete times is the implied read hold of
a write-only collection, one step behind its write frontier. A replica renders at the since.

What is abstracted away:

* `snap l h` is the content of `[l, h)` with the accumulation of everything before `l` at `l`,
  and `delta l h` is the updates with times in `[l, h)`. Both are the same for every replica,
  which is the definiteness assumption the protocol rests on. `restrict v t` removes the updates
  before `t`, and the assumptions on it state how it acts on the two kinds of content.
* Each replica's batches reach the controller in the order the replica sent them. The controller
  may interleave replicas arbitrarily. A replica that loses batches in flight re-renders, which
  `lose` models.
* Workers, shares, the partitioned merge, the stash, and blob deletion are not modeled. A batch
  is one message.
* Times are totally ordered.

Obligations the model shows the protocol needs:

* The controller accepts a batch with `lower <= U < upper` and trims it to `[U, upper)`. Accepting
  only `lower = U` stalls the subscribe once the replica that set `U` fails.
* The controller processes a replica's batches in the order they were sent.
* A replica that loses batches in flight re-renders at the since.
* The since stays one step behind `U`. A replica rendering at `U` itself after the first accepted
  batch sends a snapshot where the client expects a delta.

The full check covers 26 invariants and termination over initialization and 7 actions, which
is 216 checks. Each mutant below breaks one obligation and was confirmed to fail:

* Accepting only `l = U` in `process` fails `can_progress`.
* Forwarding a straddling batch untrimmed fails `fwd_content`, `fwd_unique`, `fwd_disjoint`, and
  `fwd_chain`.
* Processing a replica's batches in any order fails `fwd_content`, `sent_after_head`, and
  `chain_contiguous`.
* Letting the since reach `U` fails `since_behind` and `since_behind_no_snapshot`, the invariants
  that state the read-hold obligation.
* Losing batches in flight without re-rendering fails `chain_from_head`.
* Rehydrating one step past the since without the snapshot fails `can_progress`.

An earlier version stated "one step behind" with a successor function on time. The solver could
not decide its mutants and reported them unknown rather than refuted, so the model uses strict
order on discrete times instead.
-/

veil module Cutover

type replica
type time
type value

immutable relation le : time -> time -> Bool
immutable individual as_of : time
immutable individual start : time
immutable relation with_snapshot : Bool
immutable function snap : time -> time -> value
immutable function delta : time -> time -> value
immutable function restrict : value -> time -> value

-- Controller state.
individual accepted : time
relation fwd : time -> time -> Bool
function fwd_val : time -> time -> value
-- The subscribe collection's since, which is also the read hold on its inputs.
individual since : time

-- Batches in flight from a replica to the controller, oldest first from `head`.
relation sent : replica -> time -> time -> Bool
function sent_val : replica -> time -> time -> value

-- Replica state. A replica learns nothing from the controller.
relation alive : replica -> Bool
-- The as-of the replica rendered at, which its first batch starts at.
function rendered : replica -> time
-- The lower of the replica's oldest batch the controller has not processed.
function head : replica -> time
-- The lower of the replica's next description.
function next_lower : replica -> time

individual input_frontier : time

#gen_state

assumption ∀ (x : time), le x x
assumption ∀ (x y z : time), le x y ∧ le y z → le x z
assumption ∀ (x y : time), le x y ∧ le y x → x = y
assumption ∀ (x y : time), le x y ∨ le y x
-- The start is the as-of with a snapshot and one step past it without.
assumption with_snapshot → start = as_of
-- Without a snapshot the start is the least time after the as-of.
assumption ¬ with_snapshot → ¬ le start as_of
assumption ∀ (t : time), ¬ with_snapshot ∧ ¬ le t as_of → le start t
-- Restricting snapshot content to its own lower keeps it, past its lower leaves a delta.
assumption ∀ (l h : time), restrict (snap l h) l = snap l h
assumption ∀ (l t h : time), le l t ∧ ¬ l = t → restrict (snap l h) t = delta t h
assumption ∀ (l t h : time), le l t → restrict (delta l h) t = delta t h

after_init {
  accepted := start;
  fwd L H := false;
  since := as_of;
  sent R L H := false;
  alive R := true;
  rendered R := as_of;
  head R := as_of;
  next_lower R := as_of;
  input_frontier := start
}

action advance_input (t : time) {
  require le input_frontier t;
  input_frontier := t
}

-- The implied read hold of a write-only collection follows one step behind its write frontier.
-- Times are discrete, so `t < U` is `t <= U - 1`.
action advance_since (t : time) {
  require le since t;
  require ¬ le accepted t;
  since := t
}

-- Mint a description from the replica's next lower and send its batch. The first batch of a
-- rendering carries the snapshot at the rendered as-of.
action mint (r : replica) (u : time) {
  require alive r;
  require ¬ le u (next_lower r);
  require le u input_frontier;
  sent r (next_lower r) u := true;
  if next_lower r = rendered r then
    sent_val r (next_lower r) u := snap (next_lower r) u
  else
    sent_val r (next_lower r) u := delta (next_lower r) u;
  next_lower r := u
}

-- The controller processes a replica's oldest batch: drop it if it ends at or before `U`,
-- otherwise accept it trimmed to start at `U`.
action process (r : replica) (l : time) (h : time) {
  require sent r l h;
  require l = head r;
  sent r l h := false;
  head r := h;
  if ¬ le h accepted then
    fwd accepted h := true;
    fwd_val accepted h := restrict (sent_val r l h) accepted;
    accepted := h
}

-- A replica fails. Its batches in flight are lost.
action crash (r : replica) {
  require alive r;
  alive r := false;
  sent r L H := false
}

-- A replica comes back and renders its dataflow at the since, with the snapshot.
action rehydrate (r : replica) {
  require ¬ alive r;
  alive r := true;
  rendered r := since;
  head r := since;
  next_lower r := since
}

-- A live replica loses its batches in flight, for example on a reconnect, and re-renders.
action lose (r : replica) {
  require alive r;
  sent r L H := false;
  rendered r := since;
  head r := since;
  next_lower r := since
}

safety [fwd_content]
  fwd L H →
    ((with_snapshot ∧ L = start) → fwd_val L H = snap L H) ∧
    (¬ (with_snapshot ∧ L = start) → fwd_val L H = delta L H)
safety [fwd_bounded] fwd L H → ¬ le H L ∧ le H accepted
safety [fwd_unique] fwd L H1 ∧ fwd L H2 → H1 = H2
safety [fwd_disjoint] fwd L1 H1 ∧ fwd L2 H2 ∧ ¬ le L2 L1 → le H1 L2
safety [fwd_chain] fwd L H → L = start ∨ ∃ (L0 : time), fwd L0 L

invariant [accepted_reached] accepted = start ∨ ∃ (L : time), fwd L accepted
invariant [start_below_accepted] le start accepted
invariant [as_of_below_since] le as_of since
invariant [since_below_accepted] le since accepted
-- Once anything is accepted, the since is strictly behind `U`.
invariant [since_behind] ¬ accepted = start → ¬ le accepted since
-- Without a snapshot, the since is behind `U` from the start.
invariant [since_behind_no_snapshot] ¬ with_snapshot → ¬ le accepted since
invariant [sent_first]
  sent R L H ∧ L = rendered R → sent_val R L H = snap L H
invariant [sent_later]
  sent R L H ∧ ¬ L = rendered R → sent_val R L H = delta L H
invariant [sent_bounded] sent R L H → ¬ le H L
invariant [sent_after_head] sent R L H → le (head R) L ∧ le H (next_lower R)
invariant [sent_after_rendered] sent R L H → le (rendered R) L
invariant [sent_alive] sent R L H → alive R
invariant [sent_unique] sent R L H1 ∧ sent R L H2 → H1 = H2
invariant [sent_disjoint] sent R L1 H1 ∧ sent R L2 H2 ∧ ¬ le L2 L1 → le H1 L2
invariant [head_below_lower] alive R → le (head R) (next_lower R)
invariant [rendered_below_head] alive R → le (rendered R) (head R)
-- A rendering at a time other than the start only happens behind `U`.
invariant [rendered_behind]
  alive R ∧ ¬ (with_snapshot ∧ rendered R = start) → ¬ le accepted (rendered R)
invariant [rendered_at_since] alive R → le as_of (rendered R) ∧ le (rendered R) since

-- Liveness proxy: a live replica's oldest unprocessed batch starts at or before `U`, and its
-- batches in flight form a gapless chain from `head` to `next_lower`, so the controller can
-- always process its next batch.
invariant [can_progress] alive R → le (head R) accepted
invariant [chain_from_head]
  alive R ∧ ¬ head R = next_lower R → ∃ (H : time), sent R (head R) H
invariant [chain_contiguous]
  sent R L H ∧ ¬ H = next_lower R → ∃ (H2 : time), sent R H H2

#gen_spec

#time #check_invariants

end Cutover
