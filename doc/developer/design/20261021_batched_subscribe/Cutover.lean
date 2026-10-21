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
A model of the BatchedSubscribe cutover protocol in `../20261021_batched_subscribe.md`: replicas
mint batch descriptions and send batches without learning anything back, and the controller keeps
an accepted upper `U`, drops batches below it, and trims a batch that straddles it.

To run it, download Veil from `https://github.com/verse-lab/veil`, place this file at
`Examples/Materialize/Cutover.lean`, and run `lake build Examples.Materialize.Cutover`. No
Materialize build or CI job checks this file, so it can rot against the protocol it models.

What is checked:

* `fwd_content`, that every forwarded batch carries the content of its effective description.
* `fwd_bounded`, `fwd_unique`, `fwd_disjoint`, and `fwd_chain`, that the forwarded batches form
  one gapless, non-overlapping chain of descriptions from the start to `U`.
* `can_progress`, a proxy for liveness: the next batch the controller processes from a live
  replica starts at or before `U`, so it is either dropped or accepted, and never blocks. Veil
  checks safety, so this states that progress stays possible, not that it happens.

What is abstracted away:

* Batch content is an opaque `content lower upper`, the same for every replica. That is the
  definiteness assumption the protocol rests on, and it fails for replica-specific inputs such as
  introspection sources. `restrict v t` is the content `v` with updates before `t` removed, and
  the assumption on it says restriction commutes with content.
* Each replica's batches reach the controller in the order the replica sent them, which the
  compute transport provides. The controller may interleave replicas arbitrarily.
* Workers, shares, the stash, and blob deletion are not modeled. A batch is one message.
* Times are totally ordered.

Obligations the model shows the protocol needs:

* The controller accepts a batch with `lower <= U < upper` and trims it to `[U, upper)`. Accepting
  only `lower = U` drops the straddling batch, after which a replica whose boundaries differ from
  the one that set `U` never sends a batch starting at `U`.
* The controller processes a replica's batches in the order they were sent.
* A rehydrated replica starts at or before `U`, which the read holds on its inputs allow.

The full check covers 16 invariants and termination over initialization and 6 actions, which is
119 checks. Each mutant below breaks one obligation and was confirmed to fail:

* Accepting only `l = U` in `process` fails `can_progress`.
* Forwarding a straddling batch untrimmed fails `fwd_unique`, `fwd_disjoint`, and `fwd_chain`.
* Processing a replica's batches in any order fails `fwd_content` and `sent_after_head`.
* Rehydrating a replica beyond `U` fails `can_progress`.
-/

veil module Cutover

type replica
type time
type value

immutable relation le : time -> time -> Bool
immutable individual start : time
immutable function content : time -> time -> value
immutable function restrict : value -> time -> value

-- Controller state.
individual accepted : time
relation fwd : time -> time -> Bool
function fwd_val : time -> time -> value

-- Batches in flight from a replica to the controller, oldest first from `head`.
relation sent : replica -> time -> time -> Bool
function sent_val : replica -> time -> time -> value

-- Replica state. A replica learns nothing from the controller.
relation alive : replica -> Bool
-- The lower of the replica's oldest batch the controller has not processed.
function head : replica -> time
-- The lower of the replica's next description.
function next_lower : replica -> time

-- Environment.
individual input_frontier : time
individual input_since : time

#gen_state

assumption ∀ (x : time), le x x
assumption ∀ (x y z : time), le x y ∧ le y z → le x z
assumption ∀ (x y : time), le x y ∧ le y x → x = y
assumption ∀ (x y : time), le x y ∨ le y x
-- Restricting a batch's content to the times at or after `t` is the content from `t`.
assumption ∀ (l t h : time), le l t ∧ le t h → restrict (content l h) t = content t h

after_init {
  accepted := start;
  fwd L H := false;
  sent R L H := false;
  alive R := true;
  head R := start;
  next_lower R := start;
  input_frontier := start;
  input_since := start
}

action advance_input (t : time) {
  require le input_frontier t;
  input_frontier := t
}

-- Input read holds follow the accepted upper and never pass it.
action advance_since (t : time) {
  require le input_since t;
  require le t accepted;
  input_since := t
}

-- Mint a description from the replica's next lower and send its batch.
action mint (r : replica) (u : time) {
  require alive r;
  require ¬ le u (next_lower r);
  require le u input_frontier;
  sent r (next_lower r) u := true;
  sent_val r (next_lower r) u := content (next_lower r) u;
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

-- A replica comes back and renders its dataflow at a replayed as-of the read holds allow.
action rehydrate (r : replica) (t : time) {
  require ¬ alive r;
  require le input_since t;
  require le t accepted;
  alive r := true;
  head r := t;
  next_lower r := t
}

safety [fwd_content] fwd L H → fwd_val L H = content L H
safety [fwd_bounded] fwd L H → ¬ le H L ∧ le H accepted
safety [fwd_unique] fwd L H1 ∧ fwd L H2 → H1 = H2
safety [fwd_disjoint] fwd L1 H1 ∧ fwd L2 H2 ∧ ¬ le L2 L1 → le H1 L2
safety [fwd_chain] fwd L H → L = start ∨ ∃ (L0 : time), fwd L0 L

invariant [accepted_reached] accepted = start ∨ ∃ (L : time), fwd L accepted
invariant [start_below_accepted] le start accepted
invariant [sent_content] sent R L H → sent_val R L H = content L H
invariant [sent_bounded] sent R L H → ¬ le H L
invariant [sent_after_head] sent R L H → le (head R) L ∧ le H (next_lower R)
invariant [sent_alive] sent R L H → alive R
invariant [sent_unique] sent R L H1 ∧ sent R L H2 → H1 = H2
invariant [sent_disjoint] sent R L1 H1 ∧ sent R L2 H2 ∧ ¬ le L2 L1 → le H1 L2
invariant [head_below_lower] alive R → le (head R) (next_lower R)
invariant [since_below_accepted] le input_since accepted

-- Liveness proxy: a live replica's oldest unprocessed batch starts at or before `U`.
invariant [can_progress] alive R → le (head R) accepted

#gen_spec

#time #check_invariants

end Cutover
