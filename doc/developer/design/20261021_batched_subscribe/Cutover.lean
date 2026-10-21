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
mint batch descriptions and send batches, the controller accepts the batch whose lower equals its
accepted upper `U`, and replicas learn `U` through feedback.

To run it, download Veil from `https://github.com/verse-lab/veil`, place this file at
`Examples/Materialize/Cutover.lean`, and run `lake build Examples.Materialize.Cutover`. No
Materialize build or CI job checks this file, so it can rot against the protocol it models.

What is checked:

* `fwd_content`, that every forwarded batch carries the content of its description.
* `fwd_bounded`, `fwd_unique`, `fwd_disjoint`, and `fwd_chain`, that the forwarded batches form
  one gapless, non-overlapping chain of descriptions from the as-of to `U`.
* `can_progress`, a proxy for liveness: a live replica that has applied the current `U` either
  stands at `U` or has a batch starting at `U` in flight, and holds the data to mint from `U`.
  Veil checks safety, so this states that progress stays enabled, not that it happens.

What is abstracted away:

* Batch content is an opaque `content lower upper`, the same for every replica. That is the
  definiteness assumption the protocol rests on, and it fails for replica-specific inputs such as
  introspection sources.
* Messages between a replica and the controller can be delayed and reordered arbitrarily. The
  real transport is ordered, and the protocol does not rely on it.
* Workers, shares, the stash, and blob deletion are not modeled. A batch is one message.
* The input frontier is one global time, and input read holds are a single `input_since`.
* Times are totally ordered.

Obligations the model shows the protocol needs:

* Acceptance requires `lower = U`. Accepting `lower <= U < upper` forwards overlapping batches.
* A replica keeps its data from the `U` it last learned, not from its own last minted upper.
  Discarding data at its own upper leaves it unable to mint from `U` when its batches were
  rejected.
* Applying feedback rewinds the replica to `U` when it is ahead. A replica that only jumps
  forward never mints a batch starting at `U` once its own boundaries straddle it, so the
  subscribe stalls when the replica that set `U` fails.
* A replica rehydrates at `U`, and input read holds stay at or below `U`.

The full check covers 14 invariants and termination over initialization and 8 actions, which is
135 checks. Each mutant below breaks one obligation and was confirmed to fail:

* Accepting `le l U ∧ ¬ le h U` in `process` fails `fwd_unique`, `fwd_disjoint`, and `fwd_chain`.
* Setting `retain` to the replica's own last minted upper in `apply_feedback` fails
  `retain_below_lower` and `retain_below_known`.
* Applying feedback only when it moves the replica forward fails `can_progress`.
* Letting `learn` report a time beyond `U` fails `known_below_accepted`.

Not modeled, and so not checked: a replica rehydrating at a stale as-of whose inputs were
compacted, which disables `rehydrate` rather than violating an invariant.
-/

veil module Cutover

type replica
type time
type value

immutable relation le : time -> time -> Bool
immutable individual as_of : time
immutable function content : time -> time -> value

-- Controller state.
individual accepted : time
relation fwd : time -> time -> Bool
function fwd_val : time -> time -> value

-- Batches in flight from a replica to the controller.
relation sent : replica -> time -> time -> Bool
function sent_val : replica -> time -> time -> value

-- Replica state.
relation alive : replica -> Bool
-- The lower of the replica's next description.
function next_lower : replica -> time
-- The accepted upper the replica last learned.
function known : replica -> time
-- The replica holds the updates at times at or after this.
function retain : replica -> time
-- Set when the replica applies feedback, cleared when it learns a new `U`.
relation applied : replica -> Bool

-- Environment.
individual input_frontier : time
individual input_since : time

#gen_state

assumption ∀ (x : time), le x x
assumption ∀ (x y z : time), le x y ∧ le y z → le x z
assumption ∀ (x y : time), le x y ∧ le y x → x = y
assumption ∀ (x y : time), le x y ∨ le y x

after_init {
  accepted := as_of;
  fwd L H := false;
  sent R L H := false;
  alive R := true;
  next_lower R := as_of;
  known R := as_of;
  retain R := as_of;
  applied R := true;
  input_frontier := as_of;
  input_since := as_of
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
  require le (retain r) (next_lower r);
  sent r (next_lower r) u := true;
  sent_val r (next_lower r) u := content (next_lower r) u;
  next_lower r := u
}

-- The controller processes a batch: accept it if it starts at `U`, drop it otherwise.
action process (r : replica) (l : time) (h : time) {
  require sent r l h;
  sent r l h := false;
  if l = accepted then
    fwd l h := true;
    fwd_val l h := sent_val r l h;
    accepted := h
}

-- Feedback reaches a replica, possibly stale, but never older than what it already knows.
action learn (r : replica) (t : time) {
  require alive r;
  require le (known r) t;
  require le t accepted;
  known r := t;
  applied r := false
}

-- The replica applies what it learned: it rewinds or jumps to `U` and drops older data.
action apply_feedback (r : replica) {
  require alive r;
  next_lower r := known r;
  retain r := known r;
  applied r := true
}

-- A replica fails. Its batches in flight are lost.
action crash (r : replica) {
  require alive r;
  alive r := false;
  sent r L H := false
}

-- A replica comes back and renders its dataflow at the accepted upper.
action rehydrate (r : replica) {
  require ¬ alive r;
  require le input_since accepted;
  alive r := true;
  next_lower r := accepted;
  known r := accepted;
  retain r := accepted;
  applied r := true
}

safety [fwd_content] fwd L H → fwd_val L H = content L H
safety [fwd_bounded] fwd L H → ¬ le H L ∧ le H accepted
safety [fwd_unique] fwd L H1 ∧ fwd L H2 → H1 = H2
safety [fwd_disjoint] fwd L1 H1 ∧ fwd L2 H2 ∧ ¬ le L2 L1 → le H1 L2
safety [fwd_chain] fwd L H → L = as_of ∨ ∃ (L0 : time), fwd L0 L

invariant [accepted_reached] accepted = as_of ∨ ∃ (L : time), fwd L accepted
invariant [as_of_below_accepted] le as_of accepted
invariant [sent_content] sent R L H → sent_val R L H = content L H
invariant [sent_bounded] sent R L H → ¬ le H L
invariant [known_below_accepted] alive R → le (known R) accepted
invariant [retain_below_lower] alive R → le (retain R) (next_lower R)
invariant [retain_below_known] alive R → le (retain R) (known R)
invariant [since_below_accepted] le input_since accepted

-- Liveness proxy: a live replica that applied the current `U` stands at it, or has a batch
-- starting at it in flight.
invariant [can_progress]
  alive R ∧ applied R ∧ known R = accepted →
    next_lower R = accepted ∨ ∃ (H : time), sent R accepted H

#gen_spec

#time #check_invariants

end Cutover
