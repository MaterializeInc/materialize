import Veil

/-
A model of the rendition handover protocol specified in
`doc/developer/platform/architecture-storage.md`, checked against that document's definiteness
claim: a collection transitioning through renditions is still perceived by every reader as one
definite collection advancing in time.

To run it, clone `https://github.com/verse-lab/veil`, place this file at
`Examples/Materialize/RenditionHandover.lean`, and run
`lake build Examples.Materialize.RenditionHandover`. No Materialize build or CI job checks this
file, so it can rot against the protocol it models.

What is checked, in increasing difficulty:

* `gc_safety`, that a rendition is never deleted while a reader can still reach it.
* `handover_agreement`, that independent readers agree wherever both have produced output.
* `frontier_soundness`, that a reader's output at a time is the contents of the rendition
  actually active at that time.

What is abstracted away:

* A collection is an opaque accumulated `value` per (rendition, time). No update sets, no
  consolidation, no compaction internals, no timely.
* Times are totally ordered. `architecture-storage.md` says to replace max and min with join and
  meet for partially ordered times, so whether the protocol survives antichains is not answered
  here.
* Concurrent writers into one shard of one rendition are assumed to produce identical data, which
  is that document's own precondition. Divergence across renditions is what handover exists to
  reconcile, and is what this model checks.

Three obligations the protocol relies on that `architecture-storage.md` does not state:

* A reader observes the metadata over every interval it crosses. The frontier keeps what it
  observed true, rather than excusing it from observing. A reader advancing on the frontier alone
  produces the wrong contents once a handover falls in the interval it skipped.
* The metadata collection is append-only in time order, so appending a handover advances its
  frontier past that handover's time. `persist` supplies this through `upper`. Without it, a
  handover appended at an earlier time overwrites a later one's suffix and leaves two renditions
  active at one time.
* Beyond the metadata frontier exactly one rendition is active, across the whole remaining suffix.
  The protocol checks the outgoing rendition at a single time while the handover rewrites the
  entire suffix, and only this makes that check sufficient.
-/

veil module RenditionHandover

type rendition
type time
type reader
type value

immutable relation le : time -> time -> Bool

-- What a rendition accumulates to at a time. Fixed once written.
immutable function contents : rendition -> time -> value

-- The metadata collection, accumulated: `active r t` iff `r` is the active rendition at `t`.
relation active : rendition -> time -> Bool
-- The metadata collection is sealed through here.
individual meta_frontier : time

relation started : reader -> Bool
function cursor : reader -> time
function cur_rend : reader -> rendition
function read_cap : reader -> time
-- `produced r t` iff reader `r` has emitted its output for time `t`.
relation produced : reader -> time -> Bool
function output : reader -> time -> value

relation deleted : rendition -> Bool

immutable individual zero : time
immutable individual r0 : rendition

#gen_state

assumption ∀ (x : time), le x x
assumption ∀ (x y z : time), le x y ∧ le y z → le x z
assumption ∀ (x y : time), le x y ∧ le y x → x = y
assumption ∀ (x y : time), le x y ∨ le y x
assumption ∀ (x : time), le zero x

after_init {
  active R T := decide $ R = r0;
  meta_frontier := zero;
  started R := false;
  produced R T := false;
  deleted R := false
}

-- Append a handover to the metadata collection.
--
-- Requiring the handover time to be strictly beyond the metadata frontier is the writer-side
-- obligation `architecture-storage.md` leaves implicit. Readers rely on a frontier advance as
-- proof that nothing changed below it, so inserting below an advanced frontier makes that proof
-- false.
action insert_handover (r1 : rendition) (r2 : rendition) (t : time) {
  require active r1 t;
  require r1 ≠ r2;
  require ¬ le t meta_frontier;
  active r1 T := active r1 T && !(le t T);
  active r2 T := active r2 T || le t T;
  -- Appending at `t` advances the metadata collection's upper, which is what stops a later
  -- handover from being written at an earlier time and clobbering this one's suffix.
  meta_frontier := t
}

action advance_meta_frontier (t : time) {
  require le meta_frontier t;
  meta_frontier := t
}

action reader_start (rdr : reader) (r : rendition) (t : time) {
  require ¬ started rdr;
  require le t meta_frontier;
  require active r t;
  require ¬ deleted r;
  started rdr := true;
  cursor rdr := t;
  cur_rend rdr := r;
  read_cap rdr := t;
  produced rdr t := true;
  output rdr t := contents r t
}

-- Advance across an interval the metadata shows no rendition change over.
--
-- The final `require` is the reader's observation of the metadata. Dropping it, so that the
-- reader advances on the frontier alone, is what breaks `frontier_soundness`.
action reader_advance (rdr : reader) (t : time) {
  require started rdr;
  require le (cursor rdr) t;
  require le t meta_frontier;
  require ∀ (s : time), le (cursor rdr) s ∧ le s t → active (cur_rend rdr) s;
  cursor rdr := t;
  produced rdr t := true;
  output rdr t := contents (cur_rend rdr) t
}

-- Cross a handover.
--
-- The strategy in `architecture-storage.md` is to emit the `rendition1` snapshot negated plus the
-- `rendition2` snapshot, so the result accumulates to `rendition2` at the handover time. That
-- accumulated result is what is modeled.
action reader_handover (rdr : reader) (r2 : rendition) (t : time) {
  require started rdr;
  require le (cursor rdr) t;
  require le t meta_frontier;
  require active r2 t;
  require ¬ active (cur_rend rdr) t;
  require ¬ deleted r2;
  cur_rend rdr := r2;
  cursor rdr := t;
  produced rdr t := true;
  output rdr t := contents r2 t
}

action downgrade_cap (rdr : reader) (t : time) {
  require started rdr;
  require le (read_cap rdr) t;
  require le t (cursor rdr);
  read_cap rdr := t
}

-- Delete a rendition no reader can still reach, given the capabilities they hold.
action gc_rendition (r : rendition) {
  require ∀ (rdr : reader) (t : time),
    started rdr ∧ le (read_cap rdr) t → ¬ active r t;
  require ∀ (rdr : reader), started rdr → cur_rend rdr ≠ r;
  deleted r := true
}

-- A rendition is never deleted while a reader still needs it.
safety [gc_safety] started R → ¬ deleted (cur_rend R)

-- The central definiteness claim: independent readers agree wherever both have produced.
safety [handover_agreement] produced R1 T ∧ produced R2 T → output R1 T = output R2 T

-- A reader's output at a time is the contents of the rendition actually active then.
--
-- This is the writer-side obligation seen from the reader. Violate the frontier discipline in
-- `insert_handover` and this is what breaks, silently.
safety [frontier_soundness] produced R T ∧ active RD T → output R T = contents RD T

-- Supporting: the metadata names exactly one active rendition per time.
invariant [unique_active] active R1 T ∧ active R2 T → R1 = R2

-- Supporting: beyond the metadata frontier exactly one rendition is active, for the whole
-- remaining suffix. Each handover rewrites the suffix from its time and advances the frontier to
-- it, so no handover can ever land inside an already-decided stretch. Without this, a handover
-- could be appended above a different rendition's era and leave two active at one time.
invariant [suffix_uniform]
  le meta_frontier T ∧ le meta_frontier U ∧ active R T → active R U

-- Supporting: a reader has only produced output at times it has reached.
invariant [produced_below_cursor] produced R T → started R ∧ le T (cursor R)

-- Supporting: a reader's capability never passes its cursor.
invariant [cap_below_cursor] started R → le (read_cap R) (cursor R)

-- Supporting: a reader never reads beyond the metadata frontier. This is the reader-side half of
-- the frontier discipline, and it is what makes the writer's obligation in `insert_handover`
-- sufficient. Without it a reader can produce output at a time a later handover still claims.
invariant [cursor_below_frontier] started R → le (cursor R) meta_frontier

invariant [produced_below_frontier] produced R T → le T meta_frontier

#time #gen_spec

#check_invariants

end RenditionHandover
