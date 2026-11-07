-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

module

public import Veil

/-
A model of the rendezvous publication protocol and the compute lifecycle that drives it.

See `README.md` next to this file for how to run it, the lifecycle facts it assumes with their
citations, and what the checks found.

Scope. One index id, one worker of each runtime, the `oks` arrangement only.

* `writer` is one incarnation of the index on the maintenance runtime: one trace, its writer, and
  its attachment to a point.
* `point` is one `Rendezvous`, the publication point of one registry slot.
* `imp` is one import on the interactive runtime: the subscription, the reader that holds the
  point, and the trace it hands downstream, which reads the point's published chain.
* The interactive runtime's peer bundle of incarnation `w` is indexed by `w` itself.
* `epoch` is one controller connection, ordered as the process accepted them. Both runtimes
  receive the same command history for it, abstracted to `goal e`, the incarnation of the index
  that the history installs. A runtime may never see a connection the other one reconciles.

The collection is abstract. Its content at each time is fixed and identical across incarnations,
so an incarnation is characterized by its `upper`, its span boundaries (`w_cut`), and its
compaction frontiers. A downstream read through frontier `f` is correct exactly when the chain the
trace reads covers `f`, has a boundary at `f`, and is logically compacted no further than the
read's `as_of`.

Times and epochs are totally ordered. `zero` is the minimum time, `top` stands for the empty
frontier, and `ezero` is the epoch both runtimes start in, before any connection.

The registry keys a slot by the dataflow's `as_of`, and the explicit-state check gives every
incarnation the same `as_of`, the worst case. Design switches, all `true` in the committed design
and flipped one at a time by the mutation runs the README lists:

* `barrier`: a runtime creates dataflows only after the other runtime finished its drops.
* `barrier_first`: the barrier sits between the drops and the creates, not after the creates.
* `epoch_match`: the barrier admits a runtime once the other one finished the drops of the same
  or a later connection, not of any connection.
* `single_attach`: `Rendezvous::attach` refuses a point that was ever attached, and panics.
* `fresh_slot`: the registry does not hand out a slot whose writer closed.
-/

veil module RendezvousProtocol

-- The initializer and `x_create` assign more fields than the default depth elaborates.
set_option maxRecDepth 8192
-- Without it, `#gen_spec` warns that it cannot synthesize the model checker's `LocalRProp`
-- instance for the invariants within the default budget.
set_option synthInstance.maxHeartbeats 400000

type time
type epoch
type writer
type point
type imp

enum phase = { serving, dropping, waiting, creating }

instantiate tot : TotalOrder time
instantiate etot : TotalOrder epoch

immutable individual zero : time
immutable individual top : time
immutable individual ezero : epoch
immutable individual barrier : Bool
immutable individual barrier_first : Bool
immutable individual epoch_match : Bool
immutable individual single_attach : Bool
immutable individual fresh_slot : Bool
-- The `as_of` of each incarnation's dataflow, as the controller chose it.
immutable function wasof : writer → time

-- The controller: its current connection, the connections it opened, and the incarnation each
-- connection's history installs.
individual ctl : epoch
relation opened : epoch → Bool
function goal : epoch → writer
-- Whether a connection's history named the incarnation. A replaced incarnation never returns.
relation assigned : writer → Bool

-- Reconciliation, per runtime: the connection it reconciles or serves, its phase, and the latest
-- connection whose drops it finished, which is what the barrier publishes.
individual m_ep : epoch
individual m_ph : phase
individual m_arr : epoch
individual i_ep : epoch
individual i_ph : phase
individual i_arr : epoch

-- Maintenance runtime, per incarnation.
relation m_live : writer → Bool
relation m_dropped : writer → Bool
relation w_closed : writer → Bool
-- The connection whose create phase rendered the incarnation.
function w_ep : writer → epoch
function w_pt : writer → point
function w_upper : writer → time
relation w_cut : writer → time → Bool
function w_since : writer → time
-- The physical compaction frontier the spine applied, read from the holds at its last sync.
function w_phys : writer → time

-- Publication points.
relation p_used : point → Bool
-- The slot the registry map returns for the id.
relation p_inmap : point → Bool
function p_asof : point → time
-- Whether a writer ever attached.
relation p_attached : point → Bool
-- Whether the attached writer closed or dropped.
relation p_closed : point → Bool
-- The writer that attached last, the only one unless `single_attach` is off.
function p_cur : point → writer
function p_upper : point → time
relation p_cut : point → time → Bool
function p_since : point → time
-- The point's hold accumulations, one registration per holder.
relation acc : point → imp → Bool
relation acc_peer : point → writer → Bool
-- A holder of the slot's `Arc` the lifecycle does not account for, such as a peek in flight.
relation stray : point → Bool

-- Interactive runtime: the peer bundle of each incarnation.
relation i_peer : writer → Bool
relation i_peer_done : writer → Bool
function peer_ep : writer → epoch
function peer_pt : writer → point
function peer_l : writer → time

-- Interactive runtime: imports.
relation x_live : imp → Bool
relation x_dropped : imp → Bool
-- The import's capabilities are empty: its read is over.
relation x_done : imp → Bool
-- The incarnation whose peer bundle the import was built from.
function x_inc : imp → writer
function x_ep : imp → epoch
function x_pt : imp → point
-- The frontier the import's stream acknowledged.
function x_ack : imp → time
-- The latest `Upper` in the import's queue.
function x_q : imp → time
-- The read's `as_of`, which is also the logical hold of the trace handed downstream.
function x_l : imp → time
function x_ph : imp → time
-- The items delivered to the import cover the chain through here.
function x_deliv : imp → time
-- An item arrived whose lower was not where the delivered items ended.
relation x_hole : imp → Bool
-- Whether the subscription was seeded from a writer's view, and which one.
relation x_seeded : imp → Bool
function x_sub : imp → writer
function x_until : imp → time
-- The importer's own, possibly stale, observation of the published upper.
function x_snap : imp → time

-- A panic on either runtime halts the process.
individual panicked : Bool

#gen_state

theory ghost relation lt (x y : time) := tot.le x y ∧ x ≠ y
theory ghost relation elt (x y : epoch) := etot.le x y ∧ x ≠ y

-- A slot is alive while a holder of its `Arc` exists: the publisher's `PublishToken`, the peer
-- bundle, an importing dataflow's tokens, or a stray holder.
ghost relation referenced (q : point) :=
  (∃ w, m_live w ∧ w_pt w = q) ∨ (∃ w, i_peer w ∧ peer_pt w = q) ∨ (∃ x, x_live x ∧ x_pt x = q)
    ∨ stray q

-- `get_or_create` returns the slot in the map when it is alive, its `as_of` matches, and, under
-- the fresh-slot rule, its writer has not closed.
ghost relation reusable (q : point) (w : writer) :=
  p_inmap q ∧ referenced q ∧ p_asof q = wasof w ∧ (fresh_slot → ¬ p_closed q)

assumption ∀ t, tot.le zero t
assumption ∀ t, tot.le t top
assumption zero ≠ top
assumption ∀ e, etot.le ezero e
assumption ∀ w, wasof w ≠ top
assumption [design] barrier = true ∧ barrier_first = true ∧ epoch_match = true ∧ single_attach = true ∧ fresh_slot = true

after_init {
  ctl := ezero
  opened E := decide (E = ezero)
  assigned U := false
  m_ep := ezero
  m_ph := serving
  m_arr := ezero
  i_ep := ezero
  i_ph := serving
  i_arr := ezero
  m_live U := false
  m_dropped U := false
  w_closed U := false
  w_cut U T := false
  p_used P := false
  p_inmap P := false
  p_attached P := false
  p_closed P := false
  p_cut P T := false
  acc P X := false
  acc_peer P U := false
  stray P := false
  i_peer U := false
  i_peer_done U := false
  x_live X := false
  x_dropped X := false
  x_done X := false
  x_hole X := false
  x_seeded X := false
  panicked := false
}

-- A new controller connection. Its history keeps the current incarnation or names a new one.
action reconnect (e : epoch) (w : writer) {
  require ¬ panicked
  require elt ctl e
  require w = goal ctl ∨ ¬ assigned w
  opened e := true
  goal e := w
  assigned w := true
  ctl := e
}

-- A runtime sees the nonce of a newer connection, while it serves the current one or waits at its
-- barrier, and starts reconciling. Its drop and create phases issue no receive, so a nonce change
-- reaches it only in these two phases.
action m_begin (e : epoch) {
  require ¬ panicked
  require m_ph = serving ∨ m_ph = waiting
  require opened e ∧ elt m_ep e
  m_ep := e
  m_ph := dropping
}

action i_begin (e : epoch) {
  require ¬ panicked
  require i_ph = serving ∨ i_ph = waiting
  require opened e ∧ elt i_ep e
  i_ep := e
  i_ph := dropping
}

-- A runtime finished the drops its history asks for and publishes its arrival.
action m_arrive {
  require ¬ panicked
  require m_ph = dropping
  require ∀ U, m_live U → U = goal m_ep
  m_arr := m_ep
  m_ph := waiting
}

action i_arrive {
  require ¬ panicked
  require i_ph = dropping
  require ∀ U, i_peer U → U = goal i_ep
  require ∀ X, x_live X → x_inc X = goal i_ep
  i_arr := i_ep
  i_ph := waiting
}

action m_pass {
  require ¬ panicked
  require m_ph = waiting
  -- The barrier, given the other runtime's phase and latest arrival. A later arrival counts, as
  -- the other runtime may never see this connection.
  require barrier → (epoch_match → etot.le m_ep i_arr) ∧ (¬ epoch_match → i_ph ≠ dropping)
  m_ph := creating
}

action i_pass {
  require ¬ panicked
  require i_ph = waiting
  require barrier → (epoch_match → etot.le i_ep m_arr) ∧ (¬ epoch_match → m_ph ≠ dropping)
  i_ph := creating
}

action m_finish {
  require ¬ panicked
  require m_ph = creating
  m_ph := serving
}

action i_finish {
  require ¬ panicked
  require i_ph = creating
  i_ph := serving
}

-- `get_or_create` found no reusable slot: a fresh, unattached point replaces the map entry.
procedure new_slot (p : point) (w : writer) {
  p_used p := true
  p_inmap Q := decide (Q = p)
  p_asof p := wasof w
  p_attached p := false
  p_closed p := false
  p_upper p := zero
  p_cut p T := decide (T = zero)
  p_since p := zero
}

-- `Rendezvous::attach` of a freshly rendered trace, which is empty and compacted to its `as_of`.
procedure attach (w : writer) (p : point) {
  if single_attach ∧ p_attached p then
    -- The point had a writer: `attach` refuses it.
    panicked := true
  else
    m_live w := true
    w_ep w := m_ep
    w_pt w := p
    w_closed w := false
    w_upper w := zero
    w_cut w T := decide (T = zero)
    w_since w := wasof w
    w_phys w := zero
    -- Seed live subscribers with the new view.
    x_q X := if x_live X ∧ x_pt X = p then zero else x_q X
    x_sub X := if x_live X ∧ x_pt X = p then w else x_sub X
    x_seeded X := decide (x_seeded X ∨ (x_live X ∧ x_pt X = p))
    p_attached p := true
    p_closed p := false
    p_cur p := w
    p_upper p := zero
    p_cut p T := decide (T = zero)
    p_since p := wasof w
}

-- Maintenance renders the incarnation its history installs and publishes it into the slot
-- `get_or_create` returns, here an existing one.
action m_create_reuse (w : writer) (p : point) {
  require ¬ panicked
  -- Creates follow the barrier, or precede it with `barrier_first` off.
  require m_ph = creating ∨ (¬ barrier_first ∧ m_ph = waiting)
  require w = goal m_ep
  require ¬ m_live w ∧ ¬ m_dropped w
  require reusable p w
  attach w p
}

action m_create_fresh (w : writer) (p : point) {
  require ¬ panicked
  require m_ph = creating ∨ (¬ barrier_first ∧ m_ph = waiting)
  require w = goal m_ep
  require ¬ m_live w ∧ ¬ m_dropped w
  require ¬ p_used p
  require ∀ Q, ¬ reusable Q w
  new_slot p w
  attach w p
}

-- The writer inserts a span `[w_upper w, t)` and `publish_chain` mirrors it.
action m_seal (w : writer) (t : time) {
  require ¬ panicked
  require m_live w
  require lt (w_upper w) t
  require t ≠ top
  let lo := w_upper w
  w_upper w := t
  w_cut w t := true
  if ¬ w_closed w then
    p_upper (w_pt w) := t
    p_cut (w_pt w) T := w_cut w T
    x_hole X := decide (x_hole X ∨ (x_live X ∧ x_pt X = w_pt w ∧ x_deliv X ≠ lo))
    x_deliv X := if x_live X ∧ x_pt X = w_pt w ∧ x_deliv X = lo then t else x_deliv X
    x_q X := if x_live X ∧ x_pt X = w_pt w then t else x_q X
}

-- A merge removes the boundary at `c`, which the applied physical frontier has passed.
action m_merge (w : writer) (c : time) {
  require ¬ panicked
  require m_live w
  require w_cut w c
  require lt c (w_phys w)
  w_cut w c := false
  if ¬ w_closed w then
    p_cut (w_pt w) T := w_cut w T
}

-- `sync_holds`: read the meets under the lock, publish the logical one, apply both to the spine.
-- Each frontier either moves to a value the holds allow or stays, as the joins with the spine's
-- current frontiers make it.
action m_sync (w : writer) (ph : time) (s : time) {
  require ¬ panicked
  require m_live w
  require tot.le ph (w_upper w)
  require ph = w_phys w ∨ (tot.le (w_phys w) ph ∧ ∀ X, acc (w_pt w) X → tot.le ph (x_ph X))
  require s ≠ top
  require s = w_since w ∨ (tot.le (w_since w) s ∧ (∀ X, acc (w_pt w) X → tot.le s (x_l X))
    ∧ (∀ V, acc_peer (w_pt w) V → tot.le s (peer_l V)))
  w_phys w := ph
  w_since w := s
  if ¬ w_closed w then
    p_since (w_pt w) := s
}

-- Reconciliation drops an incarnation its history replaces: the writer drops, sealing its spine to
-- the empty frontier, and the trace box closes the point first so the seal stays out of it.
action m_drop (w : writer) {
  require ¬ panicked
  require m_ph = dropping
  require m_live w
  require w ≠ goal m_ep
  w_closed w := true
  p_closed (w_pt w) := true
  let lo := w_upper w
  w_upper w := top
  w_cut w top := true
  if ¬ w_closed w then
    p_upper (w_pt w) := top
    p_cut (w_pt w) T := w_cut w T
    x_hole X := decide (x_hole X ∨ (x_live X ∧ x_pt X = w_pt w ∧ x_deliv X ≠ lo))
    x_deliv X := if x_live X ∧ x_pt X = w_pt w ∧ x_deliv X = lo then top else x_deliv X
    x_q X := if x_live X ∧ x_pt X = w_pt w then top else x_q X
  m_live w := false
  m_dropped w := true
}

-- `peer_bundle`: the interactive runtime records the incarnation its history installs and holds
-- its slot logically at the join of `as_of` and the published `since`.
procedure bind_peer (w : writer) (p : point) {
  i_peer w := true
  peer_ep w := i_ep
  peer_pt w := p
  peer_l w := if tot.le (wasof w) (p_since p) then p_since p else wasof w
  acc_peer p w := true
}

action i_peer_reuse (w : writer) (p : point) {
  require ¬ panicked
  require i_ph = creating ∨ (¬ barrier_first ∧ i_ph = waiting)
  require w = goal i_ep
  require ¬ i_peer w ∧ ¬ i_peer_done w
  require reusable p w
  bind_peer w p
}

action i_peer_fresh (w : writer) (p : point) {
  require ¬ panicked
  require i_ph = creating ∨ (¬ barrier_first ∧ i_ph = waiting)
  require w = goal i_ep
  require ¬ i_peer w ∧ ¬ i_peer_done w
  require ¬ p_used p
  require ∀ Q, ¬ reusable Q w
  new_slot p w
  bind_peer w p
}

action i_peer_drop (w : writer) {
  require ¬ panicked
  require i_ph = dropping
  require i_peer w
  require w ≠ goal i_ep
  i_peer w := false
  i_peer_done w := true
  acc_peer P w := false
}

-- Something clones a slot's `Arc` outside the lifecycle, or drops that clone.
action stray_take (p : point) {
  require ¬ panicked
  require p_used p ∧ referenced p
  stray p := true
}

action stray_drop (p : point) {
  require ¬ panicked
  stray p := false
}

-- The importer reads the published upper outside the lock. Only the stale-seed mutation uses it.
action x_observe (x : imp) (w : writer) {
  require ¬ panicked
  require i_peer w
  x_snap x := p_upper (peer_pt w)
}

-- An interactive dataflow imports the index through peer bundle `w`: `fresh_reader`, then
-- `subscribe`, seeded with the published chain and upper.
action x_create (x : imp) (w : writer) (u : time) {
  require ¬ panicked
  require i_ph = creating ∨ i_ph = serving ∨ (¬ barrier_first ∧ i_ph = waiting)
  require i_peer w
  require ¬ x_live x ∧ ¬ x_dropped x
  require lt (peer_l w) u
  -- Seed and registration happen under one lock acquisition.
  require x_snap x = p_upper (peer_pt w)
  -- `fresh_reader` expects `reader_at` to accept the peer's hold.
  if ¬ tot.le (p_since (peer_pt w)) (peer_l w) then
    panicked := true
  x_live x := true
  x_inc x := w
  x_ep x := i_ep
  x_pt x := peer_pt w
  x_l x := peer_l w
  x_ph x := p_upper (peer_pt w)
  acc (peer_pt w) x := true
  x_ack x := zero
  x_q x := x_snap x
  x_deliv x := x_snap x
  x_done x := false
  x_hole x := false
  x_until x := u
  x_seeded x := p_attached (peer_pt w)
  x_sub x := p_cur (peer_pt w)
}

-- The import operator drains its queue, acknowledging the latest `Upper`.
action x_drain (x : imp) {
  require ¬ panicked
  require x_live x ∧ ¬ x_done x
  x_ack x := x_q x
  x_ph x := x_q x
  if x_q x = top ∨ tot.le (x_until x) (x_q x) then
    x_done x := true
    acc (x_pt x) x := false
}

-- `drop_dataflow` of the importing dataflow, in any phase but the barrier wait.
action x_drop (x : imp) {
  require ¬ panicked
  require i_ph ≠ waiting
  require x_live x
  x_live x := false
  x_dropped x := true
  acc P x := false
}

safety [no_panic] ¬ panicked

-- A downstream read through the acknowledged frontier misses updates when the chain does not cover
-- it, panics in `chain_through` when a span straddles it, double counts or misses items when
-- delivery had a hole, and reads collapsed times when the chain is compacted past the read's
-- `as_of`.
safety [ack_covered] x_live X ∧ ¬ x_done X → tot.le (x_ack X) (p_upper (x_pt X))
safety [ack_on_boundary] x_live X ∧ ¬ x_done X ∧ x_ack X ≠ zero → p_cut (x_pt X) (x_ack X)
safety [no_hole] x_live X ∧ ¬ x_done X → ¬ x_hole X
safety [since_below_read] x_live X ∧ ¬ x_done X → tot.le (p_since (x_pt X)) (x_l X)

-- A live import reads the chain of the writer whose view seeded it.
safety [reads_own_chain] x_live X ∧ x_seeded X → p_cur (x_pt X) = x_sub X

-- A peer and an import are bound to their own incarnation's point, or to one no writer attached
-- yet, never to another incarnation's.
safety [peer_own_writer] i_peer U ∧ p_attached (peer_pt U) → p_cur (peer_pt U) = U
safety [imp_own_writer] x_live X ∧ p_attached (x_pt X) → p_cur (x_pt X) = x_inc X

-- Old readers never hold back a later incarnation's trace.
safety [holds_own_incarnation] acc P X ∧ m_live U ∧ w_pt U = P → x_inc X = U
safety [peer_holds_own_incarnation] acc_peer P V ∧ m_live U ∧ w_pt U = P → V = U

-- No reader sees the empty frontier, since a dropped writer says nothing about later times.
safety [no_empty_upper] p_used P → p_upper P ≠ top
safety [no_empty_queue] x_live X → x_q X ≠ top

-- Holds balance: a holder is registered exactly while it is live.
safety [holds_balance] acc P X ↔ (x_live X ∧ ¬ x_done X ∧ x_pt X = P)
safety [peer_holds_balance] acc_peer P U ↔ (i_peer U ∧ peer_pt U = P)

-- The writer compacts no further than any live hold on its point.
safety [since_below_holds] acc P X ∧ m_live U ∧ w_pt U = P → tot.le (w_since U) (x_l X)

-- A subscriber has seen every upper its point published.
safety [queue_current] x_live X → x_q X = p_upper (x_pt X)

-- Supporting invariants. They make the clauses above inductive for the committed design.
-- Reconciliation bookkeeping.
invariant [opened_le_ctl] opened E → etot.le E ctl
invariant [opened_eps] opened ctl ∧ opened m_ep ∧ opened i_ep ∧ opened m_arr ∧ opened i_arr
invariant [arr_le_ep] etot.le m_arr m_ep ∧ etot.le i_arr i_ep
invariant [m_arr_ep] m_ph ≠ dropping → m_arr = m_ep
invariant [i_arr_ep] i_ph ≠ dropping → i_arr = i_ep
invariant [m_reconciling] m_ph ≠ serving → elt ezero m_ep
invariant [i_reconciling] i_ph ≠ serving → elt ezero i_ep
invariant [goal_assigned] opened E ∧ elt ezero E → assigned (goal E)
-- A replaced incarnation never returns, so the connections that install one are contiguous.
invariant [goal_convex]
  opened E1 ∧ opened E2 ∧ opened E3 ∧ elt ezero E1 ∧ etot.le E1 E2 ∧ etot.le E2 E3 ∧
    goal E1 = goal E3 → goal E2 = goal E1
-- What each runtime holds is the incarnation of the latest connection whose drops it finished.
invariant [m_live_goal] m_live U → U = goal m_arr
invariant [i_peer_goal] i_peer U → U = goal i_arr
invariant [x_live_goal] x_live X → x_inc X = goal i_arr
-- The barrier: a runtime past it is no later than the other runtime's latest arrival, and so is
-- every connection whose create phase built something still live.
invariant [m_passed] (m_ph = creating ∨ m_ph = serving) → etot.le m_ep i_arr
invariant [i_passed] (i_ph = creating ∨ i_ph = serving) → etot.le i_ep m_arr
invariant [w_ep_bound]
  m_live U → goal (w_ep U) = U ∧ opened (w_ep U) ∧ elt ezero (w_ep U) ∧ etot.le (w_ep U) m_arr ∧
    etot.le (w_ep U) i_arr
invariant [peer_ep_bound]
  i_peer U → goal (peer_ep U) = U ∧ opened (peer_ep U) ∧ elt ezero (peer_ep U) ∧
    etot.le (peer_ep U) i_arr ∧ etot.le (peer_ep U) m_arr
invariant [x_ep_bound]
  x_live X → goal (x_ep X) = x_inc X ∧ opened (x_ep X) ∧ elt ezero (x_ep X) ∧
    etot.le (x_ep X) i_arr ∧ etot.le (x_ep X) m_arr
-- Consequences: a runtime past the barrier sees only its connection's incarnation on the other
-- runtime.
invariant [i_passed_m_live] (i_ph = creating ∨ i_ph = serving) ∧ m_live U → U = goal i_ep
invariant [m_passed_i_peer] (m_ph = creating ∨ m_ph = serving) ∧ i_peer U → U = goal m_ep
invariant [m_passed_x_live]
  (m_ph = creating ∨ m_ph = serving) ∧ x_live X → x_inc X = goal m_ep
invariant [live_assigned] (m_live U ∨ m_dropped U ∨ i_peer U ∨ i_peer_done U) → assigned U
-- Lifecycle of one incarnation.
invariant [live_open] m_live U → ¬ w_closed U ∧ ¬ m_dropped U
invariant [writer_used] (m_live U ∨ m_dropped U) → p_used (w_pt U) ∧ p_attached (w_pt U)
invariant [peer_used] i_peer U → p_used (peer_pt U)
invariant [imp_used] x_live X → p_used (x_pt X)
invariant [imp_peer] x_live X → (i_peer (x_inc X) ∨ i_peer_done (x_inc X)) ∧ x_pt X = peer_pt (x_inc X)
invariant [unused_clean] ¬ p_used P → ¬ p_attached P ∧ ¬ p_closed P ∧ ¬ acc P X ∧ ¬ acc_peer P U ∧ ¬ stray P
invariant [attached_cur] p_attached P → (m_live (p_cur P) ∨ m_dropped (p_cur P)) ∧ w_pt (p_cur P) = P
invariant [closed_dropped] p_attached P → (p_closed P ↔ m_dropped (p_cur P))
invariant [closed_attached] p_closed P → p_attached P
invariant [one_writer] (m_live U1 ∨ m_dropped U1) ∧ (m_live U2 ∨ m_dropped U2) ∧ w_pt U1 = w_pt U2 → U1 = U2
invariant [peer_key] i_peer U → tot.le (wasof U) (peer_l U)
invariant [imp_key] x_live X → tot.le (wasof (x_inc X)) (x_l X)
-- The point mirrors its live writer.
invariant [mirror_upper] m_live U → p_upper (w_pt U) = w_upper U
invariant [mirror_cut] m_live U → (p_cut (w_pt U) T ↔ w_cut U T)
invariant [mirror_since] m_live U → p_since (w_pt U) = w_since U
invariant [upper_cut] p_used P → p_cut P (p_upper P)
invariant [w_upper_cut] m_live U → w_cut U (w_upper U)
invariant [phys_le_upper] m_live U → tot.le (w_phys U) (w_upper U)
invariant [phys_le_holds] acc P X ∧ m_live U ∧ w_pt U = P → tot.le (w_phys U) (x_ph X)
invariant [ack_le_q] x_live X → tot.le (x_ack X) (x_q X)
invariant [ph_is_ack] x_live X ∧ x_ack X ≠ zero → x_ph X = x_ack X
invariant [ph_le_upper] x_live X → tot.le (x_ph X) (p_upper (x_pt X))
invariant [deliv_current] x_live X → x_deliv X = p_upper (x_pt X)
invariant [peer_since] i_peer U → tot.le (p_since (peer_pt U)) (peer_l U)
invariant [seeded_attached] x_live X → (x_seeded X ↔ p_attached (x_pt X))
invariant [sub_is_cur] x_live X ∧ x_seeded X → x_sub X = p_cur (x_pt X)
invariant [unattached_empty] p_used P ∧ ¬ p_attached P → p_upper P = zero ∧ p_since P = zero

#time #gen_spec

#check_invariants

-- Explicit-state cross-check. Every incarnation has the same `as_of`, the case in which the
-- registry finds an earlier incarnation's slot under the new incarnation's key. Three epochs are
-- the start and two connections, enough for a runtime to skip one.
#model_check { time := Fin 3, epoch := Fin 3, writer := Fin 2, point := Fin 2, imp := Fin 1 } { zero := 0, top := 2, ezero := 0, barrier := true, barrier_first := true, epoch_match := true, single_attach := true, fresh_slot := true, wasof := fun _ => 0 }

end RendezvousProtocol
