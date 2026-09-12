// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Cursor-based helpers for reading co-arranged batches and computing per-key output deltas.

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::marker::PhantomData;

use differential_dataflow::consolidation::consolidate;
use differential_dataflow::difference::{Abelian, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::cursor::cursor_list;
use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::{BatchReader, Builder, Cursor, Description, Navigable};
use timely::PartialOrder;
use timely::container::PushInto;
use timely::progress::{Antichain, Timestamp};

use super::CoReduceTactic;

/// Owns the cursor's current key.
pub(super) fn own_current_key<C, K>(cursor: &C, storage: &C::Storage) -> K
where
    C: Cursor,
    C::KeyContainer: BatchContainer<Owned = K>,
{
    <C::KeyContainer as BatchContainer>::into_owned(cursor.get_key(storage).expect("key exists"))
}

/// Advances the cursor forward to `target`, returning whether it is present.
///
/// Keys are visited in ascending order across a round, so advancing each source cursor
/// forward to successive targets is monotone. On a match the cursor is left positioned
/// at the key with values at their start. On a miss it stops at the first key greater
/// than `target`.
pub(super) fn seek_owned_key<C, K>(cursor: &mut C, storage: &C::Storage, target: &K) -> bool
where
    C: Cursor,
    C::KeyContainer: BatchContainer<Owned = K>,
    K: Ord,
{
    while cursor.key_valid(storage) {
        match own_current_key::<C, K>(cursor, storage).cmp(target) {
            Ordering::Less => cursor.step_key(storage),
            Ordering::Equal => return true,
            Ordering::Greater => return false,
        }
    }
    false
}

/// Records every update time of every key in the cursor into `pending`.
///
/// Staged times survive rounds in which the combined frontier does not advance, so a
/// batch that arrives while another input lags is not lost. Diffs and values are not
/// staged: the per-key history is re-read from the input traces at process time.
pub(super) fn stage_times<C, K>(
    cursor: &mut C,
    storage: &C::Storage,
    pending: &mut BTreeMap<K, Vec<C::Time>>,
) where
    C: Cursor,
    C::KeyContainer: BatchContainer<Owned = K>,
    K: Ord + Clone,
    C::Time: Clone,
{
    while cursor.key_valid(storage) {
        let key = own_current_key::<C, K>(cursor, storage);
        let times = pending.entry(key).or_default();
        while cursor.val_valid(storage) {
            cursor.map_times(storage, |t, _d| times.push(C::owned_time(t)));
            cursor.step_val(storage);
        }
        cursor.step_key(storage);
    }
}

/// Reads all `(value, time, diff)` updates of the cursor's current key, owning values.
///
/// The cursor must be positioned at the key. The value cursor is advanced to its end.
/// Unlike a threshold reduce, values are retained: the closure sees per-input value
/// multisets, so it can key output on input values.
pub(super) fn read_key_values<C, V>(
    cursor: &mut C,
    storage: &C::Storage,
    out: &mut Vec<(V, C::Time, C::Diff)>,
) where
    C: Cursor,
    C::ValContainer: BatchContainer<Owned = V>,
    V: Clone,
    C::Time: Clone,
    C::Diff: Clone,
{
    while cursor.val_valid(storage) {
        let val: V = <C::ValContainer as BatchContainer>::into_owned(cursor.val(storage));
        cursor.map_times(storage, |t, d| {
            out.push((val.clone(), C::owned_time(t), C::owned_diff(d)));
        });
        cursor.step_val(storage);
    }
}

/// Per-key state assembled during a round.
pub(super) struct KeyWork<T, V, D, V2, R2> {
    /// Per input: full `(value, time, diff)` history read from that input's trace.
    pub(super) inputs: Vec<Vec<(V, T, D)>>,
    /// Prior output `(value, time, diff)` read from the output trace.
    pub(super) prior: Vec<(V2, T, R2)>,
    /// In-region interesting seed times (batch and pending times below the round upper).
    pub(super) seeds: Vec<T>,
}

/// Evaluates one key over the round's interesting times.
///
/// At each interesting time `t`, consolidates each input's `(value, diff)` accumulated
/// up to `t`, calls `logic` to get the desired output multiset at `t`, and emits the
/// per-value delta against the current output (prior plus already-produced). Synthesizes
/// join-closure times so a non-linear closure is evaluated wherever its output can
/// change. Times at or beyond `upper_limit` are deferred into `new_interesting`.
#[allow(clippy::too_many_arguments)]
pub(super) fn compute_key<T, K, V, D, V2, R2, L>(
    key: &K,
    kw: &KeyWork<T, V, D, V2, R2>,
    upper_limit: &Antichain<T>,
    logic: &mut L,
    held_times: &[T],
    updates: &mut [Vec<(V2, T, R2)>],
    new_interesting: &mut Vec<T>,
) where
    T: Lattice + Ord + Clone,
    V: Ord + Clone,
    D: Semigroup + Clone,
    V2: Ord + Clone,
    R2: Abelian + Clone,
    L: FnMut(&K, &[&[(V, D)]], &mut Vec<(V2, R2)>),
{
    // Partners for synthetic-time joins: every input time and prior-output time. Both
    // lie in the join-closure of input times, so including prior is redundant but harmless.
    let mut partners: Vec<T> = Vec::new();
    for hist in &kw.inputs {
        partners.extend(hist.iter().map(|(_v, t, _d)| t.clone()));
    }
    partners.extend(kw.prior.iter().map(|(_v, t, _d)| t.clone()));
    partners.extend(kw.seeds.iter().cloned());
    partners.sort();
    partners.dedup();

    let mut queued: BTreeSet<T> = BTreeSet::new();
    let mut worklist: BinaryHeap<Reverse<T>> = BinaryHeap::new();
    for t in &kw.seeds {
        if upper_limit.less_equal(t) {
            new_interesting.push(t.clone());
        } else if queued.insert(t.clone()) {
            worklist.push(Reverse(t.clone()));
        }
    }

    // Output produced this round for this key, diffed against.
    let mut produced: Vec<(V2, T, R2)> = Vec::new();
    // Reusable buffers.
    let mut asof: Vec<Vec<(V, D)>> = (0..kw.inputs.len()).map(|_| Vec::new()).collect();
    let mut desired: Vec<(V2, R2)> = Vec::new();

    while let Some(Reverse(t)) = worklist.pop() {
        // Per-input consolidated (value, diff) as of `t`.
        for (i, hist) in kw.inputs.iter().enumerate() {
            asof[i].clear();
            for (v, ti, d) in hist {
                if PartialOrder::less_equal(ti, &t) {
                    asof[i].push((v.clone(), d.clone()));
                }
            }
            consolidate(&mut asof[i]);
        }
        let slices: Vec<&[(V, D)]> = asof.iter().map(|v| v.as_slice()).collect();

        desired.clear();
        logic(key, &slices, &mut desired);
        consolidate(&mut desired);

        // Current output as of `t`, per value: prior + produced with time <= t.
        let mut current: Vec<(V2, R2)> = Vec::new();
        for (v, ti, d) in kw.prior.iter().chain(produced.iter()) {
            if PartialOrder::less_equal(ti, &t) {
                current.push((v.clone(), d.clone()));
            }
        }
        consolidate(&mut current);

        // Emit per-value delta = desired - current. Both are consolidated and sorted by
        // value, so merge-walk them.
        emit_deltas(&desired, &current, &t, held_times, updates, &mut produced);

        // Synthesize joins of `t` with every partner not already <= t.
        for tp in &partners {
            if PartialOrder::less_equal(tp, &t) {
                continue;
            }
            let synth = t.join(tp);
            if synth == t {
                continue;
            }
            if upper_limit.less_equal(&synth) {
                new_interesting.push(synth);
            } else if queued.insert(synth.clone()) {
                worklist.push(Reverse(synth));
            }
        }
    }
}

/// Merge-walks `desired` and `current` (both consolidated, sorted by value) and pushes
/// `desired - current` per value into `produced` and the covering held time's updates.
pub(super) fn emit_deltas<T, V2, R2>(
    desired: &[(V2, R2)],
    current: &[(V2, R2)],
    t: &T,
    held_times: &[T],
    updates: &mut [Vec<(V2, T, R2)>],
    produced: &mut Vec<(V2, T, R2)>,
) where
    T: Lattice + Ord + Clone,
    V2: Ord + Clone,
    R2: Abelian + Clone,
{
    let mut di = 0;
    let mut ci = 0;
    let mut push = |v: &V2, delta: R2, produced: &mut Vec<(V2, T, R2)>| {
        if delta.is_zero() {
            return;
        }
        produced.push((v.clone(), t.clone(), delta.clone()));
        // Latest held time covering `t`. One must exist: `t` is reachable from a batch
        // or a pending time, both of which retain a capability at or below their time.
        let idx = held_times
            .iter()
            .enumerate()
            .rev()
            .find(|(_, ct)| PartialOrder::less_equal(*ct, t))
            .map(|(i, _)| i)
            .expect("a held time covers every produced time");
        updates[idx].push((v.clone(), t.clone(), delta));
    };
    while di < desired.len() && ci < current.len() {
        match desired[di].0.cmp(&current[ci].0) {
            Ordering::Less => {
                push(&desired[di].0, desired[di].1.clone(), produced);
                di += 1;
            }
            Ordering::Greater => {
                let mut neg = current[ci].1.clone();
                neg.negate();
                push(&current[ci].0, neg, produced);
                ci += 1;
            }
            Ordering::Equal => {
                let mut delta = desired[di].1.clone();
                let mut neg = current[ci].1.clone();
                neg.negate();
                delta.plus_equals(&neg);
                push(&desired[di].0, delta, produced);
                di += 1;
                ci += 1;
            }
        }
    }
    while di < desired.len() {
        push(&desired[di].0, desired[di].1.clone(), produced);
        di += 1;
    }
    while ci < current.len() {
        let mut neg = current[ci].1.clone();
        neg.negate();
        push(&current[ci].0, neg, produced);
        ci += 1;
    }
}

/// Builds one output batch per held time, folding later held times into each batch's
/// upper so the descriptions tile `[lower, upper)` in ascending order. Zero-width
/// batches are skipped. Shared by the cursor and reference tactics so both satisfy the
/// driver's tiling contract by construction.
pub(super) fn build_tiled_batches<T, Bu>(
    held_times: &[T],
    builders: Vec<Bu>,
    lower: &Antichain<T>,
    upper: &Antichain<T>,
) -> Vec<(T, Bu::Output)>
where
    T: Timestamp + Lattice,
    Bu: Builder<Time = T>,
{
    let mut produced = Vec::new();
    let mut output_lower = lower.clone();
    for (index, builder) in builders.into_iter().enumerate() {
        let mut output_upper = upper.clone();
        for time in &held_times[index + 1..] {
            output_upper.insert(time.clone());
        }
        if output_upper != output_lower {
            let description = Description::new(
                output_lower.clone(),
                output_upper.clone(),
                Antichain::from_elem(T::minimum()),
            );
            produced.push((held_times[index].clone(), builder.done(description)));
            output_lower = output_upper;
        }
    }
    produced
}

/// State of an in-flight round.
///
/// `builders` and `updates` are allocated once in `begin`, sized from `held`, and
/// mutated by successive `step` calls. They must persist across calls: a fuel-limited
/// round spans many activations, and rebuilding them would discard prior activations'
/// rows.
struct RoundState<B0, B1, BOut, K, V2, R2, Bu>
where
    B0: BatchReader,
{
    history0: Vec<B0>,
    history1: Vec<B1>,
    output: Vec<BOut>,
    lower: Antichain<B0::Time>,
    upper: Antichain<B0::Time>,
    held_times: Vec<B0::Time>,
    builders: Vec<Bu>,
    updates: Vec<Vec<(V2, B0::Time, R2)>>,
    /// Dirty keys not yet processed this round, ascending. Values are in-region seeds.
    remaining: BTreeMap<K, Vec<B0::Time>>,
    /// Beyond-upper synthetic times discovered this round, folded into pending at finish.
    deferred: BTreeMap<K, Vec<B0::Time>>,
}

/// The conventional cursor-based [`CoReduceTactic`]: incremental per-key evaluation
/// over the interesting-time join-closure, reading full histories through batch
/// cursors.
pub struct CursorTactic<B0, B1, BOut, K, V, V2, R2, Bu, L>
where
    B0: BatchReader,
{
    logic: L,
    /// Staged interesting times by key, surviving across rounds. Holds both times not
    /// yet retired (one input's frontier lags) and beyond-upper synthetic times.
    pending: BTreeMap<K, Vec<B0::Time>>,
    round: Option<RoundState<B0, B1, BOut, K, V2, R2, Bu>>,
    _marker: PhantomData<(V, V2, R2)>,
}

impl<B0, B1, BOut, K, V, V2, R2, Bu, L> CursorTactic<B0, B1, BOut, K, V, V2, R2, Bu, L>
where
    B0: BatchReader,
    K: Ord,
{
    pub fn new(logic: L) -> Self {
        Self {
            logic,
            pending: BTreeMap::new(),
            round: None,
            _marker: PhantomData,
        }
    }
}

impl<B0, B1, BOut, K, V, V2, R2, Bu, L> CoReduceTactic<B0, B1, BOut>
    for CursorTactic<B0, B1, BOut, K, V, V2, R2, Bu, L>
where
    B0: BatchReader + Navigable + Clone,
    B1: BatchReader<Time = B0::Time> + Navigable + Clone,
    BOut: BatchReader<Time = B0::Time> + Navigable + Clone,
    B0::Time: Timestamp + Lattice + Ord,
    B0::Cursor: Cursor<Time = B0::Time>,
    <B0::Cursor as Cursor>::Diff: Semigroup + Clone + 'static,
    <B0::Cursor as Cursor>::KeyContainer: BatchContainer<Owned = K>,
    <B0::Cursor as Cursor>::ValContainer: BatchContainer<Owned = V>,
    B1::Cursor: Cursor<Time = B0::Time, Diff = <B0::Cursor as Cursor>::Diff>,
    <B1::Cursor as Cursor>::KeyContainer: BatchContainer<Owned = K>,
    <B1::Cursor as Cursor>::ValContainer: BatchContainer<Owned = V>,
    BOut::Cursor: Cursor<Time = B0::Time, Diff = R2, ValOwn = V2>,
    <BOut::Cursor as Cursor>::KeyContainer: BatchContainer<Owned = K>,
    <BOut::Cursor as Cursor>::ValContainer: BatchContainer<Owned = V2>,
    K: Ord + Clone + 'static,
    V: Ord + Clone + 'static,
    V2: Ord + Clone + 'static,
    R2: Abelian + Clone + 'static,
    Bu: Builder<Time = B0::Time, Output = BOut> + 'static,
    Bu::Input: Default + PushInto<((K, V2), B0::Time, R2)>,
    L: FnMut(&K, &[&[(V, <B0::Cursor as Cursor>::Diff)]], &mut Vec<(V2, R2)>) + 'static,
{
    fn begin(
        &mut self,
        history0: Vec<B0>,
        new0: Vec<B0>,
        history1: Vec<B1>,
        new1: Vec<B1>,
        output: Vec<BOut>,
        lower: &Antichain<B0::Time>,
        upper: &Antichain<B0::Time>,
        held: &Antichain<B0::Time>,
    ) {
        assert!(self.round.is_none(), "begin called with a round in flight");
        for batch in &new0 {
            let mut cursor = batch.cursor();
            stage_times(&mut cursor, batch, &mut self.pending);
        }
        for batch in &new1 {
            let mut cursor = batch.cursor();
            stage_times(&mut cursor, batch, &mut self.pending);
        }
        // Partition the staged dirty set into this round's in-region seeds (processed
        // under fuel) and beyond-upper times kept staged for a later round.
        let mut remaining: BTreeMap<K, Vec<B0::Time>> = BTreeMap::new();
        let mut kept: BTreeMap<K, Vec<B0::Time>> = BTreeMap::new();
        for (key, times) in std::mem::take(&mut self.pending) {
            let mut seeds = Vec::new();
            let mut carried = Vec::new();
            for t in times {
                if upper.less_equal(&t) {
                    carried.push(t);
                } else {
                    seeds.push(t);
                }
            }
            if !seeds.is_empty() {
                remaining.insert(key.clone(), seeds);
            }
            if !carried.is_empty() {
                kept.insert(key, carried);
            }
        }
        self.pending = kept;
        let held_times: Vec<B0::Time> = held.elements().to_vec();
        let builders: Vec<Bu> = (0..held_times.len()).map(|_| Bu::new()).collect();
        let updates: Vec<Vec<(V2, B0::Time, R2)>> =
            (0..held_times.len()).map(|_| Vec::new()).collect();
        self.round = Some(RoundState {
            history0,
            history1,
            output,
            lower: lower.clone(),
            upper: upper.clone(),
            held_times,
            builders,
            updates,
            remaining,
            deferred: BTreeMap::new(),
        });
    }

    fn step(&mut self, fuel: usize) -> bool {
        let Some(r) = self.round.as_mut() else {
            return true;
        };
        if r.remaining.is_empty() {
            return true;
        }
        // Build cursors over the frozen batch vectors. Batch handles are Rc-backed, so
        // the clones are cheap. Rebuilding per step re-seeks from scratch, which makes
        // total seek work across a fuel-split round O(N^2 / fuel) in the round's
        // dirty-key count N. Dormant while production callers drain a round's whole
        // dirty set in one activation. If a smaller budget ever makes this bite,
        // persist the cursors in RoundState instead.
        let (mut input0_src, input0_stor) = cursor_list(r.history0.clone());
        let (mut input1_src, input1_stor) = cursor_list(r.history1.clone());
        let (mut out_cur, out_stor) = cursor_list(r.output.clone());

        let mut processed_keys = 0usize;
        while processed_keys < fuel {
            let Some((key, seeds)) = r.remaining.pop_first() else {
                break;
            };
            processed_keys += 1;

            let mut kw = KeyWork {
                inputs: vec![Vec::new(); 2],
                prior: Vec::new(),
                seeds,
            };
            if seek_owned_key(&mut input0_src, &input0_stor, &key) {
                read_key_values(&mut input0_src, &input0_stor, &mut kw.inputs[0]);
            }
            if seek_owned_key(&mut input1_src, &input1_stor, &key) {
                read_key_values(&mut input1_src, &input1_stor, &mut kw.inputs[1]);
            }
            if seek_owned_key(&mut out_cur, &out_stor, &key) {
                read_key_values(&mut out_cur, &out_stor, &mut kw.prior);
            }

            let mut new_interesting = Vec::new();
            compute_key(
                &key,
                &kw,
                &r.upper,
                &mut self.logic,
                &r.held_times,
                &mut r.updates,
                &mut new_interesting,
            );

            // Push this key's updates into each held time's builder, preserving
            // ascending key order across the builder. The builder assumes value-sorted
            // input per key, but the updates arrive time-ordered. A stable sort by
            // value reorders them into (value, time) order.
            for i in 0..r.held_times.len() {
                if r.updates[i].is_empty() {
                    continue;
                }
                r.updates[i].sort_by(|a, b| a.0.cmp(&b.0));
                let mut buffer = <Bu::Input>::default();
                for (v2, time, r2) in r.updates[i].drain(..) {
                    buffer.push_into(((key.clone(), v2), time, r2));
                }
                r.builders[i].push(&mut buffer);
            }

            if !new_interesting.is_empty() {
                new_interesting.sort();
                new_interesting.dedup();
                r.deferred.entry(key).or_default().extend(new_interesting);
            }
        }
        r.remaining.is_empty()
    }

    fn finish(&mut self) -> (Vec<(B0::Time, BOut)>, Antichain<B0::Time>) {
        let r = self.round.take().expect("finish called without a round");
        assert!(r.remaining.is_empty(), "finish called with keys remaining");
        let produced = build_tiled_batches(&r.held_times, r.builders, &r.lower, &r.upper);
        // Fold the round's deferred times back into pending, which still holds the
        // carried beyond-upper times from begin.
        for (key, mut times) in r.deferred {
            let entry = self.pending.entry(key).or_default();
            entry.append(&mut times);
            entry.sort();
            entry.dedup();
        }
        let mut frontier = Antichain::new();
        for times in self.pending.values() {
            for t in times {
                frontier.insert(t.clone());
            }
        }
        (produced, frontier)
    }
}
