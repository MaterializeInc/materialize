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

use differential_dataflow::consolidation::consolidate;
use differential_dataflow::difference::{Abelian, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::implementations::BatchContainer;
use timely::PartialOrder;
use timely::progress::Antichain;

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
/// at the key (values at their start); on a miss it stops at the first key greater than
/// `target`.
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
    cap_times: &[T],
    cap_updates: &mut [Vec<(V2, T, R2)>],
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
    // lie in the join-closure of input times; including prior is redundant but harmless.
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
        emit_deltas(
            &desired,
            &current,
            &t,
            cap_times,
            cap_updates,
            &mut produced,
        );

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
/// `desired - current` per value into `produced` and the covering capability's updates.
pub(super) fn emit_deltas<T, V2, R2>(
    desired: &[(V2, R2)],
    current: &[(V2, R2)],
    t: &T,
    cap_times: &[T],
    cap_updates: &mut [Vec<(V2, T, R2)>],
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
        // Latest capability covering `t`. One must exist: `t` is reachable from a batch
        // or a pending time, both of which retain a capability at or below their time.
        let idx = cap_times
            .iter()
            .enumerate()
            .rev()
            .find(|(_, ct)| PartialOrder::less_equal(*ct, t))
            .map(|(i, _)| i)
            .expect("a capability covers every produced time");
        cap_updates[idx].push((v.clone(), t.clone(), delta));
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
