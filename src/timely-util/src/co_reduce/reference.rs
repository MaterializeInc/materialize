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

//! A model-derived reference tactic: from-scratch evaluation per key per interesting time. A testing oracle, not a production engine.

use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;

use differential_dataflow::consolidation::consolidate;
use differential_dataflow::difference::{Abelian, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::cursor::cursor_list;
use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::{
    BatchCursor, BatchDiff, BatchReader, Builder, Cursor, Navigable, Trace, TraceReader,
};
use timely::PartialOrder;
use timely::container::PushInto;
use timely::progress::{Antichain, Timestamp};

use super::CoReduceTactic;
use super::cursor::{build_tiled_batches, read_key_values, seek_owned_key, stage_times};

/// State of an in-flight round.
///
/// Mirrors the cursor tactic's round state. `builders` and `updates` are allocated once
/// in `begin`, sized from `held`, and mutated by `step`.
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
    /// Beyond-upper closure times discovered this round, folded into pending at finish.
    deferred: BTreeMap<K, Vec<B0::Time>>,
}

/// A model-derived [`CoReduceTactic`]: for each key it recomputes output from scratch
/// over the join-closure of the interesting times, diffing the desired output at each
/// time against prior plus already-produced output. Correct by construction and slow by
/// construction, so it is a differential-testing oracle for [`CursorTactic`], not a
/// production engine.
///
/// [`CursorTactic`]: super::CursorTactic
pub struct ReferenceTactic<B0, B1, BOut, K, V, V2, R2, Bu, L>
where
    B0: BatchReader,
{
    logic: L,
    /// Staged interesting times by key, surviving across rounds. Holds both times not
    /// yet retired (one input's frontier lags) and beyond-upper closure times.
    pending: BTreeMap<K, Vec<B0::Time>>,
    round: Option<RoundState<B0, B1, BOut, K, V2, R2, Bu>>,
    _marker: PhantomData<(V, V2, R2)>,
}

impl<B0, B1, BOut, K, V, V2, R2, Bu, L> ReferenceTactic<B0, B1, BOut, K, V, V2, R2, Bu, L>
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
    for ReferenceTactic<B0, B1, BOut, K, V, V2, R2, Bu, L>
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

    fn step(&mut self, _fuel: usize) -> bool {
        let Some(r) = self.round.as_mut() else {
            return true;
        };
        let (mut input0_src, input0_stor) = cursor_list(r.history0.clone());
        let (mut input1_src, input1_stor) = cursor_list(r.history1.clone());
        let (mut out_cur, out_stor) = cursor_list(r.output.clone());

        while let Some((key, seeds)) = r.remaining.pop_first() {
            let mut hist0: Vec<(V, B0::Time, <B0::Cursor as Cursor>::Diff)> = Vec::new();
            let mut hist1: Vec<(V, B0::Time, <B0::Cursor as Cursor>::Diff)> = Vec::new();
            let mut prior: Vec<(V2, B0::Time, R2)> = Vec::new();
            if seek_owned_key(&mut input0_src, &input0_stor, &key) {
                read_key_values(&mut input0_src, &input0_stor, &mut hist0);
            }
            if seek_owned_key(&mut input1_src, &input1_stor, &key) {
                read_key_values(&mut input1_src, &input1_stor, &mut hist1);
            }
            if seek_owned_key(&mut out_cur, &out_stor, &key) {
                read_key_values(&mut out_cur, &out_stor, &mut prior);
            }

            // The time universe: seeds, both input histories, prior output. Closed under
            // join by naive fixpoint. The closure of a finite set under an idempotent,
            // commutative, associative join is finite, so this terminates.
            let mut times: Vec<B0::Time> = seeds;
            times.extend(hist0.iter().map(|(_, t, _)| t.clone()));
            times.extend(hist1.iter().map(|(_, t, _)| t.clone()));
            times.extend(prior.iter().map(|(_, t, _)| t.clone()));
            times.sort();
            times.dedup();
            loop {
                let mut grown = Vec::new();
                for i in 0..times.len() {
                    for j in (i + 1)..times.len() {
                        let joined = times[i].join(&times[j]);
                        if times.binary_search(&joined).is_err() {
                            grown.push(joined);
                        }
                    }
                }
                if grown.is_empty() {
                    break;
                }
                times.extend(grown);
                times.sort();
                times.dedup();
            }

            // Beyond-upper times are withheld for a later round.
            let mut deferred = Vec::new();
            times.retain(|t| {
                if r.upper.less_equal(t) {
                    deferred.push(t.clone());
                    false
                } else {
                    true
                }
            });
            if !deferred.is_empty() {
                deferred.sort();
                deferred.dedup();
                r.deferred.entry(key.clone()).or_default().extend(deferred);
            }

            // Evaluate ascending by Ord. Correctness requires Ord to be a linear
            // extension of the partial order, so that every t' < t is evaluated before t
            // and `produced` is complete when t reads it.
            let mut produced: Vec<(V2, B0::Time, R2)> = Vec::new();
            for t in &times {
                let mut acc0: Vec<(V, <B0::Cursor as Cursor>::Diff)> = hist0
                    .iter()
                    .filter(|(_, ti, _)| PartialOrder::less_equal(ti, t))
                    .map(|(v, _, d)| (v.clone(), d.clone()))
                    .collect();
                consolidate(&mut acc0);
                let mut acc1: Vec<(V, <B0::Cursor as Cursor>::Diff)> = hist1
                    .iter()
                    .filter(|(_, ti, _)| PartialOrder::less_equal(ti, t))
                    .map(|(v, _, d)| (v.clone(), d.clone()))
                    .collect();
                consolidate(&mut acc1);

                let mut desired_list: Vec<(V2, R2)> = Vec::new();
                (self.logic)(&key, &[&acc0, &acc1], &mut desired_list);
                consolidate(&mut desired_list);
                let desired: BTreeMap<V2, R2> = desired_list.into_iter().collect();

                // Current output as of `t`: prior plus this-round produced, values with
                // time <= t. `consolidate` combines per value and drops zeros.
                let mut current_list: Vec<(V2, R2)> = prior
                    .iter()
                    .chain(produced.iter())
                    .filter(|(_, ti, _)| PartialOrder::less_equal(ti, t))
                    .map(|(v, _, d)| (v.clone(), d.clone()))
                    .collect();
                consolidate(&mut current_list);
                let current: BTreeMap<V2, R2> = current_list.into_iter().collect();

                // Delta per value over the union of both maps. The (present, absent)
                // cases avoid an additive-zero constructor, which R2: Abelian lacks: the
                // desired-only delta is `desired[v]`, the current-only delta is
                // `-current[v]`, computed directly.
                let values: BTreeSet<V2> = desired.keys().chain(current.keys()).cloned().collect();
                for v in values {
                    let delta = match (desired.get(&v), current.get(&v)) {
                        (Some(d), Some(c)) => {
                            let mut delta = d.clone();
                            let mut neg = c.clone();
                            neg.negate();
                            delta.plus_equals(&neg);
                            delta
                        }
                        (Some(d), None) => d.clone(),
                        (None, Some(c)) => {
                            let mut neg = c.clone();
                            neg.negate();
                            neg
                        }
                        (None, None) => unreachable!("value came from the union of both maps"),
                    };
                    if delta.is_zero() {
                        continue;
                    }
                    // Assign the delta to the latest held time covering t, as the driver
                    // mints capabilities only at held times.
                    let idx = r
                        .held_times
                        .iter()
                        .enumerate()
                        .rev()
                        .find(|(_, ht)| PartialOrder::less_equal(*ht, t))
                        .map(|(i, _)| i)
                        .expect("a held time covers every produced time");
                    produced.push((v.clone(), t.clone(), delta.clone()));
                    r.updates[idx].push((v, t.clone(), delta));
                }
            }

            // Push into builders, value-sorted, same as the cursor tactic.
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
        }
        true
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

/// Runs the two-input reduction with the model-derived [`ReferenceTactic`], the analogue
/// of [`super::co_reduce2`]. Same result contract, intended for differential testing of
/// the two tactics against each other. A testing oracle, not a stable entry point to
/// build on.
pub fn co_reduce2_reference<'scope, T, Tr1, Tr2, K, V, V2, R2, Bu, Out, L>(
    input0: Arranged<'scope, Tr1>,
    input1: Arranged<'scope, Tr2>,
    name: &str,
    fuel: usize,
    logic: L,
) -> Arranged<'scope, TraceAgent<Out>>
where
    T: Timestamp + Lattice + Ord,
    // The inputs are read only through their batch cursors, so all key, value, and diff
    // opinions are stated on `BatchCursor<Tr>` rather than on the trace itself, which in this
    // differential version carries only a time opinion. Both batches must be `Navigable` so the
    // operator can build cursors over them.
    Tr1: TraceReader<Time = T> + Clone + 'static,
    Tr1::Batch: Navigable,
    Tr2: TraceReader<Time = T> + Clone + 'static,
    Tr2::Batch: Navigable,
    BatchCursor<Tr1>: Cursor<Time = T>,
    <BatchCursor<Tr1> as Cursor>::Diff: Semigroup + Clone + 'static,
    <BatchCursor<Tr1> as Cursor>::KeyContainer: BatchContainer<Owned = K>,
    <BatchCursor<Tr1> as Cursor>::ValContainer: BatchContainer<Owned = V>,
    // `Tr2`'s diff is pinned to `Tr1`'s so both inputs feed the closure as the one input diff
    // type, and its key and value containers carry the same owned types.
    BatchCursor<Tr2>: Cursor<Time = T, Diff = BatchDiff<Tr1>>,
    <BatchCursor<Tr2> as Cursor>::KeyContainer: BatchContainer<Owned = K>,
    <BatchCursor<Tr2> as Cursor>::ValContainer: BatchContainer<Owned = V>,
    K: Ord + Clone + 'static,
    V: Ord + Clone + 'static,
    V2: Ord + Clone + 'static,
    R2: Abelian + Clone + 'static,
    Out: Trace<Time = T> + 'static,
    Out::Batch: Navigable,
    BatchCursor<Out>: Cursor<Time = T, Diff = R2, ValOwn = V2>,
    <BatchCursor<Out> as Cursor>::KeyContainer: BatchContainer<Owned = K>,
    <BatchCursor<Out> as Cursor>::ValContainer: BatchContainer<Owned = V2>,
    // `Bu::Input` is only required to be default-constructible and to accept output tuples.
    // This admits builders whose input is not a `Vec` (for example a columnar stack), which
    // a `Vec`-typed bound would reject.
    Bu: Builder<Time = T, Output = Out::Batch> + 'static,
    Bu::Input: Default + PushInto<((K, V2), T, R2)>,
    L: FnMut(&K, &[&[(V, BatchDiff<Tr1>)]], &mut Vec<(V2, R2)>) + 'static,
{
    super::co_reduce2_with_tactic(
        input0,
        input1,
        name,
        fuel,
        ReferenceTactic::<Tr1::Batch, Tr2::Batch, Out::Batch, K, V, V2, R2, Bu, L>::new(logic),
    )
}
