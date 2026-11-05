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

//! ## Notation
//!
//! Collections are represented with capital letters (T, S, R), collection traces as bold letters
//! (𝐓, 𝐒, 𝐑), and difference traces as δ𝐓.
//!
//! Indexing a collection trace 𝐓 to obtain its version at `t` is written as 𝐓(t). Indexing a
//! collection to obtain the multiplicity of a record `x` is written as T\[x\]. These can be combined
//! to obtain the multiplicity of a record `x` at some version `t` as 𝐓(t)\[x\].
//!
//! ## Overview
//!
//! Reclocking transforms a source collection `S` that evolves with some timestamp `FromTime` into
//! a collection `T` that evolves with some other timestamp `IntoTime`. The reclocked collection T
//! contains all updates `u ∈ S` that are not beyond some `FromTime` frontier R(t). The collection
//! `R` is called the remap collection.
//!
//! More formally, for some arbitrary time `t` of `IntoTime` and some arbitrary record `x`, the
//! reclocked collection `T(t)[x]` is defined to be the `sum{δ𝐒(s)[x]: !(𝐑(t) ⪯ s)}`. Since this
//! holds for any record we can write the definition of Reclock(𝐒, 𝐑) as:
//!
//! > Reclock(𝐒, 𝐑) ≜ 𝐓: ∀ t ∈ IntoTime : 𝐓(t) = sum{δ𝐒(s): !(𝐑(t) ⪯ s)}
//!
//! In order for the reclocked collection `T` to have a sensible definition of progress we require
//! that `t1 ≤ t2 ⇒ 𝐑(t1) ⪯ 𝐑(t2)` where the first `≤` is the partial order of `IntoTime` and the
//! second one the partial order of `FromTime` antichains.
//!
//! ## Total order simplification
//!
//! In order to simplify the implementation we will require that `IntoTime` is a total order. This
//! limitation can be lifted in the future but further elaboration on the mechanics of reclocking
//! is required to ensure a correct implementation.
//!
//! ## The difference trace
//!
//! By the definition of difference traces we have:
//!
//! ```text
//!     δ𝐓(t) = T(t) - sum{δ𝐓(s): s < t}
//! ```
//!
//! Due to the total order assumption we only need to consider two cases.
//!
//! **Case 1:** `t` is the minimum timestamp
//!
//! In this case `sum{δ𝐓(s): s < t}` is the empty set and so we obtain:
//!
//! ```text
//!     δ𝐓(min) = T(min) = sum{δ𝐒(s): !(𝐑(min) ≤ s}
//! ```
//!
//! **Case 2:** `t` is a timestamp with a predecessor `prev`
//!
//! In this case `sum{δ𝐓(s): s < t}` is equal to `T(prev)` because:
//!
//! ```text
//!     sum{δ𝐓(s): s < t} = sum{δ𝐓(s): s ≤ prev} + sum{δ𝐓(s): prev < s < t}
//!                       = T(prev) + ∅
//!                       = T(prev)
//! ```
//!
//! And therefore the difference trace of T is:
//!
//! ```text
//!     δ𝐓(t) = 𝐓(t) - 𝐓(prev)
//!           = sum{δ𝐒(s): !(𝐑(t) ⪯ s)} - sum{δ𝐒(s): !(𝐑(prev) ⪯ s)}
//!           = sum{δ𝐒(s): (𝐑(prev) ⪯ s) ∧ !(𝐑(t) ⪯ s)}
//! ```
//!
//! ## Unique mapping property
//!
//! Given the definition above we can derive the fact that for any source difference δ𝐒(s) there is
//! at most one target timestamp t that it must be reclocked to. This property can be exploited by
//! the implementation of the operator as it can safely discard source updates once a matching
//! δT(t) has been found, making it "stateless" with respect to the source trace. A formal proof of
//! this property is [provided below](#unique-mapping-property-proof).
//!
//! ## Operational description
//!
//! The operator follows a run-to-completion model where on each scheduling it completes all
//! outstanding work that can be completed.
//!
//! ### Unique mapping property proof
//!
//! This section contains the formal proof the unique mapping property. The proof follows the
//! structure proof notation created by Leslie Lamport. Readers unfamiliar with structured proofs
//! can read about them here <https://lamport.azurewebsites.net/pubs/proof.pdf>.
//!
//! #### Statement
//!
//! AtMostOne(X, φ(x)) ≜ ∀ x1, x2 ∈ X : φ(x1) ∧ φ(x2) ⇒ x1 = x2
//!
//! * **THEOREM** UniqueMapping ≜
//!     * **ASSUME**
//!         * **NEW** (FromTime, ⪯) ∈ PartiallyOrderedTimestamps
//!         * **NEW** (IntoTime, ≤) ∈ TotallyOrderedTimestamps
//!         * **NEW** 𝐒 ∈ SetOfCollectionTraces(FromTime)
//!         * **NEW** 𝐑 ∈ SetOfCollectionTraces(IntoTime)
//!         * ∀ t ∈ IntoTime: 𝐑(t) ∈ SetOfAntichains(FromTime)
//!         * ∀ t1, t1 ∈ IntoTime: t1 ≤ t2 ⇒ 𝐑(t1) ⪯ 𝐑(t2)
//!         * **NEW** 𝐓 = Reclock(𝐒, 𝐑)
//!     * **PROVE**  ∀ s ∈ FromTime : AtMostOne(IntoTime, δ𝐒(s) ∈ δ𝐓(x))
//!
//! #### Proof
//!
//! 1. **SUFFICES ASSUME** ∃ s ∈ FromTime: ¬AtMostOne(IntoTime, δ𝐒(s) ∈ δ𝐓(x))
//!     * **PROVE FALSE**
//!     * _By proof by contradiction._
//! 2. **PICK** s ∈ FromTime : ¬AtMostOne(IntoTime, δ𝐒(s) ∈ δ𝐓(x))
//!    * _Proof: Such time exists by <1>1._
//! 3. ∃ t1, t2 ∈ IntoTime : t1 ≠ t2 ∧ δ𝐒(s) ∈ δ𝐓(t1) ∧ δ𝐒(s) ∈ δ𝐓(t2)
//!     1. ¬(∀ x1, x2 ∈ X : (δ𝐒(s) ∈ δ𝐓(x1)) ∧ (δ𝐒(s) ∈ δ𝐓(x2)) ⇒ x1 = x2)
//!         * _Proof: By <1>2 and definition of AtMostOne._
//!     2. Q.E.D
//!         * _Proof: By <2>1, quantifier negation rules, and theorem of propositional logic ¬(P ⇒ Q) ≡ P ∧ ¬Q._
//! 4. **PICK** t1, t2 ∈ IntoTime : t1 < t2 ∧ δ𝐒(s) ∈ δ𝐓(t1) ∧ δ𝐒(s) ∈ δ𝐓(t2)
//!    * _Proof: By <1>3. Assume t1 < t2 without loss of generality._
//! 5. ¬(𝐑(t1) ⪯ s)
//!     1. **CASE** t1 = min(IntoTime)
//!         1. δ𝐓(t1) = sum{δ𝐒(s): !(𝐑(t1)) ⪯ s}
//!             * _Proof: By definition of δ𝐓(min)._
//!         2. δ𝐒(s) ∈ δ𝐓(t1)
//!             * _Proof: By <1>4._
//!         3. Q.E.D
//!             * _Proof: By <3>1 and <3>2._
//!     2. **CASE** t1 > min(IntoTime)
//!         1. **PICK** t1_prev = Predecessor(t1)
//!             * _Proof: Predecessor exists because the set {t: t < t1} is non-empty since it must contain at least min(IntoTime)._
//!         2. δ𝐓(t1) = sum{δ𝐒(s): (𝐑(t1_prev) ⪯ s) ∧ !(𝐑(t1) ⪯ s)}
//!             * _Proof: By definition of δ𝐓(t)._
//!         3. δ𝐒(s) ∈ δ𝐓(t1)
//!             * _Proof: By <1>4._
//!         3. Q.E.D
//!             * _Proof: By <3>2 and <3>3._
//!     3. Q.E.D
//!         * _Proof: From cases <2>1 and <2>2 which are exhaustive_
//! 6. **PICK** t2_prev ∈ IntoTime : t2_prev = Predecessor(t2)
//!    * _Proof: Predecessor exists because by <1>4 the set {t: t < t2} is non empty since it must contain at least t1._
//! 7. t1 ≤ t2_prev
//!    * _Proof: t1 ∈ {t: t < t2} and t2_prev is the maximum element of the set._
//! 8. 𝐑(t2) ⪯ s
//!     1. t2 > min(IntoTime)
//!         * _Proof: By <1>5._
//!     2. **PICK** t2_prev = Predecessor(t2)
//!         * _Proof: Predecessor exists because the set {t: t < t2} is non-empty since it must contain at least min(IntoTime)._
//!     3. δ𝐓(t) = sum{δ𝐒(s): (𝐑(t2_prev) ⪯ s) ∧ !(𝐑(t) ⪯ s)}
//!         * _Proof: By definition of δ𝐓(t)_
//!     4. δ𝐒(s) ∈ δ𝐓(t1)
//!         * _Proof: By <1>4._
//!     5. Q.E.D
//!         * _Proof: By <2>3 and <2>4._
//! 9. 𝐑(t1) ⪯ 𝐑(t2_prev)
//!     * _Proof: By <1>.7 and hypothesis on R_
//! 10. 𝐑(t1) ⪯ s
//!     * _Proof: By <1>8 and <1>9._
//! 11. Q.E.D
//!     * _Proof: By <1>5 and <1>10_

use std::cmp::{Ordering, Reverse};
use std::collections::VecDeque;
use std::collections::binary_heap::{BinaryHeap, PeekMut};
use std::iter::FromIterator;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::{AsCollection, ExchangeData, VecCollection};
use mz_ore::Overflowing;
use mz_ore::collections::CollectionExt;
use timely::communication::{Pull, Push};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::operators::capture::Event;
use timely::dataflow::operators::generic::OutputBuilder;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::frontier::{AntichainRef, MutableAntichain};
use timely::progress::{Antichain, Timestamp};

/// Constructs an operator that reclocks a `source` collection varying with some time `FromTime`
/// into the corresponding `reclocked` collection varying over some time `IntoTime` using the
/// provided `remap` collection.
///
/// In order for the operator to read the `source` collection a `Pusher` is returned which can be
/// used with timely's capture facilities to connect a collection from a foreign scope to this
/// operator.
pub fn reclock<'scope, D, FromTime, IntoTime, R>(
    remap_collection: VecCollection<'scope, IntoTime, FromTime, Overflowing<i64>>,
    as_of: Antichain<IntoTime>,
) -> (
    Box<dyn Push<Event<FromTime, Vec<(D, FromTime, R)>>>>,
    VecCollection<'scope, IntoTime, D, R>,
)
where
    D: ExchangeData,
    FromTime: Timestamp,
    IntoTime: Timestamp + Lattice + TotalOrder,
    R: Semigroup + 'static,
{
    let scope = remap_collection.scope();
    let mut builder = OperatorBuilder::new("Reclock".into(), scope.clone());
    // Here we create a channel that can be used to send data from a foreign scope into this
    // operator. The channel is associated with this operator's address so that it is activated
    // every time events are available for consumption. This mechanism is similar to Timely's input
    // handles where data can be introduced into a timely scope from an exogenous source.
    let info = builder.operator_info();
    let channel_id = scope.worker().new_identifier();
    let (pusher, mut events) = scope
        .worker()
        .pipeline::<Event<FromTime, Vec<(D, FromTime, R)>>>(channel_id, info.address);

    let mut remap_input = builder.new_input(remap_collection.inner, Pipeline);
    // Keep the output disconnected from the remap collection input so that we can drop the output
    // capability when we finish reclocking the source, which can happen before the
    // remap_collection reaches the input frontier. See the `test_finalized_source` for an example.
    let (output, reclocked) = builder.new_output_connection([]);
    let mut output = OutputBuilder::from(output);

    builder.build(move |caps| {
        let mut capset = CapabilitySet::from_elem(caps.into_element());
        capset.downgrade(&as_of.borrow());

        // Received remap updates at times `into_time` greater or equal to `remap_input`'s input
        // frontier. As the input frontier advances, we drop elements out of this priority queue
        // and mint new associations.
        let mut pending_remap: BinaryHeap<Reverse<(IntoTime, FromTime, i64)>> = BinaryHeap::new();
        // The bindings of `remap_input` at times beyond its since and not beyond the input
        // frontier.
        let mut remap_trace = RemapTrace::new(as_of.clone());

        // A stash of source updates for which we don't know the corresponding binding yet.
        let mut deferred_source_updates: Vec<ChainBatch<_, _, _>> = Vec::new();
        // The frontier of the `events` input
        let mut source_frontier = MutableAntichain::from_elem(FromTime::minimum());

        // Accumulation buffer for `remap_input` updates.
        use timely::progress::ChangeBatch;
        let mut remap_accum_buffer: ChangeBatch<(IntoTime, FromTime)> = ChangeBatch::new();

        // The operator drains `remap_input` and organizes new bindings that are not beyond
        // `remap_input`'s frontier into the time ordered `remap_trace`.
        //
        // All received data events can either be reclocked to a time included in the
        // `remap_trace`, or deferred until new associations are minted. Each data event that
        // happens at some `FromTime` is mapped to the first `IntoTime` whose associated antichain
        // is not less or equal to the input `FromTime`.
        //
        // As progress events are received from the `events` input, we can advance our
        // held capability to track the least `IntoTime` a newly received `FromTime` could possibly
        // map to and also compact the maintained `remap_trace` to that time.
        move |frontiers| {
            let Some(cap) = capset.get(0).cloned() else {
                remap_input.for_each(|_, _| {});
                return;
            };
            let mut output = output.activate();
            let mut session = output.session(&cap);

            // STEP 1. Accept new bindings into `pending_remap`.
            // Advance all `into` times by `as_of`, and consolidate all updates at that frontier.
            remap_input.for_each(|_, data| {
                for (from, mut into, diff) in data.drain(..) {
                    into.advance_by(as_of.borrow());
                    remap_accum_buffer.update((into, from), diff.into_inner());
                }
            });
            // Drain consolidated bindings into the `pending_remap` heap.
            // Only do this once any of the `remap_input` frontier has passed `as_of`.
            // For as long as the input frontier is less-equal `as_of`, we have no finalized times.
            if !PartialOrder::less_equal(&frontiers[0].frontier(), &as_of.borrow()) {
                for ((into, from), diff) in remap_accum_buffer.drain() {
                    pending_remap.push(Reverse((into, from, diff)));
                }
            }

            // STEP 2. Extract bindings not beyond `remap_frontier` and commit them into `remap_trace`.
            let remap_upper = frontiers[0].frontier().to_owned();
            while let Some(Reverse((into, _, _))) = pending_remap.peek()
                && !remap_upper.less_equal(into)
            {
                // The heap yields the updates of a time together, and all of them are in it
                // because the remap frontier has passed that time.
                let into = into.clone();
                let updates = std::iter::from_fn(|| {
                    let update = pending_remap.peek_mut()?;
                    (update.0.0 == into).then(|| {
                        let Reverse((_, from, diff)) = PeekMut::pop(update);
                        (from, diff)
                    })
                });
                remap_trace.push(into.clone(), updates);
            }

            // STEP 3. Receive new data updates
            //         The `events` input describes arbitrary progress and data over `FromTime`,
            //         which must be translated to `IntoTime`. Each `FromTime` can be found as the
            //         first `IntoTime` associated with a `[FromTime]` that is not less or equal to
            //         the input `FromTime`. Received events that are not yet associated to an
            //         `IntoTime` are collected, and formed into a "chain batch": a sequence of
            //         chains that results from sorting the updates by `FromTime`, and then
            //         segmenting the sequence at elements where the partial order on `FromTime` is
            //         violated.
            let mut stash = Vec::new();
            // Consolidate progress updates before applying them to `source_frontier`, to avoid quadratic
            // behavior in overload scenarios.
            let mut change_batch = ChangeBatch::<FromTime, 2>::default();
            while let Some(event) = events.pull() {
                match event {
                    Event::Progress(changes) => {
                        change_batch.extend(changes.drain(..));
                    }
                    Event::Messages(_, data) => stash.append(data),
                }
            }
            source_frontier.update_iter(change_batch.drain());
            stash.sort_unstable_by(|(_, t1, _): &(D, FromTime, R), (_, t2, _)| t1.cmp(t2));
            let mut new_source_updates = ChainBatch::from_iter(stash);

            // STEP 4: Reclock the source frontier and the new and deferred updates
            //         This only makes sense if there are any times for which we can correctly
            //         accumulate the remap trace, which is what we check here.
            if remap_trace
                .since()
                .iter()
                .all(|t| !remap_upper.less_equal(t))
            {
                // 4.1. Reclock `source_frontier`
                //      If any FromTime in the source frontier could still be reclocked to a
                //      binding then we must keep our capability to emit data at that binding's
                //      time and not compact past it. Without such a binding the frontier maps to
                //      `remap_upper`.
                let source_frontier = source_frontier.frontier();
                let reclocked_source_frontier = match remap_trace
                    .cursor()
                    .seek(|bound| source_frontier.iter().any(|t| !bound.less_equal(t)))
                {
                    Some((time, _)) => Antichain::from_elem(time.clone()),
                    None => remap_upper.clone(),
                };

                // 4.2. Extract updates from `new_source_updates` and `deferred_source_updates`
                //      The cursor stops only at bindings that some pending update maps to, and
                //      each stop extracts every update that maps there.
                let mut cursor = remap_trace.cursor();
                while !(new_source_updates.is_empty() && deferred_source_updates.is_empty())
                    && let Some((cur_time, cur_frontier)) = cursor.seek(|frontier| {
                        new_source_updates.can_extract(frontier)
                            || deferred_source_updates
                                .iter()
                                .any(|batch| batch.can_extract(frontier))
                    })
                {
                    for (data, _, diff) in new_source_updates.extract(cur_frontier) {
                        session.give((data, cur_time.clone(), diff));
                    }
                    deferred_source_updates.retain_mut(|batch| {
                        for (data, _, diff) in batch.extract(cur_frontier) {
                            session.give((data, cur_time.clone(), diff));
                        }
                        // Retain non-empty batches
                        !batch.is_empty()
                    });
                }

                // STEP 5. Downgrade capability and compact remap trace if our since frontier
                //         advanced
                if PartialOrder::less_than(remap_trace.since(), &reclocked_source_frontier) {
                    capset.downgrade(&reclocked_source_frontier.borrow());
                    remap_trace.advance_since(reclocked_source_frontier);
                }
            }

            // STEP 6. Tidy up deferred updates
            //         Deferred updates are represented as a list of chain batches where each batch
            //         contains two times the updates of the batch proceeding it. This organization
            //         leads to a logarithmic number of batches with respect to the outstanding
            //         number of updates.
            deferred_source_updates.sort_unstable_by_key(|b| Reverse(b.len()));
            if !new_source_updates.is_empty() {
                deferred_source_updates.push(new_source_updates);
            }
            let dsu = &mut deferred_source_updates;
            while dsu.len() > 1 && (dsu[dsu.len() - 1].len() >= dsu[dsu.len() - 2].len() / 2) {
                let a = dsu.pop().unwrap();
                let b = dsu.pop().unwrap();
                dsu.push(a.merge_with(b));
            }

            // If using less than a quarter of the capacity, shrink the container. To avoid having
            // to resize the container on a subsequent push, shrink to 2x the length, which is
            // what push would grow it to.
            if deferred_source_updates.len() < deferred_source_updates.capacity() / 4 {
                deferred_source_updates.shrink_to(deferred_source_updates.len() * 2);
            }

            // If this condition holds then this operator has no more work to do, so we drop the capability.
            if source_frontier.frontier().is_empty() && deferred_source_updates.is_empty() {
                capset = CapabilitySet::new();
            }
        }
    });

    (Box::new(pusher), reclocked.as_collection())
}

/// A batch of differential updates that vary over some partial order. This type maintains the data
/// as a set of chains that allows for efficient extraction of batches given a frontier.
#[derive(Debug, PartialEq)]
struct ChainBatch<D, T, R> {
    /// A list of chains (sets of mutually comparable times) sorted by the partial order.
    chains: Vec<VecDeque<(D, T, R)>>,
}

impl<D, T: Timestamp, R> ChainBatch<D, T, R> {
    /// Extracts all updates with time not greater or equal to any time in `upper`.
    fn extract<'a>(
        &'a mut self,
        upper: AntichainRef<'a, T>,
    ) -> impl Iterator<Item = (D, T, R)> + 'a {
        self.chains.retain(|chain| !chain.is_empty());
        self.chains.iter_mut().flat_map(move |chain| {
            // A chain is a sorted list of mutually comparable elements so we keep extracting
            // elements that are not beyond upper.
            std::iter::from_fn(move || {
                let (_, into, _) = chain.front()?;
                if !upper.less_equal(into) {
                    chain.pop_front()
                } else {
                    None
                }
            })
        })
    }

    /// Returns whether [`Self::extract`] would extract any update for `upper`.
    fn can_extract(&self, upper: AntichainRef<'_, T>) -> bool {
        self.chains
            .iter()
            .any(|chain| chain.front().is_some_and(|(_, t, _)| !upper.less_equal(t)))
    }

    fn merge_with(
        mut self: ChainBatch<D, T, R>,
        mut other: ChainBatch<D, T, R>,
    ) -> ChainBatch<D, T, R>
    where
        D: ExchangeData,
        T: Timestamp,
        R: Semigroup,
    {
        let mut updates1 = self.chains.drain(..).flatten().peekable();
        let mut updates2 = other.chains.drain(..).flatten().peekable();

        let merged = std::iter::from_fn(|| {
            match (updates1.peek(), updates2.peek()) {
                (Some((d1, t1, _)), Some((d2, t2, _))) => {
                    match (t1, d1).cmp(&(t2, d2)) {
                        Ordering::Less => updates1.next(),
                        Ordering::Greater => updates2.next(),
                        // If the same (d, t) pair is found, consolidate their diffs
                        Ordering::Equal => {
                            let (d1, t1, mut r1) = updates1.next().unwrap();
                            while let Some((_, _, r)) =
                                updates1.next_if(|(d, t, _)| (d, t) == (&d1, &t1))
                            {
                                r1.plus_equals(&r);
                            }
                            while let Some((_, _, r)) =
                                updates2.next_if(|(d, t, _)| (d, t) == (&d1, &t1))
                            {
                                r1.plus_equals(&r);
                            }
                            Some((d1, t1, r1))
                        }
                    }
                }
                (Some(_), None) => updates1.next(),
                (None, Some(_)) => updates2.next(),
                (None, None) => None,
            }
        });

        ChainBatch::from_iter(merged.filter(|(_, _, r)| !r.is_zero()))
    }

    /// Returns the number of updates in the batch.
    fn len(&self) -> usize {
        self.chains.iter().map(|chain| chain.len()).sum()
    }

    /// Returns true if the batch contains no updates.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<D, T: Timestamp, R> FromIterator<(D, T, R)> for ChainBatch<D, T, R> {
    /// Computes the chain decomposition of updates according to the partial order `T`.
    fn from_iter<I: IntoIterator<Item = (D, T, R)>>(updates: I) -> Self {
        let mut chains = vec![];
        let mut updates = updates.into_iter();
        if let Some((d, t, r)) = updates.next() {
            let mut chain = VecDeque::new();
            chain.push_back((d, t, r));
            for (d, t, r) in updates {
                let prev_t = &chain[chain.len() - 1].1;
                if !PartialOrder::less_equal(prev_t, &t) {
                    chains.push(chain);
                    chain = VecDeque::new();
                }
                chain.push_back((d, t, r));
            }
            chains.push(chain);
        }
        Self { chains }
    }
}

/// The bindings of a remap collection at its since and at every later time that has an update.
///
/// Reclocking a `FromTime` means finding the first binding that is not less or equal to it.
/// Bindings only advance with time (see the module docs). The trace stores some bindings whole, as
/// keyframes, and the updates at the times between them. [`Cursor::seek`] binary searches the
/// keyframes and applies the updates after the last one the predicate fails for.
///
/// This approach strikes a balance between updates and materialized antichains to improve the
/// performance of reclock while limiting memory usage. This is most often an issue when snapshot
/// and CDC run concurrently. A snapshotting export holds back the since, resulting in a
/// trace that gradually accumulates updates over time. CDC events map to the newest bindings.
/// With only updates stored, finding those means summing across the majority of the trace.
///
/// At the other extreme, materializing the frontier at every time requires memory proportional
/// to the antichain width for each time. This ends up consuming memory for elements that may not
/// have changed (for example UUIDs of GTID set that are no longer written to). Historically,
/// this memory intensive approach was dropped.
struct RemapTrace<FromTime, IntoTime> {
    /// When non-empty, this equals `keyframes[0].0`. This will differ from keyframes once the
    /// since is empty, which happens after the remap input closes. Calling `advance_by` with an
    /// empty frontier doesn't advance time, so keyframes holds a stale value. Steps 4 and 5 in
    /// reclock rely on empty antichain behavior, so we maintain it rather than deriving or using
    /// a bool/Option.
    since: Antichain<IntoTime>,
    /// In strictly increasing time order. Never empty, and the first is the binding at the since.
    keyframes: VecDeque<(IntoTime, Antichain<FromTime>)>,
    /// The updates between keyframes and after the last one, in time order. Updates at a
    /// keyframe's time are applied to that keyframe and not stored here.
    updates: VecDeque<(FromTime, IntoTime, i64)>,
    /// The number of updates pushed since the last keyframe was created, including the ones
    /// applied to it.
    run: usize,
    /// Working space for [`updated`]. It keeps its buffers between uses.
    scratch: MutableAntichain<FromTime>,
}

/// Returns the frontier that `frontier` becomes when `updates` are applied to it. `scratch` may
/// hold anything, and is left holding the result.
fn updated<T: Timestamp>(
    scratch: &mut MutableAntichain<T>,
    frontier: &Antichain<T>,
    updates: impl IntoIterator<Item = (T, i64)>,
) -> Antichain<T> {
    // TODO: A `MutableAntichain` does more work than the trace needs. A sorted vector of
    // elements, where each update inserts or removes one, could replace it. It does two things
    // that reclock can assume away, because the module docs require 𝐑(t) to be an antichain.
    //
    // * It counts how many times each element was added. The remap collection holds every
    //   element once.
    // * It works out the minimal elements again whenever an update touches the frontier, by
    //   comparing elements with each other. Every element the remap collection holds at a time
    //   is minimal.
    //
    // The `reclock_sparse_gtid_10_uuids` bench measured 3.0 ms at 100,000 bindings this way,
    // against 0.59 ms with sorted vectors.
    scratch.clear();
    scratch.update_iter(
        frontier
            .iter()
            .cloned()
            .map(|from| (from, 1))
            .chain(updates),
    );
    // The result keeps only the minimal elements. An element that is not minimal would be
    // dropped here, and lost for good once the element below it is retracted. The counts are
    // exactly the frontier when no such element exists and none is held twice.
    let (elements, once) = scratch
        .updates()
        .fold((0, true), |(elements, once), (_, count)| {
            (elements + 1, once && *count == 1)
        });
    assert!(
        once && elements == scratch.frontier().len(),
        "the remap collection must be an antichain at every time"
    );
    scratch.frontier().to_owned()
}

impl<FromTime, IntoTime> RemapTrace<FromTime, IntoTime>
where
    FromTime: Timestamp,
    IntoTime: Timestamp + Lattice + TotalOrder,
{
    /// Returns a trace whose frontier at `since` is empty, until updates arrive at that time.
    fn new(since: Antichain<IntoTime>) -> Self {
        let mut time = IntoTime::minimum();
        time.advance_by(since.borrow());
        Self {
            since,
            keyframes: VecDeque::from([(time, Antichain::new())]),
            updates: VecDeque::new(),
            run: 0,
            scratch: MutableAntichain::new(),
        }
    }

    fn since(&self) -> &Antichain<IntoTime> {
        &self.since
    }

    /// Applies the updates of time `into`. Every time must be pushed once, with all its updates,
    /// in time order and not before the since.
    fn push(&mut self, into: IntoTime, updates: impl IntoIterator<Item = (FromTime, i64)>) {
        let (key_time, key_frontier) = self
            .keyframes
            .back()
            .expect("the trace holds the binding at the since");

        // The last update, if it lies after the last keyframe.
        let after_keyframe = self.updates.back().filter(|(_, time, _)| key_time < time);

        // invariant: times arrive in order. Only the since can be pushed at the time of a
        // keyframe, which stands there before the since has updates of its own.
        let in_order = match after_keyframe {
            Some((_, time, _)) => *time < into,
            None => *key_time <= into,
        };
        assert!(in_order, "remap updates out of time order");

        let mut count = 0;
        let updates = updates.into_iter().inspect(|_| count += 1);
        if *key_time == into {
            let (_, key_frontier) = self
                .keyframes
                .back_mut()
                .expect("the trace holds the binding at the since");
            *key_frontier = updated(&mut self.scratch, key_frontier, updates);
        } else if self.run >= key_frontier.len() {
            // A keyframe is a materialized antichain, consuming memory proportional to its width.
            // This creates a keyframe based on the number of updates that have arrived since
            // the last keyframe. If most of the elements end up changing, this results in storing
            // a keyframe for each time. The expectation is that there are many updates being
            // made, in which case this trades memory for speed.
            //
            // A Kafka source doesn't run concurrent snapshot and CDC, so its trace is going to
            // remain relatively small and this extra space isn't much of a concern. A MySQL source
            // is more likely to see an increase in memory usage where the GTID set contains many
            // UUIDs that are all advancing (e.g. multi-master setup, where MZ reads from the
            // replica).
            let start = self
                .updates
                .partition_point(|(_, time, _)| time <= key_time);
            let frontier = updated(
                &mut self.scratch,
                key_frontier,
                self.updates
                    .range(start..)
                    .map(|(from, _, diff)| (from.clone(), *diff))
                    .chain(updates),
            );
            self.keyframes.push_back((into, frontier));
            self.run = 0;
        } else {
            self.updates
                .extend(updates.map(|(from, diff)| (from, into.clone(), diff)));
        }
        self.run += count;
    }

    /// Advances the since to `since`, which must not be behind it, and drops the bindings before
    /// it.
    fn advance_since(&mut self, since: Antichain<IntoTime>) {
        // The binding at the since is the last keyframe at or before it, with the updates up to
        // the since applied.
        while self
            .keyframes
            .get(1)
            .is_some_and(|(time, _)| !since.less_than(time))
        {
            self.keyframes.pop_front();
        }
        let (key_time, key_frontier) = self
            .keyframes
            .front_mut()
            .expect("the trace holds the binding at the since");
        while self
            .updates
            .front()
            .is_some_and(|(_, time, _)| time < key_time)
        {
            self.updates.pop_front();
        }
        let folded = self
            .updates
            .partition_point(|(_, time, _)| !since.less_than(time));
        if folded > 0 {
            *key_frontier = updated(
                &mut self.scratch,
                key_frontier,
                self.updates
                    .drain(..folded)
                    .map(|(from, _, diff)| (from, diff)),
            );
        }
        key_time.advance_by(since.borrow());
        shrink(&mut self.keyframes);
        shrink(&mut self.updates);
        self.since = since;
    }

    /// Returns a cursor before the binding at the since.
    fn cursor(&self) -> Cursor<'_, FromTime, IntoTime> {
        Cursor {
            trace: self,
            keyframe_pos: None,
            update_pos: 0,
            frontier: MutableAntichain::new(),
        }
    }
}

/// A position in a [`RemapTrace`] that only moves forward.
struct Cursor<'a, FromTime, IntoTime> {
    trace: &'a RemapTrace<FromTime, IntoTime>,
    /// The index of the last keyframe at or before the cursor. `None` before the since.
    keyframe_pos: Option<usize>,
    /// The index of the first update after the cursor.
    update_pos: usize,
    /// The frontier at the cursor, when that lies between keyframes. It keeps its buffers as the
    /// cursor moves.
    frontier: MutableAntichain<FromTime>,
}

impl<'a, FromTime, IntoTime> Cursor<'a, FromTime, IntoTime>
where
    FromTime: Timestamp,
    IntoTime: Timestamp + Lattice + TotalOrder,
{
    /// Moves to the first binding after the cursor that satisfies `pred`, and returns that
    /// binding with its time. Returns `None` if there is none.
    ///
    /// Once `pred` holds for a binding it must hold for every later binding.
    fn seek(
        &mut self,
        mut pred: impl FnMut(AntichainRef<'_, FromTime>) -> bool,
    ) -> Option<(&'a IntoTime, AntichainRef<'_, FromTime>)> {
        let RemapTrace {
            keyframes, updates, ..
        } = self.trace;

        // Starting point for the seek.
        let start_pos = self.keyframe_pos.map_or(0, |keyframe| keyframe + 1);

        // Finds the first keyframe after the cursor's current keyframe position that satisfies
        // `pred`. If none does, `found_pos` is one past the last keyframe. The binding sought is
        // either that keyframe or lies between it and the keyframe before it.
        let found_pos = match keyframes.get(start_pos) {
            Some((next_time, frontier)) if !pred(frontier.borrow()) => keyframes
                .partition_point(|(time, frontier)| time <= next_time || !pred(frontier.borrow())),
            _ => start_pos,
        };
        // Jump to the keyframe before `found_pos` and step forward from there, stopping at
        // `found_pos`.
        if let Some(prev_pos) = found_pos.checked_sub(1)
            && self
                .keyframe_pos
                .is_none_or(|keyframe_pos| keyframe_pos < prev_pos)
        {
            let (time, _) = &keyframes[prev_pos];
            self.keyframe_pos = Some(prev_pos);
            self.update_pos = updates.partition_point(|(_, t, _)| t <= time);
        }
        while self.step() {
            let keyframe = self.keyframe_pos.expect("the cursor stepped");
            let at_found = keyframe == found_pos && !self.between();
            if at_found || pred(self.current().1) {
                return Some(self.current());
            }
        }
        None
    }

    /// Moves to the next binding. Returns false at the end of the trace.
    fn step(&mut self) -> bool {
        let RemapTrace {
            keyframes, updates, ..
        } = self.trace;
        let Some(keyframe_pos) = self.keyframe_pos else {
            // Every update lies after the first keyframe, which `update_pos` already says.
            self.keyframe_pos = Some(0);
            return true;
        };
        let next_keyframe_pos = keyframes.get(keyframe_pos + 1);
        match updates.get(self.update_pos) {
            Some((_, time, _)) if next_keyframe_pos.is_none_or(|(next, _)| time < next) => {
                let end = self.update_pos
                    + updates
                        .range(self.update_pos..)
                        .take_while(|(_, t, _)| t == time)
                        .count();
                let tick = updates
                    .range(self.update_pos..end)
                    .map(|(from, _, diff)| (from.clone(), *diff));
                if self.between() {
                    self.frontier.update_iter(tick);
                } else {
                    // The cursor leaves its keyframe. Seeding the frontier with the keyframe's
                    // elements in the same call as the updates works it out once.
                    let (_, frontier) = &keyframes[keyframe_pos];
                    let seed = frontier.iter().cloned().map(|from| (from, 1));
                    self.frontier.clear();
                    self.frontier.update_iter(seed.chain(tick));
                }
                self.update_pos = end;
                true
            }
            _ if next_keyframe_pos.is_some() => {
                self.keyframe_pos = Some(keyframe_pos + 1);
                true
            }
            _ => false,
        }
    }

    /// Returns whether the cursor has stepped past its keyframe onto a time that only has
    /// updates. Returns false before the since.
    ///
    /// When this is true, the frontier at the cursor is in `self.frontier`. When it is false, the
    /// cursor sits on the keyframe and the frontier is the keyframe's frontier.
    fn between(&self) -> bool {
        let RemapTrace {
            keyframes, updates, ..
        } = self.trace;
        let (Some(keyframe_pos), Some(update_pos)) =
            (self.keyframe_pos, self.update_pos.checked_sub(1))
        else {
            return false;
        };
        // `update_pos` is the position of the last update the cursor consumed. Its time tells
        // where the cursor is relative to the keyframe.
        //
        // * A later time means the cursor consumed that update after it reached the keyframe. It
        //   has moved past the keyframe.
        // * An earlier time means the cursor consumed that update on its way to the keyframe, and
        //   nothing since. It still sits on the keyframe.
        //
        // The times cannot be equal. The updates at a keyframe's time are part of the keyframe
        // and never appear in `updates`.
        updates[update_pos].1 > keyframes[keyframe_pos].0
    }

    /// Returns the time and frontier at the cursor, which must not be before the since.
    fn current(&self) -> (&'a IntoTime, AntichainRef<'_, FromTime>) {
        let keyframe_pos = self.keyframe_pos.expect("the cursor is past the since");
        let RemapTrace {
            keyframes, updates, ..
        } = self.trace;
        if self.between() {
            let (_, time, _) = &updates[self.update_pos - 1];
            (time, self.frontier.frontier())
        } else {
            let (time, frontier) = &keyframes[keyframe_pos];
            (time, frontier.borrow())
        }
    }
}

/// Shrinks `deque` if it uses less than a quarter of its capacity. It keeps twice the length,
/// which is what a push would grow it to.
fn shrink<T>(deque: &mut VecDeque<T>) {
    if deque.len() < deque.capacity() / 4 {
        deque.shrink_to(deque.len() * 2);
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicUsize;
    use std::sync::mpsc::{Receiver, TryRecvError};

    use differential_dataflow::consolidation;
    use differential_dataflow::input::{Input, InputSession};
    use mz_ore::cast::CastFrom;
    use serde::{Deserialize, Serialize};
    use timely::dataflow::operators::capture::{Event, Extract};
    use timely::dataflow::operators::vec::UnorderedInput;
    use timely::dataflow::operators::vec::unordered_input::UnorderedHandle;
    use timely::dataflow::operators::{ActivateCapability, Capture};
    use timely::progress::timestamp::Refines;
    use timely::progress::{PathSummary, Stamp};
    use timely::worker::Worker;

    use crate::capture::PusherCapture;
    use crate::order::Partitioned;

    use super::*;

    type Diff = Overflowing<i64>;
    type FromTime = Partitioned<u64, u64>;
    type IntoTime = u64;
    type BindingHandle<FromTime> = InputSession<IntoTime, FromTime, Diff>;
    type DataHandle<D, FromTime> = (
        UnorderedHandle<FromTime, (D, FromTime, Diff)>,
        ActivateCapability<FromTime>,
    );
    type ReclockedStream<D> = Receiver<Event<IntoTime, Vec<(D, IntoTime, Diff)>>>;

    /// A helper function that sets up a dataflow program to test the reclocking operator. Each
    /// test provides a test logic closure which accepts four arguments:
    ///
    /// * A reference to the worker that allows the test to step the computation
    /// * A [`BindingHandle`] that allows the test to manipulate the remap bindings
    /// * A [`DataHandle`] that allows the test to submit the data to be reclocked
    /// * A [`ReclockedStream`] that allows observing the result of the reclocking process
    ///
    /// Note that the `DataHandle` contains a capability that should be dropped or downgraded before
    /// calling [`step`] to process data at the time.
    fn harness<FromTime, D, F, R>(as_of: Antichain<IntoTime>, test_logic: F) -> R
    where
        FromTime: Timestamp + Refines<()>,
        D: ExchangeData,
        F: FnOnce(
                &mut Worker,
                BindingHandle<FromTime>,
                DataHandle<D, FromTime>,
                ReclockedStream<D>,
            ) -> R
            + Send
            + Sync
            + 'static,
        R: Send + 'static,
    {
        timely::execute_directly(move |worker| {
            let (bindings, data, data_cap, reclocked) = worker.dataflow::<(), _, _>(|scope| {
                let (bindings, data_pusher, reclocked) =
                    scope.scoped::<IntoTime, _, _>("IntoScope", move |scope| {
                        let (binding_handle, binding_collection) = scope.new_collection();
                        let (data_pusher, reclocked_collection) =
                            reclock(binding_collection, as_of);
                        let reclocked_capture = reclocked_collection.inner.capture();
                        (binding_handle, data_pusher, reclocked_capture)
                    });

                let (data, data_cap) = scope.scoped::<FromTime, _, _>("FromScope", move |scope| {
                    let ((handle, cap), data) = scope.new_unordered_input::<(D, FromTime, Diff)>();
                    data.capture_into(PusherCapture(data_pusher));
                    (handle, cap)
                });

                (bindings, data, data_cap, reclocked)
            });

            test_logic(worker, bindings, (data, data_cap), reclocked)
        })
    }

    /// Steps the worker four times which is the required number of times for both data and
    /// frontier updates to propagate across the two scopes and into the probing channels.
    fn step(worker: &mut Worker) {
        for _ in 0..4 {
            worker.step();
        }
    }

    #[mz_ore::test]
    fn basic_reclocking() {
        let as_of = Antichain::from_elem(IntoTime::minimum());
        harness::<FromTime, _, _, _>(
            as_of,
            |worker, bindings, (mut data, data_cap), reclocked| {
                // Reclock everything at the minimum IntoTime
                bindings.close();
                data.activate()
                    .session(&data_cap)
                    .give(('a', Partitioned::minimum(), Diff::ONE));
                drop(data_cap);
                step(worker);
                let extracted = reclocked.extract();
                let expected = vec![(0, vec![('a', 0, Diff::ONE)])];
                assert_eq!(extracted, expected);
            },
        )
    }

    /// Generates a `Partitioned<u64, u64>` Antichain where all the provided
    /// partitions are at the specified offset and the gaps in between are filled with range
    /// timestamps at offset zero.
    fn partitioned_frontier<I>(items: I) -> Antichain<Partitioned<u64, u64>>
    where
        I: IntoIterator<Item = (u64, u64)>,
    {
        let mut frontier = Antichain::new();
        let mut prev = 0;
        for (pid, offset) in items {
            if prev < pid {
                frontier.insert(Partitioned::new_range(prev, pid - 1, 0));
            }
            frontier.insert(Partitioned::new_singleton(pid, offset));
            prev = pid + 1
        }
        frontier.insert(Partitioned::new_range(prev, u64::MAX, 0));
        frontier
    }

    #[mz_ore::test]
    fn test_basic_usage() {
        let as_of = Antichain::from_elem(IntoTime::minimum());
        harness(
            as_of,
            |worker, mut bindings, (mut data, data_cap), reclocked| {
                // Reclock offsets 1 and 3 to timestamp 1000
                bindings.update_at(Partitioned::minimum(), 0, Diff::ONE);
                bindings.update_at(Partitioned::minimum(), 1000, Diff::MINUS_ONE);
                for time in partitioned_frontier([(0, 4)]) {
                    bindings.update_at(time, 1000, Diff::ONE);
                }
                bindings.advance_to(1001);
                bindings.flush();
                data.activate().session(&data_cap).give_iterator(
                    vec![
                        (1, Partitioned::new_singleton(0, 1), Diff::ONE),
                        (1, Partitioned::new_singleton(0, 1), Diff::ONE),
                        (3, Partitioned::new_singleton(0, 3), Diff::ONE),
                    ]
                    .into_iter(),
                );

                step(worker);
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Messages(
                        Stamp::from_elem(0u64),
                        vec![
                            (1, 1000, Diff::ONE),
                            (1, 1000, Diff::ONE),
                            (3, 1000, Diff::ONE)
                        ]
                    ))
                );
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(0, -1), (1000, 1)]))
                );

                // Reclock more messages for offsets 3 to the same timestamp
                data.activate().session(&data_cap).give_iterator(
                    vec![
                        (3, Partitioned::new_singleton(0, 3), Diff::ONE),
                        (3, Partitioned::new_singleton(0, 3), Diff::ONE),
                    ]
                    .into_iter(),
                );
                step(worker);
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Messages(
                        Stamp::from_elem(1000u64),
                        vec![(3, 1000, Diff::ONE), (3, 1000, Diff::ONE)]
                    ))
                );

                // Drop the capability which should advance the reclocked frontier to 1001.
                drop(data_cap);
                step(worker);
                assert_eq!(reclocked.try_recv(), Ok(Event::Progress(vec![(1000, -1)])));
            },
        );
    }

    /// Reclocks updates against many bindings while the source frontier holds the since at the
    /// as_of, the way an export does while it snapshots.
    #[mz_ore::test]
    fn test_reclock_pinned_since() {
        harness(
            Antichain::from_elem(0),
            |worker, mut bindings, (mut data, data_cap), reclocked| {
                // The binding at time `t` covers the offsets of partition 0 below `t`.
                let mut mint = |times: std::ops::RangeInclusive<u64>| {
                    let upper = times.end() + 1;
                    for t in times {
                        let prev = match t {
                            0 => Antichain::new(),
                            1 => Antichain::from_elem(Partitioned::minimum()),
                            t => partitioned_frontier([(0, t - 1)]),
                        };
                        let next = match t {
                            0 => Antichain::from_elem(Partitioned::minimum()),
                            t => partitioned_frontier([(0, t)]),
                        };
                        for time in prev {
                            bindings.update_at(time, t, Diff::MINUS_ONE);
                        }
                        for time in next {
                            bindings.update_at(time, t, Diff::ONE);
                        }
                    }
                    bindings.advance_to(upper);
                    bindings.flush();
                };
                let reclocked_data = || {
                    let mut updates: Vec<_> = reclocked
                        .try_iter()
                        .flat_map(|event| match event {
                            Event::Messages(_, updates) => updates,
                            Event::Progress(_) => vec![],
                        })
                        .collect();
                    updates.sort();
                    updates
                };

                mint(0..=1000);
                // `data_cap` stays at the minimum, which pins the since at the as_of.
                let offsets = [0, 1, 63, 64, 500, 998, 999, 1000, 5000];
                data.activate().session(&data_cap).give_iterator(
                    offsets
                        .iter()
                        .map(|o| (*o, Partitioned::new_singleton(0, *o), Diff::ONE)),
                );
                step(worker);
                let expected: Vec<_> = offsets[..7]
                    .iter()
                    .map(|o| (*o, o + 1, Diff::ONE))
                    .collect();
                assert_eq!(reclocked_data(), expected);

                // The updates beyond the last binding wait for the bindings that cover them.
                mint(1001..=5001);
                step(worker);
                assert_eq!(
                    reclocked_data(),
                    vec![(1000, 1001, Diff::ONE), (5000, 5001, Diff::ONE)]
                );
            },
        );
    }

    // Test that once the input stream has reached the empty frontier with no pending data we
    // eagerly downgrade the output frontier to the empty frontier regardless of the remap stream.
    #[mz_ore::test]
    fn test_finalized_source() {
        let as_of = Antichain::from_elem(IntoTime::minimum());
        harness(
            as_of,
            |worker, mut bindings, (mut data, data_cap), reclocked| {
                // The starts by producing data in the source stream and advancing the frontier to
                // the empty frontier. The reclock operator buffers the data and holds the
                // appropriate capability to reclock the pending data in the future.
                data.activate().session(&data_cap).give_iterator(
                    vec![
                        (1, Partitioned::new_singleton(0, 1), Diff::ONE),
                        (2, Partitioned::new_singleton(0, 2), Diff::ONE),
                    ]
                    .into_iter(),
                );
                drop(data_cap);
                step(worker);

                // Reclock offset 0 and 1 to timestamp 1000
                bindings.update_at(Partitioned::minimum(), 0, Diff::ONE);
                bindings.update_at(Partitioned::minimum(), 1000, Diff::MINUS_ONE);
                for time in partitioned_frontier([(0, 2)]) {
                    bindings.update_at(time, 1000, Diff::ONE);
                }
                bindings.advance_to(1001);
                bindings.flush();
                step(worker);

                // At this point the reclock operator is able to release one of the updates and
                // downgrade the output capability to timestamp 1001. It can't yet downgrade to the
                // empty frontier since the other update is still pending.
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Messages(
                        Stamp::from_elem(0u64),
                        vec![(1, 1000, Diff::ONE)]
                    ))
                );
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(0, -1), (1001, 1)]))
                );

                // Reclock offset 2 to timestamp 2000
                for time in partitioned_frontier([(0, 2)]) {
                    bindings.update_at(time, 2000, Diff::MINUS_ONE);
                }
                for time in partitioned_frontier([(0, 3)]) {
                    bindings.update_at(time, 2000, Diff::ONE);
                }
                bindings.advance_to(2001);
                bindings.flush();
                step(worker);

                // Now the reclock operator reclocks the last data item and since the source
                // frontier is empty there will be no more work to be done, so the output frontier
                // downgrades to the empty frontier even though the frontier of the remap bindings
                // is still at [2001].
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Messages(
                        Stamp::from_elem(1001u64),
                        vec![(2, 2000, Diff::ONE)]
                    ))
                );
                assert_eq!(reclocked.try_recv(), Ok(Event::Progress(vec![(1001, -1)])));
            },
        );
    }

    #[mz_ore::test]
    fn test_reclock_frontier() {
        let as_of = Antichain::from_elem(IntoTime::minimum());
        harness::<_, (), _, _>(
            as_of,
            |worker, mut bindings, (_data, data_cap), reclocked| {
                // Initialize the bindings such that the minimum IntoTime contains the minimum FromTime
                // frontier.
                bindings.update_at(Partitioned::minimum(), 0, Diff::ONE);
                bindings.advance_to(1);
                bindings.flush();
                step(worker);
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(0, -1), (1, 1)]))
                );

                // Mint a couple of bindings for multiple partitions
                bindings.update_at(Partitioned::minimum(), 1000, Diff::MINUS_ONE);
                for time in partitioned_frontier([(1, 10)]) {
                    bindings.update_at(time.clone(), 1000, Diff::ONE);
                    bindings.update_at(time, 2000, Diff::MINUS_ONE);
                }
                for time in partitioned_frontier([(1, 10), (2, 10)]) {
                    bindings.update_at(time, 2000, Diff::ONE);
                }
                bindings.advance_to(2001);
                bindings.flush();

                // The initial frontier should now map to the minimum between the two partitions
                step(worker);
                step(worker);
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(1, -1), (1000, 1)]))
                );

                // Downgrade data frontier such that only one of the partitions is advanced
                let mut part1_cap = data_cap.delayed(&Partitioned::new_singleton(1, 9));
                let mut part2_cap = data_cap.delayed(&Partitioned::new_singleton(2, 0));
                let _rest_cap = data_cap.delayed(&Partitioned::new_range(3, u64::MAX, 0));
                drop(data_cap);
                step(worker);
                assert_eq!(reclocked.try_recv(), Err(TryRecvError::Empty));

                // Downgrade the data frontier past the first binding
                part1_cap.downgrade(&Partitioned::new_singleton(1, 10));
                step(worker);
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(1000, -1), (2000, 1)]))
                );

                // Downgrade the data frontier past the second binding
                part2_cap.downgrade(&Partitioned::new_singleton(2, 10));
                step(worker);
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(2000, -1), (2001, 1)]))
                );

                // Advance the binding frontier and confirm that we get to the next timestamp
                bindings.advance_to(3001);
                bindings.flush();
                step(worker);
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(2001, -1), (3001, 1)]))
                );
            },
        );
    }

    #[mz_ore::test]
    fn test_reclock() {
        let as_of = Antichain::from_elem(IntoTime::minimum());
        harness(
            as_of,
            |worker, mut bindings, (mut data, data_cap), reclocked| {
                // Initialize the bindings such that the minimum IntoTime contains the minimum FromTime
                // frontier.
                bindings.update_at(Partitioned::minimum(), 0, Diff::ONE);

                // Setup more precise capabilities for the rest of the test
                let mut part0_cap = data_cap.delayed(&Partitioned::new_singleton(0, 0));
                let rest_cap = data_cap.delayed(&Partitioned::new_range(1, u64::MAX, 0));
                drop(data_cap);

                // Reclock offsets 1 and 2 to timestamp 1000
                data.activate().session(&part0_cap).give_iterator(
                    vec![
                        (1, Partitioned::new_singleton(0, 1), Diff::ONE),
                        (2, Partitioned::new_singleton(0, 2), Diff::ONE),
                    ]
                    .into_iter(),
                );

                part0_cap.downgrade(&Partitioned::new_singleton(0, 3));
                bindings.update_at(Partitioned::minimum(), 1000, Diff::MINUS_ONE);
                bindings.update_at(part0_cap.time().clone(), 1000, Diff::ONE);
                bindings.update_at(rest_cap.time().clone(), 1000, Diff::ONE);
                bindings.advance_to(1001);
                bindings.flush();
                step(worker);
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Messages(
                        Stamp::from_elem(0),
                        vec![(1, 1000, Diff::ONE), (2, 1000, Diff::ONE)]
                    ))
                );
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(0, -1), (1000, 1)]))
                );
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(1000, -1), (1001, 1)]))
                );

                // Reclock offsets 3 and 4 to timestamp 2000
                data.activate().session(&part0_cap).give_iterator(
                    vec![
                        (3, Partitioned::new_singleton(0, 3), Diff::ONE),
                        (3, Partitioned::new_singleton(0, 3), Diff::ONE),
                        (4, Partitioned::new_singleton(0, 4), Diff::ONE),
                    ]
                    .into_iter(),
                );
                bindings.update_at(part0_cap.time().clone(), 2000, Diff::MINUS_ONE);
                part0_cap.downgrade(&Partitioned::new_singleton(0, 5));
                bindings.update_at(part0_cap.time().clone(), 2000, Diff::ONE);
                bindings.advance_to(2001);
                bindings.flush();
                step(worker);
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Messages(
                        Stamp::from_elem(1001),
                        vec![
                            (3, 2000, Diff::ONE),
                            (3, 2000, Diff::ONE),
                            (4, 2000, Diff::ONE)
                        ]
                    ))
                );
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(1001, -1), (2000, 1)]))
                );
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(2000, -1), (2001, 1)]))
                );
            },
        );
    }

    #[mz_ore::test]
    fn test_reclock_gh16318() {
        let as_of = Antichain::from_elem(IntoTime::minimum());
        harness(
            as_of,
            |worker, mut bindings, (mut data, data_cap), reclocked| {
                // Initialize the bindings such that the minimum IntoTime contains the minimum FromTime
                // frontier.
                bindings.update_at(Partitioned::minimum(), 0, Diff::ONE);
                // First mint bindings for 0 at timestamp 1000
                bindings.update_at(Partitioned::minimum(), 1000, Diff::MINUS_ONE);
                for time in partitioned_frontier([(0, 50)]) {
                    bindings.update_at(time, 1000, Diff::ONE);
                }
                // Then only for 1 at timestamp 2000
                for time in partitioned_frontier([(0, 50)]) {
                    bindings.update_at(time, 2000, Diff::MINUS_ONE);
                }
                for time in partitioned_frontier([(0, 50), (1, 50)]) {
                    bindings.update_at(time, 2000, Diff::ONE);
                }
                // Then again only for 0 at timestamp 3000
                for time in partitioned_frontier([(0, 50), (1, 50)]) {
                    bindings.update_at(time, 3000, Diff::MINUS_ONE);
                }
                for time in partitioned_frontier([(0, 100), (1, 50)]) {
                    bindings.update_at(time, 3000, Diff::ONE);
                }
                bindings.advance_to(3001);
                bindings.flush();

                // Reclockng (0, 50) must ignore the updates on the FromTime frontier that happened at
                // timestamp 2000 since those are completely unrelated
                data.activate().session(&data_cap).give((
                    50,
                    Partitioned::new_singleton(0, 50),
                    Diff::ONE,
                ));
                drop(data_cap);
                step(worker);
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Messages(
                        Stamp::from_elem(0),
                        vec![(50, 3000, Diff::ONE),]
                    ))
                );
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(0, -1), (1000, 1)]))
                );
                assert_eq!(reclocked.try_recv(), Ok(Event::Progress(vec![(1000, -1)])));
            },
        );
    }

    /// Test that compact(reclock(remap, source)) == reclock(compact(remap), source)
    #[mz_ore::test]
    fn test_compaction() {
        let mut remap = vec![];
        remap.push((Partitioned::minimum(), 0, Diff::ONE));
        // Reclock offsets 1 and 2 to timestamp 1000
        remap.push((Partitioned::minimum(), 1000, Diff::MINUS_ONE));
        for time in partitioned_frontier([(0, 3)]) {
            remap.push((time, 1000, Diff::ONE));
        }
        // Reclock offsets 3 and 4 to timestamp 2000
        for time in partitioned_frontier([(0, 3)]) {
            remap.push((time, 2000, Diff::MINUS_ONE));
        }
        for time in partitioned_frontier([(0, 5)]) {
            remap.push((time, 2000, Diff::ONE));
        }

        let source_updates = vec![
            (1, Partitioned::new_singleton(0, 1), Diff::ONE),
            (2, Partitioned::new_singleton(0, 2), Diff::ONE),
            (3, Partitioned::new_singleton(0, 3), Diff::ONE),
            (4, Partitioned::new_singleton(0, 4), Diff::ONE),
        ];

        let since = Antichain::from_elem(1500);

        // Compute reclock(remap, source)
        let as_of = Antichain::from_elem(IntoTime::minimum());
        let remap1 = remap.clone();
        let source_updates1 = source_updates.clone();
        let reclock_remap = harness(
            as_of,
            move |worker, mut bindings, (mut data, data_cap), reclocked| {
                for (from_ts, into_ts, diff) in remap1 {
                    bindings.update_at(from_ts, into_ts, diff);
                }
                bindings.close();
                data.activate()
                    .session(&data_cap)
                    .give_iterator(source_updates1.iter().cloned());
                drop(data_cap);
                step(worker);
                reclocked.extract()
            },
        );
        // Compute compact(reclock(remap, source))
        let mut compact_reclock_remap = reclock_remap;
        for (t, updates) in compact_reclock_remap.iter_mut() {
            t.advance_by(since.borrow());
            for (_, t, _) in updates.iter_mut() {
                t.advance_by(since.borrow());
            }
        }

        // Compute compact(remap)
        let mut compact_remap = remap;
        for (_, t, _) in compact_remap.iter_mut() {
            t.advance_by(since.borrow());
        }
        consolidation::consolidate_updates(&mut compact_remap);
        // Compute reclock(compact(remap), source)
        let reclock_compact_remap = harness(
            since,
            move |worker, mut bindings, (mut data, data_cap), reclocked| {
                for (from_ts, into_ts, diff) in compact_remap {
                    bindings.update_at(from_ts, into_ts, diff);
                }
                bindings.close();
                data.activate()
                    .session(&data_cap)
                    .give_iterator(source_updates.iter().cloned());
                drop(data_cap);
                step(worker);
                reclocked.extract()
            },
        );

        let expected = vec![(
            1500,
            vec![
                (1, 1500, Diff::ONE),
                (2, 1500, Diff::ONE),
                (3, 2000, Diff::ONE),
                (4, 2000, Diff::ONE),
            ],
        )];
        assert_eq!(expected, reclock_compact_remap);
        assert_eq!(expected, compact_reclock_remap);
    }

    /// Returns the updates of a remap collection whose binding at time `i` is the `i`th of
    /// `frontiers`.
    fn remap_updates(
        frontiers: impl IntoIterator<Item = Antichain<FromTime>>,
    ) -> Vec<(FromTime, IntoTime, i64)> {
        let mut updates = vec![];
        let mut prev = Antichain::new();
        for (time, frontier) in frontiers.into_iter().enumerate() {
            let time = u64::cast_from(time);
            let mut batch: Vec<_> = prev.into_iter().map(|t| (t, time, -1)).collect();
            batch.extend(frontier.iter().cloned().map(|t| (t, time, 1)));
            consolidation::consolidate_updates(&mut batch);
            updates.extend(batch);
            prev = frontier;
        }
        updates
    }

    /// Returns the updates of a remap collection with partitions `0..parts`, all at offset 0 at
    /// time 0. Each time from 1 to `times` advances partition `time % parts` by one.
    fn round_robin_remap(parts: usize, times: u64) -> Vec<(FromTime, IntoTime, i64)> {
        let mut offsets = vec![0; parts];
        remap_updates((0..=times).map(|time| {
            if time > 0 {
                offsets[usize::cast_from(time) % parts] += 1;
            }
            partitioned_frontier(
                offsets
                    .iter()
                    .enumerate()
                    .map(|(pid, offset)| (u64::cast_from(pid), *offset)),
            )
        }))
    }

    /// The pids that [`growing_remap`] draws from are below this.
    const GROWING_PIDS: usize = 301;

    /// Returns the updates of a remap collection that holds partition 0 at time 0 and gains a
    /// partition at every fifth time up to `times`. The new pids are scattered, which makes each
    /// one split a range of the binding. Every other time advances one partition by one.
    fn growing_remap(times: u64) -> Vec<(FromTime, IntoTime, i64)> {
        let mut offsets = BTreeMap::from([(0, 0)]);
        remap_updates((0..=times).map(|time| {
            if time > 0 && time % 5 == 0 {
                // 101 is prime, which keeps the pids distinct for the first 100 additions.
                assert!(time / 5 < 101, "too many partitions");
                offsets.insert(time / 5 * 37 % 101 * 3, 0);
            } else if time > 0 {
                let index = usize::cast_from(time) % offsets.len();
                let offset = offsets.values_mut().nth(index).expect("index is in range");
                *offset += 1;
            }
            partitioned_frontier(offsets.iter().map(|(pid, offset)| (*pid, *offset)))
        }))
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn remap_trace_seek_matches_scan() {
        // With one partition each time has as many updates as its bindings hold elements, so
        // every binding is a keyframe. 70 partitions leave updates between keyframes.
        for (parts, times, between) in [(1, 1000, false), (70, 350, true)] {
            let updates = round_robin_remap(parts, times);
            let mut trace = RemapTrace::new(Antichain::from_elem(0));
            push_all(&mut trace, &updates);
            assert!(trace.keyframes.len() > 2, "{parts} partitions");
            assert_eq!(!trace.updates.is_empty(), between, "{parts} partitions");

            for since in [0, times / 3, times / 2] {
                trace.advance_since(Antichain::from_elem(since));
                assert_seeks_match_scan(&trace, &updates, parts, since);
            }
        }
    }

    /// Compaction moves the since past the last binding when the remap frontier advances without
    /// new bindings. The bindings pushed after that must build on the binding at the since.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn remap_trace_push_after_compaction() {
        // 70 partitions leave updates after the last keyframe for compaction to apply to it.
        for parts in [1, 70] {
            let mut updates = round_robin_remap(parts, 400);
            // Leave the times from 201 to 219 without bindings.
            for (_, into, _) in &mut updates {
                if *into > 200 {
                    *into += 19;
                }
            }
            let split = updates.partition_point(|(_, into, _)| *into <= 200);
            let mut trace = RemapTrace::new(Antichain::from_elem(0));
            push_all(&mut trace, &updates[..split]);
            trace.advance_since(Antichain::from_elem(210));
            push_all(&mut trace, &updates[split..]);
            assert_seeks_match_scan(&trace, &updates, parts, 210);
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn remap_trace_seek_matches_scan_with_growing_partitions() {
        let updates = growing_remap(350);
        let mut trace = RemapTrace::new(Antichain::from_elem(0));
        push_all(&mut trace, &updates);
        assert!(trace.keyframes.len() > 2);
        assert!(!trace.updates.is_empty());

        for since in [0, 120, 175] {
            trace.advance_since(Antichain::from_elem(since));
            assert_seeks_match_scan(&trace, &updates, GROWING_PIDS, since);
        }
    }

    /// Pushes `updates`, which must be in time order, into `trace` one time at a time.
    fn push_all(trace: &mut RemapTrace<FromTime, IntoTime>, updates: &[(FromTime, IntoTime, i64)]) {
        for tick in updates.chunk_by(|(_, into1, _), (_, into2, _)| into1 == into2) {
            let (_, into, _) = tick[0];
            trace.push(
                into,
                tick.iter().map(|(from, _, diff)| (from.clone(), *diff)),
            );
        }
    }

    /// Asserts that seeking `trace`, whose since is `since`, finds the same binding for every
    /// offset of partitions `0..parts` as scanning the bindings that `updates` accumulate to.
    fn assert_seeks_match_scan(
        trace: &RemapTrace<FromTime, IntoTime>,
        updates: &[(FromTime, IntoTime, i64)],
        parts: usize,
        since: IntoTime,
    ) {
        // The binding at every time, accumulated from the front.
        let mut accum = MutableAntichain::new();
        let mut all_bindings: Vec<(IntoTime, Antichain<FromTime>)> = vec![];
        for (from, into, diff) in updates {
            accum.update_iter([(from.clone(), *diff)]);
            match all_bindings.last_mut() {
                Some((time, binding)) if time == into => *binding = accum.frontier().to_owned(),
                _ => all_bindings.push((*into, accum.frontier().to_owned())),
            }
        }
        // The since takes the binding of the last time at or before it.
        let first = all_bindings.partition_point(|(time, _)| *time <= since) - 1;
        let mut bindings = all_bindings[first..].to_vec();
        bindings[0].0 = since;

        for pid in 0..u64::cast_from(parts) {
            // One cursor serves the queries of a partition. It only moves forward, so it skips the
            // queries that map to the binding it is already at.
            let mut cursor = trace.cursor();
            let mut cursor_time = None;
            for offset in 0.. {
                let query = Partitioned::new_singleton(pid, offset);
                let expected = bindings
                    .iter()
                    .find(|(_, binding)| !binding.less_equal(&query))
                    .map(|(time, _)| *time);
                let from_fresh = trace
                    .cursor()
                    .seek(|binding| !binding.less_equal(&query))
                    .map(|(time, _)| *time);
                assert_eq!(
                    from_fresh, expected,
                    "fresh cursor, since {since}, {query:?}"
                );
                if expected != cursor_time {
                    cursor_time = cursor
                        .seek(|binding| !binding.less_equal(&query))
                        .map(|(time, _)| *time);
                    assert_eq!(
                        cursor_time, expected,
                        "moved cursor, since {since}, {query:?}"
                    );
                }
                if expected.is_none() {
                    break;
                }
            }
        }
    }

    #[mz_ore::test]
    fn test_chainbatch_merge() {
        let a = ChainBatch::from_iter([('a', 0, 1)]);
        let b = ChainBatch::from_iter([('a', 0, -1), ('a', 1, 1)]);
        assert_eq!(a.merge_with(b), ChainBatch::from_iter([('a', 1, 1)]));
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn test_binding_consolidation() {
        use std::sync::atomic::Ordering;

        #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        struct Time(u64);

        // A counter of the number of active Time instances
        static INSTANCES: AtomicUsize = AtomicUsize::new(0);

        impl Time {
            fn new(time: u64) -> Self {
                INSTANCES.fetch_add(1, Ordering::Relaxed);
                Self(time)
            }
        }

        impl Clone for Time {
            fn clone(&self) -> Self {
                INSTANCES.fetch_add(1, Ordering::Relaxed);
                Self(self.0)
            }
        }

        impl Drop for Time {
            fn drop(&mut self) {
                INSTANCES.fetch_sub(1, Ordering::Relaxed);
            }
        }

        impl Timestamp for Time {
            type Summary = ();

            fn minimum() -> Self {
                Time::new(0)
            }
        }

        impl PathSummary<Time> for () {
            fn results_in(&self, src: &Time) -> Option<Time> {
                Some(src.clone())
            }

            fn followed_by(&self, _other: &()) -> Option<Self> {
                Some(())
            }
        }

        impl Refines<()> for Time {
            fn to_inner(_: ()) -> Self {
                Self::minimum()
            }
            fn to_outer(self) -> () {}
            fn summarize(_path: ()) {}
        }

        impl PartialOrder for Time {
            fn less_equal(&self, other: &Self) -> bool {
                self.0.less_equal(&other.0)
            }
        }

        let as_of = 1000;

        // Test that supplying a single big batch of unconsolidated bindings gets
        // consolidated after a single worker step.
        harness::<Time, u64, _, _>(
            Antichain::from_elem(as_of),
            move |worker, mut bindings, _, _| {
                step(worker);
                let instances_before = INSTANCES.load(Ordering::Relaxed);
                for ts in 0..as_of {
                    if ts > 0 {
                        bindings.update_at(Time::new(ts - 1), ts, Diff::MINUS_ONE);
                    }
                    bindings.update_at(Time::new(ts), ts, Diff::ONE);
                }
                bindings.advance_to(as_of);
                bindings.flush();
                step(worker);
                let instances_after = INSTANCES.load(Ordering::Relaxed);
                // The extra instances live in a ChangeBatch which considers compaction when more
                // than 32 elements are inside.
                assert!(instances_after - instances_before < 32);
            },
        );

        // Test that a slow feed of uncompacted bindings over multiple steps never leads to an
        // excessive number of bindings held in memory.
        harness::<Time, u64, _, _>(
            Antichain::from_elem(as_of),
            move |worker, mut bindings, _, _| {
                step(worker);
                let instances_before = INSTANCES.load(Ordering::Relaxed);
                for ts in 0..as_of {
                    if ts > 0 {
                        bindings.update_at(Time::new(ts - 1), ts, Diff::MINUS_ONE);
                    }
                    bindings.update_at(Time::new(ts), ts, Diff::ONE);
                    bindings.advance_to(ts + 1);
                    bindings.flush();
                    step(worker);
                    let instances_now = INSTANCES.load(Ordering::Relaxed);
                    // The extra instances live in a ChangeBatch which considers compaction when
                    // more than 32 elements are inside.
                    assert!(instances_now - instances_before < 32);
                }
            },
        );
    }

    #[cfg(feature = "count-allocations")]
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn test_shrinking() {
        let as_of = 1000_u64;

        // This workflow accumulates updates in remap_trace, advances the source frontier,
        // and validates that memory was reclaimed.  To avoid errant test failures due to
        // optimizations, this only validates that memory is reclaimed, not how much.
        harness::<FromTime, u64, _, _>(
            Antichain::from_elem(0),
            move |worker, mut bindings, (_data, mut data_cap), _| {
                let info1 = allocation_counter::measure(|| {
                    step(worker);
                    for ts in 0..as_of {
                        if ts > 0 {
                            bindings.update_at(
                                Partitioned::new_singleton(0, ts - 1),
                                ts,
                                Diff::MINUS_ONE,
                            );
                        }
                        bindings.update_at(Partitioned::new_singleton(0, ts), ts, Diff::ONE);
                        bindings.advance_to(ts + 1);
                        bindings.flush();
                        step(worker);
                    }
                });
                println!("info = {info1:?}");

                let info2 = allocation_counter::measure(|| {
                    data_cap.downgrade(&Partitioned::new_singleton(0, as_of));
                    step(worker);
                });
                println!("info = {info2:?}");
                assert!(info2.bytes_current < 0);
            },
        );
    }
}
