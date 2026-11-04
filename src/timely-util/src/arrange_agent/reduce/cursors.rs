// Copyright (c) 2015 Frank McSherry
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
// Portions of this file are derived from the cursor reduce tactic in
// differential-dataflow 0.25.1. The original source code was retrieved from:
//
//     https://github.com/TimelyDataflow/differential-dataflow/blob/6e53ac674feacac83b0c3c5d123bdd5db488ef9d/differential-dataflow/src/operators/reduce.rs
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

//! A verbatim copy of differential's private `mod cursors`, which holds [`CursorTactic`].
//!
//! NOTE: Differential's master-next branch exports this as
//! `differential_dataflow::operators::cursor::reduce::CursorTactic`, so this copy goes away with
//! the master-next port (MaterializeInc/materialize#38805). Upstream paths are the only change.

use super::*;

/// The conventional cursor-based [`ReduceTactic`].
pub struct CursorTactic<B1, B2, Bu, L, P>
where
    B1: BatchReader + Navigable,
    B2: BatchReader<Time = B1::Time> + Navigable,
    B1::Cursor: Cursor<Time = B1::Time>,
    for<'a> B2::Cursor:
        Cursor<Key<'a> = <B1::Cursor as Cursor>::Key<'a>, ValOwn: Data, Time = B1::Time>,
{
    logic: L,
    push: P,
    // Outstanding `(key, time)` synthetic interesting moments, sorted by `(key, time)`, and the
    // buffers into which we assemble the next round's moments.
    pending_keys: <B1::Cursor as Cursor>::KeyContainer,
    pending_time: <B1::Cursor as Cursor>::TimeContainer,
    next_pending_keys: <B1::Cursor as Cursor>::KeyContainer,
    next_pending_time: <B1::Cursor as Cursor>::TimeContainer,
    // Buffers reused across activations.
    interesting_times: Vec<B1::Time>,
    new_interesting_times: Vec<B1::Time>,
    // Output batches may need to be built piecemeal, and these temp storage help there.
    output_upper: Antichain<B1::Time>,
    output_lower: Antichain<B1::Time>,
    _marker: PhantomData<(B2, Bu)>,
}

impl<B1, B2, Bu, L, P> CursorTactic<B1, B2, Bu, L, P>
where
    B1: BatchReader + Navigable,
    B2: BatchReader<Time = B1::Time> + Navigable,
    B1::Cursor: Cursor<Time = B1::Time>,
    for<'a> B2::Cursor:
        Cursor<Key<'a> = <B1::Cursor as Cursor>::Key<'a>, ValOwn: Data, Time = B1::Time>,
{
    /// Construct a tactic that applies `logic` to each key and shapes output with `push`.
    pub fn new(logic: L, push: P) -> Self {
        CursorTactic {
            logic,
            push,
            pending_keys: <B1::Cursor as Cursor>::KeyContainer::with_capacity(0),
            pending_time: <B1::Cursor as Cursor>::TimeContainer::with_capacity(0),
            next_pending_keys: <B1::Cursor as Cursor>::KeyContainer::with_capacity(0),
            next_pending_time: <B1::Cursor as Cursor>::TimeContainer::with_capacity(0),
            interesting_times: Vec::new(),
            new_interesting_times: Vec::new(),
            output_upper: Antichain::from_elem(<B1::Time as Timestamp>::minimum()),
            output_lower: Antichain::from_elem(<B1::Time as Timestamp>::minimum()),
            _marker: PhantomData,
        }
    }
}

impl<B1, B2, Bu, L, P> ReduceTactic<B1, B2> for CursorTactic<B1, B2, Bu, L, P>
where
    B1: BatchReader + Navigable,
    B2: BatchReader<Time = B1::Time> + Navigable,
    B1::Cursor: Cursor<Time = B1::Time>,
    for<'a> B2::Cursor:
        Cursor<Key<'a> = <B1::Cursor as Cursor>::Key<'a>, ValOwn: Data, Time = B1::Time>,
    Bu: Builder<Time = B1::Time, Output = B2, Input: Default>,
    L: FnMut(
        <B1::Cursor as Cursor>::Key<'_>,
        &[(
            <B1::Cursor as Cursor>::Val<'_>,
            <B1::Cursor as Cursor>::Diff,
        )],
        &mut Vec<(<B2::Cursor as Cursor>::ValOwn, <B2::Cursor as Cursor>::Diff)>,
        &mut Vec<(<B2::Cursor as Cursor>::ValOwn, <B2::Cursor as Cursor>::Diff)>,
    ),
    P: FnMut(
        &mut Bu::Input,
        <B1::Cursor as Cursor>::Key<'_>,
        &mut Vec<(
            <B2::Cursor as Cursor>::ValOwn,
            B1::Time,
            <B2::Cursor as Cursor>::Diff,
        )>,
    ),
{
    fn retire(
        &mut self,
        source_batches: Vec<B1>,
        output_batches: Vec<B2>,
        input_batches: Vec<B1>,
        lower: &Antichain<B1::Time>,
        upper: &Antichain<B1::Time>,
        held: &Antichain<B1::Time>,
    ) -> (Vec<(B1::Time, B2)>, Antichain<B1::Time>) {
        let mut produced = Vec::new();

        // We have compute needs only if we hold a time in the interval [lower, upper); otherwise we
        // could not transmit outputs even if they were (incorrectly) non-zero, and we leave the held
        // times unchanged.
        if held.elements().iter().any(|time| !upper.less_equal(time)) {
            // cursors for navigating input, output, and new-batch updates.
            let (mut source_cursor, ref source_storage) = cursor_list(source_batches);
            let (mut output_cursor, ref output_storage) = cursor_list(output_batches);
            let (mut batch_cursor, ref batch_storage) = cursor_list(input_batches);

            // Prepare an output buffer and builder for each held time.
            // TODO: It would be better if all updates went into one batch, but timely dataflow prevents
            //       this as long as it requires that there is only one capability for each message.
            let mut buffers = Vec::<(
                B1::Time,
                Vec<(
                    <B2::Cursor as Cursor>::ValOwn,
                    B1::Time,
                    <B2::Cursor as Cursor>::Diff,
                )>,
            )>::new();
            let mut builders = Vec::new();
            for time in held.elements().iter() {
                buffers.push((time.clone(), Vec::new()));
                builders.push(Bu::new());
            }
            // Temporary staging for output building.
            let mut buffer = Bu::Input::default();

            // Reuseable state for performing the computation.
            let mut thinker = history_replay::HistoryReplayer::new();

            // March through the keys we must work on, merging `batch_cursor` and pending keys.
            // The interesting moments need to be in the interval to prompt work.
            let mut pending_pos = 0;
            while batch_cursor.key_valid(batch_storage) || pending_pos < self.pending_keys.len() {
                // Determine the next key we will work on; could be synthetic, could be from a batch.
                let key1 = self.pending_keys.get(pending_pos);
                let key2 = batch_cursor.get_key(batch_storage);
                let key = match (key1, key2) {
                    (Some(key1), Some(key2)) => ::std::cmp::min(key1, key2),
                    (Some(key1), None) => key1,
                    (None, Some(key2)) => key2,
                    (None, None) => unreachable!(),
                };

                // Populate `interesting_times` with interesting times not beyond `upper`.
                // TODO: This could just be `pending_time` and indexes within `lower .. upper`.
                let prior_pos = pending_pos;
                self.interesting_times.clear();
                while self.pending_keys.get(pending_pos) == Some(key) {
                    let owned_time =
                        <B1::Cursor as Cursor>::owned_time(self.pending_time.index(pending_pos));
                    if !upper.less_equal(&owned_time) {
                        self.interesting_times.push(owned_time);
                    }
                    pending_pos += 1;
                }

                // tidy up times, removing redundancy.
                sort_dedup(&mut self.interesting_times);

                // If there are new updates, or pending times, we must investigate!
                if batch_cursor.get_key(batch_storage) == Some(key)
                    || !self.interesting_times.is_empty()
                {
                    // do the per-key computation.
                    thinker.compute(
                        key,
                        (&mut source_cursor, source_storage),
                        (&mut output_cursor, output_storage),
                        (&mut batch_cursor, batch_storage),
                        &self.interesting_times,
                        &mut self.logic,
                        upper,
                        &mut buffers[..],
                        &mut self.new_interesting_times,
                    );

                    // Advance the cursor if this key, so that the loop's validity check registers the work as done.
                    if batch_cursor.get_key(batch_storage) == Some(key) {
                        batch_cursor.step_key(batch_storage);
                    }

                    // Merge novel pending times with any prior pending times we did not process.
                    // TODO: This could be a merge, not a sort_dedup, because both lists should be sorted.
                    for pos in prior_pos..pending_pos {
                        let owned_time =
                            <B1::Cursor as Cursor>::owned_time(self.pending_time.index(pos));
                        if upper.less_equal(&owned_time) {
                            self.new_interesting_times.push(owned_time);
                        }
                    }
                    sort_dedup(&mut self.new_interesting_times);
                    for time in self.new_interesting_times.drain(..) {
                        self.next_pending_keys.push_ref(key);
                        self.next_pending_time.push_own(&time);
                    }

                    // Sort each buffer by value and move into the corresponding builder.
                    // TODO: This makes assumptions about at least one of (i) the stability of `sort_by`,
                    //       (ii) that the buffers are time-ordered, and (iii) that the builders accept
                    //       arbitrarily ordered times.
                    for index in 0..buffers.len() {
                        buffers[index].1.sort_by(|x, y| x.0.cmp(&y.0));
                        (self.push)(&mut buffer, key, &mut buffers[index].1);
                        buffers[index].1.clear();
                        builders[index].push(&mut buffer);
                    }
                } else {
                    // copy over the pending key and times.
                    for pos in prior_pos..pending_pos {
                        self.next_pending_keys
                            .push_ref(self.pending_keys.index(pos));
                        self.next_pending_time
                            .push_ref(self.pending_time.index(pos));
                    }
                }
            }
            // Drop to avoid lifetime issues that would lock `pending_{keys, time}`.
            drop(thinker);

            // We start sealing output batches from the lower limit (previous upper limit).
            // In principle, we could update `lower` itself, and it should arrive at `upper` by the
            // end of the process.
            self.output_lower.clear();
            self.output_lower.extend(lower.borrow().iter().cloned());

            // build each batch (because only one capability per message).
            for (index, builder) in builders.drain(..).enumerate() {
                // Form the upper limit of the next batch, which includes all times greater
                // than the input batch, or the held times from i + 1 onward.
                self.output_upper.clear();
                self.output_upper.extend(upper.borrow().iter().cloned());
                for time in &held.elements()[index + 1..] {
                    self.output_upper.insert_ref(time);
                }

                if self.output_upper.borrow() != self.output_lower.borrow() {
                    let description = Description::new(
                        self.output_lower.clone(),
                        self.output_upper.clone(),
                        Antichain::from_elem(<B1::Time as Timestamp>::minimum()),
                    );
                    let batch = builder.done(description);

                    // hand the batch back to the driver to ship and commit, tagged with its time.
                    produced.push((held.elements()[index].clone(), batch));

                    self.output_lower.clear();
                    self.output_lower
                        .extend(self.output_upper.borrow().iter().cloned());
                }
            }
            // This should be true, as the final iteration introduces no held times, and
            // uses exactly `upper` to determine the upper bound. Good to check though.
            assert!(self.output_upper.borrow() == upper.borrow());

            // Refresh pending keys and times.
            self.pending_keys.clear();
            std::mem::swap(&mut self.next_pending_keys, &mut self.pending_keys);
            self.pending_time.clear();
            std::mem::swap(&mut self.next_pending_time, &mut self.pending_time);

            // Compute the new frontier of interesting times for the operator to hold.
            let mut frontier = Antichain::<B1::Time>::new();
            let mut owned_time = <B1::Time as Timestamp>::minimum();
            for pos in 0..self.pending_time.len() {
                <B1::Cursor as Cursor>::clone_time_onto(
                    self.pending_time.index(pos),
                    &mut owned_time,
                );
                frontier.insert_ref(&owned_time);
            }

            (produced, frontier)
        } else {
            // No work: leave the held times unchanged, so the driver's downgrade is a no-op.
            (produced, held.clone())
        }
    }
}

/// Implementation based on replaying historical and new updates together.
mod history_replay {

    use timely::progress::Antichain;

    use differential_dataflow::lattice::Lattice;
    use differential_dataflow::operators::ValueHistory;
    use differential_dataflow::trace::Cursor;

    use crate::arrange_agent::reduce::sort_dedup;

    // NOTE: Not upstream. `ValueHistory::replay_key` and the `HistoryReplay` it returns are private
    // to differential, so this rebuilds `replay_key` from the public `load_iter`. It returns the
    // history itself, whose public methods match `HistoryReplay`'s. `scratch` stages the key's edits
    // and keeps its capacity across keys, so loading a key allocates no more than upstream does. The
    // call sites differ from upstream in passing `scratch` and binding the returned `&mut` without
    // `mut`.
    trait ReplayKey<V, T, D> {
        fn replay_key<'a, 'history, C>(
            &'history mut self,
            scratch: &mut Vec<(V, T, D)>,
            cursor: &mut C,
            storage: &'a C::Storage,
            key: C::Key<'a>,
            meet: Option<&T>,
        ) -> &'history mut ValueHistory<V, T, D>
        where
            C: Cursor<Val<'a> = V, Time = T, Diff = D>;
    }

    impl<V: Copy + Ord, T: Ord + Clone + Lattice, D: differential_dataflow::difference::Semigroup>
        ReplayKey<V, T, D> for ValueHistory<V, T, D>
    {
        fn replay_key<'a, 'history, C>(
            &'history mut self,
            scratch: &mut Vec<(V, T, D)>,
            cursor: &mut C,
            storage: &'a C::Storage,
            key: C::Key<'a>,
            meet: Option<&T>,
        ) -> &'history mut ValueHistory<V, T, D>
        where
            C: Cursor<Val<'a> = V, Time = T, Diff = D>,
        {
            // As upstream's `Cursor::populate_key`: an absent key loads an empty history. The edits
            // arrive grouped by value, which is the order `load_iter` consolidates in.
            scratch.clear();
            cursor.seek_key(storage, key);
            if cursor.get_key(storage) == Some(key) {
                cursor.rewind_vals(storage);
                while let Some(val) = cursor.get_val(storage) {
                    cursor.map_times(storage, |time, diff| {
                        scratch.push((val, C::owned_time(time), C::owned_diff(diff)));
                    });
                    cursor.step_val(storage);
                }
            }
            self.load_iter(scratch.drain(..), meet);
            self
        }
    }

    /// The `HistoryReplayer` is a compute strategy based on moving through existing inputs, interesting times, etc in
    /// time order, maintaining consolidated representations of updates with respect to future interesting times.
    pub struct HistoryReplayer<V1, V2, V, T, D1, D2> {
        input_history: ValueHistory<V1, T, D1>,
        output_history: ValueHistory<V2, T, D2>,
        batch_history: ValueHistory<V1, T, D1>,
        input_buffer: Vec<(V1, D1)>,
        output_buffer: Vec<(V, D2)>,
        update_buffer: Vec<(V, D2)>,
        output_produced: Vec<((V, T), D2)>,
        synth_times: Vec<T>,
        meets: Vec<T>,
        times_current: Vec<T>,
        temporary: Vec<T>,
        // Not upstream: the `ReplayKey::replay_key` scratch of each history.
        input_edits: Vec<(V1, T, D1)>,
        output_edits: Vec<(V2, T, D2)>,
        batch_edits: Vec<(V1, T, D1)>,
    }

    impl<V1, V2, V, T, D1, D2> HistoryReplayer<V1, V2, V, T, D1, D2>
    where
        V1: Copy + Ord,
        V2: Copy + Ord,
        V: Clone + Ord,
        T: Ord + Clone + Lattice,
        D1: Clone + differential_dataflow::difference::Semigroup,
        D2: Clone + differential_dataflow::difference::Semigroup,
    {
        pub fn new() -> Self {
            HistoryReplayer {
                input_history: ValueHistory::new(),
                output_history: ValueHistory::new(),
                batch_history: ValueHistory::new(),
                input_buffer: Vec::new(),
                output_buffer: Vec::new(),
                update_buffer: Vec::new(),
                output_produced: Vec::new(),
                synth_times: Vec::new(),
                meets: Vec::new(),
                times_current: Vec::new(),
                temporary: Vec::new(),
                input_edits: Vec::new(),
                output_edits: Vec::new(),
                batch_edits: Vec::new(),
            }
        }
        #[inline(never)]
        pub fn compute<'a, K, C1, C2, C3, L>(
            &mut self,
            key: K,
            (source_cursor, source_storage): (&mut C1, &'a C1::Storage),
            (output_cursor, output_storage): (&mut C2, &'a C2::Storage),
            (batch_cursor, batch_storage): (&mut C3, &'a C3::Storage),
            times: &Vec<T>,
            logic: &mut L,
            upper_limit: &Antichain<T>,
            outputs: &mut [(T, Vec<(V, T, D2)>)],
            new_interesting: &mut Vec<T>,
        ) where
            C1: Cursor<Key<'a> = K, Val<'a> = V1, Time = T, Diff = D1>,
            C2: Cursor<Key<'a> = K, Val<'a> = V2, ValOwn = V, Time = T, Diff = D2>,
            C3: Cursor<Key<'a> = K, Val<'a> = V1, Time = T, Diff = D1>,
            K: Copy + Ord,
            L: FnMut(K, &[(V1, D1)], &mut Vec<(V, D2)>, &mut Vec<(V, D2)>),
        {
            // The work we need to perform is at times defined principally by the contents of `batch_cursor`
            // and `times`, respectively "new work we just received" and "old times we were warned about".
            //
            // Our first step is to identify these times, so that we can use them to restrict the amount of
            // information we need to recover from `input` and `output`; as all times of interest will have
            // some time from `batch_cursor` or `times`, we can compute their meet and advance all other
            // loaded times by performing the lattice `join` with this value.

            // Load the batch contents.
            let batch_replay = self.batch_history.replay_key(
                &mut self.batch_edits,
                batch_cursor,
                batch_storage,
                key,
                None,
            );

            // We determine the meet of times we must reconsider (those from `batch` and `times`). This meet
            // can be used to advance other historical times, which may consolidate their representation. As
            // a first step, we determine the meets of each *suffix* of `times`, which we will use as we play
            // history forward.

            self.meets.clear();
            self.meets.extend(times.iter().cloned());
            for index in (1..self.meets.len()).rev() {
                self.meets[index - 1] = self.meets[index - 1].meet(&self.meets[index]);
            }

            // Determine the meet of times in `batch` and `times`.
            let mut meet = None;
            update_meet(&mut meet, self.meets.get(0));
            update_meet(&mut meet, batch_replay.meet());

            // Having determined the meet, we can load the input and output histories, where we
            // advance all times by joining them with `meet`. The resulting times are more compact
            // and guaranteed to accumulate identically for times greater or equal to `meet`.

            // Load the input and output histories.
            let input_replay = self.input_history.replay_key(
                &mut self.input_edits,
                source_cursor,
                source_storage,
                key,
                meet.as_ref(),
            );
            let output_replay = self.output_history.replay_key(
                &mut self.output_edits,
                output_cursor,
                output_storage,
                key,
                meet.as_ref(),
            );

            self.synth_times.clear();
            self.times_current.clear();
            self.output_produced.clear();

            // The frontier of times we may still consider.
            // Derived from frontiers of our update histories, supplied times, and synthetic times.

            let mut times_slice = &times[..];
            let mut meets_slice = &self.meets[..];

            // We have candidate times from `batch` and `times`, as well as times identified by either
            // `input` or `output`. Finally, we may have synthetic times produced as the join of times
            // we consider in the course of evaluation. As long as any of these times exist, we need to
            // keep examining times.
            while let Some(next_time) = [
                batch_replay.time(),
                times_slice.first(),
                input_replay.time(),
                output_replay.time(),
                self.synth_times.last(),
            ]
            .into_iter()
            .flatten()
            .min()
            .cloned()
            {
                // Advance input and output history replayers. This marks applicable updates as active.
                input_replay.step_while_time_is(&next_time);
                output_replay.step_while_time_is(&next_time);

                // One of our goals is to determine if `next_time` is "interesting", meaning whether we
                // have any evidence that we should re-evaluate the user logic at this time. For a time
                // to be "interesting" it would need to be the join of times that include either a time
                // from `batch`, `times`, or `synth`. Neither `input` nor `output` times are sufficient.

                // Advance batch history, and capture whether an update exists at `next_time`.
                let mut interesting = batch_replay.step_while_time_is(&next_time);
                if interesting {
                    if let Some(meet) = meet.as_ref() {
                        batch_replay.advance_buffer_by(meet);
                    }
                }

                // advance both `synth_times` and `times_slice`, marking this time interesting if in either.
                while self.synth_times.last() == Some(&next_time) {
                    // We don't know enough about `next_time` to avoid putting it in to `times_current`.
                    // TODO: If we knew that the time derived from a canceled batch update, we could remove the time.
                    self.times_current.push(
                        self.synth_times
                            .pop()
                            .expect("failed to pop from synth_times"),
                    ); // <-- TODO: this could be a min-heap.
                    interesting = true;
                }
                while times_slice.first() == Some(&next_time) {
                    // We know nothing about why we were warned about `next_time`, and must include it to scare future times.
                    self.times_current.push(times_slice[0].clone());
                    times_slice = &times_slice[1..];
                    meets_slice = &meets_slice[1..];
                    interesting = true;
                }

                // Times could also be interesting if an interesting time is less than them, as they would join
                // and become the time itself. They may not equal the current time because whatever frontier we
                // are tracking may not have advanced far enough.
                // TODO: `batch_history` may or may not be super compact at this point, and so this check might
                //       yield false positives if not sufficiently compact. Maybe we should look into this and see.
                interesting = interesting
                    || batch_replay
                        .buffer()
                        .iter()
                        .any(|&((_, ref t), _)| t.less_equal(&next_time));
                interesting =
                    interesting || self.times_current.iter().any(|t| t.less_equal(&next_time));

                // We should only process times that are not in advance of `upper_limit`.
                //
                // We have no particular guarantee that known times will not be in advance of `upper_limit`.
                // We may have the guarantee that synthetic times will not be, as we test against the limit
                // before we add the time to `synth_times`.
                if !upper_limit.less_equal(&next_time) {
                    // DETERMINATION (times only). Determine synthetic interesting times.
                    //
                    // Synthetic interesting times are produced differently for interesting and uninteresting
                    // times. An uninteresting time must join with an interesting time to become interesting,
                    // which means joins with `self.batch_history` and  `self.times_current`. I think we can
                    // skip `self.synth_times` as we haven't gotten to them yet, but we will and they will be
                    // joined against everything.

                    // Any time, even uninteresting times, must be joined with the current accumulation of
                    // batch times as well as the current accumulation of `times_current`.
                    self.temporary.extend(
                        batch_replay
                            .buffer()
                            .iter()
                            .map(|((_, time), _)| time)
                            .filter(|time| !time.less_equal(&next_time))
                            .map(|time| time.join(&next_time)),
                    );
                    self.temporary.extend(
                        self.times_current
                            .iter()
                            .filter(|time| !time.less_equal(&next_time))
                            .map(|time| time.join(&next_time)),
                    );

                    // An interesting time additionally joins with `input` and `output` history and this round's
                    // produced output: it carries the seed, so those joins stay interesting (an uninteresting
                    // time does not, as `input`/`output` times are not themselves seeds). We advance the buffers
                    // by `meet` first, exactly as evaluation reads them below; by join preservation the advanced
                    // and unadvanced times spawn the same synthetics, so this matches the pre-split behavior.
                    if interesting {
                        if let Some(meet) = meet.as_ref() {
                            input_replay.advance_buffer_by(meet)
                        };
                        if let Some(meet) = meet.as_ref() {
                            output_replay.advance_buffer_by(meet)
                        };
                        self.temporary.extend(
                            input_replay
                                .buffer()
                                .iter()
                                .map(|((_, time), _)| time)
                                .filter(|time| !time.less_equal(&next_time))
                                .map(|time| time.join(&next_time)),
                        );
                        self.temporary.extend(
                            output_replay
                                .buffer()
                                .iter()
                                .map(|((_, time), _)| time)
                                .filter(|time| !time.less_equal(&next_time))
                                .map(|time| time.join(&next_time)),
                        );
                        self.temporary.extend(
                            self.output_produced
                                .iter()
                                .map(|((_, time), _)| time)
                                .filter(|time| !time.less_equal(&next_time))
                                .map(|time| time.join(&next_time)),
                        );
                    }
                    sort_dedup(&mut self.temporary);

                    // Introduce synthetic times, and re-organize if we add any.
                    let synth_len = self.synth_times.len();
                    for time in self.temporary.drain(..) {
                        // We can either service `join` now, or must delay for the future.
                        if upper_limit.less_equal(&time) {
                            debug_assert!(outputs.iter().any(|(t, _)| t.less_equal(&time)));
                            new_interesting.push(time);
                        } else {
                            self.synth_times.push(time);
                        }
                    }
                    if self.synth_times.len() > synth_len {
                        self.synth_times.sort_by(|x, y| y.cmp(x));
                        self.synth_times.dedup();
                    }

                    // EVALUATION (values only).
                    // We should re-evaluate the computation if this is an interesting time.
                    // If the time is uninteresting (and our logic is sound) it is not possible for there to be
                    // output produced. This sounds like a good test to have for debug builds!
                    if interesting {
                        // Assemble the input collection at `next_time`. (`self.input_buffer` cleared just after use).
                        // The buffers were advanced by `meet` in the determination step above.
                        debug_assert!(self.input_buffer.is_empty());
                        for ((value, time), diff) in input_replay.buffer().iter() {
                            if time.less_equal(&next_time) {
                                self.input_buffer.push((*value, diff.clone()));
                            }
                        }
                        for ((value, time), diff) in batch_replay.buffer().iter() {
                            if time.less_equal(&next_time) {
                                self.input_buffer.push((*value, diff.clone()));
                            }
                        }
                        differential_dataflow::consolidation::consolidate(&mut self.input_buffer);

                        // Assemble the output collection at `next_time`. (`self.output_buffer` cleared just after use).
                        for ((value, time), diff) in output_replay.buffer().iter() {
                            if time.less_equal(&next_time) {
                                self.output_buffer
                                    .push((C2::owned_val(*value), diff.clone()));
                            }
                        }
                        for ((value, time), diff) in self.output_produced.iter() {
                            if time.less_equal(&next_time) {
                                self.output_buffer.push(((*value).to_owned(), diff.clone()));
                            }
                        }
                        differential_dataflow::consolidation::consolidate(&mut self.output_buffer);

                        // Apply user logic if non-empty input or output and see what happens!
                        if !self.input_buffer.is_empty() || !self.output_buffer.is_empty() {
                            logic(
                                key,
                                &self.input_buffer[..],
                                &mut self.output_buffer,
                                &mut self.update_buffer,
                            );
                            self.input_buffer.clear();
                            self.output_buffer.clear();

                            // Having subtracted output updates from user output, consolidate the results to determine
                            // if there is anything worth reporting. Note: this also orders the results by value, so
                            // that could make the above merging plan even easier.
                            //
                            // Stash produced updates into both capability-indexed buffers and `output_produced`.
                            // The two locations are important, in that we will compact `output_produced` as we move
                            // through times, but we cannot compact the output buffers because we need their actual
                            // times.
                            differential_dataflow::consolidation::consolidate(
                                &mut self.update_buffer,
                            );
                            if !self.update_buffer.is_empty() {
                                // We *should* be able to find a capability for `next_time`. Any thing else would
                                // indicate a logical error somewhere along the way; either we release a capability
                                // we should have kept, or we have computed the output incorrectly (or both!)
                                let idx = outputs
                                    .iter()
                                    .rev()
                                    .position(|(time, _)| time.less_equal(&next_time));
                                let idx = outputs.len() - idx.expect("failed to find index") - 1;
                                for (val, diff) in self.update_buffer.drain(..) {
                                    self.output_produced
                                        .push(((val.clone(), next_time.clone()), diff.clone()));
                                    outputs[idx].1.push((val, next_time.clone(), diff));
                                }

                                // Advance times in `self.output_produced` and consolidate the representation.
                                // NOTE: We only do this when we add records; it could be that there are situations
                                //       where we want to consolidate even without changes (because an initially
                                //       large collection can now be collapsed).
                                if let Some(meet) = meet.as_ref() {
                                    for entry in &mut self.output_produced {
                                        (entry.0).1.join_assign(meet);
                                    }
                                }
                                differential_dataflow::consolidation::consolidate(
                                    &mut self.output_produced,
                                );
                            }
                        }
                    }
                } else if interesting {
                    // We cannot process `next_time` now, and must delay it.
                    //
                    // I think we are probably only here because of an uninteresting time declared interesting,
                    // as initial interesting times are filtered to be in interval, and synthetic times are also
                    // filtered before introducing them to `self.synth_times`.
                    new_interesting.push(next_time.clone());
                    debug_assert!(outputs.iter().any(|(t, _)| t.less_equal(&next_time)))
                }

                // Update `meet` to track the meet of each source of times.
                meet = None;
                update_meet(&mut meet, batch_replay.meet());
                update_meet(&mut meet, input_replay.meet());
                update_meet(&mut meet, output_replay.meet());
                for time in self.synth_times.iter() {
                    update_meet(&mut meet, Some(time));
                }
                update_meet(&mut meet, meets_slice.first());

                // Update `times_current` by the frontier.
                if let Some(meet) = meet.as_ref() {
                    for time in self.times_current.iter_mut() {
                        *time = time.join(meet);
                    }
                }

                sort_dedup(&mut self.times_current);
            }

            // Normalize the representation of `new_interesting`, deduplicating and ordering.
            sort_dedup(new_interesting);
        }
    }

    /// Updates an optional meet by an optional time.
    fn update_meet<T: Lattice + Clone>(meet: &mut Option<T>, other: Option<&T>) {
        if let Some(time) = other {
            if let Some(meet) = meet.as_mut() {
                meet.meet_assign(time);
            } else {
                *meet = Some(time.clone());
            }
        }
    }
}
