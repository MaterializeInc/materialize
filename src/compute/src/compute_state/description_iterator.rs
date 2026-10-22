// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A fueled scan of held arrangement batches that emits the updates of one description.

use differential_dataflow::consolidation::consolidate;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::cursor::{CursorList, cursor_list};
use differential_dataflow::trace::{BatchReader, Cursor, Navigable};
use mz_expr::SafeMfpPlan;
use mz_repr::fixed_length::ExtendDatums;
use mz_repr::{DatumVec, Diff, Row, RowArena, Timestamp};
use timely::order::PartialOrder;
use timely::progress::Antichain;

use crate::compute_state::peek_result_iterator::evaluate_position;
use crate::render::errors::DataflowErrorSer;

/// The times a [`DescriptionIterator`] emits, and the time it emits each one at.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the batched subscribe sink is not wired up")
)]
#[derive(Clone, Debug)]
pub(super) enum DescriptionRange {
    /// The first description of a scan rendered at `as_of`. Emits every update with a time
    /// before `upper`, at that time advanced to `as_of`, rows and errors alike.
    First {
        as_of: Antichain<Timestamp>,
        upper: Antichain<Timestamp>,
    },
    /// A description after the first. Emits every update with `lower <= time < upper`, at its
    /// own time.
    Later {
        lower: Antichain<Timestamp>,
        upper: Antichain<Timestamp>,
    },
}

impl DescriptionRange {
    /// Returns the time an update at `time` is emitted at, or `None` if the range excludes it.
    pub(super) fn place(&self, mut time: Timestamp) -> Option<Timestamp> {
        match self {
            DescriptionRange::First { as_of, upper } => {
                if upper.less_equal(&time) {
                    return None;
                }
                time.advance_by(as_of.borrow());
                Some(time)
            }
            DescriptionRange::Later { lower, upper } => {
                (lower.less_equal(&time) && !upper.less_equal(&time)).then_some(time)
            }
        }
    }
}

/// The outcome of a single fueled [`DescriptionIterator::step`].
#[derive(Debug, PartialEq)]
pub(super) enum DescriptionStep {
    /// The updates of one cursor position, consolidated per time and never empty. Diffs may be
    /// negative.
    Updates(Vec<(Row, Timestamp, Diff)>),
    /// Evaluating the `map_filter_project` on one cursor position failed. The position's updates
    /// become updates of the error, consolidated per time and never empty. The scan continues.
    Errors(Vec<(DataflowErrorSer, Timestamp, Diff)>),
    /// Every position has been visited. Further steps also return `Done`, and cost no fuel.
    Done,
    /// The budget is spent. The scan resumes at the next unvisited position.
    OutOfFuel,
}

/// Scans a set of held batches and emits the updates that fall into one [`DescriptionRange`],
/// after applying a `map_filter_project`.
///
/// Updates of one cursor position are consolidated per time. Two positions that the
/// `map_filter_project` maps to the same row are emitted separately, so the output is not
/// consolidated across positions.
pub(super) struct DescriptionIterator<B: Navigable> {
    cursor: CursorList<B::Cursor>,
    storage: Vec<B>,
    map_filter_project: SafeMfpPlan,
    range: DescriptionRange,
    row_builder: Row,
    datum_vec: DatumVec,
    /// Scratch space for the in-range times of the current position.
    times: Vec<(Timestamp, Diff)>,
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the batched subscribe sink is not wired up")
)]
impl<B> DescriptionIterator<B>
where
    B: BatchReader<Time = Timestamp> + Navigable,
    for<'a> B::Cursor: Cursor<
            Key<'a>: ExtendDatums,
            Val<'a>: ExtendDatums,
            Time = Timestamp,
            DiffGat<'a> = &'a Diff,
        >,
{
    /// Builds a scan over `batches`, which must include every held batch that overlaps the
    /// range. See [`select_batches`].
    pub(super) fn new(
        batches: Vec<B>,
        map_filter_project: SafeMfpPlan,
        range: DescriptionRange,
    ) -> Self {
        let (cursor, storage) = cursor_list(batches);
        Self {
            cursor,
            storage,
            map_filter_project,
            range,
            row_builder: Row::default(),
            datum_vec: DatumVec::new(),
            times: Vec::new(),
        }
    }

    /// Advances until a cursor position yields updates or errors, the batches are exhausted, or
    /// `fuel` runs out. Charges one unit of `fuel` per cursor position visited,
    /// whether or not the position yields updates, so the total cost of a scan does not depend
    /// on how it is sliced.
    ///
    /// A zero budget makes no progress and returns [`DescriptionStep::OutOfFuel`].
    pub(super) fn step(&mut self, fuel: &mut usize) -> DescriptionStep {
        loop {
            // Validity checks run before the charge, so an exhausted scan costs nothing.
            if !self.cursor.key_valid(&self.storage) {
                return DescriptionStep::Done;
            }
            if !self.cursor.val_valid(&self.storage) {
                self.cursor.step_key(&self.storage);
                if !self.cursor.key_valid(&self.storage) {
                    return DescriptionStep::Done;
                }
            }

            if *fuel == 0 {
                return DescriptionStep::OutOfFuel;
            }
            *fuel -= 1;

            let step = self.extract_current_updates();
            self.cursor.step_val(&self.storage);
            if let Some(step) = step {
                return step;
            }
        }
    }

    /// Returns the in-range updates of the current position, consolidated per time, or the
    /// error evaluating the position raised at those times. Returns `None` if the position has
    /// no in-range update or the `map_filter_project` filters it out.
    fn extract_current_updates(&mut self) -> Option<DescriptionStep> {
        self.times.clear();
        let range = &self.range;
        let times = &mut self.times;
        self.cursor.map_times(&self.storage, |time, diff| {
            if let Some(time) = range.place(<B::Cursor as Cursor>::owned_time(time)) {
                times.push((time, *diff));
            }
        });
        consolidate(&mut self.times);
        // A position with no update in the range is not part of the description's collection,
        // so the `map_filter_project` must not run on it. An error it raised there would be an
        // update the description does not contain.
        if self.times.is_empty() {
            return None;
        }

        let arena = RowArena::new();
        let key = self.cursor.key(&self.storage);
        let val = self.cursor.val(&self.storage);
        let mut datums = self.datum_vec.borrow();
        let evaluated = evaluate_position(
            &key,
            &val,
            None::<&Row>,
            &self.map_filter_project,
            &arena,
            &mut datums,
            &mut self.row_builder,
        );
        match evaluated {
            Ok(Some(row)) => Some(DescriptionStep::Updates(
                self.times
                    .drain(..)
                    .map(|(time, diff)| (row.clone(), time, diff))
                    .collect(),
            )),
            Ok(None) => None,
            Err(error) => {
                let error = DataflowErrorSer::from(error);
                Some(DescriptionStep::Errors(
                    self.times
                        .drain(..)
                        .map(|(time, diff)| (error.clone(), time, diff))
                        .collect(),
                ))
            }
        }
    }
}

/// Returns the batches in `held` whose description overlaps `[lower, upper)`.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the batched subscribe sink is not wired up")
)]
pub(super) fn select_batches<B>(
    held: &[B],
    lower: &Antichain<Timestamp>,
    upper: &Antichain<Timestamp>,
) -> Vec<B>
where
    B: BatchReader<Time = Timestamp> + Clone,
{
    held.iter()
        .filter(|batch| {
            let description = batch.description();
            PartialOrder::less_than(description.lower(), upper)
                && PartialOrder::less_than(lower, description.upper())
        })
        .cloned()
        .collect()
}

#[cfg(test)]
pub(crate) mod tests;
