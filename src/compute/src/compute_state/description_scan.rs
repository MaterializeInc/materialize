// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A fueled scan of one description over held ok and error batches, as one suspendable object.
//!
//! A [`DescriptionScan`] walks the error batches first and the ok batches after, spending a
//! single budget across both, the order [`PeekScan`](super::peek_scan::PeekScan) uses. It
//! performs no IO and never awaits, so it runs on the worker or on the offload driver alike.

use std::mem;

use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::trace::{BatchReader, Cursor, Navigable};
use mz_expr::SafeMfpPlan;
use mz_ore::soft_panic_or_log;
use mz_repr::fixed_length::ExtendDatums;
use mz_repr::{Diff, Row, Timestamp};
use timely::order::PartialOrder;

use crate::compute_state::description_iterator::{
    DescriptionIterator, DescriptionRange, DescriptionStep,
};
use crate::compute_state::error_scan::ErrorScan;
use crate::compute_state::peek_offload::{OffloadConfig, OffloadScan};
use crate::compute_state::peek_scan::entry_byte_len;
use crate::render::errors::DataflowErrorSer;

/// The outcome of a fueled [`DescriptionScan::step`].
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum DescriptionScanOutcome {
    /// The fuel ran out, or the buffered updates passed the cut and wait for
    /// [`DescriptionScan::take_batch`]. Stepping again resumes where this step stopped.
    Suspended,
    /// The scan is over, and leaves the remaining buffered updates to
    /// [`DescriptionScan::take_batch`].
    Finished,
}

/// Updates of one description that a [`DescriptionScan`] hands out.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct DescriptionUpdates {
    /// Row updates.
    pub(crate) rows: Vec<(Row, Timestamp, Diff)>,
    /// Error updates, of the error collection and of the `map_filter_project`.
    pub(crate) errors: Vec<(DataflowErrorSer, Timestamp, Diff)>,
}

impl DescriptionUpdates {
    pub(crate) fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.errors.is_empty()
    }

    /// Consolidates the rows per `(time, row)` and the errors per `(time, error)`.
    pub(crate) fn consolidate(&mut self) {
        consolidate_updates(&mut self.rows);
        consolidate_updates(&mut self.errors);
    }

    #[expect(dead_code, reason = "the batched subscribe sink is not wired up")]
    pub(crate) fn extend(&mut self, other: DescriptionUpdates) {
        self.rows.extend(other.rows);
        self.errors.extend(other.errors);
    }

    /// The bytes the updates are accounted at, see [`update_byte_len`] and
    /// [`error_update_byte_len`].
    #[expect(dead_code, reason = "the batched subscribe sink is not wired up")]
    pub(crate) fn byte_len(&self) -> usize {
        let rows = self
            .rows
            .iter()
            .map(|(row, _, _)| update_byte_len(row))
            .fold(0, usize::saturating_add);
        self.errors
            .iter()
            .map(|(error, _, _)| error_update_byte_len(error))
            .fold(rows, usize::saturating_add)
    }
}

/// Scans the updates of one description, first its error updates and then its row updates.
///
/// Row updates are handed out as they pass the cut. Error updates are held until the scan
/// finishes and handed out with its last hand-out, so that they are consolidated per
/// `(time, error)` across the whole description.
pub(crate) struct DescriptionScan<Ok: Navigable, Err: Navigable> {
    range: DescriptionRange,
    /// The error walk, `None` once it has ended.
    errors: Option<ErrorScan<Err>>,
    oks: DescriptionIterator<Ok>,
    buffer: Vec<(Row, Timestamp, Diff)>,
    /// The bytes accounted to `buffer`, see [`update_byte_len`].
    buffered_bytes: usize,
    /// The error updates found so far, handed out once the scan has ended.
    error_buffer: Vec<(DataflowErrorSer, Timestamp, Diff)>,
    cut_bytes: usize,
    ended: bool,
}

/// The bytes one buffered update is accounted at, the peek's per-row accounting plus the time
/// and the diff that a peek answer does not carry.
pub(crate) fn update_byte_len(row: &Row) -> usize {
    entry_byte_len(row)
        .saturating_add(size_of::<Timestamp>())
        .saturating_add(size_of::<Diff>())
}

/// The bytes one error update is accounted at, its serialized error plus the time and the diff.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the batched subscribe sink is not wired up")
)]
pub(crate) fn error_update_byte_len(error: &DataflowErrorSer) -> usize {
    error
        .byte_len()
        .saturating_add(size_of::<Timestamp>())
        .saturating_add(size_of::<Diff>())
}

impl<Ok, Err> DescriptionScan<Ok, Err>
where
    Ok: BatchReader<Time = Timestamp> + Navigable,
    for<'a> Ok::Cursor: Cursor<
            Key<'a>: ExtendDatums,
            Val<'a>: ExtendDatums,
            Time = Timestamp,
            DiffGat<'a> = &'a Diff,
        >,
    Err: BatchReader<Time = Timestamp> + Navigable,
    for<'a> Err::Cursor: Cursor<
            Key<'a> = &'a DataflowErrorSer,
            Time = Timestamp,
            TimeGat<'a>: PartialOrder<Timestamp>,
            DiffGat<'a> = &'a Diff,
        >,
{
    /// Opens a scan of `range` over `ok_batches` and `err_batches`, each of which must include
    /// every held batch that overlaps the range.
    ///
    /// The buffered row updates are handed out once their accounted bytes exceed `cut_bytes`, so
    /// a hand-out can exceed the cut by the updates of one cursor position.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the batched subscribe sink is not wired up")
    )]
    pub(crate) fn new(
        ok_batches: Vec<Ok>,
        err_batches: Vec<Err>,
        map_filter_project: SafeMfpPlan,
        range: DescriptionRange,
        cut_bytes: usize,
    ) -> Self {
        Self {
            range: range.clone(),
            errors: Some(ErrorScan::from_batches(err_batches)),
            oks: DescriptionIterator::new(ok_batches, map_filter_project, range),
            buffer: Vec::new(),
            buffered_bytes: 0,
            error_buffer: Vec::new(),
            cut_bytes,
            ended: false,
        }
    }

    /// Advances the scan until it ends, the buffered updates pass the cut, or `fuel` runs out,
    /// whichever comes first. Decrements `fuel` by the cursor positions visited, in either walk.
    ///
    /// A scan whose buffer has passed the cut makes no progress until
    /// [`DescriptionScan::take_batch`] takes it. Stepping a finished scan is a defect in the
    /// driver.
    pub(crate) fn step(&mut self, fuel: &mut usize) -> DescriptionScanOutcome {
        if self.ended {
            soft_panic_or_log!("description scan stepped after it ended");
            return DescriptionScanOutcome::Finished;
        }
        if self.past_cut() {
            return DescriptionScanOutcome::Suspended;
        }

        if let Some(errors) = &mut self.errors {
            if !errors.step_description(&self.range, fuel, &mut self.error_buffer) {
                return DescriptionScanOutcome::Suspended;
            }
            // Dropped here, so a scan in its ok walk stops pinning error batches.
            self.errors = None;
        }

        loop {
            match self.oks.step(fuel) {
                DescriptionStep::Updates(updates) => {
                    for update in updates {
                        self.buffered_bytes = self
                            .buffered_bytes
                            .saturating_add(update_byte_len(&update.0));
                        self.buffer.push(update);
                    }
                    if self.past_cut() {
                        return DescriptionScanOutcome::Suspended;
                    }
                }
                DescriptionStep::Errors(errors) => self.error_buffer.extend(errors),
                DescriptionStep::OutOfFuel => return DescriptionScanOutcome::Suspended,
                DescriptionStep::Done => {
                    self.ended = true;
                    return DescriptionScanOutcome::Finished;
                }
            }
        }
    }

    /// Takes the buffered updates, consolidated, once the row updates have passed the cut or the
    /// scan has finished. Returns `None` before that, and when nothing remains. The error updates
    /// are taken once the scan has finished.
    ///
    /// Equal `(time, row)` pairs that a cut separated land in different hand-outs.
    pub(crate) fn take_batch(&mut self) -> Option<DescriptionUpdates> {
        if !self.past_cut() && !self.ended {
            return None;
        }
        self.buffered_bytes = 0;
        let mut batch = DescriptionUpdates {
            rows: mem::take(&mut self.buffer),
            errors: if self.ended {
                mem::take(&mut self.error_buffer)
            } else {
                Vec::new()
            },
        };
        batch.consolidate();
        (!batch.is_empty()).then_some(batch)
    }

    fn past_cut(&self) -> bool {
        self.buffered_bytes > self.cut_bytes
    }
}

impl<Ok, Err> OffloadScan for DescriptionScan<Ok, Err>
where
    Ok: BatchReader<Time = Timestamp> + Navigable,
    for<'a> Ok::Cursor: Cursor<
            Key<'a>: ExtendDatums,
            Val<'a>: ExtendDatums,
            Time = Timestamp,
            DiffGat<'a> = &'a Diff,
        >,
    Err: BatchReader<Time = Timestamp> + Navigable,
    for<'a> Err::Cursor: Cursor<
            Key<'a> = &'a DataflowErrorSer,
            Time = Timestamp,
            TimeGat<'a>: PartialOrder<Timestamp>,
            DiffGat<'a> = &'a Diff,
        >,
    Self: Send + 'static,
{
    type Batch = DescriptionUpdates;
    type Outcome = DescriptionScanOutcome;

    fn step(&mut self, _config: &OffloadConfig, fuel: &mut usize) -> DescriptionScanOutcome {
        DescriptionScan::step(self, fuel)
    }

    fn suspended(outcome: &DescriptionScanOutcome) -> bool {
        matches!(outcome, DescriptionScanOutcome::Suspended)
    }

    fn take_batch(&mut self) -> Option<Self::Batch> {
        DescriptionScan::take_batch(self)
    }
}

#[cfg(test)]
mod tests;
