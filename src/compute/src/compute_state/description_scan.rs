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
use mz_compute_client::protocol::response::PeekError;
use mz_expr::SafeMfpPlan;
use mz_ore::soft_panic_or_log;
use mz_repr::fixed_length::ExtendDatums;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use timely::order::PartialOrder;

use crate::compute_state::description_iterator::{
    DescriptionIterator, DescriptionRange, DescriptionStep,
};
use crate::compute_state::error_scan::{ErrorFilter, ErrorScan, ErrorScanStep};
use crate::compute_state::peek_offload::{OffloadConfig, OffloadScan};
use crate::compute_state::peek_scan::entry_byte_len;
use crate::render::errors::DataflowErrorSer;

/// The outcome of a fueled [`DescriptionScan::step`].
#[derive(Clone, Debug, PartialEq)]
pub(super) enum DescriptionScanOutcome {
    /// The fuel ran out, or the buffered updates passed the cut and wait for
    /// [`DescriptionScan::take_batch`]. Stepping again resumes where this step stopped.
    Suspended,
    /// The scan is over. `Ok` leaves the remaining buffered updates to
    /// [`DescriptionScan::take_batch`]. `Err` carries why the description is poisoned, and the
    /// scan then holds no updates.
    Finished(Result<(), String>),
}

/// Scans the updates of one description, after an error walk over the same description found
/// it clean.
pub(super) struct DescriptionScan<Ok: Navigable, Err: Navigable> {
    target_id: GlobalId,
    /// The error walk, `None` once it has found the description clean.
    errors: Option<(ErrorScan<Err>, ErrorFilter)>,
    oks: DescriptionIterator<Ok>,
    buffer: Vec<(Row, Timestamp, Diff)>,
    /// The bytes accounted to `buffer`, see [`update_byte_len`].
    buffered_bytes: usize,
    cut_bytes: usize,
    ended: Option<Result<(), String>>,
}

/// The bytes one buffered update is accounted at, the peek's per-row accounting plus the time
/// and the diff that a peek answer does not carry.
fn update_byte_len(row: &Row) -> usize {
    entry_byte_len(row)
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
    /// The buffered updates are handed out once their accounted bytes exceed `cut_bytes`, so a
    /// hand-out can exceed the cut by the updates of one cursor position.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the batched subscribe sink is not wired up")
    )]
    pub(super) fn new(
        ok_batches: Vec<Ok>,
        err_batches: Vec<Err>,
        map_filter_project: SafeMfpPlan,
        range: DescriptionRange,
        cut_bytes: usize,
        target_id: GlobalId,
    ) -> Self {
        let errors = (
            ErrorScan::from_batches(err_batches),
            ErrorFilter::Description(range.clone()),
        );
        Self {
            target_id,
            errors: Some(errors),
            oks: DescriptionIterator::new(ok_batches, map_filter_project, range),
            buffer: Vec::new(),
            buffered_bytes: 0,
            cut_bytes,
            ended: None,
        }
    }

    /// Advances the scan until it ends, the buffered updates pass the cut, or `fuel` runs out,
    /// whichever comes first. Decrements `fuel` by the cursor positions visited, in either walk.
    ///
    /// A scan whose buffer has passed the cut makes no progress until
    /// [`DescriptionScan::take_batch`] takes it. Stepping a finished scan is a defect in the
    /// driver.
    pub(super) fn step(&mut self, fuel: &mut usize) -> DescriptionScanOutcome {
        if let Some(ended) = &self.ended {
            soft_panic_or_log!("description scan stepped after it ended");
            return DescriptionScanOutcome::Finished(ended.clone());
        }
        if self.past_cut() {
            return DescriptionScanOutcome::Suspended;
        }

        if let Some((errors, filter)) = &mut self.errors {
            match errors.step(filter, self.target_id, fuel) {
                ErrorScanStep::OutOfFuel => return DescriptionScanOutcome::Suspended,
                ErrorScanStep::Finished(Err(error)) => return self.fail(error),
                // Dropped here, so a scan in its ok walk stops pinning error batches.
                ErrorScanStep::Finished(Ok(_)) => self.errors = None,
            }
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
                DescriptionStep::OutOfFuel => return DescriptionScanOutcome::Suspended,
                DescriptionStep::Error(error) => return self.fail(error),
                DescriptionStep::Done => {
                    self.ended = Some(Ok(()));
                    return DescriptionScanOutcome::Finished(Ok(()));
                }
            }
        }
    }

    /// Takes the buffered updates, consolidated per `(time, row)`, once they have passed the cut
    /// or the scan has finished. Returns `None` before that, and when nothing remains.
    ///
    /// Equal `(time, row)` pairs that a cut separated land in different hand-outs.
    pub(super) fn take_batch(&mut self) -> Option<Vec<(Row, Timestamp, Diff)>> {
        if !self.past_cut() && self.ended.is_none() {
            return None;
        }
        self.buffered_bytes = 0;
        let mut batch = mem::take(&mut self.buffer);
        consolidate_updates(&mut batch);
        (!batch.is_empty()).then_some(batch)
    }

    fn past_cut(&self) -> bool {
        self.buffered_bytes > self.cut_bytes
    }

    /// Ends the scan with `error`, discarding what it has buffered.
    fn fail(&mut self, error: PeekError) -> DescriptionScanOutcome {
        self.errors = None;
        self.buffer = Vec::new();
        self.buffered_bytes = 0;
        let message = error.to_string();
        self.ended = Some(Err(message.clone()));
        DescriptionScanOutcome::Finished(Err(message))
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
    type Batch = Vec<(Row, Timestamp, Diff)>;
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
