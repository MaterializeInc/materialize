// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Cutting a persist-backed subscribe's event stream into the batches a
//! subscribe sink produces, so a `SUBSCRIBE` served from a shard, in
//! `environmentd` or on a replica, is indistinguishable to its consumer from
//! one served by a dataflow.

use mz_expr::{ColumnOrder, compare_columns};
use mz_ore::iter::consolidate_update_iter;
use mz_repr::{DatumVec, Diff, Row, Timestamp, UpdateCollection};
use mz_storage_client::storage_collections::Update;
use timely::PartialOrder;
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;

use crate::protocol::response::SubscribeBatch;

/// Cuts a collection's snapshot-then-listen stream into [`SubscribeBatch`]es
/// with the contents and boundaries the compute subscribe sink produces
/// (`mz_compute::sink::subscribe`): one batch per frontier advance at or past
/// `as_of`, holding the consolidated updates below the new frontier in the
/// subscribe's row order, and a closing batch at the empty frontier once
/// `up_to` is reached.
#[derive(Debug)]
pub struct PersistTailBatcher {
    as_of: Antichain<Timestamp>,
    up_to: Antichain<Timestamp>,
    with_snapshot: bool,
    /// The subscribe's row order within a timestamp, see
    /// `SubscribeOutput::row_order`.
    order: Vec<ColumnOrder>,
    /// A batch whose rows exceed this many bytes is replaced by an error.
    max_result_size: usize,
    /// Upper of the last batch produced, so the lower of the next one.
    prev_upper: Antichain<Timestamp>,
    /// Updates at or beyond `prev_upper`, not yet shipped.
    rows: Vec<(Row, Timestamp, Diff)>,
    errors: Vec<(String, Timestamp, Diff)>,
    /// Once an error is reported every later batch repeats it. Like the
    /// compute sink, the subscribe protocol cannot retract an error
    /// (database-issues#5182).
    poison: Option<String>,
    /// Whether the snapshot may be shipped in pieces, see
    /// [`Self::push_snapshot_chunk`].
    chunk_snapshot: bool,
    /// Set once a piece of the snapshot has been shipped. The frontier does
    /// not record this, so it is what makes a resume impossible.
    snapshot_shipped: bool,
    finished: bool,
}

impl PersistTailBatcher {
    /// `chunk_snapshot` lets the snapshot ship in pieces, which the caller
    /// allows only for an output whose rows are independent of one another
    /// within a timestamp, see [`Self::push_snapshot_chunk`].
    pub fn new(
        as_of: Timestamp,
        up_to: Option<Timestamp>,
        with_snapshot: bool,
        order: Vec<ColumnOrder>,
        max_result_size: usize,
        chunk_snapshot: bool,
    ) -> Self {
        Self {
            as_of: Antichain::from_elem(as_of),
            up_to: up_to.map(Antichain::from_elem).unwrap_or_default(),
            with_snapshot,
            order,
            max_result_size,
            prev_upper: Antichain::from_elem(TimelyTimestamp::minimum()),
            rows: Vec::new(),
            errors: Vec::new(),
            poison: None,
            chunk_snapshot,
            snapshot_shipped: false,
            finished: false,
        }
    }

    /// Buffers updates from the stream. The snapshot arrives at `as_of` and is
    /// dropped without `with_snapshot`. Nothing at or beyond `up_to` is ever
    /// emitted.
    pub fn push(&mut self, updates: &[Update]) {
        for (data, time, diff) in updates {
            if !self.should_emit(time) {
                continue;
            }
            let diff = Diff::from(*diff);
            match &data.0 {
                Ok(row) => self.rows.push((row.clone(), *time, diff)),
                Err(error) => self.errors.push((error.to_string(), *time, diff)),
            }
        }
    }

    fn should_emit(&self, time: &Timestamp) -> bool {
        let beyond_as_of = if self.with_snapshot {
            self.as_of.less_equal(time)
        } else {
            self.as_of.less_than(time)
        };
        beyond_as_of && !self.up_to.less_equal(time)
    }

    /// Buffers a consolidated piece of the snapshot and returns the batch that
    /// ships it, which is what keeps a large snapshot from being held whole.
    ///
    /// The batch claims no frontier advance: its bounds are both the `as_of`,
    /// so the formatter emits its rows and holds the progress message until
    /// the timestamp completes. The client therefore sees the same rows in the
    /// same places as it would from one batch, but earlier and in pieces.
    ///
    /// Shipping early gives up the frontier's account of what the subscriber
    /// has seen, see [`Self::resume_point`], and it costs the check against
    /// `max_result_size` across the whole snapshot, which now bounds a chunk.
    /// Without `chunk_snapshot` the updates are only buffered, like
    /// [`Self::push`].
    pub fn push_snapshot_chunk(&mut self, updates: &[Update]) -> Option<SubscribeBatch> {
        self.push(updates);
        if !self.chunk_snapshot || self.finished {
            return None;
        }
        // Only the snapshot's own timestamp is complete in a chunk. Nothing
        // later is expected here, since chunks arrive before any other event,
        // but a later update would have to wait for its frontier.
        let as_of = *self.as_of.as_option().expect("as_of is never empty");
        self.sort_rows();
        self.sort_errors();
        let split = self.rows.partition_point(|(_, t, _)| *t <= as_of);
        let rows = self.take_rows(split);
        let split = self.errors.partition_point(|(_, t, _)| *t <= as_of);
        let errors = self.take_errors(split);
        if rows.len() == 0 && errors.is_empty() && self.poison.is_none() {
            return None;
        }
        self.snapshot_shipped = true;
        let mut batch = SubscribeBatch {
            lower: self.as_of.clone(),
            upper: self.as_of.clone(),
            updates: self.updates_or_error(rows, errors),
        };
        batch.to_error_if_exceeds(self.max_result_size);
        Some(batch)
    }

    /// Records that every update below `upper` has been pushed and returns the
    /// batches this completes. Empty once the batcher is finished.
    pub fn progress(&mut self, upper: Antichain<Timestamp>) -> Vec<SubscribeBatch> {
        if self.finished {
            return Vec::new();
        }
        let mut batches = Vec::new();
        batches.extend(self.batch(upper.clone()));
        if PartialOrder::less_equal(&self.up_to, &upper) {
            self.finished = true;
            batches.extend(self.batch(Antichain::new()));
        }
        batches
    }

    /// Ends the subscribe early, so a client sees an end instead of a stall
    /// when the stream stops before the collection closes.
    pub fn close(&mut self) -> Option<SubscribeBatch> {
        if self.finished {
            return None;
        }
        self.finished = true;
        self.batch(Antichain::new())
    }

    /// Where to resume reading after the stream was cut off: the `as_of` and
    /// whether a snapshot is needed. Everything below `prev_upper` has been
    /// shipped, so a new stream at `prev_upper - 1` without a snapshot emits
    /// exactly the updates from `prev_upper` on. Before the first batch the
    /// subscribe starts over, snapshot included.
    ///
    /// `None` once part of the snapshot has shipped but its timestamp has not
    /// completed: the frontier does not say how much of it went out, and
    /// reading it again would repeat rows the client already has.
    ///
    /// Updates pushed since the last batch are dropped: the new stream emits
    /// them again.
    pub fn resume_point(&mut self) -> Option<(Timestamp, bool)> {
        self.rows.clear();
        self.errors.clear();
        match self.prev_upper.as_option() {
            Some(upper) if *upper != Timestamp::minimum() => Some((
                upper
                    .checked_sub(1)
                    .expect("a shipped upper is past the minimum"),
                false,
            )),
            _ if self.snapshot_shipped => None,
            _ => Some((
                *self.as_of.as_option().expect("as_of is never empty"),
                self.with_snapshot,
            )),
        }
    }

    /// The frontier everything shipped so far lies below.
    pub fn frontier(&self) -> &Antichain<Timestamp> {
        &self.prev_upper
    }

    fn batch(&mut self, upper: Antichain<Timestamp>) -> Option<SubscribeBatch> {
        // Like the compute sink: no batch before the frontier reaches `as_of`,
        // and none when the frontier did not move.
        if !PartialOrder::less_equal(&self.as_of, &upper) || upper == self.prev_upper {
            return None;
        }

        let rows = self.take_rows_below(&upper);
        let errors = self.take_errors_below(&upper);
        let updates = self.updates_or_error(rows, errors);

        let mut batch = SubscribeBatch {
            lower: std::mem::replace(&mut self.prev_upper, upper.clone()),
            upper,
            updates,
        };
        batch.to_error_if_exceeds(self.max_result_size);
        Some(batch)
    }

    /// A batch's updates, or the error that replaces them. The first error a
    /// subscribe sees poisons every later batch.
    fn updates_or_error(
        &mut self,
        rows: UpdateCollection,
        errors: Vec<(String, Timestamp, Diff)>,
    ) -> Result<Vec<UpdateCollection>, String> {
        match (&self.poison, errors.first()) {
            (Some(error), _) => Err(error.clone()),
            (None, Some((error, _, _))) => {
                self.poison = Some(error.clone());
                Err(error.clone())
            }
            (None, None) => Ok(vec![rows]),
        }
    }

    /// Removes and returns the rows below `upper`, sorted by time and row order
    /// and consolidated, as `SubscribeFormatter::format_batch` expects.
    fn take_rows_below(&mut self, upper: &Antichain<Timestamp>) -> UpdateCollection {
        self.sort_rows();
        let split = self.rows.partition_point(|(_, t, _)| !upper.less_equal(t));
        self.take_rows(split)
    }

    /// Removes and returns the first `split` rows. The caller sorts first with
    /// [`Self::sort_rows`], which both places the split and puts the rows in
    /// the order the formatter expects.
    fn take_rows(&mut self, split: usize) -> UpdateCollection {
        let shipped = self.rows.drain(..split).collect::<Vec<_>>();
        let byte_len = shipped.iter().map(|(row, _, _)| row.byte_len()).sum();
        let mut builder = UpdateCollection::builder(byte_len, shipped.len());
        let updates = shipped.iter().map(|(row, t, d)| (row.as_row_ref(), *t, *d));
        for (row, time, diff) in consolidate_update_iter(updates) {
            builder.push((row, &time, diff));
        }
        builder.build()
    }

    fn sort_rows(&mut self) {
        let order = self.order.as_slice();
        let mut left_datums = DatumVec::new();
        let mut right_datums = DatumVec::new();
        self.rows.sort_unstable_by(|(r0, t0, _), (r1, t1, _)| {
            t0.cmp(t1).then_with(|| {
                let left = left_datums.borrow_with(r0);
                let right = right_datums.borrow_with(r1);
                compare_columns(order, &left, &right, || r0.cmp(r1))
            })
        });
    }

    fn take_errors_below(
        &mut self,
        upper: &Antichain<Timestamp>,
    ) -> Vec<(String, Timestamp, Diff)> {
        self.sort_errors();
        let split = self
            .errors
            .partition_point(|(_, t, _)| !upper.less_equal(t));
        self.take_errors(split)
    }

    /// Removes and returns the first `split` errors, consolidated. The caller
    /// sorts first with [`Self::sort_errors`].
    fn take_errors(&mut self, split: usize) -> Vec<(String, Timestamp, Diff)> {
        consolidate_update_iter(self.errors.drain(..split)).collect()
    }

    fn sort_errors(&mut self) {
        self.errors
            .sort_unstable_by(|(e0, t0, _), (e1, t1, _)| t0.cmp(t1).then_with(|| e0.cmp(e1)));
    }
}

#[cfg(test)]
mod tests {
    use mz_repr::Datum;
    use mz_storage_types::sources::SourceData;

    use super::*;

    fn row(i: i64) -> SourceData {
        SourceData(Ok(Row::pack_slice(&[Datum::Int64(i)])))
    }

    fn rows(batch: &SubscribeBatch) -> Vec<(i64, u64, i64)> {
        batch
            .updates
            .as_ref()
            .expect("no error")
            .iter()
            .flat_map(|updates| {
                updates
                    .iter()
                    .map(|(row, time, diff)| {
                        (
                            row.unpack_first().unwrap_int64(),
                            u64::from(*time),
                            diff.into_inner(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// The snapshot and later updates come out consolidated, at the frontier
    /// advances a compute subscribe sink would report them at, and the
    /// batches chain through their lower and upper frontiers.
    #[mz_ore::test]
    fn batches_follow_progress() {
        let mut batcher =
            PersistTailBatcher::new(Timestamp::new(10), None, true, vec![], usize::MAX, false);
        // Snapshot at 10, with a duplicate to consolidate and a listen update
        // at 12 that is not yet complete at frontier 12.
        batcher.push(&[
            (row(2), Timestamp::new(10), 1),
            (row(1), Timestamp::new(10), 1),
            (row(2), Timestamp::new(10), 1),
            (row(3), Timestamp::new(12), 1),
        ]);

        assert!(
            batcher
                .progress(Antichain::from_elem(Timestamp::new(9)))
                .is_empty()
        );

        let batches = batcher.progress(Antichain::from_elem(Timestamp::new(12)));
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].lower, Antichain::from_elem(Timestamp::minimum()));
        assert_eq!(batches[0].upper, Antichain::from_elem(Timestamp::new(12)));
        assert_eq!(rows(&batches[0]), vec![(1, 10, 1), (2, 10, 2)]);

        let batches = batcher.progress(Antichain::from_elem(Timestamp::new(13)));
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].lower, Antichain::from_elem(Timestamp::new(12)));
        assert_eq!(rows(&batches[0]), vec![(3, 12, 1)]);
    }

    /// Without a snapshot the updates at `as_of` are dropped, and reaching
    /// `up_to` closes the stream with a batch at the empty frontier that
    /// excludes updates at or beyond `up_to`.
    #[mz_ore::test]
    fn no_snapshot_and_up_to() {
        let mut batcher = PersistTailBatcher::new(
            Timestamp::new(10),
            Some(Timestamp::new(12)),
            false,
            vec![],
            usize::MAX,
            false,
        );
        batcher.push(&[
            (row(1), Timestamp::new(10), 1),
            (row(2), Timestamp::new(11), 1),
            (row(3), Timestamp::new(12), 1),
        ]);
        let batches = batcher.progress(Antichain::from_elem(Timestamp::new(13)));
        assert_eq!(batches.len(), 2);
        assert_eq!(rows(&batches[0]), vec![(2, 11, 1)]);
        assert!(batches[1].upper.is_empty());
        assert!(rows(&batches[1]).is_empty());
        assert!(
            batcher
                .progress(Antichain::from_elem(Timestamp::new(14)))
                .is_empty()
        );
    }

    /// An error in the collection poisons every batch from the one that
    /// reports it onwards.
    #[mz_ore::test]
    fn errors_poison() {
        let mut batcher =
            PersistTailBatcher::new(Timestamp::new(10), None, true, vec![], usize::MAX, false);
        let error = SourceData(Err(mz_storage_types::errors::DataflowError::from(
            mz_expr::EvalError::DivisionByZero,
        )));
        batcher.push(&[
            (row(1), Timestamp::new(10), 1),
            (error, Timestamp::new(11), 1),
        ]);

        let batches = batcher.progress(Antichain::from_elem(Timestamp::new(11)));
        assert_eq!(rows(&batches[0]), vec![(1, 10, 1)]);

        let batches = batcher.progress(Antichain::from_elem(Timestamp::new(12)));
        assert!(batches[0].updates.is_err());
        let batches = batcher.progress(Antichain::from_elem(Timestamp::new(13)));
        assert!(batches[0].updates.is_err());
    }

    /// Before any batch shipped, resuming starts over with the snapshot. After
    /// one, it resumes one below the shipped frontier without a snapshot, and
    /// unshipped updates are dropped so the new stream can deliver them.
    #[mz_ore::test]
    fn resume_point() {
        let mut batcher =
            PersistTailBatcher::new(Timestamp::new(10), None, true, vec![], usize::MAX, false);
        batcher.push(&[(row(1), Timestamp::new(10), 1)]);
        assert_eq!(batcher.resume_point(), Some((Timestamp::new(10), true)));
        assert!(batcher.rows.is_empty());

        batcher.push(&[(row(1), Timestamp::new(10), 1)]);
        let batches = batcher.progress(Antichain::from_elem(Timestamp::new(12)));
        assert_eq!(rows(&batches[0]), vec![(1, 10, 1)]);
        batcher.push(&[(row(2), Timestamp::new(12), 1)]);
        assert_eq!(batcher.resume_point(), Some((Timestamp::new(11), false)));
        assert!(batcher.rows.is_empty());
    }

    /// Chunks of the snapshot ship as they arrive, each as its own batch at
    /// the `as_of`, and the timestamp's progress waits for the frontier.
    #[mz_ore::test]
    fn snapshot_ships_in_chunks() {
        let as_of = Timestamp::new(10);
        let mut batcher = PersistTailBatcher::new(as_of, None, true, vec![], usize::MAX, true);
        let first = batcher
            .push_snapshot_chunk(&[(row(1), as_of, 1), (row(2), as_of, 1)])
            .expect("chunk ships");
        assert_eq!(rows(&first), vec![(1, 10, 1), (2, 10, 1)]);
        assert_eq!(first.lower, Antichain::from_elem(as_of));
        assert_eq!(first.upper, Antichain::from_elem(as_of));

        let second = batcher
            .push_snapshot_chunk(&[(row(3), as_of, 1)])
            .expect("chunk ships");
        assert_eq!(rows(&second), vec![(3, 10, 1)]);

        // The listen updates that follow still wait for the frontier.
        batcher.push(&[(row(4), Timestamp::new(11), 1)]);
        let batches = batcher.progress(Antichain::from_elem(Timestamp::new(11)));
        assert_eq!(batches.len(), 1);
        assert!(rows(&batches[0]).is_empty());
        assert_eq!(batches[0].upper, Antichain::from_elem(Timestamp::new(11)));
        let batches = batcher.progress(Antichain::from_elem(Timestamp::new(12)));
        assert_eq!(rows(&batches[0]), vec![(4, 11, 1)]);
    }

    /// An output that orders or groups a timestamp needs all of it, so its
    /// chunks are only buffered and ship with the timestamp.
    #[mz_ore::test]
    fn ordered_output_holds_the_snapshot() {
        let as_of = Timestamp::new(10);
        let mut batcher = PersistTailBatcher::new(as_of, None, true, vec![], usize::MAX, false);
        assert!(batcher.push_snapshot_chunk(&[(row(2), as_of, 1)]).is_none());
        assert!(batcher.push_snapshot_chunk(&[(row(1), as_of, 1)]).is_none());
        let batches = batcher.progress(Antichain::from_elem(Timestamp::new(11)));
        assert_eq!(rows(&batches[0]), vec![(1, 10, 1), (2, 10, 1)]);
    }

    /// Once part of the snapshot is out there is nothing to resume from: the
    /// frontier never advanced, and reading again would repeat those rows.
    #[mz_ore::test]
    fn resume_point_after_a_shipped_chunk() {
        let as_of = Timestamp::new(10);
        let mut batcher = PersistTailBatcher::new(as_of, None, true, vec![], usize::MAX, true);
        assert!(batcher.push_snapshot_chunk(&[(row(1), as_of, 1)]).is_some());
        assert_eq!(batcher.resume_point(), None);

        // Once the snapshot's timestamp completes the frontier covers it again.
        let _ = batcher.progress(Antichain::from_elem(Timestamp::new(11)));
        assert_eq!(batcher.resume_point(), Some((Timestamp::new(10), false)));
    }
}
