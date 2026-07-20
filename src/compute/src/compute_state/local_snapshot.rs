// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! An owned, `Send` point-in-time snapshot of a local arrangement.
//!
//! [`snapshot_local`] and [`LocalSnapshot::into_cursor`] back the offloaded fast-path index
//! peek's cursor walk (`crate::compute_state::IndexOffloadPeek::spawn_offloaded_walk`), which
//! runs on its own async task off the maintenance worker's thread. `#[allow(dead_code)]` remains
//! for `since`/`upper`, which today are exercised only by this module's `Send` assertion tests.
#![allow(dead_code)]

use differential_dataflow::trace::{Navigable, TraceReader};
use timely::order::PartialOrder;
use timely::progress::Antichain;

use crate::compute_state::peek_result_iterator::{TraceCursor, TraceStorage};

/// An owned, `Send` point-in-time snapshot of a local arrangement, taken from a trace's
/// `Arc`-backed batches via [`TraceReader::cursor_through`].
///
/// A cursor borrowed straight off a live [`TraceReader`] stays tied to that reader for its
/// whole walk; for the traces this crate maintains, the reader is `Rc`-based and so `!Send`.
/// This snapshot instead owns the covering batches outright (an owned [`CursorList`] over their
/// `Arc<Batch>` cursors), so it does not borrow from the trace at all and is `Send`. The
/// maintenance worker can keep merging and compacting the trace while the snapshot's cursor is
/// walked elsewhere: the owned `Arc` batches keep their contents alive and unchanged regardless
/// of what the trace does to its own bookkeeping afterwards, so there is no torn read.
///
/// [`CursorList`]: differential_dataflow::trace::cursor::CursorList
pub struct LocalSnapshot<Tr>
where
    Tr: TraceReader<Batch: Navigable>,
{
    cursor: TraceCursor<Tr>,
    storage: TraceStorage<Tr>,
    since: Antichain<Tr::Time>,
    upper: Antichain<Tr::Time>,
}

impl<Tr> LocalSnapshot<Tr>
where
    Tr: TraceReader<Batch: Navigable>,
{
    /// The trace's logical compaction frontier at the time the snapshot was taken.
    ///
    /// Times not beyond this frontier may have been coalesced together; see
    /// [`TraceReader::get_logical_compaction`].
    pub fn since(&self) -> &Antichain<Tr::Time> {
        &self.since
    }

    /// The frontier through which the snapshot's batches are known to be complete.
    ///
    /// This is the `as_of` passed to [`snapshot_local`]: the snapshot's cursor covers exactly
    /// the updates at times not beyond it.
    pub fn upper(&self) -> &Antichain<Tr::Time> {
        &self.upper
    }

    /// Consumes the snapshot, returning its owned cursor and backing storage.
    ///
    /// For building a [`PeekResultIterator`] directly over the snapshot's already-owned
    /// batches, instead of a cursor borrowed live from the trace.
    ///
    /// [`PeekResultIterator`]: crate::compute_state::peek_result_iterator::PeekResultIterator
    pub(crate) fn into_cursor(self) -> (TraceCursor<Tr>, TraceStorage<Tr>) {
        (self.cursor, self.storage)
    }
}

/// Errors that can occur when taking an owned snapshot of a local trace at a point in time.
#[derive(Debug, PartialEq, Eq)]
pub enum SnapshotError {
    /// The trace's logical compaction frontier has advanced past `as_of`, so the batches
    /// backing a snapshot at that time could no longer distinguish updates at or before it.
    CompactedPast,
    /// The trace could not produce a covering, batch-aligned cursor through `as_of`. Callers
    /// should only pass an `as_of` that is a frontier the trace has actually reached, e.g. one
    /// obtained from [`TraceReader::read_upper`].
    NotYetAvailable,
}

/// Takes an owned, `Send` snapshot of `trace`'s contents as of `as_of`.
///
/// `as_of` must be a frontier the trace has actually reached (for example one obtained from
/// [`TraceReader::read_upper`]); it is passed directly to [`TraceReader::cursor_through`], which
/// requires a batch-aligned upper and panics otherwise. The returned snapshot owns the
/// `Arc`-backed batches covering `as_of`, so it stays valid and `Send` independent of `trace`
/// and can be handed to another thread to walk.
pub fn snapshot_local<Tr>(
    trace: &mut Tr,
    as_of: &Antichain<Tr::Time>,
) -> Result<LocalSnapshot<Tr>, SnapshotError>
where
    Tr: TraceReader<Batch: Navigable>,
{
    let since = trace.get_logical_compaction().to_owned();
    if !PartialOrder::less_equal(&since, as_of) {
        return Err(SnapshotError::CompactedPast);
    }

    let (cursor, storage) = trace
        .cursor_through(as_of.borrow())
        .ok_or(SnapshotError::NotYetAvailable)?;

    Ok(LocalSnapshot {
        cursor,
        storage,
        since,
        upper: as_of.clone(),
    })
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use differential_dataflow::trace::{Builder, Cursor, Description, Trace};
    use mz_repr::{Datum, Diff, Row, Timestamp};
    use mz_row_spine::RowRowBuilder;
    use mz_timely_util::columnation::ColumnationStack;
    use static_assertions::assert_impl_all;
    use timely::PartialOrder;
    use timely::container::PushInto;
    use timely::dataflow::operators::generic::OperatorInfo;
    use timely::progress::{Antichain, Timestamp as _};

    use super::snapshot_local;
    use crate::typedefs::RowRowSpine;

    // Over the production Row/Timestamp/Diff layout. Proves that an owned snapshot cursor over
    // `Arc` batches is `Send`, and that a `PeekResultIterator` built over it is too, so the walk
    // can move to a thread other than the maintenance worker (task 1b).
    assert_impl_all!(super::LocalSnapshot<RowRowSpine<Timestamp, Diff>>: Send);
    assert_impl_all!(
        crate::compute_state::peek_result_iterator::PeekResultIterator<
            RowRowSpine<Timestamp, Diff>,
        >: Send
    );

    fn row(x: i64) -> Row {
        Row::pack_slice(&[Datum::Int64(x)])
    }

    /// Builds a one-batch `RowRowSpine` with two rows at different times, `[0, upper)`.
    fn spine_with_rows(
        upper: Timestamp,
        rows: Vec<((Row, Row), Timestamp, Diff)>,
    ) -> RowRowSpine<Timestamp, Diff> {
        let operator = OperatorInfo::new(0, 0, Rc::from(vec![0]));
        let mut trace: RowRowSpine<Timestamp, Diff> = Trace::new(operator, None, None);

        let lower = Antichain::from_elem(Timestamp::minimum());
        let upper = Antichain::from_elem(upper);
        let description = Description::new(
            lower,
            upper.clone(),
            Antichain::from_elem(Timestamp::minimum()),
        );

        let mut chunk = ColumnationStack::default();
        for row in rows {
            chunk.push_into(row);
        }
        let batch = RowRowBuilder::<Timestamp, Diff>::seal(&mut vec![chunk], description);
        Trace::insert(&mut trace, batch);

        trace
    }

    #[mz_ore::test]
    fn snapshot_reports_since_and_upper() {
        let mut trace = spine_with_rows(
            Timestamp::new(10),
            vec![((row(1), row(2)), Timestamp::new(1), Diff::ONE)],
        );
        let as_of = Antichain::from_elem(Timestamp::new(10));

        let snapshot = snapshot_local(&mut trace, &as_of).expect("snapshot should succeed");

        assert_eq!(snapshot.upper(), &as_of);
        assert_eq!(
            snapshot.since(),
            &Antichain::from_elem(Timestamp::minimum())
        );
    }

    /// A snapshot taken with `as_of` covering the whole batch owns updates at every time in the
    /// batch; filtering those updates down to a chosen time (as `PeekResultIterator` does via
    /// `map_times`) yields only the rows visible as of that time.
    #[mz_ore::test]
    fn snapshot_returns_expected_rows_at_chosen_time() {
        let mut trace = spine_with_rows(
            Timestamp::new(10),
            vec![
                ((row(1), row(2)), Timestamp::new(1), Diff::ONE),
                ((row(3), row(4)), Timestamp::new(5), Diff::ONE),
            ],
        );
        let as_of = Antichain::from_elem(Timestamp::new(10));

        let snapshot = snapshot_local(&mut trace, &as_of).expect("snapshot should succeed");
        let (mut cursor, storage) = snapshot.into_cursor();

        let peek_time = Timestamp::new(3);
        let mut visible = Vec::new();
        while cursor.key_valid(&storage) {
            while cursor.val_valid(&storage) {
                let key: Vec<Datum> = cursor.key(&storage).into_iter().collect();
                let val: Vec<Datum> = cursor.val(&storage).into_iter().collect();
                let mut copies = Diff::ZERO;
                cursor.map_times(&storage, |time, diff| {
                    if time.less_equal(&peek_time) {
                        copies += diff;
                    }
                });
                if !copies.is_zero() {
                    visible.push((key, val, copies));
                }
                cursor.step_val(&storage);
            }
            cursor.step_key(&storage);
        }

        assert_eq!(
            visible,
            vec![(vec![Datum::Int64(1)], vec![Datum::Int64(2)], Diff::ONE)]
        );
    }
}
