// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Coalescing of index peeks that are pending against the same arrangement.
//!
//! When multiple index peeks are pending against the same index at the same
//! timestamp, fulfilling each one independently walks the arrangement once per
//! peek. Each walk pays for cursor setup, key/value stepping, and key/value
//! datum decoding. This module amortizes that cost by performing a single
//! arrangement walk per group and demultiplexing each visited row into the
//! individual peeks (applying each peek's own map-filter-project and finishing).
//!
//! The shared, per-row work is: cursor stepping, key/value datum extraction, and
//! the `map_times` accumulation that yields the multiplicity at the read
//! timestamp. The per-peek work is: evaluating that peek's `MapFilterProject`
//! and accumulating into that peek's finishing. Because all peeks in a group
//! read at the same timestamp, the multiplicity and the error-trace result are
//! identical across the group and are computed once.

use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use differential_dataflow::trace::cursor::{BatchCursor, CursorList};
use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::{Cursor, Navigable, TraceReader};
use mz_expr::row::RowCollection;
use mz_expr::{RowComparator, RowSetFinishing, SafeMfpPlan};
use mz_ore::cast::CastFrom;
use mz_ore::result::ResultExt;
use mz_repr::fixed_length::ExtendDatums;
use mz_repr::{DatumVec, Diff, GlobalId, Row, RowArena, Timestamp};
use timely::order::PartialOrder;

use bytesize::ByteSize;
use mz_compute_client::protocol::response::PeekResponse;

/// The merged cursor a [`TraceReader::cursor`] hands out over all of a trace's
/// batches.
type TraceCursor<Tr> = CursorList<BatchCursor<Tr>>;
/// Backing storage for a [`TraceCursor`]: the batches the cursor borrows from.
type TraceStorage<Tr> = Vec<<Tr as TraceReader>::Batch>;

/// Accumulates rows for a single peek and applies its [`RowSetFinishing`].
///
/// This is the per-peek half of peek fulfillment, shared between the single-peek
/// path and the coalesced path so that finishing and thinning cannot diverge.
pub(super) struct PeekAccumulator {
    /// The finishing's ordering, used both for thinning and for the final
    /// [`RowCollection`].
    order_by: Vec<mz_expr::ColumnOrder>,
    /// Comparator derived from `order_by`, reused across thinning passes.
    comparator: RowComparator,
    /// A bound on the number of rows we must retain, driven by the finishing's
    /// limit and offset. `None` means we must retain every row.
    max_results: Option<usize>,
    /// The maximum permitted response size in bytes.
    max_result_size: usize,
    /// Whether this peek may be diverted to the peek stash on overflow.
    stash_eligible: bool,
    /// The byte threshold above which a stash-eligible peek diverts to the stash.
    stash_threshold_bytes: usize,
    /// Accumulated results that we are likely to return.
    results: Vec<(Row, NonZeroUsize)>,
    /// Running byte size of `results`, used for stash and max-size decisions.
    total_size: usize,
    /// Time spent sorting during thinning, surfaced for metrics.
    sort_time: Duration,
}

/// The outcome of pushing a single row into a [`PeekAccumulator`].
pub(super) enum PushOutcome {
    /// The row was accepted. Keep feeding the accumulator.
    Continue,
    /// Enough rows have been collected (limit reached with no ordering). The
    /// accumulator is complete and should be finished.
    Complete,
    /// The accumulator wants to divert to the peek stash: it is stash-eligible
    /// and has exceeded the stash threshold.
    Stash,
    /// The result would exceed the maximum response size. Terminal error.
    MaxSizeExceeded,
}

impl PeekAccumulator {
    pub(super) fn new(
        finishing: &RowSetFinishing,
        max_result_size: usize,
        stash_eligible: bool,
        stash_threshold_bytes: usize,
    ) -> Self {
        Self {
            order_by: finishing.order_by.clone(),
            comparator: RowComparator::new(finishing.order_by.clone()),
            max_results: finishing.num_rows_needed(),
            max_result_size,
            stash_eligible,
            stash_threshold_bytes,
            results: Vec::new(),
            total_size: 0,
            sort_time: Duration::ZERO,
        }
    }

    /// Time spent sorting during thinning so far.
    pub(super) fn sort_time(&self) -> Duration {
        self.sort_time
    }

    /// Pushes a single `(row, copies)` result into the accumulator.
    pub(super) fn push(&mut self, row: Row, copies: NonZeroUsize) -> PushOutcome {
        let count_byte_size = size_of::<NonZeroUsize>();

        self.total_size = self
            .total_size
            .saturating_add(row.byte_len())
            .saturating_add(count_byte_size);
        if self.stash_eligible && self.total_size > self.stash_threshold_bytes {
            return PushOutcome::Stash;
        }
        if self.total_size > self.max_result_size {
            return PushOutcome::MaxSizeExceeded;
        }

        self.results.push((row, copies));

        // If we hold many more than `max_results` records, we can thin down
        // `results` using the finishing's ordering.
        if let Some(max_results) = self.max_results {
            // We use a threshold twice what we intend, to amortize the work
            // across all of the insertions. We could tighten this, but it works
            // for the moment.
            if self.results.len() >= 2 * max_results {
                if self.order_by.is_empty() {
                    self.results.truncate(max_results);
                    return PushOutcome::Complete;
                } else {
                    // We can sort `results` and then truncate to `max_results`.
                    // This has an effect similar to a priority queue, without its
                    // interactive dequeueing properties.
                    let sort_start = Instant::now();
                    self.results.sort_by(|left, right| {
                        self.comparator
                            .compare_rows(&left.0, &right.0, || left.0.cmp(&right.0))
                    });
                    self.sort_time += sort_start.elapsed();
                    let dropped = self.results.drain(max_results..);
                    let dropped_size =
                        dropped
                            .into_iter()
                            .fold(0, |acc: usize, (row, _count): (Row, _)| {
                                acc.saturating_add(row.byte_len().saturating_add(count_byte_size))
                            });
                    self.total_size = self.total_size.saturating_sub(dropped_size);
                }
            }
        }

        PushOutcome::Continue
    }

    /// Consumes the accumulated results into a [`PeekResponse`].
    ///
    /// Takes `&mut self` and drains the results so it can be called from a
    /// borrow held inside the coalesced walk.
    pub(super) fn finish(&mut self) -> PeekResponse {
        let results = std::mem::take(&mut self.results);
        let collection = RowCollection::new(results, &self.order_by);
        PeekResponse::Rows(vec![collection])
    }

    /// The error response for a result that exceeds the maximum response size.
    pub(super) fn max_size_error(&self) -> PeekResponse {
        PeekResponse::Error(format!(
            "result exceeds max size of {}",
            ByteSize::b(u64::cast_from(self.max_result_size))
        ))
    }
}

/// One index peek participating in a coalesced walk.
///
/// The literal constraints and finishing are captured up front so the walk can
/// demultiplex without holding the originating `Peek`.
pub(super) struct MuxedPeek {
    /// The peek's map-filter-project, evaluated against every visited row.
    pub(super) mfp: SafeMfpPlan,
    /// The read timestamp. Identical across a group, kept for the copies filter.
    pub(super) timestamp: Timestamp,
    /// The target index id, for error logging.
    pub(super) target_id: GlobalId,
    /// The literal key constraints, or `None` for a full scan. When present, the
    /// peek only sees rows whose key equals one of these literals. Duplicates
    /// are preserved to match the single-peek path.
    pub(super) literals: Option<Vec<Row>>,
    /// Accumulates results and applies the finishing.
    pub(super) accumulator: PeekAccumulator,
    /// Set once the peek is retired, either with a response or a stash request.
    pub(super) outcome: Option<PeekOutcome>,
}

/// The terminal outcome of a peek in a coalesced walk.
pub(super) enum PeekOutcome {
    /// The peek produced a response that should be sent to the coordinator.
    Response(PeekResponse),
    /// The peek should be diverted to the peek stash.
    Stash,
}

/// A peek's literal key constraints, laid out in the trace's native key
/// container and consumed in ascending order as the walk visits keys.
struct LiteralKeys<Tr>
where
    Tr: TraceReader<Batch: Navigable>,
    BatchCursor<Tr>: Cursor<KeyContainer: BatchContainer<Owned = Row>>,
{
    keys: <BatchCursor<Tr> as Cursor>::KeyContainer,
    /// Index of the next literal not yet consumed.
    pos: usize,
}

impl<Tr> LiteralKeys<Tr>
where
    Tr: TraceReader<Batch: Navigable>,
    BatchCursor<Tr>: Cursor<KeyContainer: BatchContainer<Owned = Row>>,
{
    /// Builds a literal cursor from owned rows. Sorts (required for the ascending
    /// merge and for `seek_key`, which only seeks forward).
    fn new(mut literals: Vec<Row>) -> Self {
        literals.sort();
        let mut keys = <BatchCursor<Tr> as Cursor>::KeyContainer::with_capacity(literals.len());
        for literal in &literals {
            keys.push_own(literal);
        }
        Self { keys, pos: 0 }
    }

    fn len(&self) -> usize {
        self.keys.len()
    }

    /// Returns the number of literals equal to `key`, advancing the cursor past
    /// all literals up to and including those equal to `key`.
    ///
    /// Must be called with `key` values that are non-decreasing across calls,
    /// which holds because both the trace keys and the literals are sorted
    /// ascending.
    fn multiplicity_at(&mut self, key: <BatchCursor<Tr> as Cursor>::Key<'_>) -> usize {
        // The key type derives its `Ord`/`Eq` over a single lifetime, so the
        // container item and `key` must be reborrowed to a common lifetime before
        // comparing. `reborrow` shortens both to this call's scope.
        while self.pos < self.keys.len() {
            let item =
                <BatchCursor<Tr> as Cursor>::KeyContainer::reborrow(self.keys.index(self.pos));
            if !(item < <BatchCursor<Tr> as Cursor>::KeyContainer::reborrow(key)) {
                break;
            }
            self.pos += 1;
        }
        let start = self.pos;
        while self.pos < self.keys.len() {
            let item =
                <BatchCursor<Tr> as Cursor>::KeyContainer::reborrow(self.keys.index(self.pos));
            if item != <BatchCursor<Tr> as Cursor>::KeyContainer::reborrow(key) {
                break;
            }
            self.pos += 1;
        }
        self.pos - start
    }
}

/// Represents the multiplicity of a visited row at the shared read timestamp.
enum Copies {
    /// The summed diff is negative: corrupt data.
    Negative(Diff),
    /// The summed diff is zero: the row does not exist at this timestamp.
    Zero,
    /// The summed diff is positive.
    Positive(NonZeroUsize),
}

/// Walks the ok stream of `oks` once, demultiplexing each visited row into the
/// peeks in `peeks`.
///
/// On return, every peek has its `outcome` populated. Peeks whose accumulation
/// completed normally get a `Response`; peeks that overflowed the stash
/// threshold get `Stash`; peeks that errored (map-filter-project error, negative
/// multiplicities, or oversized result) get an error `Response`.
///
/// The caller is responsible for the shared readiness and error-trace checks
/// before invoking this, and for acting on each peek's outcome afterward.
pub(super) fn collect_coalesced_ok_data<Tr>(oks: &mut Tr, peeks: &mut [MuxedPeek])
where
    Tr: TraceReader<Batch: Navigable>,
    for<'a> BatchCursor<Tr>: Cursor<
            Key<'a>: ExtendDatums + Eq,
            KeyContainer: BatchContainer<Owned = Row>,
            Val<'a>: ExtendDatums,
            TimeGat<'a>: PartialOrder<Timestamp>,
            DiffGat<'a> = &'a Diff,
        >,
{
    let (mut cursor, storage) = oks.cursor();

    // Per-peek literal cursors, parallel to `peeks`. `None` marks a full scan.
    let mut lit_cursors: Vec<Option<LiteralKeys<Tr>>> = peeks
        .iter()
        .map(|peek| {
            peek.literals
                .as_ref()
                .map(|literals| LiteralKeys::<Tr>::new(literals.clone()))
        })
        .collect();

    // If any peek is a full scan we must visit every key, so a linear walk is
    // as good as it gets. Otherwise we seek through the union of all literals to
    // preserve point-lookup efficiency.
    let full_scan = peeks.iter().any(|peek| peek.literals.is_none());

    let mut datum_vec = DatumVec::new();
    let mut row_builder = Row::default();
    let mut mults = vec![0usize; peeks.len()];

    if full_scan {
        while cursor.key_valid(&storage) {
            if peeks.iter().all(|peek| peek.outcome.is_some()) {
                break;
            }
            let key = cursor.key(&storage);
            for i in 0..peeks.len() {
                mults[i] = peek_multiplicity::<Tr>(&peeks[i], &mut lit_cursors[i], key);
            }
            visit_key::<Tr>(
                &mut cursor,
                &storage,
                key,
                peeks,
                &mults,
                &mut datum_vec,
                &mut row_builder,
            );
            cursor.step_key(&storage);
        }
    } else {
        // Union of all seek targets, sorted and deduplicated. Deduplication only
        // affects seeking. Per-peek multiplicity still comes from the per-peek
        // cursors, which keep duplicates.
        let mut union_rows: Vec<Row> = peeks
            .iter()
            .filter_map(|peek| peek.literals.clone())
            .flatten()
            .collect();
        union_rows.sort();
        union_rows.dedup();
        let union = LiteralKeys::<Tr>::new(union_rows);

        for ui in 0..union.len() {
            if peeks.iter().all(|peek| peek.outcome.is_some()) {
                break;
            }
            let target = union.keys.index(ui);
            cursor.seek_key(&storage, target);
            if cursor.get_key(&storage) == Some(target) {
                let key = cursor.key(&storage);
                for i in 0..peeks.len() {
                    mults[i] = peek_multiplicity::<Tr>(&peeks[i], &mut lit_cursors[i], key);
                }
                visit_key::<Tr>(
                    &mut cursor,
                    &storage,
                    key,
                    peeks,
                    &mults,
                    &mut datum_vec,
                    &mut row_builder,
                );
            } else {
                // No row for this union key. Still advance every literal cursor
                // past it so their pointers stay in step with the ascending walk.
                for lit_cursor in lit_cursors.iter_mut().flatten() {
                    lit_cursor.multiplicity_at(target);
                }
            }
        }
    }

    for peek in peeks.iter_mut() {
        if peek.outcome.is_none() {
            peek.outcome = Some(PeekOutcome::Response(peek.accumulator.finish()));
        }
    }
}

/// Computes a peek's multiplicity at `key`, advancing its literal cursor.
///
/// A retired peek contributes nothing. A full-scan peek is active exactly once.
/// A literal peek is active once per matching literal.
fn peek_multiplicity<Tr>(
    peek: &MuxedPeek,
    lit_cursor: &mut Option<LiteralKeys<Tr>>,
    key: <BatchCursor<Tr> as Cursor>::Key<'_>,
) -> usize
where
    Tr: TraceReader<Batch: Navigable>,
    BatchCursor<Tr>: Cursor<KeyContainer: BatchContainer<Owned = Row>>,
{
    if peek.outcome.is_some() {
        // Still advance the literal cursor so it does not fall behind the walk.
        if let Some(lit_cursor) = lit_cursor {
            lit_cursor.multiplicity_at(key);
        }
        return 0;
    }
    match lit_cursor {
        Some(lit_cursor) => lit_cursor.multiplicity_at(key),
        None => 1,
    }
}

/// Processes all values under the current key, feeding each active peek.
fn visit_key<Tr>(
    cursor: &mut TraceCursor<Tr>,
    storage: &TraceStorage<Tr>,
    key: <BatchCursor<Tr> as Cursor>::Key<'_>,
    peeks: &mut [MuxedPeek],
    mults: &[usize],
    datum_vec: &mut DatumVec,
    row_builder: &mut Row,
) where
    Tr: TraceReader<Batch: Navigable>,
    for<'a> BatchCursor<Tr>: Cursor<
            Key<'a>: ExtendDatums + Eq,
            KeyContainer: BatchContainer<Owned = Row>,
            Val<'a>: ExtendDatums,
            TimeGat<'a>: PartialOrder<Timestamp>,
            DiffGat<'a> = &'a Diff,
        >,
{
    // The timestamp is shared across the group, so the multiplicity is too.
    let timestamp = peeks
        .iter()
        .find(|p| p.outcome.is_none())
        .map(|p| p.timestamp);
    let Some(timestamp) = timestamp else {
        return;
    };

    while cursor.val_valid(storage) {
        let val = cursor.val(storage);
        let arena = RowArena::new();
        let mut borrow = datum_vec.borrow();
        key.extend_datums(&arena, &mut borrow, None);
        val.extend_datums(&arena, &mut borrow, None);
        let kv_len = borrow.len();

        // Summed diff at the read timestamp. Shared across the group.
        let mut copies = Diff::ZERO;
        cursor.map_times(storage, |time, diff| {
            if time.less_equal(&timestamp) {
                copies += diff;
            }
        });
        let copies = classify_copies(copies);

        // A row that does not exist at this timestamp contributes to no peek.
        if let Copies::Zero = copies {
            drop(borrow);
            cursor.step_val(storage);
            continue;
        }

        for i in 0..peeks.len() {
            let mult = mults[i];
            if mult == 0 || peeks[i].outcome.is_some() {
                continue;
            }
            let is_literal = peeks[i].literals.is_some();
            for _ in 0..mult {
                borrow.truncate(kv_len);
                if is_literal {
                    // The peek derives from an IndexedFilter join, which adds the
                    // key columns a second time. At any visited key the matching
                    // literal equals the key, so re-extending the key reproduces
                    // exactly the literal columns the join would have added.
                    key.extend_datums(&arena, &mut borrow, None);
                }
                match peeks[i]
                    .mfp
                    .evaluate_into(&mut borrow, &arena, row_builder)
                    .map(|row| row.cloned())
                    .map_err_to_string_with_causes()
                {
                    Err(err) => {
                        peeks[i].outcome = Some(PeekOutcome::Response(PeekResponse::Error(err)));
                        break;
                    }
                    Ok(None) => {
                        // Filtered out. A repeat would filter identically.
                        break;
                    }
                    Ok(Some(row)) => match &copies {
                        Copies::Negative(copies) => {
                            let datums = &*borrow;
                            tracing::error!(
                                target = %peeks[i].target_id, diff = %copies, ?datums,
                                "index peek encountered negative multiplicities in ok trace",
                            );
                            peeks[i].outcome =
                                Some(PeekOutcome::Response(PeekResponse::Error(format!(
                                    "Invalid data in source, \
                                     saw retractions ({}) for row that does not exist: {:?}",
                                    -*copies, datums,
                                ))));
                            break;
                        }
                        Copies::Zero => break,
                        Copies::Positive(copies) => match peeks[i].accumulator.push(row, *copies) {
                            PushOutcome::Continue => {}
                            PushOutcome::Complete => {
                                let response = peeks[i].accumulator.finish();
                                peeks[i].outcome = Some(PeekOutcome::Response(response));
                                break;
                            }
                            PushOutcome::Stash => {
                                peeks[i].outcome = Some(PeekOutcome::Stash);
                                break;
                            }
                            PushOutcome::MaxSizeExceeded => {
                                let response = peeks[i].accumulator.max_size_error();
                                peeks[i].outcome = Some(PeekOutcome::Response(response));
                                break;
                            }
                        },
                    },
                }
            }
        }

        drop(borrow);
        cursor.step_val(storage);
    }
}

/// Classifies a summed diff into the multiplicity cases.
fn classify_copies(copies: Diff) -> Copies {
    if copies.is_negative() {
        Copies::Negative(copies)
    } else {
        match NonZeroUsize::new(usize::try_from(copies.into_inner()).expect("non-negative")) {
            Some(copies) => Copies::Positive(copies),
            None => Copies::Zero,
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_expr::{ColumnOrder, RowSetFinishing};
    use mz_ore::num::NonNeg;
    use mz_repr::{Datum, Row};

    use super::*;

    fn row(i: i64) -> Row {
        Row::pack_slice(&[Datum::Int64(i)])
    }

    fn finishing(order_by: Vec<ColumnOrder>, limit: Option<i64>, offset: usize) -> RowSetFinishing {
        RowSetFinishing {
            order_by,
            limit: limit.map(|l| NonNeg::try_from(l).unwrap()),
            offset,
            project: vec![0],
        }
    }

    fn one() -> NonZeroUsize {
        NonZeroUsize::new(1).unwrap()
    }

    #[mz_ore::test]
    fn accumulator_collects_all_without_limit() {
        let mut acc = PeekAccumulator::new(&finishing(vec![], None, 0), usize::MAX, false, 0);
        for i in 0..5 {
            assert!(matches!(acc.push(row(i), one()), PushOutcome::Continue));
        }
        let PeekResponse::Rows(collections) = acc.finish() else {
            panic!("expected rows");
        };
        let total: usize = collections.iter().map(|c| c.count()).sum();
        assert_eq!(total, 5);
    }

    #[mz_ore::test]
    fn accumulator_completes_on_unordered_limit() {
        // limit 2, no order_by: completes once it holds 2 * limit rows.
        let mut acc = PeekAccumulator::new(&finishing(vec![], Some(2), 0), usize::MAX, false, 0);
        assert!(matches!(acc.push(row(0), one()), PushOutcome::Continue));
        assert!(matches!(acc.push(row(1), one()), PushOutcome::Continue));
        assert!(matches!(acc.push(row(2), one()), PushOutcome::Continue));
        // Fourth row reaches 2 * limit and truncates to the limit.
        assert!(matches!(acc.push(row(3), one()), PushOutcome::Complete));
        let PeekResponse::Rows(collections) = acc.finish() else {
            panic!("expected rows");
        };
        let total: usize = collections.iter().map(|c| c.count()).sum();
        assert_eq!(total, 2);
    }

    #[mz_ore::test]
    fn accumulator_thins_ordered_results() {
        // limit 1 with an ascending order_by: thinning sorts and keeps the single
        // smallest row seen so far.
        let order_by = vec![ColumnOrder {
            column: 0,
            desc: false,
            nulls_last: false,
        }];
        let mut acc = PeekAccumulator::new(&finishing(order_by, Some(1), 0), usize::MAX, false, 0);
        for i in [5, 3, 4, 1, 2] {
            let _ = acc.push(row(i), one());
        }
        let PeekResponse::Rows(collections) = acc.finish() else {
            panic!("expected rows");
        };
        // Exactly one row retained, and it is the minimum.
        let rows: Vec<_> = collections
            .iter()
            .flat_map(|c| (0..c.entries()).map(move |i| c.get(i).unwrap().0.to_owned()))
            .collect();
        assert_eq!(rows, vec![row(1)]);
    }

    #[mz_ore::test]
    fn accumulator_diverts_to_stash_over_threshold() {
        // Stash-eligible with a tiny threshold: the first row overflows.
        let mut acc = PeekAccumulator::new(&finishing(vec![], None, 0), usize::MAX, true, 0);
        assert!(matches!(acc.push(row(0), one()), PushOutcome::Stash));
    }

    #[mz_ore::test]
    fn accumulator_errors_over_max_size() {
        // Not stash-eligible with a tiny max size: the first row errors.
        let mut acc = PeekAccumulator::new(&finishing(vec![], None, 0), 1, false, 0);
        assert!(matches!(
            acc.push(row(0), one()),
            PushOutcome::MaxSizeExceeded
        ));
    }

    #[mz_ore::test]
    fn classify_copies_cases() {
        assert!(matches!(classify_copies(Diff::ZERO), Copies::Zero));
        assert!(matches!(
            classify_copies(Diff::from(3)),
            Copies::Positive(_)
        ));
        assert!(matches!(
            classify_copies(Diff::from(-2)),
            Copies::Negative(_)
        ));
    }

    /// Builds a `MuxedPeek` with an identity map-filter-project of the given arity.
    fn muxed(arity: usize, literals: Option<Vec<Row>>) -> MuxedPeek {
        use mz_expr::MapFilterProject;
        let mfp = MapFilterProject::new(arity)
            .into_plan()
            .unwrap()
            .into_nontemporal()
            .unwrap();
        MuxedPeek {
            mfp,
            timestamp: Timestamp::from(0u64),
            target_id: GlobalId::User(1),
            literals,
            accumulator: PeekAccumulator::new(&finishing(vec![], None, 0), usize::MAX, false, 0),
            outcome: None,
        }
    }

    /// Extracts each peek's rows as sorted `(datums, count)` tuples.
    fn extract(outcome: Option<PeekOutcome>) -> Vec<(Vec<i64>, usize)> {
        let Some(PeekOutcome::Response(PeekResponse::Rows(collections))) = outcome else {
            panic!("expected a rows response");
        };
        let mut out = Vec::new();
        for collection in &collections {
            for i in 0..collection.entries() {
                let (row_ref, count) = collection.get(i).unwrap();
                let datums = row_ref
                    .iter()
                    .map(|d| match d {
                        Datum::Int64(x) => x,
                        other => panic!("unexpected datum {other:?}"),
                    })
                    .collect();
                out.push((datums, count.get()));
            }
        }
        out.sort();
        out
    }

    /// Builds a `RowRow` arrangement over the given `(key, val)` pairs (each with
    /// multiplicity one at time 0), then runs `body` against the sealed trace.
    ///
    /// The trace is compacted to time 0, mirroring how a real index peek prepares
    /// its trace handle before reading.
    fn with_arrangement<F>(data: Vec<(i64, i64)>, body: F)
    where
        F: FnOnce(&mut crate::typedefs::RowRowAgent<Timestamp, Diff>) + Send + Sync + 'static,
    {
        use differential_dataflow::AsCollection;
        use mz_row_spine::{RowRowBatcher, RowRowBuilder};
        use mz_timely_util::columnation::ColumnationChunker;
        use timely::dataflow::operators::ToStream;
        use timely::progress::Antichain;

        use crate::extensions::arrange::MzArrange;

        timely::execute_directly(move |worker| {
            let mut trace = worker.dataflow::<Timestamp, _, _>(|scope| {
                let updates: Vec<((Row, Row), Timestamp, Diff)> = data
                    .iter()
                    .map(|(k, v)| ((row(*k), row(*v)), Timestamp::from(0u64), Diff::ONE))
                    .collect();
                let arranged = updates
                    .to_stream(scope)
                    .as_collection()
                    .mz_arrange::<
                        ColumnationChunker<_>,
                        RowRowBatcher<_, _>,
                        RowRowBuilder<_, _>,
                        _,
                    >("test arrangement");
                arranged.trace
            });

            // Drive the worker until the arrangement has sealed time 0.
            let target = Antichain::from_elem(Timestamp::from(1u64));
            worker.step_while(|| {
                let mut upper = Antichain::new();
                trace.read_upper(&mut upper);
                timely::PartialOrder::less_than(&upper, &target)
            });

            // Prepare the trace handle exactly as `PendingPeek::index` would.
            trace.set_logical_compaction(Antichain::from_elem(Timestamp::from(0u64)).borrow());
            trace.set_physical_compaction(Antichain::new().borrow());

            body(&mut trace);
        });
    }

    #[mz_ore::test]
    fn coalesced_full_scans_each_see_all_rows() {
        with_arrangement(vec![(1, 10), (1, 11), (2, 20)], |trace| {
            let mut group = vec![muxed(2, None), muxed(2, None)];
            collect_coalesced_ok_data(trace, &mut group);

            let expected = vec![(vec![1, 10], 1), (vec![1, 11], 1), (vec![2, 20], 1)];
            assert_eq!(extract(group[0].outcome.take()), expected);
            assert_eq!(extract(group[1].outcome.take()), expected);
        });
    }

    #[mz_ore::test]
    fn coalesced_mixed_full_scan_and_literal() {
        with_arrangement(vec![(1, 10), (1, 11), (2, 20)], |trace| {
            // A full scan and a literal peek on key = 2. The literal peek's row
            // carries the key columns twice, mirroring an IndexedFilter join.
            let mut group = vec![muxed(2, None), muxed(3, Some(vec![row(2)]))];
            collect_coalesced_ok_data(trace, &mut group);

            assert_eq!(
                extract(group[0].outcome.take()),
                vec![(vec![1, 10], 1), (vec![1, 11], 1), (vec![2, 20], 1)],
            );
            assert_eq!(extract(group[1].outcome.take()), vec![(vec![2, 20, 2], 1)]);
        });
    }

    #[mz_ore::test]
    fn coalesced_seek_mode_disjoint_literals() {
        with_arrangement(vec![(1, 10), (1, 11), (2, 20)], |trace| {
            // No full scan: the walk seeks through the union of literals and
            // demultiplexes each key to the peeks that requested it.
            let mut group = vec![muxed(3, Some(vec![row(1)])), muxed(3, Some(vec![row(2)]))];
            collect_coalesced_ok_data(trace, &mut group);

            assert_eq!(
                extract(group[0].outcome.take()),
                vec![(vec![1, 10, 1], 1), (vec![1, 11, 1], 1)],
            );
            assert_eq!(extract(group[1].outcome.take()), vec![(vec![2, 20, 2], 1)]);
        });
    }

    #[mz_ore::test]
    fn coalesced_duplicate_literal_repeats_key() {
        with_arrangement(vec![(1, 10), (2, 20)], |trace| {
            // A duplicated literal reproduces the key's rows once per occurrence,
            // matching the single-peek path.
            let mut group = vec![muxed(3, Some(vec![row(2), row(2)])), muxed(2, None)];
            collect_coalesced_ok_data(trace, &mut group);

            assert_eq!(
                extract(group[0].outcome.take()),
                vec![(vec![2, 20, 2], 1), (vec![2, 20, 2], 1)],
            );
            assert_eq!(
                extract(group[1].outcome.take()),
                vec![(vec![1, 10], 1), (vec![2, 20], 1)],
            );
        });
    }
}
