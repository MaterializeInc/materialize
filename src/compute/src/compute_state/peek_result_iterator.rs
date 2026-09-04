// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Code for extracting a peek result out of compute state/an arrangement.

use std::iter::FusedIterator;
use std::num::NonZeroI64;
use std::ops::Range;

use differential_dataflow::trace::cursor::{BatchCursor, BatchKey, CursorList};
use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::{Cursor, Navigable, TraceReader};
use mz_compute_client::protocol::response::PeekError;
use mz_repr::fixed_length::ExtendDatums;
use mz_repr::{DatumVec, Diff, GlobalId, Row, RowArena};
use timely::order::PartialOrder;

use crate::compute_state::PeekRowIterationTracker;

/// The merged cursor a [`TraceReader::cursor`] hands out over all of a trace's batches: a
/// [`CursorList`] over the per-batch cursors.
pub(super) type TraceCursor<Tr> = CursorList<BatchCursor<Tr>>;
/// Backing storage for a [`TraceCursor`]: the batches the cursor borrows from.
pub(super) type TraceStorage<Tr> = Vec<<Tr as TraceReader>::Batch>;

pub(super) struct PeekResultIterator<Tr>
where
    Tr: TraceReader<Batch: Navigable>,
{
    // For debug/trace logging.
    target_id: GlobalId,
    cursor: TraceCursor<Tr>,
    storage: TraceStorage<Tr>,
    map_filter_project: mz_expr::SafeMfpPlan,
    peek_timestamp: mz_repr::Timestamp,
    row_builder: Row,
    datum_vec: DatumVec,
    literals: Option<Literals<Tr>>,
    rows_processed: usize,
    row_iteration_tracker: PeekRowIterationTracker,
    exhausted: bool,
}

/// Helper to handle literals in peeks
struct Literals<Tr: TraceReader<Batch: Navigable>> {
    /// The literals in a container, sorted by `Ord`.
    literals: <BatchCursor<Tr> as Cursor>::KeyContainer,
    /// The range of the literals that are still available.
    range: Range<usize>,
    /// Where the cursor sits relative to the literal list.
    position: LiteralPosition,
}

/// Where a [`Literals`]' cursor sits relative to the literal list.
///
/// `Seeking` and `Exhausted` stay distinct: the former still owes rows, the latter is the end of
/// the scan. Collapsing them would empty out every literal-constrained peek.
enum LiteralPosition {
    /// A seek is outstanding, either not started or suspended part-way through the literal list.
    /// The cursor is parked on a key no literal has claimed, so no row may be read until the
    /// seek completes. [`Literals::range`] holds the literals left to try.
    Seeking,
    /// The cursor sits on the key of the literal at this index.
    At(usize),
    /// Every literal has been tried. The scan is done.
    Exhausted,
}

/// The outcome of a fueled [`Literals::seek_next_literal_key`].
enum SeekOutcome {
    /// The seek finished: the cursor sits on a matching literal, or the literals are exhausted.
    Complete,
    /// Fuel ran out with literals left to try. Seeking again resumes at the next untried
    /// literal.
    OutOfFuel,
}

impl<Tr> Literals<Tr>
where
    Tr: TraceReader<Batch: Navigable>,
    BatchCursor<Tr>: Cursor<KeyContainer: BatchContainer<Owned: Ord>>,
{
    /// Construct a new `Literals` from a mutable slice of literals. Sorts contents.
    ///
    /// The literals must be distinct. A repeated literal seeks to the same key twice and
    /// returns its rows twice, since `seek_key` seeks forward only. `MirRelationExpr` literal
    /// constraints are deduplicated by the optimizer (`mz_transform::literal_constraints`).
    ///
    /// Does not seek the trace cursor. The initial seek runs on the first fueled step instead, so
    /// its cost is charged to a budget instead of paid before any budget exists.
    fn new(
        literals: &mut [<<BatchCursor<Tr> as Cursor>::KeyContainer as BatchContainer>::Owned],
    ) -> Self {
        // We have to sort the literal constraints because cursor.seek_key can
        // seek only forward.
        literals.sort();
        let mut container =
            <BatchCursor<Tr> as Cursor>::KeyContainer::with_capacity(literals.len());
        for constraint in literals {
            container.push_own(constraint)
        }
        let range = 0..container.len();
        Self {
            literals: container,
            range,
            position: LiteralPosition::Seeking,
        }
    }

    /// Returns the current literal, if the cursor sits on one.
    ///
    /// Returns `None` while a seek is outstanding and once the literals are exhausted. In
    /// neither case does the cursor point at a row that belongs to a literal.
    fn peek(&self) -> Option<BatchKey<'_, Tr>> {
        match self.position {
            LiteralPosition::At(index) => self.literals.get(index),
            LiteralPosition::Seeking | LiteralPosition::Exhausted => None,
        }
    }

    /// Returns `true` if a seek has to run before the cursor sits on a matching literal.
    fn seek_pending(&self) -> bool {
        matches!(self.position, LiteralPosition::Seeking)
    }

    /// Returns `true` if there are no more literals to process.
    fn is_exhausted(&self) -> bool {
        matches!(self.position, LiteralPosition::Exhausted)
    }

    /// Seeks the cursor to the next key of a matching literal, if any, charging one unit of
    /// `fuel` per `seek_key` call.
    ///
    /// Returns [`SeekOutcome::OutOfFuel`] if literals remain untried when the fuel runs out.
    /// The walk resumes from that literal on the next call, so a caller must not treat a
    /// suspended seek as an end of scan.
    ///
    /// A literal list whose entries are mostly absent from the trace costs one seek per absent
    /// literal, so the walk is fueled instead of run to completion.
    fn seek_next_literal_key(
        &mut self,
        cursor: &mut TraceCursor<Tr>,
        storage: &TraceStorage<Tr>,
        fuel: &mut usize,
    ) -> SeekOutcome {
        // Until a literal matches, the cursor is parked on a key no literal claims. Recording
        // that keeps a suspended seek from being read as "sitting on the previous literal".
        self.position = LiteralPosition::Seeking;
        while !self.range.is_empty() {
            if *fuel == 0 {
                return SeekOutcome::OutOfFuel;
            }
            *fuel -= 1;
            let index = self.range.next().expect("range is not empty");
            let literal = self.literals.get(index).expect("index out of bounds");
            cursor.seek_key(storage, literal);
            if cursor.get_key(storage).map_or(true, |key| key == literal) {
                self.position = LiteralPosition::At(index);
                return SeekOutcome::Complete;
            }
            // The cursor landed on a record that has a different key,
            // meaning that there is no record whose key would match the
            // current literal.
        }
        self.position = LiteralPosition::Exhausted;
        SeekOutcome::Complete
    }
}

/// An [Iterator] that extracts a peek result from a [TraceReader].
///
/// The iterator will apply a given `MapFilterProject` and obey literal
/// constraints, if any.
impl<Tr> PeekResultIterator<Tr>
where
    Tr: TraceReader<Batch: Navigable>,
    for<'a> BatchCursor<Tr>: Cursor<
            Key<'a>: ExtendDatums + Eq,
            KeyContainer: BatchContainer<Owned = Row>,
            Val<'a>: ExtendDatums,
            TimeGat<'a>: PartialOrder<mz_repr::Timestamp>,
            DiffGat<'a> = &'a Diff,
        >,
{
    pub(super) fn new(
        target_id: GlobalId,
        map_filter_project: mz_expr::SafeMfpPlan,
        peek_timestamp: mz_repr::Timestamp,
        literal_constraints: Option<&mut [Row]>,
        trace_reader: &mut Tr,
        row_iteration_limit: Option<usize>,
        rows_iterated: usize,
    ) -> Self {
        let (cursor, storage) = trace_reader.cursor();
        let literals = literal_constraints.map(Literals::new);

        Self {
            target_id,
            cursor,
            storage,
            map_filter_project,
            peek_timestamp,
            row_builder: Row::default(),
            datum_vec: DatumVec::new(),
            literals,
            rows_processed: 0,
            row_iteration_tracker: PeekRowIterationTracker::new(row_iteration_limit, rows_iterated),
            exhausted: false,
        }
    }

    /// Returns the number of rows evaluated by the iterator.
    pub fn rows_processed(&self) -> usize {
        self.rows_processed
    }

    /// Returns `true` if the iterator has no more literals to process, or if there are no literals at all.
    fn literals_exhausted(&self) -> bool {
        self.literals.as_ref().map_or(false, Literals::is_exhausted)
    }
}

impl<Tr> FusedIterator for PeekResultIterator<Tr>
where
    Tr: TraceReader<Batch: Navigable>,
    for<'a> BatchCursor<Tr>: Cursor<
            Key<'a>: ExtendDatums + Eq,
            KeyContainer: BatchContainer<Owned = Row>,
            Val<'a>: ExtendDatums,
            TimeGat<'a>: PartialOrder<mz_repr::Timestamp>,
            DiffGat<'a> = &'a Diff,
        >,
{
}

impl<Tr> Iterator for PeekResultIterator<Tr>
where
    Tr: TraceReader<Batch: Navigable>,
    for<'a> BatchCursor<Tr>: Cursor<
            Key<'a>: ExtendDatums + Eq,
            KeyContainer: BatchContainer<Owned = Row>,
            Val<'a>: ExtendDatums,
            TimeGat<'a>: PartialOrder<mz_repr::Timestamp>,
            DiffGat<'a> = &'a Diff,
        >,
{
    type Item = Result<(Row, NonZeroI64), PeekError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut fuel = usize::MAX;
        match self.step(&mut fuel) {
            Step::Row(row) => Some(row),
            Step::Done => None,
            Step::OutOfFuel => unreachable!("stepped with unbounded fuel"),
        }
    }
}

/// The outcome of a single fueled [`PeekResultIterator::step`].
pub enum Step {
    /// A result row, or the error that ends the scan.
    ///
    /// An error is the peek's whole answer, so the iterator latches shut on one and a caller that
    /// steps again gets [`Step::Done`] rather than the next value or the same error again.
    Row(Result<(Row, NonZeroI64), PeekError>),
    /// The cursor is exhausted, or an error already ended the scan. Further steps also return
    /// `Done`, and cost no fuel.
    Done,
    /// The budget is spent. Whether the scan has work left is not implied: a walk whose last
    /// position was rejected by the `map_filter_project`, or whose last literal seek landed
    /// past the end of the trace, spends its budget and returns [`Step::Done`] on the next
    /// call.
    ///
    /// The iterator resumes exactly where it stopped. The cursor itself may sit on an
    /// arbitrary intermediate key if a literal seek was suspended, because resumption is
    /// driven by `Literals::range` rather than by cursor position.
    OutOfFuel,
}

/// The outcome of a [`PeekResultIterator::step_key`].
enum KeyStep {
    /// The cursor sits on a new key, which has at least one value.
    Advanced,
    /// No key remains.
    Exhausted,
    /// The fuel ran out inside the literal seek. The seek resumes at the next untried literal.
    OutOfFuel,
}

impl<Tr> PeekResultIterator<Tr>
where
    Tr: TraceReader<Batch: Navigable>,
    for<'a> BatchCursor<Tr>: Cursor<
            Key<'a>: ExtendDatums + Eq,
            KeyContainer: BatchContainer<Owned = Row>,
            Val<'a>: ExtendDatums,
            TimeGat<'a>: PartialOrder<mz_repr::Timestamp>,
            DiffGat<'a> = &'a Diff,
        >,
{
    /// Advances the cursor until it produces a row, the cursor is exhausted,
    /// or `fuel` runs out, whichever comes first. Decrements `fuel` by the
    /// number of cursor positions visited, a literal seek's `seek_key` calls
    /// included.
    ///
    /// Fuel is charged per cursor position, not per row returned, so a
    /// selective `map_filter_project` cannot starve the caller of yield
    /// points. The charge does not depend on how the walk is sliced: the same
    /// walk costs the same total whether it runs in one call or in
    /// single-unit steps.
    ///
    /// A zero budget makes no progress and returns [`Step::OutOfFuel`], so a
    /// caller that derives the budget from a configuration has to floor it at
    /// one to avoid rescheduling the same peek forever.
    pub fn step(&mut self, fuel: &mut usize) -> Step {
        if self.exhausted {
            return Step::Done;
        }

        let result = loop {
            // Every advance that can suspend runs before the per-position charge below, so a
            // slice that suspends has paid only for work it kept. Charging first would buy a
            // position the suspended advance never reached, and the resumed call would buy the
            // same position again, so a sliced walk would cost more than an unsliced one.
            if let Some(literals) = &mut self.literals
                && literals.seek_pending()
            {
                match literals.seek_next_literal_key(&mut self.cursor, &self.storage, fuel) {
                    SeekOutcome::Complete => {}
                    // The seek stopped part-way through the literal list: the cursor is at a
                    // valid intermediate position and no row came out of it. `Done` would drop
                    // the rows of the literals not yet tried, and there is no row to hand back,
                    // so report the budget. The literal list position is retained, so stepping
                    // again resumes with the next untried literal.
                    SeekOutcome::OutOfFuel => return Step::OutOfFuel,
                }
            }

            if self.literals_exhausted() {
                return Step::Done;
            }

            if !self.cursor.key_valid(&self.storage) {
                return Step::Done;
            }

            if !self.cursor.val_valid(&self.storage) {
                match self.step_key(fuel) {
                    KeyStep::Advanced => {}
                    KeyStep::Exhausted => return Step::Done,
                    KeyStep::OutOfFuel => return Step::OutOfFuel,
                }
            }

            if *fuel == 0 {
                return Step::OutOfFuel;
            }
            *fuel -= 1;

            // Filtered and zero-multiplicity rows still consume worker time, so
            // they count against the budget before evaluation.
            //
            // Latches for the same reason the tail below does, and does it here because this
            // error returns without passing through it.
            if let Err(error) = self.row_iteration_tracker.track_next() {
                self.exhausted = true;
                return Step::Row(Err(error));
            }

            self.rows_processed = self.rows_processed.saturating_add(1);
            match self.extract_current_row() {
                Ok(Some(row)) => break Ok(row),
                Ok(None) => {
                    // Have to keep stepping and try with the next val.
                    self.cursor.step_val(&self.storage);
                }
                Err(err) => break Err(err),
            }
        };

        if result.is_err() {
            // The peek is answered with this error, so the values after it are not part of any
            // answer. Latching leaves the cursor where it stands and reports the end to a caller
            // that steps again, rather than resuming the walk or repeating the error forever.
            self.exhausted = true;
        } else {
            self.cursor.step_val(&self.storage);
        }

        Step::Row(result)
    }

    /// Extracts and returns the row currently pointed at by our cursor. Returns
    /// `Ok(None)` if our MapFilterProject evaluates to `None`. Also returns any
    /// errors that arise from evaluating the MapFilterProject.
    fn extract_current_row(&mut self) -> Result<Option<(Row, NonZeroI64)>, PeekError> {
        // TODO: This arena could be maintained and reused for longer,
        // but it wasn't clear at what interval we should flush
        // it to ensure we don't accidentally spike our memory use.
        // This choice is conservative, and not the end of the world
        // from a performance perspective.
        let arena = RowArena::new();

        let key_item = self.cursor.key(&self.storage);
        let row_item = self.cursor.val(&self.storage);

        // An optional literal that we might have added to the borrow. Needs to be declared
        // before the borrow to ensure correct drop order.
        let maybe_literal;
        let mut borrow = self.datum_vec.borrow();
        key_item.extend_datums(&arena, &mut borrow, None);
        row_item.extend_datums(&arena, &mut borrow, None);

        if let Some(literals) = &mut self.literals {
            // The peek was created from an IndexedFilter join. We have to add those columns
            // here that the join would add in a dataflow.
            //
            // `step` reaches this only with a completed seek and literals left, so the cursor
            // sits on a matching literal. Reading a `None` as "no literal to add" would leave
            // the datum vec one column short and apply the MFP at the wrong arity.
            maybe_literal = literals
                .peek()
                .expect("literal position must be at a matching literal during row extraction");
            maybe_literal.extend_datums(&arena, &mut borrow, None);
        }
        if let Some(result) = self
            .map_filter_project
            .evaluate_into(&mut borrow, &arena, &mut self.row_builder)
            .map(|row| row.cloned())
            .map_err(PeekError::from)?
        {
            let mut copies = Diff::ZERO;
            self.cursor.map_times(&self.storage, |time, diff| {
                if time.less_equal(&self.peek_timestamp) {
                    copies += diff;
                }
            });
            let copies: i64 = if copies.is_negative() {
                let row = &*borrow;
                tracing::error!(
                    target = %self.target_id, diff = %copies, ?row,
                    "index peek encountered negative multiplicities in ok trace",
                );
                return Err(PeekError::unstructured(format!(
                    "Invalid data in source, \
                             saw retractions ({}) for row that does not exist: {:?}",
                    -copies, row,
                )));
            } else {
                copies.into_inner()
            };
            // if copies > 0 ... otherwise skip
            if let Some(copies) = NonZeroI64::new(copies) {
                Ok(Some((result, copies)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Steps the key forward, respecting literal constraints and charging `fuel` for the
    /// literal seek.
    fn step_key(&mut self, fuel: &mut usize) -> KeyStep {
        assert!(
            !self.cursor.val_valid(&self.storage),
            "must only step key when the vals for a key are exhausted"
        );

        if let Some(literals) = &mut self.literals {
            match literals.seek_next_literal_key(&mut self.cursor, &self.storage, fuel) {
                SeekOutcome::Complete => {}
                SeekOutcome::OutOfFuel => return KeyStep::OutOfFuel,
            }

            if literals.is_exhausted() {
                return KeyStep::Exhausted;
            }
        } else {
            self.cursor.step_key(&self.storage);
        }

        if !self.cursor.key_valid(&self.storage) {
            // We're exhausted!
            return KeyStep::Exhausted;
        }

        assert!(
            self.cursor.val_valid(&self.storage),
            "there must always be at least one val per key"
        );

        KeyStep::Advanced
    }
}

#[cfg(test)]
mod tests;
