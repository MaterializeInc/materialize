// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Code for extracting a peek result out of compute state/an arrangement.

use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::{Cursor, TraceReader};
use mz_compute_client::protocol::command::PeekDescription;
use mz_ore::result::ResultExt;
use mz_repr::fixed_length::ToDatumIter;
use mz_repr::{DatumVec, Diff, GlobalId, Row, RowArena};
use std::collections::VecDeque;
use std::iter::FusedIterator;
use std::ops::{AddAssign, Range};

pub struct PeekResultIterator<Tr>
where
    Tr: TraceReader,
{
    // For debug/trace logging.
    #[allow(unused)]
    target_id: GlobalId,
    cursor: Tr::Cursor,
    storage: Tr::Storage,
    map_filter_project: mz_expr::SafeMfpPlan,
    peek_timestamps: PeekDescription,
    row_builder: Row,
    datum_vec: DatumVec,
    literals: Option<Literals<Tr>>,
    extracted_updates: VecDeque<(Row, mz_repr::Timestamp, Diff)>,
}

/// Helper to handle literals in peeks
struct Literals<Tr: TraceReader> {
    /// The literals in a container, sorted by `Ord`.
    literals: Tr::KeyContainer,
    /// The range of the literals that are still available.
    range: Range<usize>,
    /// The current index in the literals.
    current_index: Option<usize>,
}

impl<Tr: TraceReader<KeyOwn: Ord>> Literals<Tr> {
    /// Construct a new `Literals` from a mutable slice of literals. Sorts contents.
    fn new(literals: &mut [Tr::KeyOwn], cursor: &mut Tr::Cursor, storage: &Tr::Storage) -> Self {
        // We have to sort the literal constraints because cursor.seek_key can
        // seek only forward.
        literals.sort();
        let mut container = Tr::KeyContainer::with_capacity(literals.len());
        for constraint in literals {
            container.push_own(constraint)
        }
        let range = 0..container.len();
        let mut this = Self {
            literals: container,
            range,
            current_index: None,
        };
        this.seek_next_literal_key(cursor, storage);
        this
    }

    /// Returns the current literal, if any.
    fn peek(&self) -> Option<Tr::Key<'_>> {
        self.current_index
            .and_then(|index| self.literals.get(index))
    }

    /// Returns `true` if there are no more literals to process.
    fn is_exhausted(&self) -> bool {
        self.current_index.is_none()
    }

    /// Seeks the cursor to the next key of a matching literal, if any.
    fn seek_next_literal_key(&mut self, cursor: &mut Tr::Cursor, storage: &Tr::Storage) {
        while let Some(index) = self.range.next() {
            let literal = self.literals.get(index).expect("index out of bounds");
            cursor.seek_key(storage, literal);
            if cursor.get_key(storage).map_or(true, |key| key == literal) {
                self.current_index = Some(index);
                return;
            }
            // The cursor landed on a record that has a different key,
            // meaning that there is no record whose key would match the
            // current literal.
        }
        self.current_index = None;
    }
}

/// An [Iterator] that extracts a peek result from a [TraceReader].
///
/// The iterator will apply a given `MapFilterProject` and obey literal
/// constraints, if any.
impl<Tr> PeekResultIterator<Tr>
where
    for<'a> Tr: TraceReader<
            Key<'a>: ToDatumIter + Eq,
            KeyOwn = Row,
            Val<'a>: ToDatumIter,
            TimeGat<'a> = &'a mz_repr::Timestamp,
            DiffGat<'a> = &'a Diff,
        >,
{
    pub fn new(
        target_id: GlobalId,
        map_filter_project: mz_expr::SafeMfpPlan,
        peek_timestamps: PeekDescription,
        literal_constraints: Option<&mut [Row]>,
        trace_reader: &mut Tr,
    ) -> Self {
        let (mut cursor, storage) = trace_reader.cursor();
        let literals = literal_constraints
            .map(|constraints| Literals::new(constraints, &mut cursor, &storage));

        Self {
            target_id,
            cursor,
            storage,
            map_filter_project,
            peek_timestamps,
            row_builder: Row::default(),
            datum_vec: DatumVec::new(),
            literals,
            extracted_updates: VecDeque::with_capacity(1),
        }
    }

    /// Returns `true` if the iterator has no more literals to process.
    fn literals_exhausted(&self) -> bool {
        self.literals.as_ref().map_or(false, Literals::is_exhausted)
    }
}

impl<Tr> FusedIterator for PeekResultIterator<Tr> where
    for<'a> Tr: TraceReader<
            Key<'a>: ToDatumIter + Eq,
            KeyOwn = Row,
            Val<'a>: ToDatumIter,
            TimeGat<'a> = &'a mz_repr::Timestamp,
            DiffGat<'a> = &'a Diff,
        >
{
}

impl<Tr> Iterator for PeekResultIterator<Tr>
where
    for<'a> Tr: TraceReader<
            Key<'a>: ToDatumIter + Eq,
            KeyOwn = Row,
            Val<'a>: ToDatumIter,
            TimeGat<'a> = &'a mz_repr::Timestamp,
            DiffGat<'a> = &'a Diff,
        >,
{
    type Item = Result<(Row, mz_repr::Timestamp, Diff), String>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = loop {
            if let Some(result) = self.extracted_updates.pop_front() {
                break Ok(result);
            }

            if self.literals_exhausted() {
                return None;
            }

            if !self.cursor.key_valid(&self.storage) {
                return None;
            }

            if !self.cursor.val_valid(&self.storage) {
                let exhausted = self.step_key();
                if exhausted {
                    return None;
                }
            }

            match self.extract_current_row() {
                Ok(()) => self.cursor.step_val(&self.storage),
                Err(err) => break Err(err),
            }
        };

        Some(result)
    }
}

impl<Tr> PeekResultIterator<Tr>
where
    for<'a> Tr: TraceReader<
            Key<'a>: ToDatumIter + Eq,
            KeyOwn = Row,
            Val<'a>: ToDatumIter,
            TimeGat<'a> = &'a mz_repr::Timestamp,
            DiffGat<'a> = &'a Diff,
        >,
{
    /// Extracts and returns the row currently pointed at by our cursor. Returns
    /// `Ok(None)` if our MapFilterProject evaluates to `None`. Also returns any
    /// errors that arise from evaluating the MapFilterProject.
    fn extract_current_row(&mut self) -> Result<(), String> {
        // TODO: This arena could be maintained and reused for longer,
        // but it wasn't clear at what interval we should flush
        // it to ensure we don't accidentally spike our memory use.
        // This choice is conservative, and not the end of the world
        // from a performance perspective.
        let arena = RowArena::new();

        let key_item = self.cursor.key(&self.storage);
        let key = key_item.to_datum_iter();
        let row_item = self.cursor.val(&self.storage);
        let row = row_item.to_datum_iter();

        // An optional literal that we might have added to the borrow. Needs to be declared
        // before the borrow to ensure correct drop order.
        let maybe_literal;
        let mut borrow = self.datum_vec.borrow();
        borrow.extend(key);
        borrow.extend(row);

        if let Some(literals) = &mut self.literals
            && let Some(literal) = literals.peek()
        {
            // The peek was created from an IndexedFilter join. We have to add those columns
            // here that the join would add in a dataflow.
            maybe_literal = literal;
            borrow.extend(maybe_literal.to_datum_iter());
        }
        if let Some(result) = self
            .map_filter_project
            .evaluate_into(&mut borrow, &arena, &mut self.row_builder)
            .map(|row| row.cloned())
            .map_err_to_string_with_causes()?
        {
            self.cursor.map_times(&self.storage, |time, diff| {
                if diff.is_zero() || !self.peek_timestamps.contains(time) {
                    return;
                }
                let time = self.peek_timestamps.advance(*time);
                if let Some((_, last_time, last_diff)) = self.extracted_updates.back_mut()
                    && *last_time == time
                {
                    last_diff.add_assign(*diff);
                    if last_diff.is_zero() {
                        self.extracted_updates.pop_back();
                    }
                } else {
                    self.extracted_updates
                        .push_back((result.clone(), time, *diff))
                }
            });

            Ok(())
        } else {
            Ok(())
        }
    }

    /// Steps the key forward, respecting literal constraints.
    ///
    /// Returns `true` if we are exhausted.
    fn step_key(&mut self) -> bool {
        assert!(
            !self.cursor.val_valid(&self.storage),
            "must only step key when the vals for a key are exhausted"
        );

        if let Some(literals) = &mut self.literals {
            literals.seek_next_literal_key(&mut self.cursor, &self.storage);

            if literals.is_exhausted() {
                return true;
            }
        } else {
            self.cursor.step_key(&self.storage);
        }

        if !self.cursor.key_valid(&self.storage) {
            // We're exhausted!
            return true;
        }

        assert!(
            self.cursor.val_valid(&self.storage),
            "there must always be at least one val per key"
        );

        false
    }
}
