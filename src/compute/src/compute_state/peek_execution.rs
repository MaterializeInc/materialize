// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Code for executing a peek against cluster/worker state.

use std::num::NonZeroUsize;
use std::ops::DerefMut;

use differential_dataflow::IntoOwned;
use differential_dataflow::trace::{Cursor, TraceReader};
use mz_ore::result::ResultExt;
use mz_repr::fixed_length::ToDatumIter;
use mz_repr::{DatumVec, Diff, Row, RowArena};
use timely::order::PartialOrder;

pub struct PeekResultIterator<Tr, L>
where
    Tr: TraceReader,
    L: Iterator<Item = Row>,
{
    exhausted: bool,
    cursor: <Tr as TraceReader>::Cursor,
    storage: <Tr as TraceReader>::Storage,
    map_filter_project: mz_expr::SafeMfpPlan,
    peek_timestamp: mz_repr::Timestamp,
    row_builder: Row,
    datum_vec: DatumVec,
    has_literal_constraints: bool,
    literals: L,
    current_literal: Option<Row>,
}

impl<Tr, L> PeekResultIterator<Tr, L>
where
    Tr: TraceReader,
    for<'a> Tr: TraceReader<DiffGat<'a> = &'a Diff>,
    for<'a> Tr::Key<'a>: ToDatumIter + IntoOwned<'a, Owned = Row> + Eq,
    for<'a> Tr::Val<'a>: ToDatumIter,
    for<'a> Tr::TimeGat<'a>: PartialOrder<mz_repr::Timestamp>,
    L: Iterator<Item = Row>,
{
    pub fn new(
        map_filter_project: mz_expr::SafeMfpPlan,
        peek_timestamp: mz_repr::Timestamp,
        has_literal_constraints: bool,
        literals: L,
        oks_handle: &mut Tr,
    ) -> Self {
        let (cursor, storage): (<Tr as TraceReader>::Cursor, <Tr as TraceReader>::Storage) =
            oks_handle.cursor();

        let mut result = Self {
            exhausted: false,
            cursor,
            storage,
            map_filter_project,
            peek_timestamp,
            row_builder: Row::default(),
            datum_vec: DatumVec::new(),
            has_literal_constraints,
            literals,
            current_literal: None,
        };

        if result.has_literal_constraints {
            result.seek_to_next_literal();
        }

        result
    }
}

impl<Tr, L> Iterator for PeekResultIterator<Tr, L>
where
    for<'a> Tr: TraceReader<DiffGat<'a> = &'a Diff>,
    for<'a> Tr::Key<'a>: ToDatumIter + IntoOwned<'a, Owned = Row> + Eq,
    for<'a> Tr::Val<'a>: ToDatumIter,
    for<'a> Tr::TimeGat<'a>: PartialOrder<mz_repr::Timestamp>,
    L: Iterator<Item = Row>,
{
    type Item = Result<(Row, NonZeroUsize), String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        if !self.cursor.key_valid(&self.storage) {
            return None;
        }

        if !self.cursor.val_valid(&self.storage) {
            let exhausted = self.step();
            if exhausted {
                return None;
            }
        }

        let result = loop {
            match self.extract_current_row() {
                Ok(Some(row)) => break Ok(row),
                Ok(None) => {
                    // Have to keep stepping.
                    let exhausted = self.step();
                    if exhausted {
                        return None;
                    }
                }
                Err(err) => break Err(err),
            }
        };

        self.step();

        Some(result)
    }
}

impl<Tr, L> PeekResultIterator<Tr, L>
where
    for<'a> Tr: TraceReader<DiffGat<'a> = &'a Diff>,
    for<'a> Tr::Key<'a>: ToDatumIter + IntoOwned<'a, Owned = Row> + Eq,
    for<'a> Tr::Val<'a>: ToDatumIter,
    for<'a> Tr::TimeGat<'a>: PartialOrder<mz_repr::Timestamp>,
    L: Iterator<Item = Row>,
{
    /// Extracts and returns the row currently pointed at by our cursor. Returns
    /// `Ok(None)` if our MapFilterProject evaluates to `None`. Also returns any
    /// errors that arise from evaluating the MapFilterProject.
    fn extract_current_row(&mut self) -> Result<Option<(Row, NonZeroUsize)>, String> {
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

        let mut borrow = self.datum_vec.borrow();
        borrow.extend(key);
        borrow.extend(row);

        if self.has_literal_constraints {
            // The peek was created from an IndexedFilter join. We have to add those columns
            // here that the join would add in a dataflow.
            let datum_vec = borrow.deref_mut();
            // unwrap is ok, because it could be None only if !has_literal_constraints or if
            // the iteration is finished. In the latter case we already exited the while
            // loop.
            datum_vec.extend(self.current_literal.as_ref().unwrap().iter());
        }
        if let Some(result) = self
            .map_filter_project
            .evaluate_into(&mut borrow, &arena, &mut self.row_builder)
            .map(|row| row.cloned())
            .map_err_to_string_with_causes()?
        {
            let mut copies = Diff::ZERO;
            self.cursor.map_times(&self.storage, |time, diff| {
                if time.less_equal(&self.peek_timestamp) {
                    copies += diff;
                }
            });
            let copies: usize = if copies.is_negative() {
                return Err(format!(
                    "Invalid data in source, saw retractions ({}) for row that does not exist: {:?}",
                    -copies, &*borrow,
                ));
            } else {
                copies.into_inner().try_into().unwrap()
            };
            // if copies > 0 ... otherwise skip
            if let Some(copies) = NonZeroUsize::new(copies) {
                Ok(Some((result, copies)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Steps our cursor forward, respecting literal constraints.
    ///
    /// Returns `true` if we are exhausted.
    fn step(&mut self) -> bool {
        self.cursor.step_val(&self.storage);

        if self.cursor.val_valid(&self.storage) {
            return false;
        }

        // Assumes that there is at least one val per key. Otherwise we'd have
        // to slap a loop around this and check that val_valid after stepping
        // they key.
        if !self.has_literal_constraints {
            self.cursor.step_key(&self.storage);
            !self.cursor.key_valid(&self.storage)
        } else {
            self.seek_to_next_literal();
            !self.cursor.key_valid(&self.storage)
        }
    }

    /// Seeks our cursor to the next literal constraint. If there are no more
    /// literal constraints, marks self as `exhausted`.
    fn seek_to_next_literal(&mut self) {
        loop {
            // Go to the next literal constraint.
            // (i.e., to the next OR argument in something like `c=3 OR c=7 OR c=9`)
            self.current_literal = self.literals.next();
            match &self.current_literal {
                None => {
                    // We ran out of literals, so we manually set exhausted
                    // to true so that we can early-return in `next()`.
                    self.exhausted = true;
                    return;
                }
                Some(current_literal) => {
                    // NOTE(vmarcos): We expect the extra allocations below to be manageable
                    // since we only perform as many of them as there are literals.
                    self.cursor
                        .seek_key(&self.storage, IntoOwned::borrow_as(current_literal));
                    if !self.cursor.key_valid(&self.storage) {
                        return;
                    }
                    if self.cursor.get_key(&self.storage).unwrap()
                        == IntoOwned::borrow_as(current_literal)
                    {
                        // The cursor found a record whose key matches the current literal.
                        // We return and calls to `next()` will start
                        // returning it's vals.
                        return;
                    }
                    // The cursor landed on a record that has a different key, meaning that there is
                    // no record whose key would match the current literal.
                }
            }
        }
    }
}
