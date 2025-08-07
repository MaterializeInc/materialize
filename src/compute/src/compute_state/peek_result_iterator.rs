// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Code for extracting a peek result out of compute state/an arrangement.

use std::iter::FusedIterator;
use std::num::NonZeroI64;
use std::ops::DerefMut;

use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::{Cursor, TraceReader};
use mz_ore::result::ResultExt;
use mz_repr::fixed_length::ToDatumIter;
use mz_repr::{DatumVec, Diff, GlobalId, Row, RowArena};
use timely::order::PartialOrder;

pub struct PeekResultIterator<Tr>
where
    Tr: TraceReader,
{
    // For debug/trace logging.
    target_id: GlobalId,
    literals_exhausted: bool,
    cursor: <Tr as TraceReader>::Cursor,
    storage: <Tr as TraceReader>::Storage,
    map_filter_project: mz_expr::SafeMfpPlan,
    peek_timestamp: mz_repr::Timestamp,
    row_builder: Row,
    datum_vec: DatumVec,
    has_literal_constraints: bool,
    literals: Box<dyn Iterator<Item = Row>>,
    current_literal: Option<Row>,
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
            TimeGat<'a>: PartialOrder<mz_repr::Timestamp>,
            DiffGat<'a> = &'a Diff,
        >,
{
    pub fn new(
        target_id: GlobalId,
        map_filter_project: mz_expr::SafeMfpPlan,
        peek_timestamp: mz_repr::Timestamp,
        mut literal_constraints: Option<Vec<Row>>,
        trace_reader: &mut Tr,
    ) -> Self {
        let (cursor, storage) = trace_reader.cursor();

        // We have to sort the literal constraints because cursor.seek_key can
        // seek only forward.
        if let Some(literal_constraints) = literal_constraints.as_mut() {
            literal_constraints.sort();
        }
        let has_literal_constraints = literal_constraints.is_some();
        let literals = literal_constraints.into_iter().flatten();

        let mut result = Self {
            target_id,
            literals_exhausted: false,
            cursor,
            storage,
            map_filter_project,
            peek_timestamp,
            row_builder: Row::default(),
            datum_vec: DatumVec::new(),
            has_literal_constraints,
            literals: Box::new(literals),
            current_literal: None,
        };

        if result.has_literal_constraints {
            result.seek_to_next_literal();
        }

        result
    }
}

impl<Tr> FusedIterator for PeekResultIterator<Tr> where
    for<'a> Tr: TraceReader<
            Key<'a>: ToDatumIter + Eq,
            KeyOwn = Row,
            Val<'a>: ToDatumIter,
            TimeGat<'a>: PartialOrder<mz_repr::Timestamp>,
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
            TimeGat<'a>: PartialOrder<mz_repr::Timestamp>,
            DiffGat<'a> = &'a Diff,
        >,
{
    type Item = Result<(Row, NonZeroI64), String>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = loop {
            if self.literals_exhausted {
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
                Ok(Some(row)) => break Ok(row),
                Ok(None) => {
                    // Have to keep stepping and try with the next val.
                    self.cursor.step_val(&self.storage);
                }
                Err(err) => break Err(err),
            }
        };

        self.cursor.step_val(&self.storage);

        Some(result)
    }
}

impl<Tr> PeekResultIterator<Tr>
where
    for<'a> Tr: TraceReader<
            Key<'a>: ToDatumIter + Eq,
            KeyOwn = Row,
            Val<'a>: ToDatumIter,
            TimeGat<'a>: PartialOrder<mz_repr::Timestamp>,
            DiffGat<'a> = &'a Diff,
        >,
{
    /// Extracts and returns the row currently pointed at by our cursor. Returns
    /// `Ok(None)` if our MapFilterProject evaluates to `None`. Also returns any
    /// errors that arise from evaluating the MapFilterProject.
    fn extract_current_row(&mut self) -> Result<Option<(Row, NonZeroI64)>, String> {
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
            let copies: i64 = if copies.is_negative() {
                let row = &*borrow;
                tracing::error!(
                    target = %self.target_id, diff = %copies, ?row,
                    "index peek encountered negative multiplicities in ok trace",
                );
                return Err(format!(
                    "Invalid data in source, \
                             saw retractions ({}) for row that does not exist: {:?}",
                    -copies, row,
                ));
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

    /// Steps the key forward, respecting literal constraints.
    ///
    /// Returns `true` if we are exhausted.
    fn step_key(&mut self) -> bool {
        assert!(
            !self.cursor.val_valid(&self.storage),
            "must only step key when the vals for a key are exhausted"
        );

        if !self.has_literal_constraints {
            self.cursor.step_key(&self.storage);
        } else {
            self.seek_to_next_literal();

            if self.literals_exhausted {
                return true;
            }
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

    /// Seeks our cursor to the next literal constraint. If there are no more
    /// literal constraints, marks self as `exhausted`.
    fn seek_to_next_literal(&mut self) {
        loop {
            // Go to the next literal constraint.
            // (i.e., to the next OR argument in something like `c=3 OR c=7 OR c=9`)
            self.current_literal = self.literals.next();
            match &self.current_literal {
                None => {
                    // We ran out of literals, so we set literals_exhausted to
                    // true so that we can early-return in `next()`.
                    self.literals_exhausted = true;
                    return;
                }
                Some(current_literal) => {
                    let mut key_con = Tr::KeyContainer::with_capacity(1);
                    key_con.push_own(current_literal);
                    let current_literal = key_con.get(0).unwrap();
                    // NOTE(vmarcos): We expect the extra allocations below to be manageable
                    // since we only perform as many of them as there are literals.
                    self.cursor.seek_key(&self.storage, current_literal);

                    if self
                        .cursor
                        .get_key(&self.storage)
                        .map_or(true, |key| key == current_literal)
                    {
                        // The cursor found a record whose key matches the
                        // current literal, or we have no more keys and are
                        // therefore exhausted.

                        // We return and calls to `next()` will start
                        // returning its vals.
                        return;
                    }
                    // The cursor landed on a record that has a different key, meaning that there is
                    // no record whose key would match the current literal.
                }
            }
        }
    }
}
