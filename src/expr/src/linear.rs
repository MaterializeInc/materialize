// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use crate::{scalar::EvalError, ScalarExpr};
use repr::{Datum, Row, RowArena, RowPacker};

/// A compound operator that can be applied row-by-row.
///
/// This operator integrates the map, filter, and project operators.
/// It applies a sequences of map expressions, which are allowed to
/// refer to previous expressions, interleaved with predicates which
/// must be satisfied for an output to be produced. If all predicates
/// evaluate to `Datum::True` the data at the identified columns are
/// collected and produced as output in a packed `Row`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct MapFilterProject {
    /// A sequence of expressions that should be appended to the row.
    ///
    /// Many of these expressions may not be produced in the output,
    /// and may only be present as common subexpressions.
    pub expressions: Vec<ScalarExpr>,
    /// Expressions that must evaluate to `Datum::True` for the output
    /// row to be produced.
    ///
    /// Each entry is pre-pended with a column identifier indicating
    /// the column *before* which the predicate should first be applied.
    /// Most commonly this would be one plus the largest column identifier
    /// in the predicate's support, but it could be larger to implement
    /// guarded evaluation of predicates.
    ///
    /// This list should be sorted by the first field.
    pub predicates: Vec<(usize, ScalarExpr)>,
    /// A sequence of column identifiers whose data form the output row.
    pub projection: Vec<usize>,
    /// The expected number of input columns.
    ///
    /// This is needed to enure correct identification of newly formed
    /// columns in the output.
    pub input_arity: usize,
}

impl MapFilterProject {
    /// Create a no-op operator for an input of a supplied arity.
    pub fn new(input_arity: usize) -> Self {
        Self {
            expressions: Vec::new(),
            predicates: Vec::new(),
            projection: (0..input_arity).collect(),
            input_arity,
        }
    }

    /// Evaluates the linear operator on a supplied list of datums.
    ///
    /// The arguments are the initial datums associated with the row,
    /// and an appropriately lifetimed arena for temporary allocations
    /// needed by scalar evaluation.
    ///
    /// An `Ok` result will either be `None` if any predicate did not
    /// evaluate to `Datum::True`, or the values of the columns listed
    /// by `self.projection` if all predicates passed. If an error
    /// occurs in the evaluation it is returned as an `Err` variant.
    /// As the evaluation exits early with failed predicates, it may
    /// miss some errors that would occur later in evaluation.
    pub fn evaluate<'a>(
        &'a self,
        datums: &mut Vec<Datum<'a>>,
        arena: &'a RowArena,
        row_packer: &mut RowPacker,
    ) -> Result<Option<Row>, EvalError> {
        let mut expression = 0;
        for (support, predicate) in self.predicates.iter() {
            while self.input_arity + expression < *support {
                datums.push(self.expressions[expression].eval(&datums[..], &arena)?);
                expression += 1;
            }
            if predicate.eval(&datums[..], &arena)? != Datum::True {
                return Ok(None);
            }
        }
        row_packer.extend(self.projection.iter().map(|i| datums[*i]));
        Ok(Some(row_packer.finish_and_reuse()))
    }

    /// Retain only the indicated columns in the presented order.
    pub fn project<I>(&mut self, columns: I)
    where
        I: IntoIterator<Item = usize>,
    {
        self.projection = columns.into_iter().map(|c| self.projection[c]).collect();
    }

    /// Retain only rows satisfing these predicates.
    ///
    /// This method introduces predicates as eagerly as the can be evaluated,
    /// which may not be desired for predicates that may cause exceptions.
    /// If fine manipulation is required, the predicates can be added manually.
    pub fn filter<I>(&mut self, predicates: I)
    where
        I: IntoIterator<Item = ScalarExpr>,
    {
        for mut predicate in predicates {
            // Correct column references.
            predicate.permute(&self.projection[..]);
            // Insert predicate as eagerly as it can be evaluated:
            // just after the largest column in its support is formed.
            let max_support = predicate
                .support()
                .into_iter()
                .max()
                .map(|c| c + 1)
                .unwrap_or(0);
            self.predicates.push((max_support, predicate))
        }
        // Stable sort predicates by position at which they take effect.
        self.predicates
            .sort_by_key(|(position, _predicate)| *position);
    }

    /// Append the result of evaluating expressions to each row.
    pub fn map<I>(&mut self, expressions: I)
    where
        I: IntoIterator<Item = ScalarExpr>,
    {
        for mut expression in expressions {
            // Correct column references.
            expression.permute(&self.projection[..]);
            self.expressions.push(expression);
            self.projection
                .push(self.input_arity + self.expressions.len() - 1);
        }
    }

    /// As the arguments to `Map`, `Filter`, and `Project` operators.
    ///
    /// In principle, this operator can be implemented as a sequence of
    /// more elemental operators, likely less efficiently.
    pub fn as_map_filter_project(&self) -> (Vec<ScalarExpr>, Vec<ScalarExpr>, Vec<usize>) {
        let map = self.expressions.clone();
        let filter = self
            .predicates
            .iter()
            .map(|(_pos, predicate)| predicate.clone())
            .collect::<Vec<_>>();
        let project = self.projection.clone();
        (map, filter, project)
    }

    /// Optimize the internal expression evaluation order.
    pub fn optimize(&mut self) {
        // This should probably resemble existing scalar cse.
        unimplemented!()
    }
}
