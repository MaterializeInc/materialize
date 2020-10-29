// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use crate::{scalar::EvalError, RelationExpr, ScalarExpr};
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

    /// True if the operator describes the identity transformation.
    pub fn is_identity(&self) -> bool {
        self.expressions.is_empty()
            && self.predicates.is_empty()
            && self.projection.len() == self.input_arity
            && self.projection.iter().enumerate().all(|(i, p)| i == *p)
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
        self.evaluate_iter(datums, arena).map(|result| {
            result.map(|result| {
                row_packer.extend(result);
                row_packer.finish_and_reuse()
            })
        })
    }

    /// A version of `evaluate` which produces an iterator over `Datum`
    /// as output.
    ///
    /// This version is used internally by `evaluate` and can be useful
    /// when one wants to capture the resulting datums without packing
    /// and then unpacking a row.
    pub fn evaluate_iter<'b, 'a: 'b>(
        &'a self,
        datums: &'b mut Vec<Datum<'a>>,
        arena: &'a RowArena,
    ) -> Result<Option<impl Iterator<Item = Datum<'a>> + 'b>, EvalError> {
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
        while expression < self.expressions.len() {
            datums.push(self.expressions[expression].eval(&datums[..], &arena)?);
            expression += 1;
        }
        Ok(Some(self.projection.iter().map(move |i| datums[*i])))
    }

    /// Retain only the indicated columns in the presented order.
    pub fn project<I>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = usize> + std::fmt::Debug,
    {
        self.projection = columns.into_iter().map(|c| self.projection[c]).collect();
        self
    }

    /// Retain only rows satisfing these predicates.
    ///
    /// This method introduces predicates as eagerly as the can be evaluated,
    /// which may not be desired for predicates that may cause exceptions.
    /// If fine manipulation is required, the predicates can be added manually.
    pub fn filter<I>(mut self, predicates: I) -> Self
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
        self
    }

    /// Append the result of evaluating expressions to each row.
    pub fn map<I>(mut self, expressions: I) -> Self
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
        self
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

    /// Determines if a scalar expression must be equal to a literal datum.
    pub fn literal_constraint(&self, expr: &ScalarExpr) -> Option<Datum> {
        for (_pos, predicate) in self.predicates.iter() {
            if let ScalarExpr::CallBinary {
                func: crate::BinaryFunc::Eq,
                expr1,
                expr2,
            } = predicate
            {
                if let Some(Ok(datum1)) = expr1.as_literal() {
                    if &**expr2 == expr {
                        return Some(datum1);
                    }
                }
                if let Some(Ok(datum2)) = expr2.as_literal() {
                    if &**expr1 == expr {
                        return Some(datum2);
                    }
                }
            }
        }
        None
    }

    /// Determines if a sequence of scalar expressions must be equal to a literal row.
    ///
    /// This method returns `None` on an empty `exprs`, which might be surprising, but
    /// seems to line up with its callers' expectations of that being a non-constraint.
    /// The caller knows if `exprs` is empty, and can modify their behavior appopriately.
    /// if they would rather have a literal empty row.
    pub fn literal_constraints(&self, exprs: &[ScalarExpr]) -> Option<Row> {
        if exprs.is_empty() {
            return None;
        }
        let mut row_packer = RowPacker::new();
        for expr in exprs {
            if let Some(literal) = self.literal_constraint(expr) {
                row_packer.push(literal);
            } else {
                return None;
            }
        }
        Some(row_packer.finish_and_reuse())
    }

    /// Extracts any MapFilterProject at the root of the expression.
    ///
    /// The expression will be modified to extract any maps, filters, and
    /// projections, which will be return as `Self`. If there are no maps,
    /// filters, or projections the method will return an identity operator.
    pub fn extract_from_expression(expr: &RelationExpr) -> (Self, &RelationExpr) {
        // TODO: This could become iterative rather than recursive if
        // we were able to fuse MFP operators from below, rather than
        // from above.
        match expr {
            RelationExpr::Map { input, scalars } => {
                let (mfp, expr) = Self::extract_from_expression(input);
                (mfp.map(scalars.iter().cloned()), expr)
            }
            RelationExpr::Filter { input, predicates } => {
                let (mfp, expr) = Self::extract_from_expression(input);
                (mfp.filter(predicates.iter().cloned()), expr)
            }
            RelationExpr::Project { input, outputs } => {
                let (mfp, expr) = Self::extract_from_expression(input);
                (mfp.project(outputs.iter().cloned()), expr)
            }
            x => (Self::new(x.arity()), x),
        }
    }

    /// Optimize the internal expression evaluation order.
    pub fn optimize(&mut self) {
        // This should probably resemble existing scalar cse.
        unimplemented!()
    }
}
