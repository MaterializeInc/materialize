// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::{scalar::EvalError, MirRelationExpr, MirScalarExpr};
use repr::{Datum, Row, RowArena};

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
    pub expressions: Vec<MirScalarExpr>,
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
    pub predicates: Vec<(usize, MirScalarExpr)>,
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
    #[inline(always)]
    pub fn evaluate<'a>(
        &'a self,
        datums: &mut Vec<Datum<'a>>,
        arena: &'a RowArena,
    ) -> Result<Option<Row>, EvalError> {
        let passed_predicates = self.evaluate_inner(datums, arena)?;
        if !passed_predicates {
            Ok(None)
        } else {
            // We determine the capacity first, to ensure that we right-size
            // the row allocation and need not re-allocate once it is formed.
            let capacity = repr::datums_size(self.projection.iter().map(|c| datums[*c]));
            let mut row = Row::with_capacity(capacity);
            row.extend(self.projection.iter().map(|c| datums[*c]));
            Ok(Some(row))
        }
    }

    /// A version of `evaluate` which produces an iterator over `Datum`
    /// as output.
    ///
    /// This version is used internally by `evaluate` and can be useful
    /// when one wants to capture the resulting datums without packing
    /// and then unpacking a row.
    #[inline(always)]
    pub fn evaluate_iter<'b, 'a: 'b>(
        &'a self,
        datums: &'b mut Vec<Datum<'a>>,
        arena: &'a RowArena,
    ) -> Result<Option<impl Iterator<Item = Datum<'a>> + 'b>, EvalError> {
        let passed_predicates = self.evaluate_inner(datums, arena)?;
        if !passed_predicates {
            Ok(None)
        } else {
            Ok(Some(self.projection.iter().map(move |i| datums[*i])))
        }
    }

    /// Populates `datums` with `self.expressions` and tests `self.predicates`.
    ///
    /// This does not apply `self.projection`, which is up to the calling method.
    fn evaluate_inner<'b, 'a: 'b>(
        &'a self,
        datums: &'b mut Vec<Datum<'a>>,
        arena: &'a RowArena,
    ) -> Result<bool, EvalError> {
        let mut expression = 0;
        for (support, predicate) in self.predicates.iter() {
            while self.input_arity + expression < *support {
                datums.push(self.expressions[expression].eval(&datums[..], &arena)?);
                expression += 1;
            }
            if predicate.eval(&datums[..], &arena)? != Datum::True {
                return Ok(false);
            }
        }
        while expression < self.expressions.len() {
            datums.push(self.expressions[expression].eval(&datums[..], &arena)?);
            expression += 1;
        }
        Ok(true)
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
    /// This method introduces predicates as eagerly as they can be evaluated,
    /// which may not be desired for predicates that may cause exceptions.
    /// If fine manipulation is required, the predicates can be added manually.
    pub fn filter<I>(mut self, predicates: I) -> Self
    where
        I: IntoIterator<Item = MirScalarExpr>,
    {
        for mut predicate in predicates {
            // Correct column references.
            predicate.permute(&self.projection[..]);

            // Validate column references.
            assert!(predicate
                .support()
                .into_iter()
                .all(|c| c < self.input_arity + self.expressions.len()));

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
        // We put literal errors at the end as a stop-gap to avoid erroring
        // before we are able to evaluate any predicates that might prevent it.
        self.predicates
            .sort_by_key(|(position, predicate)| (predicate.is_literal_err(), *position));
        self
    }

    /// Append the result of evaluating expressions to each row.
    pub fn map<I>(mut self, expressions: I) -> Self
    where
        I: IntoIterator<Item = MirScalarExpr>,
    {
        for mut expression in expressions {
            // Correct column references.
            expression.permute(&self.projection[..]);

            // Validate column references.
            assert!(expression
                .support()
                .into_iter()
                .all(|c| c < self.input_arity + self.expressions.len()));

            // Introduce expression and produce as output.
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
    pub fn as_map_filter_project(&self) -> (Vec<MirScalarExpr>, Vec<MirScalarExpr>, Vec<usize>) {
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
    pub fn literal_constraint(&self, expr: &MirScalarExpr) -> Option<Datum> {
        for (_pos, predicate) in self.predicates.iter() {
            if let MirScalarExpr::CallBinary {
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
    pub fn literal_constraints(&self, exprs: &[MirScalarExpr]) -> Option<Row> {
        if exprs.is_empty() {
            return None;
        }
        let mut row = Row::default();
        for expr in exprs {
            if let Some(literal) = self.literal_constraint(expr) {
                row.push(literal);
            } else {
                return None;
            }
        }
        Some(row)
    }

    /// Extracts any MapFilterProject at the root of the expression.
    ///
    /// The expression will be modified to extract any maps, filters, and
    /// projections, which will be return as `Self`. If there are no maps,
    /// filters, or projections the method will return an identity operator.
    ///
    /// This method needs to avoid extracting predicates that contain any
    /// temporal operators, per `MirScalarExpr::contains_temporal()`.
    pub fn extract_from_expression(expr: &MirRelationExpr) -> (Self, &MirRelationExpr) {
        // TODO: This could become iterative rather than recursive if
        // we were able to fuse MFP operators from below, rather than
        // from above.
        match expr {
            MirRelationExpr::Map { input, scalars } => {
                let (mfp, expr) = Self::extract_from_expression(input);
                (mfp.map(scalars.iter().cloned()), expr)
            }
            MirRelationExpr::Filter { input, predicates } => {
                if predicates.iter().any(|p| p.contains_temporal()) {
                    (Self::new(expr.arity()), expr)
                } else {
                    let (mfp, expr) = Self::extract_from_expression(input);
                    (mfp.filter(predicates.iter().cloned()), expr)
                }
            }
            MirRelationExpr::Project { input, outputs } => {
                let (mfp, expr) = Self::extract_from_expression(input);
                (mfp.project(outputs.iter().cloned()), expr)
            }
            x => (Self::new(x.arity()), x),
        }
    }
}

impl MapFilterProject {
    /// Partitions `self` into two instances, one of which can be eagerly applied.
    ///
    /// The `available` argument indicates which input columns are available (keys)
    /// and in which positions (values). This information may allow some maps and
    /// filters to execute. The `input_arity` argument reports the total number of
    /// input columns (which may include some not present in `available`)
    ///
    /// This method partitions `self` in two parts, `(before, after)`, where `before`
    /// can be applied on columns present as keys in `available`, and `after` must
    /// await the introduction of the other input columns.
    ///
    /// The `before` instance will *append* any columns that can be determined from
    /// `available` but will project away any of these columns that are not needed by
    /// `after`. Importantly, this means that `before` will leave intact *all* input
    /// columns including those not referenced in `available`.
    ///
    /// The `after` instance will presume all input columns are available, followed
    /// by the appended columns of the `before` instance. It may be that some input
    /// columns can be projected away in `before` if `after` does not need them, but
    /// we leave that as something the caller can apply if needed (it is otherwise
    /// complicated to negotiate which input columns `before` should retain).
    ///
    /// To correctly reconstruct `self` from `before` and `after`, one must introduce
    /// additional input columns, permute all input columns to their locations as
    /// expected by `self`, follow this by new columns appended by `before`, and
    /// remove all other columns that may be present.
    ///
    /// # Example
    ///
    /// ```rust
    /// use expr::{BinaryFunc, MapFilterProject, MirScalarExpr};
    ///
    /// // imagine an action on columns (a, b, c, d).
    /// let original = MapFilterProject::new(4).map(vec![
    ///    MirScalarExpr::column(0).call_binary(MirScalarExpr::column(1), BinaryFunc::AddInt64),
    ///    MirScalarExpr::column(2).call_binary(MirScalarExpr::column(4), BinaryFunc::AddInt64),
    ///    MirScalarExpr::column(3).call_binary(MirScalarExpr::column(5), BinaryFunc::AddInt64),
    /// ]).project(vec![6]);
    ///
    /// // Imagine we start with columns (b, x, a, y, c).
    /// //
    /// // The `partition` method requires a map from *expected* input columns to *actual*
    /// // input columns. In the example above, the columns a, b, and c exist, and are at
    /// // locations 2, 0, and 4 respectively. We must construct a map to this effect.
    /// let mut available_columns = std::collections::HashMap::new();
    /// available_columns.insert(0, 2);
    /// available_columns.insert(1, 0);
    /// available_columns.insert(2, 4);
    /// // Partition `original` using the available columns and current input arity.
    /// // This informs `partition` which columns are available, where they can be found,
    /// // and how many columns are not relevant but should be preserved.
    /// let (before, after) = original.partition(&available_columns, 5);
    ///
    /// // `before` sees all five input columns, and should append `a + b + c`.
    /// assert_eq!(before, MapFilterProject::new(5).map(vec![
    ///    MirScalarExpr::column(2).call_binary(MirScalarExpr::column(0), BinaryFunc::AddInt64),
    ///    MirScalarExpr::column(4).call_binary(MirScalarExpr::column(5), BinaryFunc::AddInt64),
    /// ]).project(vec![0, 1, 2, 3, 4, 6]));
    ///
    /// // `after` expects to see `(a, b, c, d, a + b + c)`.
    /// assert_eq!(after, MapFilterProject::new(5).map(vec![
    ///    MirScalarExpr::column(3).call_binary(MirScalarExpr::column(4), BinaryFunc::AddInt64)
    /// ]).project(vec![5]));
    ///
    /// // To reconstruct `self`, we must introduce the columns that are not present,
    /// // and present them in the order intended by `self`. In this example, we must
    /// // introduce colunm d and permute the columns so that they begin (a, b, c, d).
    /// // The columns x and y must be projected away, and any columns introduced by
    /// // `begin` must be retained in their current order.
    ///
    /// // The `after` instance expects to be provided with all inputs, but it
    /// // may not need all inputs. The `demand()` and `permute()` methods can
    /// // optimize the representation.
    /// ```
    pub fn partition(self, available: &HashMap<usize, usize>, input_arity: usize) -> (Self, Self) {
        // Map expressions, filter predicates, and projections for `before` and `after`.
        let mut before_expr = Vec::new();
        let mut before_pred = Vec::new();
        let mut before_proj = Vec::new();
        let mut after_expr = Vec::new();
        let mut after_pred = Vec::new();
        let mut after_proj = Vec::new();

        // Track which output columns must be preserved in the output of `before`.
        let mut demanded = HashSet::new();
        demanded.extend(0..self.input_arity);
        demanded.extend(self.projection.iter());

        // Determine which map expressions can be computed from the available subset.
        // Some expressions may depend on other expressions, but by evaluating them
        // in forward order we should accurately determine the available expressions.
        let mut available_expr = vec![false; self.input_arity];
        // Initialize available columns from `available`, which is then not used again.
        for index in available.keys() {
            available_expr[*index] = true;
        }
        for expr in self.expressions.into_iter() {
            // We treat an expression as available if its supporting columns are available,
            // and if it is not a literal (we want to avoid pushing down literals). This
            // choice is ad-hoc, but the intent is that we partition the operators so
            // that we can reduce the row representation size and total computation.
            // Pushing down literals harms the former and does nothing for the latter.
            // In the future, we'll want to have a harder think about this trade-off, as
            // we are certainly making sub-optimal decisions by pushing down all available
            // work.
            // TODO(mcsherry): establish better principles about what work to push down.
            let is_available =
                expr.support().into_iter().all(|i| available_expr[i]) && !expr.is_literal();
            if is_available {
                before_expr.push(expr);
            } else {
                demanded.extend(expr.support());
                after_expr.push(expr);
            }
            available_expr.push(is_available);
        }

        // Determine which predicates can be computed from the available subset.
        for (_when, pred) in self.predicates.into_iter() {
            let is_available = pred.support().into_iter().all(|i| available_expr[i]);
            if is_available {
                before_pred.push(pred);
            } else {
                demanded.extend(pred.support());
                after_pred.push(pred);
            }
        }

        // Map from prior output location to location in un-projected `before`.
        // This map is used to correct references in `before` but it should be
        // adjusted to reflect `before`s projection prior to use in `after`.
        let mut before_map = available.clone();
        // Input columns include any additional undescribed columns that may
        // not be captured by the `available` argument, so we must independently
        // track the current number of columns (vs relying on `before_map.len()`).
        let mut input_columns = input_arity;
        for index in self.input_arity..available_expr.len() {
            if available_expr[index] {
                before_map.insert(index, input_columns);
                input_columns += 1;
            }
        }

        // Permute the column references in `before` expressions and predicates.
        for expr in before_expr.iter_mut() {
            expr.permute_map(&before_map);
        }
        for pred in before_pred.iter_mut() {
            pred.permute_map(&before_map);
        }

        // Demand information determines `before`s output projection.
        // Specifically, we produce all input columns in the output, as well as
        // any columns that are available and demanded.
        before_proj.extend(0..input_arity);
        for index in self.input_arity..available_expr.len() {
            // If an intermediate result is both available and demanded,
            // we should produce it as output.
            if available_expr[index] && demanded.contains(&index) {
                // Use the new location of `index`.
                before_proj.push(before_map[&index]);
            }
        }

        // Map from prior output locations to location in post-`before` columns.
        // This map is used to correct references in `after`.
        // The presumption is that `after` will be presented with all input columns,
        // followed by the output columns introduced by `before` in order.
        let mut after_map = HashMap::new();
        for index in 0..self.input_arity {
            after_map.insert(index, index);
        }
        for index in self.input_arity..available_expr.len() {
            // If an intermediate result is both available and demanded,
            // it was produced as output.
            if available_expr[index] && demanded.contains(&index) {
                // We expect to find the output as far after `self.input_arity` as
                // it was produced after `input_arity` in the output of `before`.
                let location = self.input_arity
                    + (before_proj
                        .iter()
                        .position(|x| x == &before_map[&index])
                        .unwrap()
                        - input_arity);
                after_map.insert(index, location);
            }
        }
        // We must now re-map the remaining non-demanded expressions, which are
        // contiguous rather than potentially interspersed.
        for index in self.input_arity..available_expr.len() {
            if !available_expr[index] {
                after_map.insert(index, after_map.len());
            }
        }

        // Permute the column references in `after` expressions and predicates.
        for expr in after_expr.iter_mut() {
            expr.permute_map(&after_map);
        }
        for pred in after_pred.iter_mut() {
            pred.permute_map(&after_map);
        }
        // Populate `after` projection with the new locations of `self.projection`.
        for index in self.projection {
            after_proj.push(after_map[&index]);
        }

        // Form and return the before and after MapFilterProject instances.
        let before = Self::new(input_arity)
            .map(before_expr)
            .filter(before_pred)
            .project(before_proj.clone());
        let after = Self::new(self.input_arity + (before_proj.len() - input_arity))
            .map(after_expr)
            .filter(after_pred)
            .project(after_proj);
        (before, after)
    }

    /// Lists input columns whose values are used in outputs.
    ///
    /// It is entirely appropriate to determine the demand of an instance
    /// and then both apply a projection to the subject of the instance and
    /// `self.permute` this instance.
    pub fn demand(&self) -> HashSet<usize> {
        let mut demanded = HashSet::new();
        for (_index, pred) in self.predicates.iter() {
            demanded.extend(pred.support());
        }
        demanded.extend(self.projection.iter().cloned());
        for index in (0..self.expressions.len()).rev() {
            if demanded.contains(&(self.input_arity + index)) {
                demanded.extend(self.expressions[index].support());
            }
        }
        demanded.retain(|col| col < &self.input_arity);
        demanded
    }

    /// Update input column references, due to an input projection or permutation.
    ///
    /// The `shuffle` argument remaps expected column identifiers to new locations,
    /// with the expectation that `shuffle` describes all input columns, and so the
    /// intermediate results will be able to start at position `shuffle.len()`.
    ///
    /// The supplied `shuffle` may not list columns that are not "demanded" by the
    /// instance, and so we should ensure that `self` is optimized to not reference
    /// columns that are not demanded.
    pub fn permute(&mut self, shuffle: &HashMap<usize, usize>, new_input_arity: usize) {
        self.remove_undemanded();
        let mut shuffle = shuffle.clone();
        let (mut map, mut filter, mut project) = self.as_map_filter_project();
        for index in 0..map.len() {
            // Intermediate columns are just shifted.
            shuffle.insert(self.input_arity + index, new_input_arity + index);
        }
        for expr in map.iter_mut() {
            expr.permute_map(&shuffle);
        }
        for pred in filter.iter_mut() {
            pred.permute_map(&shuffle);
        }
        for proj in project.iter_mut() {
            *proj = shuffle[proj];
        }
        *self = Self::new(new_input_arity)
            .map(map)
            .filter(filter)
            .project(project)
    }
}

// Optimization routines.
impl MapFilterProject {
    /// Optimize the internal expression evaluation order.
    ///
    /// This method performs several optimizations that are meant to streamline
    /// the execution of the `MapFilterProject` instance, but not to alter its
    /// semantics. This includes extracting expressions that are used multiple
    /// times, inlining those that are not, and removing expressions that are
    /// unreferenced.
    ///
    /// # Example
    ///
    /// This example demonstrates how the re-use of one expression, converting
    /// column 1 from a string to an integer, can be extracted and the results
    /// shared among the two uses. This example is used for each of the steps
    /// along the optimization path.
    ///
    /// ```rust
    /// use expr::{MapFilterProject, MirScalarExpr, UnaryFunc, BinaryFunc};
    /// // Demonstrate extraction of common expressions (here: parsing strings).
    /// let mut map_filter_project = MapFilterProject::new(5)
    ///     .map(vec![
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64).call_binary(MirScalarExpr::column(1).call_unary(UnaryFunc::CastStringToInt64), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(1).call_unary(UnaryFunc::CastStringToInt64).call_binary(MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64), BinaryFunc::AddInt64),
    ///     ])
    ///     .project(vec![3,4,5,6]);
    ///
    /// let mut expected_optimized = MapFilterProject::new(5)
    ///     .map(vec![
    ///         MirScalarExpr::column(1).call_unary(UnaryFunc::CastStringToInt64),
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64).call_binary(MirScalarExpr::column(5), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(5).call_binary(MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64), BinaryFunc::AddInt64),
    ///     ])
    ///     .project(vec![3,4,6,7]);
    ///
    /// // Optimize the expression.
    /// map_filter_project.optimize();
    ///
    /// assert_eq!(
    ///     map_filter_project,
    ///     expected_optimized,
    /// );
    /// ```
    pub fn optimize(&mut self) {
        // Optimization memoizes individual `ScalarExpr` expressions that
        // are sure to be evaluated, canonicalizes references to the first
        // occurrence of each, inlines expressions that have a reference
        // count of one, and then removes any expressions that are not
        // referenced.
        self.memoize_expressions();
        self.inline_expressions();
        self.remove_undemanded();

        // Re-build `self` from parts to restore evaluation order invariants.
        let (map, filter, project) = self.as_map_filter_project();
        *self = Self::new(self.input_arity)
            .map(map)
            .filter(filter)
            .project(project);
    }

    /// Place each certainly evaluated expression in its own column.
    ///
    /// This method places each non-trivial, certainly evaluated expression
    /// in its own column, and deduplicates them so that all references to
    /// the same expression reference the same column.
    ///
    /// This tranformation is restricted to expressions we are certain will
    /// be evaluated, which does not include expressions in `if` statements.
    ///
    /// # Example
    ///
    /// This example demonstrates how memoization notices `MirScalarExpr`s
    /// that are used multiple times, and ensures that each are extracted
    /// into columns and then referenced by column. This pass does not try
    /// to minimize the occurrences of column references, which will happen
    /// in inliniing.
    ///
    /// ```rust
    /// use expr::{MapFilterProject, MirScalarExpr, UnaryFunc, BinaryFunc};
    /// // Demonstrate extraction of common expressions (here: parsing strings).
    /// let mut map_filter_project = MapFilterProject::new(5)
    ///     .map(vec![
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64).call_binary(MirScalarExpr::column(1).call_unary(UnaryFunc::CastStringToInt64), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(1).call_unary(UnaryFunc::CastStringToInt64).call_binary(MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64), BinaryFunc::AddInt64),
    ///     ])
    ///     .project(vec![3,4,5,6]);
    ///
    /// let mut expected_optimized = MapFilterProject::new(5)
    ///     .map(vec![
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64),
    ///         MirScalarExpr::column(1).call_unary(UnaryFunc::CastStringToInt64),
    ///         MirScalarExpr::column(5).call_binary(MirScalarExpr::column(6), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(7),
    ///         MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64),
    ///         MirScalarExpr::column(6).call_binary(MirScalarExpr::column(9), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(10),
    ///     ])
    ///     .project(vec![3,4,8,11]);
    ///
    /// // Memoize expressions, ensuring uniqueness of each `MirScalarExpr`.
    /// map_filter_project.memoize_expressions();
    ///
    /// assert_eq!(
    ///     map_filter_project,
    ///     expected_optimized,
    /// );
    /// ```
    ///
    /// Expressions may not be memoized if they are not certain to be evaluated,
    /// for example if they occur in conditional branches of a `MirScalarExpr::If`.
    ///
    /// ```rust
    /// use expr::{MapFilterProject, MirScalarExpr, UnaryFunc, BinaryFunc};
    /// // Demonstrate extraction of unconditionally evaluated expressions, as well as
    /// // the non-extraction of common expressions guarded by conditions.
    /// let mut map_filter_project = MapFilterProject::new(2)
    ///     .map(vec![
    ///         MirScalarExpr::If {
    ///             cond: Box::new(MirScalarExpr::column(0).call_binary(MirScalarExpr::column(1), BinaryFunc::Lt)),
    ///             then: Box::new(MirScalarExpr::column(0).call_binary(MirScalarExpr::column(1), BinaryFunc::DivInt64)),
    ///             els:  Box::new(MirScalarExpr::column(1).call_binary(MirScalarExpr::column(0), BinaryFunc::DivInt64)),
    ///         },
    ///         MirScalarExpr::If {
    ///             cond: Box::new(MirScalarExpr::column(0).call_binary(MirScalarExpr::column(1), BinaryFunc::Lt)),
    ///             then: Box::new(MirScalarExpr::column(1).call_binary(MirScalarExpr::column(0), BinaryFunc::DivInt64)),
    ///             els:  Box::new(MirScalarExpr::column(0).call_binary(MirScalarExpr::column(1), BinaryFunc::DivInt64)),
    ///         },
    ///     ]);
    ///
    /// let mut expected_optimized = MapFilterProject::new(2)
    ///     .map(vec![
    ///         MirScalarExpr::column(0).call_binary(MirScalarExpr::column(1), BinaryFunc::Lt),
    ///         MirScalarExpr::If {
    ///             cond: Box::new(MirScalarExpr::column(2)),
    ///             then: Box::new(MirScalarExpr::column(0).call_binary(MirScalarExpr::column(1), BinaryFunc::DivInt64)),
    ///             els:  Box::new(MirScalarExpr::column(1).call_binary(MirScalarExpr::column(0), BinaryFunc::DivInt64)),
    ///         },
    ///         MirScalarExpr::column(3),
    ///         MirScalarExpr::If {
    ///             cond: Box::new(MirScalarExpr::column(2)),
    ///             then: Box::new(MirScalarExpr::column(1).call_binary(MirScalarExpr::column(0), BinaryFunc::DivInt64)),
    ///             els:  Box::new(MirScalarExpr::column(0).call_binary(MirScalarExpr::column(1), BinaryFunc::DivInt64)),
    ///         },
    ///         MirScalarExpr::column(5),
    ///     ])
    ///     .project(vec![0,1,4,6]);
    ///
    /// // Memoize expressions, ensuring uniqueness of each `MirScalarExpr`.
    /// map_filter_project.memoize_expressions();
    ///
    /// assert_eq!(
    ///     map_filter_project,
    ///     expected_optimized,
    /// );
    /// ```
    pub fn memoize_expressions(&mut self) {
        // Record the mapping from starting column references to new column
        // references.
        let mut remaps = HashMap::new();
        for index in 0..self.input_arity {
            remaps.insert(index, index);
        }
        let mut new_expressions = Vec::new();

        // We follow the same order as for evaluation, to ensure that all
        // column references exist in time for their evaluation. We could
        // prioritize predicates, but we would need to be careful to chase
        // down column references to expressions and memoize those as well.
        let mut expression = 0;
        for (support, predicate) in self.predicates.iter_mut() {
            while self.input_arity + expression < *support {
                self.expressions[expression].permute_map(&remaps);
                memoize_expr(
                    &mut self.expressions[expression],
                    &mut new_expressions,
                    self.input_arity,
                );
                remaps.insert(
                    self.input_arity + expression,
                    self.input_arity + new_expressions.len(),
                );
                new_expressions.push(self.expressions[expression].clone());
                expression += 1;
            }
            predicate.permute_map(&remaps);
            memoize_expr(predicate, &mut new_expressions, self.input_arity);
        }
        while expression < self.expressions.len() {
            self.expressions[expression].permute_map(&remaps);
            memoize_expr(
                &mut self.expressions[expression],
                &mut new_expressions,
                self.input_arity,
            );
            remaps.insert(
                self.input_arity + expression,
                self.input_arity + new_expressions.len(),
            );
            new_expressions.push(self.expressions[expression].clone());
            expression += 1;
        }

        self.expressions = new_expressions;
        for proj in self.projection.iter_mut() {
            *proj = remaps[proj];
        }
    }

    /// This method inlines expressions with a single use.
    ///
    /// This method only inlines expressions; it does not delete expressions
    /// that are no longer referenced. The `remove_undemanded()` method does
    /// that, and should ilkely be used after this method.
    ///
    /// Inlining replaces column references when the refered-to item is either
    /// another column reference, or the only referrer of its referent. This
    /// is most common after memoization has atomized all expressions to seek
    /// out re-use: inlining re-assembles expressions that were not helpfully
    /// shared with other expressions.
    ///
    /// # Example
    ///
    /// In this example, we see that with only a single reference to columns
    /// 0 and 2, their parsing can each be inlined. Similarly, column references
    /// can be cleaned up among expressions, and in the final projection.
    ///
    /// Also notice the remaining expressions, which can be cleaned up in a later
    /// pass (the `remove_undemanded` method).
    ///
    /// ```rust
    /// use expr::{MapFilterProject, MirScalarExpr, UnaryFunc, BinaryFunc};
    /// // Use the output from first `memoize_expression` example.
    /// let mut map_filter_project = MapFilterProject::new(5)
    ///     .map(vec![
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64),
    ///         MirScalarExpr::column(1).call_unary(UnaryFunc::CastStringToInt64),
    ///         MirScalarExpr::column(5).call_binary(MirScalarExpr::column(6), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(7),
    ///         MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64),
    ///         MirScalarExpr::column(6).call_binary(MirScalarExpr::column(9), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(10),
    ///     ])
    ///     .project(vec![3,4,8,11]);
    ///
    /// let mut expected_optimized = MapFilterProject::new(5)
    ///     .map(vec![
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64),
    ///         MirScalarExpr::column(1).call_unary(UnaryFunc::CastStringToInt64),
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64).call_binary(MirScalarExpr::column(6), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64).call_binary(MirScalarExpr::column(6), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64),
    ///         MirScalarExpr::column(6).call_binary(MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(6).call_binary(MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64), BinaryFunc::AddInt64),
    ///     ])
    ///     .project(vec![3,4,8,11]);
    ///
    /// // Inline expressions that are referenced only once.
    /// map_filter_project.inline_expressions();
    ///
    /// assert_eq!(
    ///     map_filter_project,
    ///     expected_optimized,
    /// );
    /// ```
    pub fn inline_expressions(&mut self) {
        // Local copy of input_arity to avoid borrowing `self` in closures.
        let input_arity = self.input_arity;
        // Reference counts track the number of places that a reference occurs.
        let mut reference_count = vec![0; input_arity + self.expressions.len()];
        // Increment reference counts for each use
        for expr in self.expressions.iter() {
            for col in expr.support().into_iter() {
                reference_count[col] += 1;
            }
        }
        for (_, pred) in self.predicates.iter() {
            for col in pred.support().into_iter() {
                reference_count[col] += 1;
            }
        }
        for proj in self.projection.iter() {
            reference_count[*proj] += 1;
        }

        // Inline only those columns that 1. are expressions not inputs, and
        // 2a. are column references or literals or 2b. have a refcount of 1.
        let should_inline = (0..reference_count.len())
            .map(|i| {
                if i < input_arity {
                    false
                } else {
                    if let MirScalarExpr::Column(_) = self.expressions[i - input_arity] {
                        true
                    } else {
                        reference_count[i] == 1
                    }
                }
            })
            .collect::<Vec<_>>();

        // Inline expressions that are single uses, or reference columns, or literals.
        for index in 0..self.expressions.len() {
            let (prior, expr) = self.expressions.split_at_mut(index);
            expr[0].visit_mut(&mut |e| {
                if let MirScalarExpr::Column(i) = e {
                    if should_inline[*i] {
                        *e = prior[*i - input_arity].clone();
                    }
                }
            });
        }
        for (_index, pred) in self.predicates.iter_mut() {
            let expressions = &self.expressions;
            pred.visit_mut(&mut |e| {
                if let MirScalarExpr::Column(i) = e {
                    if should_inline[*i] {
                        *e = expressions[*i - input_arity].clone();
                    }
                }
            });
        }
        // We can only inline column references in `self.projection`, but we should.
        for proj in self.projection.iter_mut() {
            if *proj >= self.input_arity {
                if let MirScalarExpr::Column(i) = self.expressions[*proj - self.input_arity] {
                    *proj = i;
                }
            }
        }
    }

    /// Removes unused expressions from `self.expressions`.
    ///
    /// Expressions are "used" if they are relied upon by any output columns
    /// or any predicates, even transitively. Any expressions that are not
    /// relied upon in this way can be discarded.
    ///
    /// # Example
    ///
    /// ```rust
    /// use expr::{MapFilterProject, MirScalarExpr, UnaryFunc, BinaryFunc};
    /// // Use the output from `inline_expression` example.
    /// let mut map_filter_project = MapFilterProject::new(5)
    ///     .map(vec![
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64),
    ///         MirScalarExpr::column(1).call_unary(UnaryFunc::CastStringToInt64),
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64).call_binary(MirScalarExpr::column(6), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64).call_binary(MirScalarExpr::column(6), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64),
    ///         MirScalarExpr::column(6).call_binary(MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(6).call_binary(MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64), BinaryFunc::AddInt64),
    ///     ])
    ///     .project(vec![3,4,8,11]);
    ///
    /// let mut expected_optimized = MapFilterProject::new(5)
    ///     .map(vec![
    ///         MirScalarExpr::column(1).call_unary(UnaryFunc::CastStringToInt64),
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64).call_binary(MirScalarExpr::column(5), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(5).call_binary(MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64), BinaryFunc::AddInt64),
    ///     ])
    ///     .project(vec![3,4,6,7]);
    ///
    /// // Remove undemandedd expressions, streamlining the work done..
    /// map_filter_project.remove_undemanded();
    ///
    /// assert_eq!(
    ///     map_filter_project,
    ///     expected_optimized,
    /// );
    /// ```
    pub fn remove_undemanded(&mut self) {
        // Determine the demanded expressions to remove irrelevant ones.
        let mut demand = std::collections::HashSet::new();
        for (_index, pred) in self.predicates.iter() {
            demand.extend(pred.support());
        }
        // Start from the output columns as presumed demanded.
        // If this is not the case, the caller should project some away.
        demand.extend(self.projection.iter().cloned());
        // Proceed in *reverse* order, as expressions may depend on other
        // expressions that precede them.
        for index in (0..self.expressions.len()).rev() {
            if demand.contains(&(self.input_arity + index)) {
                demand.extend(self.expressions[index].support());
            }
        }

        // Maintain a map from initial column identifiers to locations
        // once we have removed undemanded expressions.
        let mut remap = HashMap::new();
        // This map only needs to map elements of `demand` to a new location,
        // but the logic is easier if we include all input columns (as the
        // new position is then determined by the size of the map).
        for index in 0..self.input_arity {
            remap.insert(index, index);
        }
        // Retain demanded expressions, and record their new locations.
        let mut new_expressions = Vec::new();
        for (index, expr) in self.expressions.drain(..).enumerate() {
            if demand.contains(&(index + self.input_arity)) {
                remap.insert(index + self.input_arity, remap.len());
                new_expressions.push(expr);
            }
        }
        self.expressions = new_expressions;

        // Update column identifiers; rebuild `Self` to re-establish any invariants.
        // We mirror `self.permute(&remap)` but we specifically want to remap columns
        // that are produced by `self.expressions` after the input columns.
        let (expressions, predicates, projection) = self.as_map_filter_project();
        *self = Self::new(self.input_arity)
            .map(expressions.into_iter().map(|mut e| {
                e.permute_map(&remap);
                e
            }))
            .filter(predicates.into_iter().map(|mut p| {
                p.permute_map(&remap);
                p
            }))
            .project(projection.into_iter().map(|c| remap[&c]));
    }
}

// TODO: move this elsewhere?
/// Recursively memoize parts of `expr`, storing those parts in `memoized_parts`.
///
/// A part of `expr` that is memoized is replaced by a reference to column
/// `(input_arity + pos)`, where `pos` is the position of the memoized part in
/// `memoized_parts`, and `input_arity` is the arity of the input that `expr`
/// refers to.
pub fn memoize_expr(
    expr: &mut MirScalarExpr,
    memoized_parts: &mut Vec<MirScalarExpr>,
    input_arity: usize,
) {
    expr.visit_mut_pre_post(
        &mut |e| {
            // We should not eagerly memoize `if` branches that might not be taken.
            // TODO: Memoize expressions in the intersection of `then` and `els`.
            if let MirScalarExpr::If { cond, .. } = e {
                return Some(vec![cond]);
            }
            None
        },
        &mut |e| {
            match e {
                MirScalarExpr::Column(_) | MirScalarExpr::Literal(_, _) => {
                    // Literals do not need to be memoized.
                }
                _ => {
                    if let Some(position) = memoized_parts.iter().position(|e2| e2 == e) {
                        // Any complex expression that already exists as a prior column can
                        // be replaced by a reference to that column.
                        *e = MirScalarExpr::Column(input_arity + position);
                    } else {
                        // A complex expression that does not exist should be memoized, and
                        // replaced by a reference to the column.
                        memoized_parts.push(std::mem::replace(
                            e,
                            MirScalarExpr::Column(input_arity + memoized_parts.len()),
                        ));
                    }
                }
            }
        },
    )
}
