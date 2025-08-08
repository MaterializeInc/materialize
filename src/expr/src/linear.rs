// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;

use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{Datum, Row};
use proptest::prelude::*;
use serde::{Deserialize, Serialize};

use crate::linear::proto_map_filter_project::ProtoPredicate;
use crate::visit::Visit;
use crate::{MirRelationExpr, MirScalarExpr};

include!(concat!(env!("OUT_DIR"), "/mz_expr.linear.rs"));

/// A compound operator that can be applied row-by-row.
///
/// This operator integrates the map, filter, and project operators.
/// It applies a sequences of map expressions, which are allowed to
/// refer to previous expressions, interleaved with predicates which
/// must be satisfied for an output to be produced. If all predicates
/// evaluate to `Datum::True` the data at the identified columns are
/// collected and produced as output in a packed `Row`.
///
/// This operator is a "builder" and its contents may contain expressions
/// that are not yet executable. For example, it may contain temporal
/// expressions in `self.expressions`, even though this is not something
/// we can directly evaluate. The plan creation methods will defensively
/// ensure that the right thing happens.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, Ord, PartialOrd)]
pub struct MapFilterProject {
    /// A sequence of expressions that should be appended to the row.
    ///
    /// Many of these expressions may not be produced in the output,
    /// and may only be present as common subexpressions.
    pub expressions: Vec<MirScalarExpr>,
    /// Expressions that must evaluate to `Datum::True` for the output
    /// row to be produced.
    ///
    /// Each entry is prepended with a column identifier indicating
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
    /// This is needed to ensure correct identification of newly formed
    /// columns in the output.
    pub input_arity: usize,
}

/// Use a custom [`Arbitrary`] implementation to cap the number of internal
/// [`MirScalarExpr`] referenced by the generated [`MapFilterProject`]  instances.
impl Arbitrary for MapFilterProject {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            prop::collection::vec(any::<MirScalarExpr>(), 0..10),
            prop::collection::vec((any::<usize>(), any::<MirScalarExpr>()), 0..10),
            prop::collection::vec(any::<usize>(), 0..10),
            usize::arbitrary(),
        )
            .prop_map(
                |(expressions, predicates, projection, input_arity)| MapFilterProject {
                    expressions,
                    predicates,
                    projection,
                    input_arity,
                },
            )
            .boxed()
    }
}

impl RustType<ProtoMapFilterProject> for MapFilterProject {
    fn into_proto(&self) -> ProtoMapFilterProject {
        ProtoMapFilterProject {
            expressions: self.expressions.into_proto(),
            predicates: self.predicates.into_proto(),
            projection: self.projection.into_proto(),
            input_arity: self.input_arity.into_proto(),
        }
    }

    fn from_proto(proto: ProtoMapFilterProject) -> Result<Self, TryFromProtoError> {
        Ok(MapFilterProject {
            expressions: proto.expressions.into_rust()?,
            predicates: proto.predicates.into_rust()?,
            projection: proto.projection.into_rust()?,
            input_arity: proto.input_arity.into_rust()?,
        })
    }
}

impl RustType<ProtoPredicate> for (usize, MirScalarExpr) {
    fn into_proto(&self) -> ProtoPredicate {
        ProtoPredicate {
            column_to_apply: self.0.into_proto(),
            predicate: Some(self.1.into_proto()),
        }
    }

    fn from_proto(proto: ProtoPredicate) -> Result<Self, TryFromProtoError> {
        Ok((
            proto.column_to_apply.into_rust()?,
            proto
                .predicate
                .into_rust_if_some("ProtoPredicate::predicate")?,
        ))
    }
}

impl Display for MapFilterProject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "MapFilterProject(")?;
        writeln!(f, "  expressions:")?;
        self.expressions
            .iter()
            .enumerate()
            .try_for_each(|(i, e)| writeln!(f, "    #{} <- {},", i + self.input_arity, e))?;
        writeln!(f, "  predicates:")?;
        self.predicates
            .iter()
            .try_for_each(|(before, p)| writeln!(f, "    <before: {}> {},", before, p))?;
        writeln!(f, "  projection: {:?}", self.projection)?;
        writeln!(f, "  input_arity: {}", self.input_arity)?;
        writeln!(f, ")")
    }
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

    /// Given two mfps, return an mfp that applies one
    /// followed by the other.
    /// Note that the arguments are in the opposite order
    /// from how function composition is usually written in mathematics.
    pub fn compose(before: Self, after: Self) -> Self {
        let (m, f, p) = after.into_map_filter_project();
        before.map(m).filter(f).project(p)
    }

    /// True if the operator describes the identity transformation.
    pub fn is_identity(&self) -> bool {
        self.expressions.is_empty()
            && self.predicates.is_empty()
            && self.projection.len() == self.input_arity
            && self.projection.iter().enumerate().all(|(i, p)| i == *p)
    }

    /// Retain only the indicated columns in the presented order.
    pub fn project<I>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = usize> + std::fmt::Debug,
    {
        self.projection = columns.into_iter().map(|c| self.projection[c]).collect();
        self
    }

    /// Retain only rows satisfying these predicates.
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
            assert!(
                predicate
                    .support()
                    .into_iter()
                    .all(|c| c < self.input_arity + self.expressions.len())
            );

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
            assert!(
                expression
                    .support()
                    .into_iter()
                    .all(|c| c < self.input_arity + self.expressions.len())
            );

            // Introduce expression and produce as output.
            self.expressions.push(expression);
            self.projection
                .push(self.input_arity + self.expressions.len() - 1);
        }

        self
    }

    /// Like [`MapFilterProject::as_map_filter_project`], but consumes `self` rather than cloning.
    pub fn into_map_filter_project(self) -> (Vec<MirScalarExpr>, Vec<MirScalarExpr>, Vec<usize>) {
        let predicates = self
            .predicates
            .into_iter()
            .map(|(_pos, predicate)| predicate)
            .collect();
        (self.expressions, predicates, self.projection)
    }

    /// As the arguments to `Map`, `Filter`, and `Project` operators.
    ///
    /// In principle, this operator can be implemented as a sequence of
    /// more elemental operators, likely less efficiently.
    pub fn as_map_filter_project(&self) -> (Vec<MirScalarExpr>, Vec<MirScalarExpr>, Vec<usize>) {
        self.clone().into_map_filter_project()
    }

    /// Determines if a scalar expression must be equal to a literal datum.
    pub fn literal_constraint(&self, expr: &MirScalarExpr) -> Option<Datum<'_>> {
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
    /// The caller knows if `exprs` is empty, and can modify their behavior appropriately.
    /// if they would rather have a literal empty row.
    pub fn literal_constraints(&self, exprs: &[MirScalarExpr]) -> Option<Row> {
        if exprs.is_empty() {
            return None;
        }
        let mut row = Row::default();
        let mut packer = row.packer();
        for expr in exprs {
            if let Some(literal) = self.literal_constraint(expr) {
                packer.push(literal);
            } else {
                return None;
            }
        }
        Some(row)
    }

    /// Extracts any MapFilterProject at the root of the expression.
    ///
    /// The expression will be modified to extract any maps, filters, and
    /// projections, which will be returned as `Self`. If there are no maps,
    /// filters, or projections the method will return an identity operator.
    ///
    /// The extracted expressions may contain temporal predicates, and one
    /// should be careful to apply them blindly.
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
                let (mfp, expr) = Self::extract_from_expression(input);
                (mfp.filter(predicates.iter().cloned()), expr)
            }
            MirRelationExpr::Project { input, outputs } => {
                let (mfp, expr) = Self::extract_from_expression(input);
                (mfp.project(outputs.iter().cloned()), expr)
            }
            // TODO: The recursion is quadratic in the number of Map/Filter/Project operators due to
            // this call to `arity()`.
            x => (Self::new(x.arity()), x),
        }
    }

    /// Extracts an error-free MapFilterProject at the root of the expression.
    ///
    /// The expression will be modified to extract maps, filters, and projects
    /// from the root of the expression, which will be returned as `Self`. The
    /// extraction will halt if a Map or Filter containing a literal error is
    /// reached. Otherwise, the method will return an identity operator.
    ///
    /// This method is meant to be used during optimization, where it is
    /// necessary to avoid moving around maps and filters with errors.
    pub fn extract_non_errors_from_expr(expr: &MirRelationExpr) -> (Self, &MirRelationExpr) {
        match expr {
            MirRelationExpr::Map { input, scalars }
                if scalars.iter().all(|s| !s.is_literal_err()) =>
            {
                let (mfp, expr) = Self::extract_non_errors_from_expr(input);
                (mfp.map(scalars.iter().cloned()), expr)
            }
            MirRelationExpr::Filter { input, predicates }
                if predicates.iter().all(|p| !p.is_literal_err()) =>
            {
                let (mfp, expr) = Self::extract_non_errors_from_expr(input);
                (mfp.filter(predicates.iter().cloned()), expr)
            }
            MirRelationExpr::Project { input, outputs } => {
                let (mfp, expr) = Self::extract_non_errors_from_expr(input);
                (mfp.project(outputs.iter().cloned()), expr)
            }
            x => (Self::new(x.arity()), x),
        }
    }

    /// Extracts an error-free MapFilterProject at the root of the expression.
    ///
    /// Differs from [MapFilterProject::extract_non_errors_from_expr] by taking and returning a
    /// mutable reference.
    pub fn extract_non_errors_from_expr_ref_mut(
        expr: &mut MirRelationExpr,
    ) -> (Self, &mut MirRelationExpr) {
        // This is essentially the same code as `extract_non_errors_from_expr`, except the seemingly
        // superfluous outer if, which works around a borrow-checker issue:
        // https://github.com/rust-lang/rust/issues/54663
        if matches!(expr, MirRelationExpr::Map { input: _, scalars } if scalars.iter().all(|s| !s.is_literal_err()))
            || matches!(expr, MirRelationExpr::Filter { input: _, predicates } if predicates.iter().all(|p| !p.is_literal_err()))
            || matches!(expr, MirRelationExpr::Project { .. })
        {
            match expr {
                MirRelationExpr::Map { input, scalars }
                    if scalars.iter().all(|s| !s.is_literal_err()) =>
                {
                    let (mfp, expr) = Self::extract_non_errors_from_expr_ref_mut(input);
                    (mfp.map(scalars.iter().cloned()), expr)
                }
                MirRelationExpr::Filter { input, predicates }
                    if predicates.iter().all(|p| !p.is_literal_err()) =>
                {
                    let (mfp, expr) = Self::extract_non_errors_from_expr_ref_mut(input);
                    (mfp.filter(predicates.iter().cloned()), expr)
                }
                MirRelationExpr::Project { input, outputs } => {
                    let (mfp, expr) = Self::extract_non_errors_from_expr_ref_mut(input);
                    (mfp.project(outputs.iter().cloned()), expr)
                }
                _ => unreachable!(),
            }
        } else {
            (Self::new(expr.arity()), expr)
        }
    }

    /// Removes an error-free MapFilterProject from the root of the expression.
    ///
    /// The expression will be modified to extract maps, filters, and projects
    /// from the root of the expression, which will be returned as `Self`. The
    /// extraction will halt if a Map or Filter containing a literal error is
    /// reached. Otherwise, the method will return an
    /// identity operator, and the expression will remain unchanged.
    ///
    /// This method is meant to be used during optimization, where it is
    /// necessary to avoid moving around maps and filters with errors.
    pub fn extract_non_errors_from_expr_mut(expr: &mut MirRelationExpr) -> Self {
        match expr {
            MirRelationExpr::Map { input, scalars }
                if scalars.iter().all(|s| !s.is_literal_err()) =>
            {
                let mfp =
                    Self::extract_non_errors_from_expr_mut(input).map(scalars.iter().cloned());
                *expr = input.take_dangerous();
                mfp
            }
            MirRelationExpr::Filter { input, predicates }
                if predicates.iter().all(|p| !p.is_literal_err()) =>
            {
                let mfp = Self::extract_non_errors_from_expr_mut(input)
                    .filter(predicates.iter().cloned());
                *expr = input.take_dangerous();
                mfp
            }
            MirRelationExpr::Project { input, outputs } => {
                let mfp =
                    Self::extract_non_errors_from_expr_mut(input).project(outputs.iter().cloned());
                *expr = input.take_dangerous();
                mfp
            }
            x => Self::new(x.arity()),
        }
    }

    /// Extracts temporal predicates into their own `Self`.
    ///
    /// Expressions that are used by the temporal predicates are exposed by `self.projection`,
    /// though there could be justification for extracting them as well if they are otherwise
    /// unused.
    ///
    /// This separation is valuable when the execution cannot be fused into one operator.
    pub fn extract_temporal(&mut self) -> Self {
        // Optimize the expression, as it is only post-optimization that we can be certain
        // that temporal expressions are restricted to filters. We could relax this in the
        // future to be only `inline_expressions` and `remove_undemanded`, but optimization
        // seems to be the best fit at the moment.
        self.optimize();

        // Assert that we no longer have temporal expressions to evaluate. This should only
        // occur if the optimization above results with temporal expressions yielded in the
        // output, which is out of spec for how the type is meant to be used.
        assert!(!self.expressions.iter().any(|e| e.contains_temporal()));

        // Extract temporal predicates from `self.predicates`.
        let mut temporal_predicates = Vec::new();
        self.predicates.retain(|(_position, predicate)| {
            if predicate.contains_temporal() {
                temporal_predicates.push(predicate.clone());
                false
            } else {
                true
            }
        });

        // Determine extended input columns used by temporal filters.
        let mut support = BTreeSet::new();
        for predicate in temporal_predicates.iter() {
            support.extend(predicate.support());
        }

        // Discover the locations of these columns after `self.projection`.
        let old_projection_len = self.projection.len();
        let mut new_location = BTreeMap::new();
        for original in support.iter() {
            if let Some(position) = self.projection.iter().position(|x| x == original) {
                new_location.insert(*original, position);
            } else {
                new_location.insert(*original, self.projection.len());
                self.projection.push(*original);
            }
        }
        // Permute references in extracted predicates to their new locations.
        for predicate in temporal_predicates.iter_mut() {
            predicate.permute_map(&new_location);
        }

        // Form a new `Self` containing the temporal predicates to return.
        Self::new(self.projection.len())
            .filter(temporal_predicates)
            .project(0..old_projection_len)
    }

    /// Extracts common expressions from multiple `Self` into a result `Self`.
    ///
    /// The argument `mfps` are mutated so that each are functionaly equivalent to their
    /// corresponding input, when composed atop the resulting `Self`.
    ///
    /// The `extract_exprs` argument is temporary, as we roll out the `extract_common_mfp_expressions` flag.
    pub fn extract_common(mfps: &mut [&mut Self]) -> Self {
        match mfps.len() {
            0 => {
                panic!("Cannot call method on empty arguments");
            }
            1 => {
                let output_arity = mfps[0].projection.len();
                std::mem::replace(mfps[0], MapFilterProject::new(output_arity))
            }
            _ => {
                // More generally, we convert each mfp to ANF, at which point we can
                // repeatedly extract atomic expressions that depend only on input
                // columns, migrate them to an input mfp, and repeat until no such
                // expressions exist. At this point, we can also migrate predicates
                // and then determine and push down projections.

                // Prepare a return `Self`.
                let mut result_mfp = MapFilterProject::new(mfps[0].input_arity);

                // We convert each mfp to ANF, using `memoize_expressions`.
                for mfp in mfps.iter_mut() {
                    mfp.memoize_expressions();
                }

                // We repeatedly extract common expressions, until none remain.
                let mut done = false;
                while !done {
                    // We use references to determine common expressions, and must
                    // introduce a scope here to drop the borrows before mutation.
                    let common = {
                        // The input arity may increase as we iterate, so recapture.
                        let input_arity = result_mfp.projection.len();
                        let mut prev: BTreeSet<_> = mfps[0]
                            .expressions
                            .iter()
                            .filter(|e| e.support().iter().max() < Some(&input_arity))
                            .collect();
                        let mut next = BTreeSet::default();
                        for mfp in mfps[1..].iter() {
                            for expr in mfp.expressions.iter() {
                                if prev.contains(expr) {
                                    next.insert(expr);
                                }
                            }
                            std::mem::swap(&mut prev, &mut next);
                            next.clear();
                        }
                        prev.into_iter().cloned().collect::<Vec<_>>()
                    };
                    // Without new common expressions, we should terminate the loop.
                    done = common.is_empty();

                    // Migrate each expression in `common` to `result_mfp`.
                    for expr in common.into_iter() {
                        // Update each mfp by removing expr and updating column references.
                        for mfp in mfps.iter_mut() {
                            // With `expr` next in `result_mfp`, it is as if we are rotating it to
                            // be the first expression in `mfp`, and then removing it from `mfp` and
                            // increasing the input arity of `mfp`.
                            let arity = result_mfp.projection.len();
                            let found = mfp.expressions.iter().position(|e| e == &expr).unwrap();
                            let index = arity + found;
                            // Column references change due to the rotation from `index` to `arity`.
                            let action = |c: &mut usize| {
                                if arity <= *c && *c < index {
                                    *c += 1;
                                } else if *c == index {
                                    *c = arity;
                                }
                            };
                            // Rotate `expr` from `found` to first, and then snip.
                            // Short circuit by simply removing and incrementing the input arity.
                            mfp.input_arity += 1;
                            mfp.expressions.remove(found);
                            // Update column references in expressions, predicates, and projections.
                            for e in mfp.expressions.iter_mut() {
                                e.visit_columns(action);
                            }
                            for (o, e) in mfp.predicates.iter_mut() {
                                e.visit_columns(action);
                                // Max out the offset for the predicate; optimization will correct.
                                *o = mfp.input_arity + mfp.expressions.len();
                            }
                            for c in mfp.projection.iter_mut() {
                                action(c);
                            }
                        }
                        // Install the expression and update
                        result_mfp.expressions.push(expr);
                        result_mfp.projection.push(result_mfp.projection.len());
                    }
                }
                // As before, but easier: predicates in common to all mfps.
                let common_preds: Vec<MirScalarExpr> = {
                    let input_arity = result_mfp.projection.len();
                    let mut prev: BTreeSet<_> = mfps[0]
                        .predicates
                        .iter()
                        .map(|(_, e)| e)
                        .filter(|e| e.support().iter().max() < Some(&input_arity))
                        .collect();
                    let mut next = BTreeSet::default();
                    for mfp in mfps[1..].iter() {
                        for (_, expr) in mfp.predicates.iter() {
                            if prev.contains(expr) {
                                next.insert(expr);
                            }
                        }
                        std::mem::swap(&mut prev, &mut next);
                        next.clear();
                    }
                    // Expressions in common, that we will append to `result_mfp.expressions`.
                    prev.into_iter().cloned().collect::<Vec<_>>()
                };
                for mfp in mfps.iter_mut() {
                    mfp.predicates.retain(|(_, p)| !common_preds.contains(p));
                    mfp.optimize();
                }
                result_mfp.predicates.extend(
                    common_preds
                        .into_iter()
                        .map(|e| (result_mfp.projection.len(), e)),
                );

                // Then, look for unused columns and project them away.
                let mut common_demand = BTreeSet::new();
                for mfp in mfps.iter() {
                    common_demand.extend(mfp.demand());
                }
                // columns in `common_demand` must be retained, but others
                // may be discarded.
                let common_demand = (0..result_mfp.projection.len())
                    .filter(|x| common_demand.contains(x))
                    .collect::<Vec<_>>();
                let remap = common_demand
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(new, old)| (old, new))
                    .collect::<BTreeMap<_, _>>();
                for mfp in mfps.iter_mut() {
                    mfp.permute_fn(|c| remap[&c], common_demand.len());
                }
                result_mfp = result_mfp.project(common_demand);

                // Return the resulting MFP.
                result_mfp.optimize();
                result_mfp
            }
        }
    }

    /// Returns `self`, and leaves behind an identity operator that acts on its output.
    pub fn take(&mut self) -> Self {
        let mut identity = Self::new(self.projection.len());
        std::mem::swap(self, &mut identity);
        identity
    }

    /// Convert the `MapFilterProject` into a staged evaluation plan.
    ///
    /// The main behavior is extract temporal predicates, which cannot be evaluated
    /// using the standard machinery.
    pub fn into_plan(self) -> Result<plan::MfpPlan, String> {
        plan::MfpPlan::create_from(self)
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
    /// use mz_expr::{BinaryFunc, MapFilterProject, MirScalarExpr};
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
    /// let mut available_columns = std::collections::BTreeMap::new();
    /// available_columns.insert(0, 2);
    /// available_columns.insert(1, 0);
    /// available_columns.insert(2, 4);
    /// // Partition `original` using the available columns and current input arity.
    /// // This informs `partition` which columns are available, where they can be found,
    /// // and how many columns are not relevant but should be preserved.
    /// let (before, after) = original.partition(available_columns, 5);
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
    /// // introduce column d and permute the columns so that they begin (a, b, c, d).
    /// // The columns x and y must be projected away, and any columns introduced by
    /// // `begin` must be retained in their current order.
    ///
    /// // The `after` instance expects to be provided with all inputs, but it
    /// // may not need all inputs. The `demand()` and `permute()` methods can
    /// // optimize the representation.
    /// ```
    pub fn partition(self, available: BTreeMap<usize, usize>, input_arity: usize) -> (Self, Self) {
        // Map expressions, filter predicates, and projections for `before` and `after`.
        let mut before_expr = Vec::new();
        let mut before_pred = Vec::new();
        let mut before_proj = Vec::new();
        let mut after_expr = Vec::new();
        let mut after_pred = Vec::new();
        let mut after_proj = Vec::new();

        // Track which output columns must be preserved in the output of `before`.
        let mut demanded = BTreeSet::new();
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
        let mut before_map = available;
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
        let mut after_map = BTreeMap::new();
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
    pub fn demand(&self) -> BTreeSet<usize> {
        let mut demanded = BTreeSet::new();
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
    /// The supplied `shuffle` might not list columns that are not "demanded" by the
    /// instance, and so we should ensure that `self` is optimized to not reference
    /// columns that are not demanded.
    pub fn permute_fn<F>(&mut self, remap: F, new_input_arity: usize)
    where
        F: Fn(usize) -> usize,
    {
        let (mut map, mut filter, mut project) = self.as_map_filter_project();
        let map_len = map.len();
        let action = |col: &mut usize| {
            if self.input_arity <= *col && *col < self.input_arity + map_len {
                *col = new_input_arity + (*col - self.input_arity);
            } else {
                *col = remap(*col);
            }
        };
        for expr in map.iter_mut() {
            expr.visit_columns(action);
        }
        for pred in filter.iter_mut() {
            pred.visit_columns(action);
        }
        for proj in project.iter_mut() {
            action(proj);
            assert!(*proj < new_input_arity + map.len());
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
    /// This method will inline all temporal expressions, and remove any columns
    /// that are not demanded by the output, which should transform any temporal
    /// filters to a state where the temporal expressions exist only in the list
    /// of predicates.
    ///
    /// # Example
    ///
    /// This example demonstrates how the re-use of one expression, converting
    /// column 1 from a string to an integer, can be extracted and the results
    /// shared among the two uses. This example is used for each of the steps
    /// along the optimization path.
    ///
    /// ```rust
    /// use mz_expr::{func, MapFilterProject, MirScalarExpr, UnaryFunc, BinaryFunc};
    /// // Demonstrate extraction of common expressions (here: parsing strings).
    /// let mut map_filter_project = MapFilterProject::new(5)
    ///     .map(vec![
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)).call_binary(MirScalarExpr::column(1).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(1).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)).call_binary(MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)), BinaryFunc::AddInt64),
    ///     ])
    ///     .project(vec![3,4,5,6]);
    ///
    /// let mut expected_optimized = MapFilterProject::new(5)
    ///     .map(vec![
    ///         MirScalarExpr::column(1).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)),
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)).call_binary(MirScalarExpr::column(5), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(5).call_binary(MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)), BinaryFunc::AddInt64),
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
        // Track sizes and iterate as long as they decrease.
        let mut prev_size = None;
        let mut self_size = usize::max_value();
        // Continue as long as strict improvements occur.
        while prev_size.map(|p| self_size < p).unwrap_or(true) {
            // Lock in current size.
            prev_size = Some(self_size);

            // We have an annoying pattern of mapping literals that already exist as columns (by filters).
            // Try to identify this pattern, of a map that introduces an expression equated to a prior column,
            // and then replace the mapped expression by a column reference.
            //
            // We think this is due to `LiteralLifting`, and we might investigate removing the introduciton in
            // the first place. The tell-tale that we see when we fix is a diff that look likes
            //
            // - Project (#0, #2)
            // -   Filter (#1 = 1)
            // -     Map (1)
            // -       Get l0
            // + Filter (#1 = 1)
            // +   Get l0
            //
            for (index, expr) in self.expressions.iter_mut().enumerate() {
                // If `expr` matches a filter equating it to a column < index + input_arity, rewrite it
                for (_, predicate) in self.predicates.iter() {
                    if let MirScalarExpr::CallBinary {
                        func: crate::BinaryFunc::Eq,
                        expr1,
                        expr2,
                    } = predicate
                    {
                        if let MirScalarExpr::Column(c, name) = &**expr1 {
                            if *c < index + self.input_arity && &**expr2 == expr {
                                *expr = MirScalarExpr::Column(*c, name.clone());
                            }
                        }
                        if let MirScalarExpr::Column(c, name) = &**expr2 {
                            if *c < index + self.input_arity && &**expr1 == expr {
                                *expr = MirScalarExpr::Column(*c, name.clone());
                            }
                        }
                    }
                }
            }

            // Optimization memoizes individual `ScalarExpr` expressions that
            // are sure to be evaluated, canonicalizes references to the first
            // occurrence of each, inlines expressions that have a reference
            // count of one, and then removes any expressions that are not
            // referenced.
            self.memoize_expressions();
            self.predicates.sort();
            self.predicates.dedup();
            self.inline_expressions();
            self.remove_undemanded();

            // Re-build `self` from parts to restore evaluation order invariants.
            let (map, filter, project) = self.as_map_filter_project();
            *self = Self::new(self.input_arity)
                .map(map)
                .filter(filter)
                .project(project);

            self_size = self.size();
        }
    }

    /// Total expression sizes across all expressions.
    pub fn size(&self) -> usize {
        self.expressions.iter().map(|e| e.size()).sum::<usize>()
            + self.predicates.iter().map(|(_, e)| e.size()).sum::<usize>()
    }

    /// Place each certainly evaluated expression in its own column.
    ///
    /// This method places each non-trivial, certainly evaluated expression
    /// in its own column, and deduplicates them so that all references to
    /// the same expression reference the same column.
    ///
    /// This transformation is restricted to expressions we are certain will
    /// be evaluated, which does not include expressions in `if` statements.
    ///
    /// # Example
    ///
    /// This example demonstrates how memoization notices `MirScalarExpr`s
    /// that are used multiple times, and ensures that each are extracted
    /// into columns and then referenced by column. This pass does not try
    /// to minimize the occurrences of column references, which will happen
    /// in inlining.
    ///
    /// ```rust
    /// use mz_expr::{func, MapFilterProject, MirScalarExpr, UnaryFunc, BinaryFunc};
    /// // Demonstrate extraction of common expressions (here: parsing strings).
    /// let mut map_filter_project = MapFilterProject::new(5)
    ///     .map(vec![
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)).call_binary(MirScalarExpr::column(1).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(1).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)).call_binary(MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)), BinaryFunc::AddInt64),
    ///     ])
    ///     .project(vec![3,4,5,6]);
    ///
    /// let mut expected_optimized = MapFilterProject::new(5)
    ///     .map(vec![
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)),
    ///         MirScalarExpr::column(1).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)),
    ///         MirScalarExpr::column(5).call_binary(MirScalarExpr::column(6), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(7),
    ///         MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)),
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
    /// use mz_expr::{MapFilterProject, MirScalarExpr, UnaryFunc, BinaryFunc};
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
        let mut remaps = BTreeMap::new();
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

        // Restore predicate order invariants.
        for (pos, pred) in self.predicates.iter_mut() {
            *pos = pred.support().into_iter().max().map(|x| x + 1).unwrap_or(0);
        }
    }

    /// This method inlines expressions with a single use.
    ///
    /// This method only inlines expressions; it does not delete expressions
    /// that are no longer referenced. The `remove_undemanded()` method does
    /// that, and should likely be used after this method.
    ///
    /// Inlining replaces column references when the referred-to item is either
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
    /// use mz_expr::{func, MapFilterProject, MirScalarExpr, UnaryFunc, BinaryFunc};
    /// // Use the output from first `memoize_expression` example.
    /// let mut map_filter_project = MapFilterProject::new(5)
    ///     .map(vec![
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)),
    ///         MirScalarExpr::column(1).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)),
    ///         MirScalarExpr::column(5).call_binary(MirScalarExpr::column(6), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(7),
    ///         MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)),
    ///         MirScalarExpr::column(6).call_binary(MirScalarExpr::column(9), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(10),
    ///     ])
    ///     .project(vec![3,4,8,11]);
    ///
    /// let mut expected_optimized = MapFilterProject::new(5)
    ///     .map(vec![
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)),
    ///         MirScalarExpr::column(1).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)),
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)).call_binary(MirScalarExpr::column(6), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)).call_binary(MirScalarExpr::column(6), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)),
    ///         MirScalarExpr::column(6).call_binary(MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(6).call_binary(MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)), BinaryFunc::AddInt64),
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
            expr.visit_pre(|e| {
                if let MirScalarExpr::Column(i, _name) = e {
                    reference_count[*i] += 1;
                }
            });
        }
        for (_, pred) in self.predicates.iter() {
            pred.visit_pre(|e| {
                if let MirScalarExpr::Column(i, _name) = e {
                    reference_count[*i] += 1;
                }
            });
        }
        for proj in self.projection.iter() {
            reference_count[*proj] += 1;
        }

        // Determine which expressions should be inlined because they reference temporal expressions.
        let mut is_temporal = vec![false; input_arity];
        for expr in self.expressions.iter() {
            // An express may contain a temporal expression, or reference a column containing such.
            is_temporal.push(
                expr.contains_temporal() || expr.support().into_iter().any(|col| is_temporal[col]),
            );
        }

        // Inline only those columns that 1. are expressions not inputs, and
        // 2a. are column references or literals or 2b. have a refcount of 1,
        // or 2c. reference temporal expressions (which cannot be evaluated).
        let mut should_inline = vec![false; reference_count.len()];
        for i in (input_arity..reference_count.len()).rev() {
            if let MirScalarExpr::Column(c, _) = self.expressions[i - input_arity] {
                should_inline[i] = true;
                // The reference count of the referenced column should be
                // incremented with the number of references
                // `self.expressions[i - input_arity]` has.
                // Subtract 1 because `self.expressions[i - input_arity]` is
                // itself a reference.
                reference_count[c] += reference_count[i] - 1;
            } else {
                should_inline[i] = reference_count[i] == 1 || is_temporal[i];
            }
        }
        // Inline expressions per `should_inline`.
        self.perform_inlining(should_inline);
        // We can only inline column references in `self.projection`, but we should.
        for proj in self.projection.iter_mut() {
            if *proj >= self.input_arity {
                if let MirScalarExpr::Column(i, _) = self.expressions[*proj - self.input_arity] {
                    // TODO(mgree) !!! propagate name information to projection
                    *proj = i;
                }
            }
        }
    }

    /// Inlines those expressions that are indicated by should_inline.
    /// See `inline_expressions` for usage.
    pub fn perform_inlining(&mut self, should_inline: Vec<bool>) {
        for index in 0..self.expressions.len() {
            let (prior, expr) = self.expressions.split_at_mut(index);
            #[allow(deprecated)]
            expr[0].visit_mut_post_nolimit(&mut |e| {
                if let MirScalarExpr::Column(i, _name) = e {
                    if should_inline[*i] {
                        *e = prior[*i - self.input_arity].clone();
                    }
                }
            });
        }
        for (_index, pred) in self.predicates.iter_mut() {
            let expressions = &self.expressions;
            #[allow(deprecated)]
            pred.visit_mut_post_nolimit(&mut |e| {
                if let MirScalarExpr::Column(i, _name) = e {
                    if should_inline[*i] {
                        *e = expressions[*i - self.input_arity].clone();
                    }
                }
            });
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
    /// use mz_expr::{func, MapFilterProject, MirScalarExpr, UnaryFunc, BinaryFunc};
    /// // Use the output from `inline_expression` example.
    /// let mut map_filter_project = MapFilterProject::new(5)
    ///     .map(vec![
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)),
    ///         MirScalarExpr::column(1).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)),
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)).call_binary(MirScalarExpr::column(6), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)).call_binary(MirScalarExpr::column(6), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)),
    ///         MirScalarExpr::column(6).call_binary(MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(6).call_binary(MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)), BinaryFunc::AddInt64),
    ///     ])
    ///     .project(vec![3,4,8,11]);
    ///
    /// let mut expected_optimized = MapFilterProject::new(5)
    ///     .map(vec![
    ///         MirScalarExpr::column(1).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)),
    ///         MirScalarExpr::column(0).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)).call_binary(MirScalarExpr::column(5), BinaryFunc::AddInt64),
    ///         MirScalarExpr::column(5).call_binary(MirScalarExpr::column(2).call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64)), BinaryFunc::AddInt64),
    ///     ])
    ///     .project(vec![3,4,6,7]);
    ///
    /// // Remove undemanded expressions, streamlining the work done..
    /// map_filter_project.remove_undemanded();
    ///
    /// assert_eq!(
    ///     map_filter_project,
    ///     expected_optimized,
    /// );
    /// ```
    pub fn remove_undemanded(&mut self) {
        // Determine the demanded expressions to remove irrelevant ones.
        let mut demand = BTreeSet::new();
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
        let mut remap = BTreeMap::new();
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
    #[allow(deprecated)]
    expr.visit_mut_pre_post_nolimit(
        &mut |e| {
            // We should not eagerly memoize `if` branches that might not be taken.
            // TODO: Memoize expressions in the intersection of `then` and `els`.
            if let MirScalarExpr::If { cond, .. } = e {
                return Some(vec![cond]);
            }
            // We should not eagerly memoize `COALESCE` expressions after the first,
            // as they are only meant to be evaluated if the preceding expressions
            // evaluate to NULL. We could memoize any preceding by expressions that
            // are certain not to error.
            if let MirScalarExpr::CallVariadic {
                func: crate::VariadicFunc::Coalesce,
                exprs,
            } = e
            {
                return Some(exprs.iter_mut().take(1).collect());
            }
            None
        },
        &mut |e| {
            match e {
                MirScalarExpr::Literal(_, _) => {
                    // Literals do not need to be memoized.
                }
                MirScalarExpr::Column(col, _) => {
                    // Column references do not need to be memoized, but may need to be
                    // updated if they reference a column reference themselves.
                    if *col > input_arity {
                        if let MirScalarExpr::Column(col2, _) = memoized_parts[*col - input_arity] {
                            // We do _not_ propagate column names, since mis-associating names and column
                            // references will be very confusing (and possibly bug-inducing).
                            *col = col2;
                        }
                    }
                }
                _ => {
                    if let Some(position) = memoized_parts.iter().position(|e2| e2 == e) {
                        // Any complex expression that already exists as a prior column can
                        // be replaced by a reference to that column.
                        *e = MirScalarExpr::column(input_arity + position);
                    } else {
                        // A complex expression that does not exist should be memoized, and
                        // replaced by a reference to the column.
                        memoized_parts.push(std::mem::replace(
                            e,
                            MirScalarExpr::column(input_arity + memoized_parts.len()),
                        ));
                    }
                }
            }
        },
    )
}

pub mod util {
    use std::collections::BTreeMap;

    use crate::MirScalarExpr;

    #[allow(dead_code)]
    /// A triple of actions that map from rows to (key, val) pairs and back again.
    struct KeyValRowMapping {
        /// Expressions to apply to a row to produce key datums.
        to_key: Vec<MirScalarExpr>,
        /// Columns to project from a row to produce residual value datums.
        to_val: Vec<usize>,
        /// Columns to project from the concatenation of key and value to reconstruct the row.
        to_row: Vec<usize>,
    }

    /// Derive supporting logic to support transforming rows to (key, val) pairs,
    /// and back again.
    ///
    /// We are given as input a list of key expressions and an input arity, and the
    /// requirement the produced key should be the application of the key expressions.
    /// To produce the `val` output, we will identify those input columns not found in
    /// the key expressions, and name all other columns.
    /// To reconstitute the original row, we identify the sequence of columns from the
    /// concatenation of key and val which would reconstruct the original row.
    ///
    /// The output is a pair of column sequences, the first used to reconstruct a row
    /// from the concatenation of key and value, and the second to identify the columns
    /// of a row that should become the value associated with its key.
    ///
    /// The permutations and thinning expressions generated here will be tracked in
    /// `dataflow::plan::AvailableCollections`; see the
    /// documentation there for more details.
    pub fn permutation_for_arrangement(
        key: &[MirScalarExpr],
        unthinned_arity: usize,
    ) -> (Vec<usize>, Vec<usize>) {
        let columns_in_key: BTreeMap<_, _> = key
            .iter()
            .enumerate()
            .filter_map(|(i, key_col)| key_col.as_column().map(|c| (c, i)))
            .collect();
        let mut input_cursor = key.len();
        let permutation = (0..unthinned_arity)
            .map(|c| {
                if let Some(c) = columns_in_key.get(&c) {
                    // Column is in key (and thus gone from the value
                    // of the thinned representation)
                    *c
                } else {
                    // Column remains in value of the thinned representation
                    input_cursor += 1;
                    input_cursor - 1
                }
            })
            .collect();
        let thinning = (0..unthinned_arity)
            .filter(|c| !columns_in_key.contains_key(c))
            .collect();
        (permutation, thinning)
    }

    /// Given the permutations (see [`permutation_for_arrangement`] and
    /// (`dataflow::plan::AvailableCollections`) corresponding to two
    /// collections with the same key arity,
    /// computes the permutation for the result of joining them.
    pub fn join_permutations(
        key_arity: usize,
        stream_permutation: Vec<usize>,
        thinned_stream_arity: usize,
        lookup_permutation: Vec<usize>,
    ) -> BTreeMap<usize, usize> {
        let stream_arity = stream_permutation.len();
        let lookup_arity = lookup_permutation.len();

        (0..stream_arity + lookup_arity)
            .map(|i| {
                let location = if i < stream_arity {
                    stream_permutation[i]
                } else {
                    let location_in_lookup = lookup_permutation[i - stream_arity];
                    if location_in_lookup < key_arity {
                        location_in_lookup
                    } else {
                        location_in_lookup + thinned_stream_arity
                    }
                };
                (i, location)
            })
            .collect()
    }
}

pub mod plan {
    use std::iter;

    use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
    use mz_repr::{Datum, Diff, Row, RowArena};
    use proptest::prelude::*;
    use proptest_derive::Arbitrary;
    use serde::{Deserialize, Serialize};

    use crate::{
        BinaryFunc, EvalError, MapFilterProject, MirScalarExpr, ProtoMfpPlan, ProtoSafeMfpPlan,
        UnaryFunc, UnmaterializableFunc, func,
    };

    /// A wrapper type which indicates it is safe to simply evaluate all expressions.
    #[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
    pub struct SafeMfpPlan {
        pub(crate) mfp: MapFilterProject,
    }

    impl RustType<ProtoSafeMfpPlan> for SafeMfpPlan {
        fn into_proto(&self) -> ProtoSafeMfpPlan {
            ProtoSafeMfpPlan {
                mfp: Some(self.mfp.into_proto()),
            }
        }

        fn from_proto(proto: ProtoSafeMfpPlan) -> Result<Self, TryFromProtoError> {
            Ok(SafeMfpPlan {
                mfp: proto.mfp.into_rust_if_some("ProtoSafeMfpPlan::mfp")?,
            })
        }
    }

    impl SafeMfpPlan {
        /// Remaps references to input columns according to `remap`.
        ///
        /// Leaves other column references, e.g. to newly mapped columns, unchanged.
        pub fn permute_fn<F>(&mut self, remap: F, new_arity: usize)
        where
            F: Fn(usize) -> usize,
        {
            self.mfp.permute_fn(remap, new_arity);
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
        ///
        /// The `row` is not cleared first, but emptied if the function
        /// returns `Ok(Some(row)).
        #[inline(always)]
        pub fn evaluate_into<'a, 'row>(
            &'a self,
            datums: &mut Vec<Datum<'a>>,
            arena: &'a RowArena,
            row_buf: &'row mut Row,
        ) -> Result<Option<&'row Row>, EvalError> {
            let passed_predicates = self.evaluate_inner(datums, arena)?;
            if !passed_predicates {
                Ok(None)
            } else {
                row_buf
                    .packer()
                    .extend(self.mfp.projection.iter().map(|c| datums[*c]));
                Ok(Some(row_buf))
            }
        }

        /// A version of `evaluate` which produces an iterator over `Datum`
        /// as output.
        ///
        /// This version can be useful when one wants to capture the resulting
        /// datums without packing and then unpacking a row.
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
                Ok(Some(self.mfp.projection.iter().map(move |i| datums[*i])))
            }
        }

        /// Populates `datums` with `self.expressions` and tests `self.predicates`.
        ///
        /// This does not apply `self.projection`, which is up to the calling method.
        pub fn evaluate_inner<'b, 'a: 'b>(
            &'a self,
            datums: &'b mut Vec<Datum<'a>>,
            arena: &'a RowArena,
        ) -> Result<bool, EvalError> {
            let mut expression = 0;
            for (support, predicate) in self.mfp.predicates.iter() {
                while self.mfp.input_arity + expression < *support {
                    datums.push(self.mfp.expressions[expression].eval(&datums[..], arena)?);
                    expression += 1;
                }
                if predicate.eval(&datums[..], arena)? != Datum::True {
                    return Ok(false);
                }
            }
            while expression < self.mfp.expressions.len() {
                datums.push(self.mfp.expressions[expression].eval(&datums[..], arena)?);
                expression += 1;
            }
            Ok(true)
        }

        /// Returns true if evaluation could introduce an error on non-error inputs.
        pub fn could_error(&self) -> bool {
            self.mfp.predicates.iter().any(|(_pos, e)| e.could_error())
                || self.mfp.expressions.iter().any(|e| e.could_error())
        }

        /// Returns true when `Self` is the identity.
        pub fn is_identity(&self) -> bool {
            self.mfp.is_identity()
        }
    }

    impl std::ops::Deref for SafeMfpPlan {
        type Target = MapFilterProject;
        fn deref(&self) -> &Self::Target {
            &self.mfp
        }
    }

    /// Predicates partitioned into temporal and non-temporal.
    ///
    /// Temporal predicates require some recognition to determine their
    /// structure, and it is best to do that once and re-use the results.
    ///
    /// There are restrictions on the temporal predicates we currently support.
    /// They must directly constrain `MzNow` from below or above,
    /// by expressions that do not themselves contain `MzNow`.
    /// Conjunctions of such constraints are also ok.
    #[derive(Arbitrary, Clone, Debug, PartialEq)]
    pub struct MfpPlan {
        /// Normal predicates to evaluate on `&[Datum]` and expect `Ok(Datum::True)`.
        pub(crate) mfp: SafeMfpPlan,
        /// Expressions that when evaluated lower-bound `MzNow`.
        #[proptest(strategy = "prop::collection::vec(any::<MirScalarExpr>(), 0..2)")]
        pub(crate) lower_bounds: Vec<MirScalarExpr>,
        /// Expressions that when evaluated upper-bound `MzNow`.
        #[proptest(strategy = "prop::collection::vec(any::<MirScalarExpr>(), 0..2)")]
        pub(crate) upper_bounds: Vec<MirScalarExpr>,
    }

    impl RustType<ProtoMfpPlan> for MfpPlan {
        fn into_proto(&self) -> ProtoMfpPlan {
            ProtoMfpPlan {
                mfp: Some(self.mfp.into_proto()),
                lower_bounds: self.lower_bounds.into_proto(),
                upper_bounds: self.upper_bounds.into_proto(),
            }
        }

        fn from_proto(proto: ProtoMfpPlan) -> Result<Self, TryFromProtoError> {
            Ok(MfpPlan {
                mfp: proto.mfp.into_rust_if_some("ProtoMfpPlan::mfp")?,
                lower_bounds: proto.lower_bounds.into_rust()?,
                upper_bounds: proto.upper_bounds.into_rust()?,
            })
        }
    }

    impl MfpPlan {
        /// Partitions `predicates` into non-temporal, and lower and upper temporal bounds.
        ///
        /// The first returned list is of predicates that do not contain `mz_now`.
        /// The second and third returned lists contain expressions that, once evaluated, lower
        /// and upper bound the validity interval of a record, respectively. These second two
        /// lists are populated only by binary expressions of the form
        /// ```ignore
        /// mz_now cmp_op expr
        /// ```
        /// where `cmp_op` is a comparison operator and `expr` does not contain `mz_now`.
        ///
        /// If any unsupported expression is found, for example one that uses `mz_now`
        /// in an unsupported position, an error is returned.
        pub fn create_from(mut mfp: MapFilterProject) -> Result<Self, String> {
            let mut lower_bounds = Vec::new();
            let mut upper_bounds = Vec::new();

            let mut temporal = Vec::new();

            // Optimize, to ensure that temporal predicates are move in to `mfp.predicates`.
            mfp.optimize();

            mfp.predicates.retain(|(_position, predicate)| {
                if predicate.contains_temporal() {
                    temporal.push(predicate.clone());
                    false
                } else {
                    true
                }
            });

            for predicate in temporal.into_iter() {
                // Supported temporal predicates are exclusively binary operators.
                if let MirScalarExpr::CallBinary {
                    mut func,
                    mut expr1,
                    mut expr2,
                } = predicate
                {
                    // Attempt to put `LogicalTimestamp` in the first argument position.
                    if !expr1.contains_temporal()
                        && *expr2
                            == MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzNow)
                    {
                        std::mem::swap(&mut expr1, &mut expr2);
                        func = match func {
                            BinaryFunc::Eq => BinaryFunc::Eq,
                            BinaryFunc::Lt => BinaryFunc::Gt,
                            BinaryFunc::Lte => BinaryFunc::Gte,
                            BinaryFunc::Gt => BinaryFunc::Lt,
                            BinaryFunc::Gte => BinaryFunc::Lte,
                            x => {
                                return Err(format!(
                                    "Unsupported binary temporal operation: {:?}",
                                    x
                                ));
                            }
                        };
                    }

                    // Error if MLT is referenced in an unsupported position.
                    if expr2.contains_temporal()
                        || *expr1
                            != MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzNow)
                    {
                        return Err(format!(
                            "Unsupported temporal predicate. Note: `mz_now()` must be directly compared to a mz_timestamp-castable expression. Expression found: {}",
                            MirScalarExpr::CallBinary { func, expr1, expr2 },
                        ));
                    }

                    // LogicalTimestamp <OP> <EXPR2> for several supported operators.
                    match func {
                        BinaryFunc::Eq => {
                            lower_bounds.push(*expr2.clone());
                            upper_bounds.push(
                                expr2.call_unary(UnaryFunc::StepMzTimestamp(func::StepMzTimestamp)),
                            );
                        }
                        BinaryFunc::Lt => {
                            upper_bounds.push(*expr2.clone());
                        }
                        BinaryFunc::Lte => {
                            upper_bounds.push(
                                expr2.call_unary(UnaryFunc::StepMzTimestamp(func::StepMzTimestamp)),
                            );
                        }
                        BinaryFunc::Gt => {
                            lower_bounds.push(
                                expr2.call_unary(UnaryFunc::StepMzTimestamp(func::StepMzTimestamp)),
                            );
                        }
                        BinaryFunc::Gte => {
                            lower_bounds.push(*expr2.clone());
                        }
                        _ => {
                            return Err(format!(
                                "Unsupported binary temporal operation: {:?}",
                                func
                            ));
                        }
                    }
                } else {
                    return Err(format!(
                        "Unsupported temporal predicate. Note: `mz_now()` must be directly compared to a non-temporal expression of mz_timestamp-castable type. Expression found: {}",
                        predicate,
                    ));
                }
            }

            Ok(Self {
                mfp: SafeMfpPlan { mfp },
                lower_bounds,
                upper_bounds,
            })
        }

        /// Indicates if the planned `MapFilterProject` emits exactly its inputs as outputs.
        pub fn is_identity(&self) -> bool {
            self.mfp.mfp.is_identity()
                && self.lower_bounds.is_empty()
                && self.upper_bounds.is_empty()
        }

        /// Returns `self`, and leaves behind an identity operator that acts on its output.
        pub fn take(&mut self) -> Self {
            let mut identity = Self {
                mfp: SafeMfpPlan {
                    mfp: MapFilterProject::new(self.mfp.projection.len()),
                },
                lower_bounds: Default::default(),
                upper_bounds: Default::default(),
            };
            std::mem::swap(self, &mut identity);
            identity
        }

        /// Attempt to convert self into a non-temporal MapFilterProject plan.
        ///
        /// If that is not possible, the original instance is returned as an error.
        #[allow(clippy::result_large_err)]
        pub fn into_nontemporal(self) -> Result<SafeMfpPlan, Self> {
            if self.lower_bounds.is_empty() && self.upper_bounds.is_empty() {
                Ok(self.mfp)
            } else {
                Err(self)
            }
        }

        /// Returns an iterator over mutable references to all non-temporal
        /// scalar expressions in the plan.
        ///
        /// The order of iteration is unspecified.
        pub fn iter_nontemporal_exprs(&mut self) -> impl Iterator<Item = &mut MirScalarExpr> {
            iter::empty()
                .chain(self.mfp.mfp.predicates.iter_mut().map(|(_, expr)| expr))
                .chain(&mut self.mfp.mfp.expressions)
                .chain(&mut self.lower_bounds)
                .chain(&mut self.upper_bounds)
        }

        /// Evaluate the predicates, temporal and non-, and return times and differences for `data`.
        ///
        /// If `self` contains only non-temporal predicates, the result will either be `(time, diff)`,
        /// or an evaluation error. If `self contains temporal predicates, the results can be times
        /// that are greater than the input `time`, and may contain negated `diff` values.
        ///
        /// The `row_builder` is not cleared first, but emptied if the function
        /// returns an iterator with any `Ok(_)` element.
        pub fn evaluate<'b, 'a: 'b, E: From<EvalError>, V: Fn(&mz_repr::Timestamp) -> bool>(
            &'a self,
            datums: &'b mut Vec<Datum<'a>>,
            arena: &'a RowArena,
            time: mz_repr::Timestamp,
            diff: Diff,
            valid_time: V,
            row_builder: &mut Row,
        ) -> impl Iterator<
            Item = Result<(Row, mz_repr::Timestamp, Diff), (E, mz_repr::Timestamp, Diff)>,
        > + use<E, V> {
            match self.mfp.evaluate_inner(datums, arena) {
                Err(e) => {
                    return Some(Err((e.into(), time, diff))).into_iter().chain(None);
                }
                Ok(true) => {}
                Ok(false) => {
                    return None.into_iter().chain(None);
                }
            }

            // Lower and upper bounds.
            let mut lower_bound = time;
            let mut upper_bound = None;

            // Track whether we have seen a null in either bound, as this should
            // prevent the record from being produced at any time.
            let mut null_eval = false;

            // Advance our lower bound to be at least the result of any lower bound
            // expressions.
            for l in self.lower_bounds.iter() {
                match l.eval(datums, arena) {
                    Err(e) => {
                        return Some(Err((e.into(), time, diff)))
                            .into_iter()
                            .chain(None.into_iter());
                    }
                    Ok(Datum::MzTimestamp(d)) => {
                        lower_bound = lower_bound.max(d);
                    }
                    Ok(Datum::Null) => {
                        null_eval = true;
                    }
                    x => {
                        panic!("Non-mz_timestamp value in temporal predicate: {:?}", x);
                    }
                }
            }

            // If the lower bound exceeds our `until` frontier, it should not appear in the output.
            if !valid_time(&lower_bound) {
                return None.into_iter().chain(None);
            }

            // If there are any upper bounds, determine the minimum upper bound.
            for u in self.upper_bounds.iter() {
                // We can cease as soon as the lower and upper bounds match,
                // as the update will certainly not be produced in that case.
                if upper_bound != Some(lower_bound) {
                    match u.eval(datums, arena) {
                        Err(e) => {
                            return Some(Err((e.into(), time, diff)))
                                .into_iter()
                                .chain(None.into_iter());
                        }
                        Ok(Datum::MzTimestamp(d)) => {
                            if let Some(upper) = upper_bound {
                                upper_bound = Some(upper.min(d));
                            } else {
                                upper_bound = Some(d);
                            };
                            // Force the upper bound to be at least the lower
                            // bound. The `is_some()` test should always be true
                            // due to the above block, but maintain it here in
                            // case that changes. It's hopefully optimized away.
                            if upper_bound.is_some() && upper_bound < Some(lower_bound) {
                                upper_bound = Some(lower_bound);
                            }
                        }
                        Ok(Datum::Null) => {
                            null_eval = true;
                        }
                        x => {
                            panic!("Non-mz_timestamp value in temporal predicate: {:?}", x);
                        }
                    }
                }
            }

            // If the upper bound exceeds our `until` frontier, it should not appear in the output.
            if let Some(upper) = &mut upper_bound {
                if !valid_time(upper) {
                    upper_bound = None;
                }
            }

            // Produce an output only if the upper bound exceeds the lower bound,
            // and if we did not encounter a `null` in our evaluation.
            if Some(lower_bound) != upper_bound && !null_eval {
                row_builder
                    .packer()
                    .extend(self.mfp.mfp.projection.iter().map(|c| datums[*c]));
                let upper_opt =
                    upper_bound.map(|upper_bound| Ok((row_builder.clone(), upper_bound, -diff)));
                let lower = Some(Ok((row_builder.clone(), lower_bound, diff)));
                lower.into_iter().chain(upper_opt)
            } else {
                None.into_iter().chain(None)
            }
        }

        /// Returns true if evaluation could introduce an error on non-error inputs.
        pub fn could_error(&self) -> bool {
            self.mfp.could_error()
                || self.lower_bounds.iter().any(|e| e.could_error())
                || self.upper_bounds.iter().any(|e| e.could_error())
        }

        /// Indicates that `Self` ignores its input to the extent that it can be evaluated on `&[]`.
        ///
        /// At the moment, this is only true if it projects away all columns and applies no filters,
        /// but it could be extended to plans that produce literals independent of the input.
        pub fn ignores_input(&self) -> bool {
            self.lower_bounds.is_empty()
                && self.upper_bounds.is_empty()
                && self.mfp.mfp.projection.is_empty()
                && self.mfp.mfp.predicates.is_empty()
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_ore::assert_ok;
    use mz_proto::protobuf_roundtrip;

    use crate::linear::plan::*;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // too slow
        fn mfp_plan_protobuf_roundtrip(expect in any::<MfpPlan>()) {
            let actual = protobuf_roundtrip::<_, ProtoMfpPlan>(&expect);
            assert_ok!(actual);
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
