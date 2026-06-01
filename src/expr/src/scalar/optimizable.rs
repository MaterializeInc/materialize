// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A trait for scalar expressions that can be optimized inside a `MapFilterProject`.
//!
//! This trait is implemented by both `MirScalarExpr` and `LirScalarExpr`,
//! allowing `MapFilterProject` to be parameterized over either.

use std::fmt::Debug;
use std::hash::Hash;

use mz_ore::stack::RecursionLimitError;
use serde::Serialize;

use crate::scalar::columns::Columns;
use crate::scalar::func::{BinaryFunc, UnaryFunc, VariadicFunc};
use crate::visit::{Visit, VisitChildren};
use crate::{MirScalarExpr, func};

/// A scalar expression type that can be optimized inside a `MapFilterProject`.
///
/// Implemented by `MirScalarExpr` and `LirScalarExpr`.
pub trait OptimizableExpr:
    Columns + VisitChildren<Self> + Clone + Eq + Ord + Hash + Debug + Sized + Serialize
{
    /// True if this expression is a literal.
    fn is_literal(&self) -> bool;

    /// True if this expression is a literal error.
    fn is_literal_err(&self) -> bool;

    /// True if this expression contains a temporal reference (`mz_now()`).
    fn contains_temporal(&self) -> bool;

    /// Count of AST nodes in the expression tree.
    fn size(&self) -> usize;

    /// For memoization: which children should be eagerly memoized?
    ///
    /// Returns `None` to visit all children (the common case).
    /// Returns `Some(children)` for selective descent — e.g., for `If`, only the
    /// condition should be eagerly memoized (branches may not be taken).
    fn eager_children(&mut self) -> Option<Vec<&mut Self>>;

    /// If `predicate` is `col = expr` (or `expr = col`) where `col` is a column
    /// with index < `threshold`, return a clone of that column expression.
    ///
    /// Used by `optimize()` to detect equality-derived column aliases.
    fn equality_column_alias(predicate: &Self, expr: &Self, threshold: usize) -> Option<Self>;

    /// Extract temporal bounds from a list of temporal predicates.
    ///
    /// Returns `(lower_bounds, upper_bounds)` for use in `MfpPlan`.
    fn extract_temporal_bounds(temporal: Vec<Self>) -> Result<(Vec<Self>, Vec<Self>), String>;

    /// Visit in a pre-traversal. Defaults to the `Visit` implementation, but overridable.
    fn visit_pre<F>(&self, f: &mut F) -> Result<(), RecursionLimitError>
    where
        F: FnMut(&Self),
    {
        Visit::visit_pre(self, f)
    }
}

impl OptimizableExpr for MirScalarExpr {
    fn is_literal(&self) -> bool {
        self.is_literal()
    }

    fn is_literal_err(&self) -> bool {
        self.is_literal_err()
    }

    fn contains_temporal(&self) -> bool {
        self.contains_temporal()
    }

    fn size(&self) -> usize {
        self.size()
    }

    fn eager_children(&mut self) -> Option<Vec<&mut Self>> {
        // Do not eagerly memoize `if` branches that might not be taken.
        if let MirScalarExpr::If { cond, .. } = self {
            return Some(vec![cond]);
        }

        // Do not eagerly memoize `COALESCE` expressions after the first,
        // as they are only meant to be evaluated if the preceding expressions
        // evaluate to NULL.
        if let MirScalarExpr::CallVariadic {
            func: VariadicFunc::Coalesce(_),
            exprs,
        } = self
        {
            return Some(exprs.iter_mut().take(1).collect());
        }

        // Do not deconstruct temporal filters, because `MfpPlan::create_from` expects
        // those to be in a specific form. However, attend to the expression on the
        // opposite side of mz_now().
        if let Ok((_func, other_side)) = self.as_mut_temporal_filter() {
            return Some(vec![other_side]);
        }

        None
    }

    fn equality_column_alias(predicate: &Self, expr: &Self, threshold: usize) -> Option<Self> {
        if let MirScalarExpr::CallBinary {
            func: BinaryFunc::Eq(_),
            expr1,
            expr2,
        } = predicate
        {
            if let MirScalarExpr::Column(c, name) = &**expr1 {
                if *c < threshold && &**expr2 == expr {
                    return Some(MirScalarExpr::Column(*c, name.clone()));
                }
            }
            if let MirScalarExpr::Column(c, name) = &**expr2 {
                if *c < threshold && &**expr1 == expr {
                    return Some(MirScalarExpr::Column(*c, name.clone()));
                }
            }
        }
        None
    }

    fn extract_temporal_bounds(temporal: Vec<Self>) -> Result<(Vec<Self>, Vec<Self>), String> {
        let mut lower_bounds = Vec::new();
        let mut upper_bounds = Vec::new();

        for mut predicate in temporal.into_iter() {
            let (func, expr2) = predicate.as_mut_temporal_filter()?;
            let expr2 = expr2.clone();

            match func {
                BinaryFunc::Eq(_) => {
                    lower_bounds.push(expr2.clone());
                    upper_bounds
                        .push(expr2.call_unary(UnaryFunc::StepMzTimestamp(func::StepMzTimestamp)));
                }
                BinaryFunc::Lt(_) => {
                    upper_bounds.push(expr2.clone());
                }
                BinaryFunc::Lte(_) => {
                    upper_bounds
                        .push(expr2.call_unary(UnaryFunc::StepMzTimestamp(func::StepMzTimestamp)));
                }
                BinaryFunc::Gt(_) => {
                    lower_bounds
                        .push(expr2.call_unary(UnaryFunc::StepMzTimestamp(func::StepMzTimestamp)));
                }
                BinaryFunc::Gte(_) => {
                    lower_bounds.push(expr2.clone());
                }
                _ => {
                    return Err(format!("Unsupported binary temporal operation: {:?}", func));
                }
            }
        }

        Ok((lower_bounds, upper_bounds))
    }

    fn visit_pre<F>(&self, f: &mut F) -> Result<(), RecursionLimitError>
    where
        F: FnMut(&Self),
    {
        self.visit_pre(f);
        Ok(())
    }
}
