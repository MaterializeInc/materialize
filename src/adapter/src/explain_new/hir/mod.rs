// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN` support for HIR structures.

pub(crate) mod text;

use mz_expr::{
    visit::{Visit, VisitChildren},
    Id, LocalId,
};
use mz_ore::stack::RecursionLimitError;
use mz_repr::{
    explain_new::{Explain, ExplainConfig, ExplainError, UnsupportedFormat},
    RelationType,
};
use mz_sql::plan::{HirRelationExpr, HirScalarExpr};

use super::{AnnotatedPlan, ExplainContext, ExplainSinglePlan, Explainable};

impl<'a> Explain<'a> for Explainable<'a, HirRelationExpr> {
    type Context = ExplainContext<'a>;

    type Text = ExplainSinglePlan<'a, HirRelationExpr>;

    type Json = ExplainSinglePlan<'a, HirRelationExpr>;

    type Dot = UnsupportedFormat;

    fn explain_text(
        &'a mut self,
        config: &'a ExplainConfig,
        context: &'a Self::Context,
    ) -> Result<Self::Text, ExplainError> {
        self.as_explain_single_plan(config, context)
    }

    fn explain_json(
        &'a mut self,
        config: &'a ExplainConfig,
        context: &'a Self::Context,
    ) -> Result<Self::Json, ExplainError> {
        self.as_explain_single_plan(config, context)
    }
}

impl<'a> Explainable<'a, HirRelationExpr> {
    fn as_explain_single_plan(
        &'a mut self,
        config: &'a ExplainConfig,
        context: &'a ExplainContext<'a>,
    ) -> Result<ExplainSinglePlan<'a, HirRelationExpr>, ExplainError> {
        // unless raw plans are explicitly requested
        // ensure that all nested subqueries are wrapped in Let blocks
        if !config.raw_plans {
            normalize_subqueries(self.0)?;
        }

        // TODO: use config values to infer requested
        // plan annotations
        let plan = AnnotatedPlan {
            plan: self.0,
            annotations: Default::default(),
        };
        Ok(ExplainSinglePlan { context, plan })
    }
}

/// Normalize the way subqueries appear in [`HirScalarExpr::Exists`]
/// or [`HirScalarExpr::Select`] variants.
///
/// After the transform is applied, subqueries are pulled as a value in
/// a let binding enclosing the [`HirRelationExpr`] parent of the
/// [`HirScalarExpr::Exists`] or [`HirScalarExpr::Select`] where the
/// subquery appears, and the corresponding variant references the
/// new binding with a [`HirRelationExpr::Get`].
fn normalize_subqueries<'a>(expr: &'a mut HirRelationExpr) -> Result<(), RecursionLimitError> {
    // A helper struct to represent accumulated `$local_id = $subquery`
    // bindings that need to be installed in `let ... in $expr` nodes
    // that wrap their parent $expr.
    struct Binding {
        local_id: LocalId,
        subquery: HirRelationExpr,
    }

    // Context for the transformation
    // - a stack of bindings
    let mut bindings = Vec::<Binding>::new();
    // - a generator of fresh local ids
    let mut id_gen = id_gen(expr)?.peekable();

    // Grow the `bindings` stack by collecting subqueries appearing in
    // one of the HirScalarExpr children at the given HirRelationExpr.
    // As part of this, the subquery is replaced by a `Get(id)` for a
    // fresh local id.
    let mut collect_subqueries = |expr: &mut HirRelationExpr, bindings: &mut Vec<Binding>| {
        expr.try_visit_mut_children(|expr: &mut HirScalarExpr| {
            use HirRelationExpr::Get;
            use HirScalarExpr::{Exists, Select};
            expr.visit_mut_post(&mut |expr: &mut HirScalarExpr| match expr {
                Exists(expr) | Select(expr) => match expr.as_mut() {
                    Get { .. } => (),
                    expr => {
                        // generate fresh local id
                        let local_id = id_gen.next().unwrap();
                        // generate a `Get(local_id)` to be used as a subquery replacement
                        let mut subquery = Get {
                            id: Id::Local(local_id.clone()),
                            typ: RelationType::empty(), // TODO (#13732)
                        };
                        // swap the current subquery with the replacement
                        std::mem::swap(expr, &mut subquery);
                        // push a new $local_id = $subquery binding for a wrapping Let { ... }
                        bindings.push(Binding { local_id, subquery });
                    }
                },
                _ => (),
            })
        })
    };

    // Drain the `bindings` stack by wrapping the given `HirRelationExpr` with
    // a sequence of `Let { ... }` nodes, one for each binding.
    let insert_let_bindings = |expr: &mut HirRelationExpr, bindings: &mut Vec<Binding>| {
        for binding in bindings.drain(..) {
            let name = format!("subquery-{}", Into::<u64>::into(&binding.local_id));
            let id = binding.local_id;
            let value = Box::new(binding.subquery);
            let body = Box::new(expr.take());
            *expr = HirRelationExpr::Let {
                name,
                id,
                value,
                body,
            }
        }
    };

    expr.try_visit_mut_post(&mut |expr: &mut HirRelationExpr| {
        // first grow bindings stack
        collect_subqueries(expr, &mut bindings)?;
        // then drain bindings stack
        insert_let_bindings(expr, &mut bindings);
        // done!
        Ok(())
    })
}

// Create an [`Iterator`] for [`LocalId`] values that are guaranteed to be
// fresh within the scope of the given [`HirRelationExpr`].
fn id_gen(expr: &HirRelationExpr) -> Result<impl Iterator<Item = LocalId>, RecursionLimitError> {
    let mut max_id = 0_u64;

    expr.visit_post(&mut |expr| {
        match expr {
            HirRelationExpr::Let { id, .. } => max_id = std::cmp::max(max_id, id.into()),
            _ => (),
        };
    })?;

    Ok((max_id + 1..).map(LocalId::new))
}
