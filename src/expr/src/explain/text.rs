// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN ... AS TEXT` support for structures defined in this crate.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::Ordering;

use mz_ore::assert::SOFT_ASSERTIONS;
use mz_ore::soft_assert_eq_or_log;
use mz_ore::str::{closure_to_display, separated, Indent, IndentLike, StrExt};
use mz_repr::explain::text::DisplayText;
use mz_repr::explain::{
    CompactScalars, ExprHumanizer, HumanizedAnalyses, IndexUsageType, Indices,
    PlanRenderingContext, RenderingContext, ScalarOps,
};
use mz_repr::{Datum, Diff, GlobalId, Row};
use mz_sql_parser::ast::Ident;

use crate::explain::{ExplainMultiPlan, ExplainSinglePlan};
use crate::{
    AccessStrategy, AggregateExpr, EvalError, Id, JoinImplementation, JoinInputCharacteristics,
    LocalId, MapFilterProject, MirRelationExpr, MirScalarExpr, RowSetFinishing,
};

impl<'a, T: 'a> DisplayText for ExplainSinglePlan<'a, T>
where
    T: DisplayText<PlanRenderingContext<'a, T>> + Ord,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
        let mut ctx = PlanRenderingContext::new(
            Indent::default(),
            self.context.humanizer,
            self.plan.annotations.clone(),
            self.context.config,
        );

        let mode = HumanizedExplain::new(self.context.config.redacted);

        if let Some(finishing) = &self.context.finishing {
            if ctx.config.humanized_exprs {
                let analyses = ctx.annotations.get(&self.plan.plan);
                let cols = analyses
                    .map(|analyses| analyses.column_names.clone())
                    .flatten();
                mode.expr(finishing, cols.as_ref()).fmt_text(f, &mut ctx)?;
            } else {
                mode.expr(finishing, None).fmt_text(f, &mut ctx)?;
            }
            ctx.indented(|ctx| self.plan.plan.fmt_text(f, ctx))?;
        } else {
            self.plan.plan.fmt_text(f, &mut ctx)?;
        }

        if !self.context.used_indexes.is_empty() {
            writeln!(f)?;
            self.context.used_indexes.fmt_text(f, &mut ctx)?;
        }

        if let Some(target_cluster) = self.context.target_cluster {
            writeln!(f)?;
            writeln!(f, "Target cluster: {}", target_cluster)?;
        }

        if !self.context.optimizer_notices.is_empty() {
            writeln!(f)?;
            writeln!(f, "Notices:")?;
            for notice in self.context.optimizer_notices.iter() {
                writeln!(f, "{}", notice)?;
            }
        }

        if self.context.config.timing {
            writeln!(f)?;
            writeln!(f, "Optimization time: {:?}", self.context.duration)?;
        }

        Ok(())
    }
}

impl<'a, T: 'a> DisplayText for ExplainMultiPlan<'a, T>
where
    T: DisplayText<PlanRenderingContext<'a, T>> + Ord,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
        let mut ctx = RenderingContext::new(Indent::default(), self.context.humanizer);

        let mode = HumanizedExplain::new(self.context.config.redacted);

        // Render plans.
        for (no, (id, plan)) in self.plans.iter().enumerate() {
            let mut ctx = PlanRenderingContext::new(
                ctx.indent.clone(),
                ctx.humanizer,
                plan.annotations.clone(),
                self.context.config,
            );

            if no > 0 {
                writeln!(f)?;
            }

            writeln!(f, "{}{}:", ctx.indent, id)?;
            ctx.indented(|ctx| {
                match &self.context.finishing {
                    // If present, a RowSetFinishing always applies to the first rendered plan.
                    Some(finishing) if no == 0 => {
                        if ctx.config.humanized_exprs {
                            let analyses = ctx.annotations.get(plan.plan);
                            let cols = analyses
                                .map(|analyses| analyses.column_names.clone())
                                .flatten();
                            mode.expr(finishing, cols.as_ref()).fmt_text(f, ctx)?;
                        } else {
                            mode.expr(finishing, None).fmt_text(f, ctx)?;
                        };
                        ctx.indented(|ctx| plan.plan.fmt_text(f, ctx))?;
                    }
                    // All other plans are rendered without a RowSetFinishing.
                    _ => {
                        plan.plan.fmt_text(f, ctx)?;
                    }
                }
                Ok(())
            })?;
        }

        if self.sources.iter().any(|src| !src.is_identity()) {
            // Render one blank line between the plans and sources.
            writeln!(f)?;
            for src in self.sources.iter().filter(|src| !src.is_identity()) {
                if self.context.config.humanized_exprs {
                    let mut cols = ctx.humanizer.column_names_for_id(src.id);
                    // The column names of the source needs to be extended with
                    // anonymous columns for each source expression before we can
                    // pass it to the ExplainSource rendering code.
                    if let Some(cols) = cols.as_mut() {
                        let anonymous = std::iter::repeat(String::new());
                        cols.extend(
                            anonymous.take(src.op.map(|op| op.expressions.len()).unwrap_or(0)),
                        )
                    };
                    // Render source with humanized expressions.
                    mode.expr(src, cols.as_ref()).fmt_text(f, &mut ctx)?;
                } else {
                    // Render source without humanized expressions.
                    mode.expr(src, None).fmt_text(f, &mut ctx)?;
                }
            }
        }

        if !self.context.used_indexes.is_empty() {
            writeln!(f)?;
            self.context.used_indexes.fmt_text(f, &mut ctx)?;
        }

        if let Some(target_cluster) = self.context.target_cluster {
            writeln!(f)?;
            writeln!(f, "Target cluster: {}", target_cluster)?;
        }

        if !(self.context.config.no_notices || self.context.optimizer_notices.is_empty()) {
            writeln!(f)?;
            writeln!(f, "Notices:")?;
            for notice in self.context.optimizer_notices.iter() {
                writeln!(f, "{}", notice)?;
            }
        }

        if self.context.config.timing {
            writeln!(f)?;
            writeln!(f, "Optimization time: {:?}", self.context.duration)?;
        }

        Ok(())
    }
}

impl<'a, C, M> DisplayText<C> for HumanizedExpr<'a, RowSetFinishing, M>
where
    C: AsMut<Indent>,
    M: HumanizerMode,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result {
        write!(f, "{}Finish", ctx.as_mut())?;
        // order by
        if !self.expr.order_by.is_empty() {
            let order_by = self.expr.order_by.iter().map(|e| self.child(e));
            write!(f, " order_by=[{}]", separated(", ", order_by))?;
        }
        // limit
        if let Some(limit) = self.expr.limit {
            write!(f, " limit={}", limit)?;
        }
        // offset
        if self.expr.offset > 0 {
            write!(f, " offset={}", self.expr.offset)?;
        }
        // project
        {
            let project = Indices(&self.expr.project);
            write!(f, " output=[{}]", project)?;
        }
        writeln!(f)
    }
}

impl<'a, C, M> DisplayText<C> for HumanizedExpr<'a, MapFilterProject, M>
where
    C: AsMut<Indent>,
    M: HumanizerMode,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result {
        let (scalars, predicates, outputs, input_arity) = (
            &self.expr.expressions,
            &self.expr.predicates,
            &self.expr.projection,
            &self.expr.input_arity,
        );

        // render `project` field iff not the identity projection
        if &outputs.len() != input_arity || outputs.iter().enumerate().any(|(i, p)| i != *p) {
            let outputs = Indices(outputs);
            writeln!(f, "{}project=({})", ctx.as_mut(), outputs)?;
        }
        // render `filter` field iff predicates are present
        if !predicates.is_empty() {
            let predicates = predicates.iter().map(|(_, p)| self.child(p));
            let predicates = separated(" AND ", predicates);
            writeln!(f, "{}filter=({})", ctx.as_mut(), predicates)?;
        }
        // render `map` field iff scalars are present
        if !scalars.is_empty() {
            let scalars = scalars.iter().map(|s| self.child(s));
            let scalars = separated(", ", scalars);
            writeln!(f, "{}map=({})", ctx.as_mut(), scalars)?;
        }

        Ok(())
    }
}

/// `EXPLAIN ... AS TEXT` support for [`MirRelationExpr`].
///
/// The format adheres to the following conventions:
/// 1. In general, every line corresponds to an [`MirRelationExpr`] node in the
///    plan.
/// 2. Non-recursive parameters of each sub-plan are written as `$key=$val`
///    pairs on the same line.
/// 3. A single non-recursive parameter can be written just as `$val`.
/// 4. Exceptions in (1) can be made when virtual syntax is requested (done by
///    default, can be turned off with `WITH(raw_syntax)`).
/// 5. Exceptions in (2) can be made when join implementations are rendered
///    explicitly `WITH(join_impls)`.
impl DisplayText<PlanRenderingContext<'_, MirRelationExpr>> for MirRelationExpr {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, MirRelationExpr>,
    ) -> fmt::Result {
        if ctx.config.raw_syntax {
            self.fmt_raw_syntax(f, ctx)
        } else {
            self.fmt_virtual_syntax(f, ctx)
        }
    }
}

impl MirRelationExpr {
    fn fmt_virtual_syntax(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, MirRelationExpr>,
    ) -> fmt::Result {
        // no virtual syntax support for now, evolve this
        // method as its HirRelationExpr counterpart
        self.fmt_raw_syntax(f, ctx)
    }

    fn fmt_raw_syntax(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, MirRelationExpr>,
    ) -> fmt::Result {
        use MirRelationExpr::*;

        let mode = HumanizedExplain::new(ctx.config.redacted);

        match &self {
            Constant { rows, typ: _ } => match rows {
                Ok(rows) => {
                    if !rows.is_empty() {
                        write!(f, "{}Constant", ctx.indent)?;
                        self.fmt_analyses(f, ctx)?;
                        ctx.indented(|ctx| {
                            fmt_text_constant_rows(
                                f,
                                rows.iter().map(|(x, y)| (x, y)),
                                &mut ctx.indent,
                                mode.redacted(),
                            )
                        })?;
                    } else {
                        write!(f, "{}Constant <empty>", ctx.indent)?;
                        self.fmt_analyses(f, ctx)?;
                    }
                }
                Err(err) => {
                    if mode.redacted() {
                        writeln!(f, "{}Error █", ctx.indent)?;
                    } else {
                        writeln!(f, "{}Error {}", ctx.indent, err.to_string().escaped())?;
                    }
                }
            },
            Let { id, value, body } => {
                let mut bindings = vec![(id, value.as_ref())];
                let mut head = body.as_ref();

                // Render Let-blocks nested in the body an outer Let-block in one step
                // with a flattened list of bindings
                while let Let { id, value, body } = head {
                    bindings.push((id, value.as_ref()));
                    head = body.as_ref();
                }

                if ctx.config.linear_chains {
                    writeln!(f, "{}With", ctx.indent)?;
                    ctx.indented(|ctx| {
                        for (id, value) in bindings.iter() {
                            writeln!(f, "{}cte {} =", ctx.indent, *id)?;
                            ctx.indented(|ctx| value.fmt_text(f, ctx))?;
                        }
                        Ok(())
                    })?;
                    write!(f, "{}Return", ctx.indent)?;
                    self.fmt_analyses(f, ctx)?;
                    ctx.indented(|ctx| head.fmt_text(f, ctx))?;
                } else {
                    writeln!(f, "{}With", ctx.indent)?;
                    ctx.indented(|ctx| {
                        for (id, value) in bindings.iter() {
                            writeln!(f, "{}cte {} =", ctx.indent, *id)?;
                            ctx.indented(|ctx| value.fmt_text(f, ctx))?;
                        }
                        Ok(())
                    })?;
                    write!(f, "{}Return", ctx.indent)?;
                    self.fmt_analyses(f, ctx)?;
                    ctx.indented(|ctx| head.fmt_text(f, ctx))?;
                }
            }
            LetRec {
                ids,
                values,
                limits,
                body,
            } => {
                assert_eq!(ids.len(), values.len());
                assert_eq!(ids.len(), limits.len());
                let bindings =
                    itertools::izip!(ids.iter(), values.iter(), limits.iter()).collect::<Vec<_>>();
                let head = body.as_ref();

                // Determine whether all `limits` are the same.
                // If all of them are the same, then we print it on top of the block (or not print
                // it at all if it's None). If there are differences, then we print them on the
                // ctes.
                let all_limits_same = limits
                    .iter()
                    .reduce(|first, i| if i == first { first } else { &None })
                    .unwrap_or(&None);

                if ctx.config.linear_chains {
                    unreachable!(); // We exclude this case in `as_explain_single_plan`.
                } else {
                    write!(f, "{}With Mutually Recursive", ctx.indent)?;
                    if let Some(limit) = all_limits_same {
                        write!(f, " {}", limit)?;
                    }
                    writeln!(f)?;
                    ctx.indented(|ctx| {
                        for (id, value, limit) in bindings.iter() {
                            write!(f, "{}cte", ctx.indent)?;
                            if all_limits_same.is_none() {
                                if let Some(limit) = limit {
                                    write!(f, " {}", limit)?;
                                }
                            }
                            writeln!(f, " {} =", id)?;
                            ctx.indented(|ctx| value.fmt_text(f, ctx))?;
                        }
                        Ok(())
                    })?;
                    write!(f, "{}Return", ctx.indent)?;
                    self.fmt_analyses(f, ctx)?;
                    ctx.indented(|ctx| head.fmt_text(f, ctx))?;
                }
            }
            Get {
                id,
                access_strategy: persist_or_index,
                ..
            } => {
                match id {
                    Id::Local(id) => {
                        assert!(matches!(persist_or_index, AccessStrategy::UnknownOrLocal));
                        write!(f, "{}Get {}", ctx.indent, id)?;
                    }
                    Id::Global(id) => {
                        let humanize = |id: &GlobalId| {
                            ctx.humanizer
                                .humanize_id(*id)
                                .unwrap_or_else(|| id.to_string())
                        };
                        let humanize_unqualified = |id: &GlobalId| {
                            ctx.humanizer
                                .humanize_id_unqualified(*id)
                                .unwrap_or_else(|| id.to_string())
                        };
                        let humanize_unqualified_maybe_deleted = |id: &GlobalId| {
                            ctx.humanizer
                                .humanize_id_unqualified(*id)
                                .unwrap_or("[DELETED INDEX]".to_owned())
                        };
                        match persist_or_index {
                            AccessStrategy::UnknownOrLocal => {
                                write!(f, "{}Get {}", ctx.indent, humanize(id))?;
                            }
                            AccessStrategy::Persist => {
                                write!(f, "{}ReadStorage {}", ctx.indent, humanize(id))?;
                            }
                            AccessStrategy::SameDataflow => {
                                write!(
                                    f,
                                    "{}ReadGlobalFromSameDataflow {}",
                                    ctx.indent,
                                    humanize(id)
                                )?;
                            }
                            AccessStrategy::Index(index_accesses) => {
                                let mut grouped_index_accesses = BTreeMap::new();
                                for (idx_id, usage_type) in index_accesses {
                                    grouped_index_accesses
                                        .entry(idx_id)
                                        .or_insert(Vec::new())
                                        .push(usage_type.clone());
                                }
                                write!(
                                    f,
                                    "{}ReadIndex on={} {}",
                                    ctx.indent,
                                    humanize_unqualified(id),
                                    separated(
                                        " ",
                                        grouped_index_accesses.iter().map(
                                            |(idx_id, usage_types)| {
                                                closure_to_display(move |f| {
                                                    write!(
                                                        f,
                                                        "{}=[{}]",
                                                        humanize_unqualified_maybe_deleted(idx_id),
                                                        IndexUsageType::display_vec(usage_types)
                                                    )
                                                })
                                            }
                                        )
                                    ),
                                )?;
                            }
                        }
                    }
                }
                self.fmt_analyses(f, ctx)?;
            }
            Project { outputs, input } => {
                FmtNode {
                    fmt_root: |f, ctx| {
                        let outputs = mode.seq(outputs, input.column_names(ctx));
                        let outputs = CompactScalars(outputs);
                        write!(f, "{}Project ({})", ctx.indent, outputs)?;
                        self.fmt_analyses(f, ctx)
                    },
                    fmt_children: |f, ctx| input.fmt_text(f, ctx),
                }
                .render(f, ctx)?;
            }
            Map { scalars, input } => {
                FmtNode {
                    fmt_root: |f, ctx: &mut PlanRenderingContext<'_, MirRelationExpr>| {
                        // Here, it's better to refer to `self.column_names(ctx)` rather than
                        // `input.column_names(ctx)`, because then we also get humanization for refs
                        // to cols introduced earlier by the same `Map`.
                        let scalars = mode.seq(scalars, self.column_names(ctx));
                        let scalars = CompactScalars(scalars);
                        write!(f, "{}Map ({})", ctx.indent, scalars)?;
                        self.fmt_analyses(f, ctx)
                    },
                    fmt_children: |f, ctx| input.fmt_text(f, ctx),
                }
                .render(f, ctx)?;
            }
            FlatMap { input, func, exprs } => {
                FmtNode {
                    fmt_root: |f, ctx: &mut PlanRenderingContext<'_, MirRelationExpr>| {
                        let exprs = mode.seq(exprs, input.column_names(ctx));
                        let exprs = CompactScalars(exprs);
                        write!(f, "{}FlatMap {}({})", ctx.indent, func, exprs)?;
                        self.fmt_analyses(f, ctx)
                    },
                    fmt_children: |f, ctx| input.fmt_text(f, ctx),
                }
                .render(f, ctx)?;
            }
            Filter { predicates, input } => {
                FmtNode {
                    fmt_root: |f, ctx: &mut PlanRenderingContext<'_, MirRelationExpr>| {
                        if predicates.is_empty() {
                            write!(f, "{}Filter", ctx.indent)?;
                        } else {
                            let cols = input.column_names(ctx);
                            let predicates = mode.seq(predicates, cols);
                            let predicates = separated(" AND ", predicates);
                            write!(f, "{}Filter {}", ctx.indent, predicates)?;
                        }
                        self.fmt_analyses(f, ctx)
                    },
                    fmt_children: |f, ctx| input.fmt_text(f, ctx),
                }
                .render(f, ctx)?;
            }
            Join {
                inputs,
                equivalences,
                implementation:
                    implementation @ (JoinImplementation::Differential(..)
                    | JoinImplementation::DeltaQuery(..)
                    | JoinImplementation::Unimplemented),
            } => {
                let has_equivalences = !equivalences.is_empty();

                if has_equivalences {
                    let cols = self.column_names(ctx);
                    let equivalences = separated(
                        " AND ",
                        equivalences.iter().map(|equivalence| {
                            let equivalence = mode.seq(equivalence, cols);
                            separated(" = ", equivalence)
                        }),
                    );
                    write!(f, "{}Join on=({})", ctx.indent, equivalences)?;
                } else {
                    write!(f, "{}CrossJoin", ctx.indent)?;
                }
                if let Some(name) = implementation.name() {
                    write!(f, " type={}", name)?;
                }

                self.fmt_analyses(f, ctx)?;

                if ctx.config.join_impls {
                    let input_name = &|pos: usize| -> String {
                        // Dig out a Get (or IndexedFilter), and return the name of its Id.
                        fn dig_name_from_expr(
                            h: &dyn ExprHumanizer,
                            e: &MirRelationExpr,
                        ) -> Option<String> {
                            let global_id_name = |id: &GlobalId| -> String {
                                h.humanize_id_unqualified(*id)
                                    .unwrap_or_else(|| id.to_string())
                            };
                            let (_mfp, e) = MapFilterProject::extract_from_expression(e);
                            match e {
                                Get { id, .. } => match id {
                                    Id::Local(lid) => Some(lid.to_string()),
                                    Id::Global(gid) => Some(global_id_name(gid)),
                                },
                                ArrangeBy { input, .. } => dig_name_from_expr(h, input),
                                Join {
                                    implementation: JoinImplementation::IndexedFilter(gid, ..),
                                    ..
                                } => Some(global_id_name(gid)),
                                _ => None,
                            }
                        }
                        match dig_name_from_expr(ctx.humanizer, &inputs[pos]) {
                            Some(str) => format!("%{}:{}", pos, str),
                            None => format!("%{}", pos),
                        }
                    };
                    let join_key_to_string = |key: &Vec<MirScalarExpr>| -> String {
                        if key.is_empty() {
                            "×".to_owned()
                        } else {
                            CompactScalars(mode.seq(key, None)).to_string()
                        }
                    };
                    let join_order = |start_idx: usize,
                                      start_key: &Option<Vec<MirScalarExpr>>,
                                      start_characteristics: &Option<JoinInputCharacteristics>,
                                      tail: &Vec<(
                        usize,
                        Vec<MirScalarExpr>,
                        Option<JoinInputCharacteristics>,
                    )>|
                     -> String {
                        format!(
                            "{}{}{} » {}",
                            input_name(start_idx),
                            match start_key {
                                None => "".to_owned(),
                                Some(key) => format!("[{}]", join_key_to_string(key)),
                            },
                            start_characteristics
                                .as_ref()
                                .map(|c| c.explain())
                                .unwrap_or_else(|| "".to_string()),
                            separated(
                                " » ",
                                tail.iter().map(|(pos, key, characteristics)| {
                                    format!(
                                        "{}[{}]{}",
                                        input_name(*pos),
                                        join_key_to_string(key),
                                        characteristics
                                            .as_ref()
                                            .map(|c| c.explain())
                                            .unwrap_or_else(|| "".to_string())
                                    )
                                })
                            ),
                        )
                    };
                    ctx.indented(|ctx| {
                        match implementation {
                            JoinImplementation::Differential(
                                (start_idx, start_key, start_characteristics),
                                tail,
                            ) => {
                                soft_assert_eq_or_log!(inputs.len(), tail.len() + 1);

                                writeln!(f, "{}implementation", ctx.indent)?;
                                ctx.indented(|ctx| {
                                    writeln!(
                                        f,
                                        "{}{}",
                                        ctx.indent,
                                        join_order(
                                            *start_idx,
                                            start_key,
                                            start_characteristics,
                                            tail
                                        )
                                    )
                                })?;
                            }
                            JoinImplementation::DeltaQuery(half_join_chains) => {
                                soft_assert_eq_or_log!(inputs.len(), half_join_chains.len());

                                writeln!(f, "{}implementation", ctx.indent)?;
                                ctx.indented(|ctx| {
                                    for (pos, chain) in half_join_chains.iter().enumerate() {
                                        writeln!(
                                            f,
                                            "{}{}",
                                            ctx.indent,
                                            join_order(pos, &None, &None, chain)
                                        )?;
                                    }
                                    Ok(())
                                })?;
                            }
                            JoinImplementation::IndexedFilter(_, _, _, _) => {
                                unreachable!() // because above we matched the other implementations
                            }
                            JoinImplementation::Unimplemented => {}
                        }
                        Ok(())
                    })?;
                }

                ctx.indented(|ctx| {
                    for input in inputs {
                        input.fmt_text(f, ctx)?;
                    }
                    Ok(())
                })?;
            }
            Join {
                implementation:
                    JoinImplementation::IndexedFilter(coll_id, idx_id, _key, literal_constraints),
                inputs,
                ..
            } => {
                let cse_id = match inputs.get(1).unwrap() {
                    // If the constant input is actually a Get, then let `fmt_indexed_filter` know.
                    Get { id, .. } => {
                        if let Id::Local(local_id) = id {
                            Some(local_id)
                        } else {
                            unreachable!()
                        }
                    }
                    _ => None,
                };
                Self::fmt_indexed_filter(
                    f,
                    ctx,
                    coll_id,
                    idx_id,
                    Some(literal_constraints.clone()),
                    cse_id,
                )?;
                self.fmt_analyses(f, ctx)?;
            }
            Reduce {
                group_key,
                aggregates,
                expected_group_size,
                monotonic,
                input,
            } => {
                FmtNode {
                    fmt_root: |f, ctx: &mut PlanRenderingContext<'_, MirRelationExpr>| {
                        if aggregates.len() == 0 && !ctx.config.raw_syntax {
                            write!(f, "{}Distinct", ctx.indent)?;

                            let group_key = mode.seq(group_key, input.column_names(ctx));
                            let group_key = CompactScalars(group_key);
                            write!(f, " project=[{}]", group_key)?;
                        } else {
                            write!(f, "{}Reduce", ctx.indent)?;

                            if group_key.len() > 0 {
                                let group_key = mode.seq(group_key, input.column_names(ctx));
                                let group_key = CompactScalars(group_key);
                                write!(f, " group_by=[{}]", group_key)?;
                            }
                        }
                        if aggregates.len() > 0 {
                            let cols = input.column_names(ctx);
                            let aggregates = mode.seq(aggregates, cols);
                            write!(f, " aggregates=[{}]", separated(", ", aggregates))?;
                        }
                        if *monotonic {
                            write!(f, " monotonic")?;
                        }
                        if let Some(expected_group_size) = expected_group_size {
                            write!(f, " exp_group_size={}", expected_group_size)?;
                        }
                        self.fmt_analyses(f, ctx)
                    },
                    fmt_children: |f, ctx| input.fmt_text(f, ctx),
                }
                .render(f, ctx)?;
            }
            TopK {
                group_key,
                order_key,
                limit,
                offset,
                monotonic,
                input,
                expected_group_size,
            } => {
                FmtNode {
                    fmt_root: |f, ctx: &mut PlanRenderingContext<'_, MirRelationExpr>| {
                        write!(f, "{}TopK", ctx.indent)?;
                        let cols = input.column_names(ctx);
                        if group_key.len() > 0 {
                            if cols.is_some() {
                                let group_by = mode.seq(group_key, cols);
                                write!(f, " group_by=[{}]", separated(", ", group_by))?;
                            } else {
                                let group_by = Indices(group_key);
                                write!(f, " group_by=[{}]", group_by)?;
                            }
                        }
                        if order_key.len() > 0 {
                            let order_by = mode.seq(order_key, cols);
                            write!(f, " order_by=[{}]", separated(", ", order_by))?;
                        }
                        if let Some(limit) = limit {
                            let limit = mode.expr(limit, cols);
                            write!(f, " limit={}", limit)?;
                        }
                        if offset > &0 {
                            write!(f, " offset={}", offset)?
                        }
                        if *monotonic {
                            write!(f, " monotonic")?;
                        }
                        if let Some(expected_group_size) = expected_group_size {
                            write!(f, " exp_group_size={}", expected_group_size)?;
                        }
                        self.fmt_analyses(f, ctx)
                    },
                    fmt_children: |f, ctx| input.fmt_text(f, ctx),
                }
                .render(f, ctx)?;
            }
            Negate { input } => {
                FmtNode {
                    fmt_root: |f, ctx| {
                        write!(f, "{}Negate", ctx.indent)?;
                        self.fmt_analyses(f, ctx)
                    },
                    fmt_children: |f, ctx| input.fmt_text(f, ctx),
                }
                .render(f, ctx)?;
            }
            Threshold { input } => {
                FmtNode {
                    fmt_root: |f, ctx| {
                        write!(f, "{}Threshold", ctx.indent)?;
                        self.fmt_analyses(f, ctx)
                    },
                    fmt_children: |f, ctx| input.fmt_text(f, ctx),
                }
                .render(f, ctx)?;
            }
            Union { base, inputs } => {
                write!(f, "{}Union", ctx.indent)?;
                self.fmt_analyses(f, ctx)?;
                ctx.indented(|ctx| {
                    base.fmt_text(f, ctx)?;
                    for input in inputs.iter() {
                        input.fmt_text(f, ctx)?;
                    }
                    Ok(())
                })?;
            }
            ArrangeBy { input, keys } => {
                FmtNode {
                    fmt_root: |f, ctx: &mut PlanRenderingContext<'_, MirRelationExpr>| {
                        write!(f, "{}ArrangeBy", ctx.indent)?;

                        let keys = keys.iter().map(|key| {
                            let key = mode.seq(key, input.column_names(ctx));
                            CompactScalars(key)
                        });
                        let keys = separated("], [", keys);
                        write!(f, " keys=[[{}]]", keys)?;

                        self.fmt_analyses(f, ctx)
                    },
                    fmt_children: |f, ctx| input.fmt_text(f, ctx),
                }
                .render(f, ctx)?;
            }
        }

        Ok(())
    }

    fn fmt_analyses(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &PlanRenderingContext<'_, MirRelationExpr>,
    ) -> fmt::Result {
        if ctx.config.requires_analyses() {
            if let Some(analyses) = ctx.annotations.get(self) {
                writeln!(f, " {}", HumanizedAnalyses::new(analyses, ctx))
            } else {
                writeln!(f, " // error: no analyses for subtree in map")
            }
        } else {
            writeln!(f)
        }
    }

    fn column_names<'a>(
        &'a self,
        ctx: &'a PlanRenderingContext<'_, MirRelationExpr>,
    ) -> Option<&'a Vec<String>> {
        if !ctx.config.humanized_exprs {
            None
        } else if let Some(analyses) = ctx.annotations.get(self) {
            analyses.column_names.as_ref()
        } else {
            None
        }
    }

    pub fn fmt_indexed_filter<'a, T>(
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'a, T>,
        coll_id: &GlobalId, // The id of the collection that the index is on
        idx_id: &GlobalId,  // The id of the index
        constants: Option<Vec<Row>>, // The values that we are looking up
        cse_id: Option<&LocalId>, // Sometimes, RelationCSE pulls out the const input
    ) -> fmt::Result {
        let mode = HumanizedExplain::new(ctx.config.redacted);

        let humanized_coll = ctx
            .humanizer
            .humanize_id(*coll_id)
            .unwrap_or_else(|| coll_id.to_string());
        let humanized_index = ctx
            .humanizer
            .humanize_id_unqualified(*idx_id)
            .unwrap_or("[DELETED INDEX]".to_owned());
        if let Some(constants) = constants {
            write!(
                f,
                "{}ReadIndex on={} {}=[{} ",
                ctx.as_mut(),
                humanized_coll,
                humanized_index,
                IndexUsageType::Lookup(*idx_id),
            )?;
            if let Some(cse_id) = cse_id {
                // If we were to simply print `constants` here, then the EXPLAIN output would look
                // weird: It would look like as if there was a dangling cte, because we (probably)
                // wouldn't be printing any Get that refers to that cte.
                write!(f, "values=<Get {}>]", cse_id)?;
            } else {
                if constants.len() == 1 {
                    let value = mode.expr(&constants[0], None);
                    write!(f, "value={}]", value)?;
                } else {
                    let values = mode.seq(&constants, None);
                    write!(f, "values=[{}]]", separated("; ", values))?;
                }
            }
        } else {
            // Can't happen in dataflow, only in fast path.
            write!(
                f,
                "{}ReadIndex on={} {}=[{}]",
                ctx.indent,
                humanized_coll,
                humanized_index,
                IndexUsageType::FullScan
            )?;
        }
        Ok(())
    }
}

/// A helper struct that abstracts over the formatting behavior of a
/// single-input node.
///
/// If [`mz_repr::explain::ExplainConfig::linear_chains`] is set, this will
/// render children before parents using the same indentation level, and if not
/// the children will be rendered indented after their parent.
struct FmtNode<F, G>
where
    F: FnOnce(
        &mut fmt::Formatter<'_>,
        &mut PlanRenderingContext<'_, MirRelationExpr>,
    ) -> fmt::Result,
    G: FnOnce(
        &mut fmt::Formatter<'_>,
        &mut PlanRenderingContext<'_, MirRelationExpr>,
    ) -> fmt::Result,
{
    fmt_root: F,
    fmt_children: G,
}

impl<F, G> FmtNode<F, G>
where
    F: FnOnce(
        &mut fmt::Formatter<'_>,
        &mut PlanRenderingContext<'_, MirRelationExpr>,
    ) -> fmt::Result,
    G: FnOnce(
        &mut fmt::Formatter<'_>,
        &mut PlanRenderingContext<'_, MirRelationExpr>,
    ) -> fmt::Result,
{
    fn render(
        self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, MirRelationExpr>,
    ) -> fmt::Result {
        let FmtNode {
            fmt_root,
            fmt_children,
        } = self;
        if ctx.config.linear_chains {
            // Render children before parents
            fmt_children(f, ctx)?;
            fmt_root(f, ctx)?;
        } else {
            // Render children indented after parent
            fmt_root(f, ctx)?;
            // cannot use ctx.indented() here
            *ctx.as_mut() += 1;
            fmt_children(f, ctx)?;
            *ctx.as_mut() -= 1;
        }
        Ok(())
    }
}

impl MirScalarExpr {
    pub fn format(&self, f: &mut fmt::Formatter<'_>, cols: Option<&Vec<String>>) -> fmt::Result {
        let mode = HumanizedExplain::default();
        fmt::Display::fmt(&mode.expr(self, cols), f)
    }
}

impl fmt::Display for MirScalarExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.format(f, None)
    }
}

/// A helper struct for wrapping expressions whose text output is modulated by
/// the presence of some local (inferred schema) or global (required redaction)
/// context.
#[derive(Debug, Clone)]
pub struct HumanizedExpr<'a, T, M = HumanizedExplain> {
    /// The expression to be humanized.
    pub expr: &'a T,
    /// An optional vector of inferred column names to be used when rendering
    /// column references in `expr`.
    pub cols: Option<&'a Vec<String>>,
    /// The rendering mode to use. See [`HumanizerMode`] for details.
    pub mode: M,
}

impl<'a, T, M: HumanizerMode> HumanizedExpr<'a, T, M> {
    /// Wrap the given child `expr` into a [`HumanizedExpr`] using the same
    /// `cols` and `mode` as `self`.
    pub fn child<U>(&self, expr: &'a U) -> HumanizedExpr<'a, U, M> {
        HumanizedExpr {
            expr,
            cols: self.cols,
            mode: self.mode.clone(),
        }
    }
}

/// A trait that abstracts the various ways in which we can humanize
/// expressions.
///
/// Currently, the degrees of freedom are:
/// - Humanizing for an `EXPLAIN` output vs humanizing for a notice output. This
///   is currently handled by the two different implementations of this trait -
///   [`HumanizedExplain`] vs [`HumanizedNotice`].
/// - Humanizing with redacted or non-redacted literals. This is currently
///   covered by the [`HumanizerMode::redacted`] method which is used by the
///   default implementation of [`HumanizerMode::humanize_datum`].
pub trait HumanizerMode: Sized + Clone {
    /// Default implementation of a default constructor.
    ///
    /// This will produce a [`HumanizerMode`] instance that redacts output in
    /// production deployments, but not in debug builds and in CI.
    fn default() -> Self {
        let redacted = !SOFT_ASSERTIONS.load(Ordering::Relaxed);
        Self::new(redacted)
    }

    /// Create a new instance of the optimizer mode with literal redaction
    /// determined by the `redacted` parameter value.
    fn new(redacted: bool) -> Self;

    /// Factory method that wraps the given `expr` and `cols` into a
    /// [`HumanizedExpr`] with the current `mode`.
    fn expr<'a, T>(
        &self,
        expr: &'a T,
        cols: Option<&'a Vec<String>>,
    ) -> HumanizedExpr<'a, T, Self> {
        HumanizedExpr {
            expr,
            cols,
            mode: self.clone(),
        }
    }

    /// Return `true` iff literal redaction is enabled for this mode.
    fn redacted(&self) -> bool;

    /// Render reference to column `col` which resolves to the given `ident`.
    fn humanize_ident(col: usize, ident: Ident, f: &mut fmt::Formatter<'_>) -> fmt::Result;

    /// Render a literal datum.
    ///
    /// The default implementation prints a redacted symbol (█) if redaction is
    /// enabled.
    fn humanize_datum(&self, datum: Datum<'_>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.redacted() {
            write!(f, "█")
        } else {
            write!(f, "{}", datum)
        }
    }
}

/// A [`HumanizerMode`] that is ambiguous but allows us to print valid SQL
/// statements, so it should be used in optimizer notices.
///
/// The inner parameter is `true` iff literals should be redacted.
#[derive(Debug, Clone)]
pub struct HumanizedNotice(bool);

impl HumanizedNotice {
    // TODO: move to `HumanizerMode` once we start using a Rust version with the
    // corresponding Rust stabilization issue:
    //
    // https://github.com/rust-lang/rust/pull/115822
    pub fn seq<'i, T>(
        &self,
        exprs: &'i [T],
        cols: Option<&'i Vec<String>>,
    ) -> impl Iterator<Item = HumanizedExpr<'i, T, Self>> + Clone {
        let mode = self.clone();
        exprs.iter().map(move |expr| mode.expr(expr, cols))
    }
}

impl HumanizerMode for HumanizedNotice {
    fn new(redacted: bool) -> Self {
        Self(redacted)
    }

    fn redacted(&self) -> bool {
        self.0
    }

    /// Write `ident`.
    fn humanize_ident(_col: usize, ident: Ident, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{ident}")
    }
}

/// A [`HumanizerMode`] that is unambiguous and should be used in `EXPLAIN` output.
///
/// The inner parameter is `true` iff literals should be redacted.
#[derive(Debug, Clone)]
pub struct HumanizedExplain(bool);

impl HumanizedExplain {
    // TODO: move to `HumanizerMode` once we start using a Rust version with the
    // corresponding Rust stabilization issue:
    //
    // https://github.com/rust-lang/rust/pull/115822
    pub fn seq<'i, T>(
        &self,
        exprs: &'i [T],
        cols: Option<&'i Vec<String>>,
    ) -> impl Iterator<Item = HumanizedExpr<'i, T, Self>> + Clone {
        let mode = self.clone();
        exprs.iter().map(move |expr| mode.expr(expr, cols))
    }
}

impl HumanizerMode for HumanizedExplain {
    fn new(redacted: bool) -> Self {
        Self(redacted)
    }

    fn redacted(&self) -> bool {
        self.0
    }

    /// Write `#c{ident}`.
    fn humanize_ident(col: usize, ident: Ident, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{col}{{{ident}}}")
    }
}

// A usize that directly represents a column reference.
impl<'a, M> fmt::Display for HumanizedExpr<'a, usize, M>
where
    M: HumanizerMode,
{
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.cols {
            // We have a name inferred for the column indexed by `self.expr`. Write `ident`.
            Some(cols) if cols.len() > *self.expr && !cols[*self.expr].is_empty() => {
                // Note: using unchecked here is okay since we're directly
                // converting to a string afterwards.
                let ident = Ident::new_unchecked(cols[*self.expr].clone()); // TODO: try to avoid the `.clone()` here.
                M::humanize_ident(*self.expr, ident, f)
            }
            // We don't have name inferred for this column.
            _ => {
                // Write `#c`.
                write!(f, "#{}", self.expr)
            }
        }
    }
}

impl<'a, M> ScalarOps for HumanizedExpr<'a, MirScalarExpr, M> {
    fn match_col_ref(&self) -> Option<usize> {
        self.expr.match_col_ref()
    }

    fn references(&self, col_ref: usize) -> bool {
        self.expr.references(col_ref)
    }
}

impl<'a, M> ScalarOps for HumanizedExpr<'a, usize, M> {
    fn match_col_ref(&self) -> Option<usize> {
        Some(*self.expr)
    }

    fn references(&self, col_ref: usize) -> bool {
        col_ref == *self.expr
    }
}

impl<'a, M> fmt::Display for HumanizedExpr<'a, MirScalarExpr, M>
where
    M: HumanizerMode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use MirScalarExpr::*;

        match self.expr {
            Column(i) => {
                // Delegate to the `HumanizedExpr<'a, _>` implementation.
                self.child(i).fmt(f)
            }
            Literal(row, _) => {
                // Delegate to the `HumanizedExpr<'a, _>` implementation.
                self.child(row).fmt(f)
            }
            CallUnmaterializable(func) => write!(f, "{}()", func),
            CallUnary { func, expr } => {
                if let crate::UnaryFunc::Not(_) = *func {
                    if let CallUnary { func, expr } = expr.as_ref() {
                        if let Some(is) = func.is() {
                            let expr = self.child::<MirScalarExpr>(&*expr);
                            return write!(f, "({}) IS NOT {}", expr, is);
                        }
                    }
                }
                if let Some(is) = func.is() {
                    let expr = self.child::<MirScalarExpr>(&*expr);
                    write!(f, "({}) IS {}", expr, is)
                } else {
                    let expr = self.child::<MirScalarExpr>(&*expr);
                    write!(f, "{}({})", func, expr)
                }
            }
            CallBinary { func, expr1, expr2 } => {
                let expr1 = self.child::<MirScalarExpr>(&*expr1);
                let expr2 = self.child::<MirScalarExpr>(&*expr2);
                if func.is_infix_op() {
                    write!(f, "({} {} {})", expr1, func, expr2)
                } else {
                    write!(f, "{}({}, {})", func, expr1, expr2)
                }
            }
            CallVariadic { func, exprs } => {
                use crate::VariadicFunc::*;
                let exprs = exprs.iter().map(|expr| self.child(expr));
                match func {
                    ArrayCreate { .. } => {
                        let exprs = separated(", ", exprs);
                        write!(f, "array[{}]", exprs)
                    }
                    ListCreate { .. } => {
                        let exprs = separated(", ", exprs);
                        write!(f, "list[{}]", exprs)
                    }
                    RecordCreate { .. } => {
                        let exprs = separated(", ", exprs);
                        write!(f, "row({})", exprs)
                    }
                    func if func.is_infix_op() && exprs.len() > 1 => {
                        let func = format!(" {} ", func);
                        let exprs = separated(&func, exprs);
                        write!(f, "({})", exprs)
                    }
                    func => {
                        let exprs = separated(", ", exprs);
                        write!(f, "{}({})", func, exprs)
                    }
                }
            }
            If { cond, then, els } => {
                let cond = self.child::<MirScalarExpr>(&*cond);
                let then = self.child::<MirScalarExpr>(&*then);
                let els = self.child::<MirScalarExpr>(&*els);
                write!(f, "case when {} then {} else {} end", cond, then, els)
            }
        }
    }
}

impl<'a, M> fmt::Display for HumanizedExpr<'a, AggregateExpr, M>
where
    M: HumanizerMode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.expr.is_count_asterisk() {
            return write!(f, "count(*)");
        }

        write!(
            f,
            "{}({}",
            self.child(&self.expr.func),
            if self.expr.distinct { "distinct " } else { "" }
        )?;

        self.child(&self.expr.expr).fmt(f)?;
        write!(f, ")")
    }
}

/// Render a literal value represented as a single-element [`Row`] or an
/// [`EvalError`].
///
/// The default implemntation calls [`HumanizerMode::humanize_datum`] for
/// the former and handles the error case (including redaction) directly for
/// the latter.
impl<'a, M> fmt::Display for HumanizedExpr<'a, Result<Row, EvalError>, M>
where
    M: HumanizerMode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.expr {
            Ok(row) => self.mode.humanize_datum(row.unpack_first(), f),
            Err(err) => {
                if self.mode.redacted() {
                    write!(f, "error(█)")
                } else {
                    write!(f, "error({})", err.to_string().escaped())
                }
            }
        }
    }
}

impl<'a, M> fmt::Display for HumanizedExpr<'a, Datum<'a>, M>
where
    M: HumanizerMode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.mode.humanize_datum(*self.expr, f)
    }
}

impl<'a, M> fmt::Display for HumanizedExpr<'a, Row, M>
where
    M: HumanizerMode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("(")?;
        for (i, d) in self.expr.iter().enumerate() {
            if i > 0 {
                write!(f, ", {}", self.child(&d))?;
            } else {
                write!(f, "{}", self.child(&d))?;
            }
        }
        f.write_str(")")?;
        Ok(())
    }
}

pub fn fmt_text_constant_rows<'a, I>(
    f: &mut fmt::Formatter<'_>,
    mut rows: I,
    ctx: &mut Indent,
    redacted: bool,
) -> fmt::Result
where
    I: Iterator<Item = (&'a Row, &'a Diff)>,
{
    let mut row_count = 0;
    let mut first_rows = Vec::with_capacity(20);
    for _ in 0..20 {
        if let Some((row, diff)) = rows.next() {
            row_count += diff.abs();
            first_rows.push((row, diff));
        }
    }
    let rest_of_row_count = rows.map(|(_, diff)| diff.abs()).sum::<Diff>();
    if rest_of_row_count != 0 {
        writeln!(
            f,
            "{}total_rows (diffs absed): {}",
            ctx,
            row_count + rest_of_row_count
        )?;
        writeln!(f, "{}first_rows:", ctx)?;
        ctx.indented(move |ctx| write_first_rows(f, &first_rows, ctx, redacted))?;
    } else {
        write_first_rows(f, &first_rows, ctx, redacted)?;
    }
    Ok(())
}

fn write_first_rows(
    f: &mut fmt::Formatter<'_>,
    first_rows: &Vec<(&Row, &Diff)>,
    ctx: &Indent,
    redacted: bool,
) -> fmt::Result {
    let mode = HumanizedExplain::new(redacted);
    for (row, diff) in first_rows {
        let row = mode.expr(*row, None);
        if **diff == 1 {
            writeln!(f, "{}- {}", ctx, row)?;
        } else {
            writeln!(f, "{}- ({} x {})", ctx, row, diff)?;
        }
    }
    Ok(())
}
