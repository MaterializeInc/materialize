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

use mz_ore::soft_assert;
use mz_ore::str::{bracketed, closure_to_display, separated, Indent, IndentLike, StrExt};
use mz_repr::explain::text::{fmt_text_constant_rows, DisplayText};
use mz_repr::explain::{
    CompactScalarSeq, ExprHumanizer, HumanizedAttributes, IndexUsageType, Indices,
    PlanRenderingContext, RenderingContext,
};
use mz_repr::{GlobalId, Row};
use mz_sql_parser::ast::Ident;

use crate::explain::{ExplainMultiPlan, ExplainSinglePlan};
use crate::{
    AccessStrategy, AggregateExpr, Id, JoinImplementation, JoinInputCharacteristics, LocalId,
    MapFilterProject, MirRelationExpr, MirScalarExpr, RowSetFinishing,
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

        if let Some(finishing) = &self.context.finishing {
            if ctx.config.humanized_exprs {
                let attrs = ctx.annotations.get(&self.plan.plan);
                let cols = attrs.map(|attrs| attrs.column_names.clone()).flatten();
                HumanizedExpr::new(finishing, cols.as_ref()).fmt_text(f, &mut ctx)?;
            } else {
                finishing.fmt_text(f, &mut ctx)?;
            }
            ctx.indented(|ctx| self.plan.plan.fmt_text(f, ctx))?;
        } else {
            self.plan.plan.fmt_text(f, &mut ctx)?;
        }

        if !self.context.used_indexes.is_empty() {
            writeln!(f)?;
            self.context.used_indexes.fmt_text(f, &mut ctx)?;
        }

        if !self.context.optimizer_notices.is_empty() {
            writeln!(f)?;
            writeln!(f, "Notices:")?;
            for notice in self.context.optimizer_notices.iter() {
                writeln!(f, "{}", notice)?;
            }
        }

        if self.context.config.timing {
            writeln!(f, "")?;
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
                            let attrs = ctx.annotations.get(plan.plan);
                            let cols = attrs.map(|attrs| attrs.column_names.clone()).flatten();
                            HumanizedExpr::new(finishing, cols.as_ref()).fmt_text(f, ctx)?;
                        } else {
                            finishing.fmt_text(f, ctx)?;
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
                        cols.extend(anonymous.take(src.op.expressions.len()))
                    };
                    // Render source with humanized expressions.
                    HumanizedExpr::new(src, cols.as_ref()).fmt_text(f, &mut ctx)?;
                } else {
                    // Render source without humanized expresions.
                    src.fmt_text(f, &mut ctx)?;
                }
            }
        }

        if !self.context.used_indexes.is_empty() {
            writeln!(f)?;
            self.context.used_indexes.fmt_text(f, &mut ctx)?;
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

impl<C> DisplayText<C> for RowSetFinishing
where
    C: AsMut<Indent>,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result {
        HumanizedExpr::new(self, None).fmt_text(f, ctx)
    }
}

impl<'a, C> DisplayText<C> for HumanizedExpr<'a, RowSetFinishing>
where
    C: AsMut<Indent>,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result {
        write!(f, "{}Finish", ctx.as_mut())?;
        // order by
        if !self.expr.order_by.is_empty() {
            let order_by = HumanizedExpr::seq(&self.expr.order_by, self.cols);
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
        writeln!(f, "")
    }
}

impl<C> DisplayText<C> for MapFilterProject
where
    C: AsMut<Indent>,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result {
        HumanizedExpr::new(self, None).fmt_text(f, ctx)
    }
}

impl<'a, C> DisplayText<C> for HumanizedExpr<'a, MapFilterProject>
where
    C: AsMut<Indent>,
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
            let scalars = HumanizedExpr::seq(scalars, self.cols);
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

        match &self {
            Constant { rows, typ: _ } => match rows {
                Ok(rows) => {
                    if !rows.is_empty() {
                        write!(f, "{}Constant", ctx.indent)?;
                        self.fmt_attributes(f, ctx)?;
                        ctx.indented(|ctx| {
                            fmt_text_constant_rows(
                                f,
                                rows.iter().map(|(x, y)| (x, y)),
                                &mut ctx.indent,
                            )
                        })?;
                    } else {
                        write!(f, "{}Constant <empty>", ctx.indent)?;
                        self.fmt_attributes(f, ctx)?;
                    }
                }
                Err(err) => {
                    writeln!(f, "{}Error {}", ctx.indent, err.to_string().quoted())?;
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
                    self.fmt_attributes(f, ctx)?;
                    ctx.indented(|ctx| head.fmt_text(f, ctx))?;
                } else {
                    write!(f, "{}Return", ctx.indent)?;
                    self.fmt_attributes(f, ctx)?;
                    ctx.indented(|ctx| head.fmt_text(f, ctx))?;
                    writeln!(f, "{}With", ctx.indent)?;
                    ctx.indented(|ctx| {
                        for (id, value) in bindings.iter().rev() {
                            writeln!(f, "{}cte {} =", ctx.indent, *id)?;
                            ctx.indented(|ctx| value.fmt_text(f, ctx))?;
                        }
                        Ok(())
                    })?;
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
                    write!(f, "{}Return", ctx.indent)?;
                    self.fmt_attributes(f, ctx)?;
                    ctx.indented(|ctx| head.fmt_text(f, ctx))?;
                    write!(f, "{}With Mutually Recursive", ctx.indent)?;
                    if let Some(limit) = all_limits_same {
                        write!(f, " {}", limit)?;
                    }
                    writeln!(f)?;
                    ctx.indented(|ctx| {
                        for (id, value, limit) in bindings.iter().rev() {
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
                self.fmt_attributes(f, ctx)?;
            }
            Project { outputs, input } => {
                FmtNode {
                    fmt_root: |f, ctx| {
                        // We deliberately don't print humanized indices because
                        // in practice of our projection list are quite long.
                        let outputs = Indices(outputs);
                        write!(f, "{}Project ({})", ctx.indent, outputs)?;
                        self.fmt_attributes(f, ctx)
                    },
                    fmt_children: |f, ctx| input.fmt_text(f, ctx),
                }
                .render(f, ctx)?;
            }
            Map { scalars, input } => {
                FmtNode {
                    fmt_root: |f, ctx: &mut PlanRenderingContext<'_, MirRelationExpr>| {
                        if let Some(cols) = self.column_names(ctx) {
                            let scalars = separated(", ", HumanizedExpr::seq(scalars, Some(cols)));
                            write!(f, "{}Map ({})", ctx.indent, scalars)?;
                        } else {
                            let scalars = CompactScalarSeq(scalars);
                            write!(f, "{}Map ({})", ctx.indent, scalars)?;
                        }
                        self.fmt_attributes(f, ctx)
                    },
                    fmt_children: |f, ctx| input.fmt_text(f, ctx),
                }
                .render(f, ctx)?;
            }
            FlatMap { input, func, exprs } => {
                FmtNode {
                    fmt_root: |f, ctx: &mut PlanRenderingContext<'_, MirRelationExpr>| {
                        if let Some(cols) = input.column_names(ctx) {
                            let exprs = separated(", ", HumanizedExpr::seq(exprs, Some(cols)));
                            write!(f, "{}FlatMap {}({})", ctx.indent, func, exprs)?;
                        } else {
                            let exprs = CompactScalarSeq(exprs);
                            write!(f, "{}FlatMap {}({})", ctx.indent, func, exprs)?;
                        }
                        self.fmt_attributes(f, ctx)
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
                            let predicates = HumanizedExpr::seq(predicates, cols);
                            let predicates = separated(" AND ", predicates);
                            write!(f, "{}Filter {}", ctx.indent, predicates)?;
                        }
                        self.fmt_attributes(f, ctx)
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
                            let equivalences = equivalence.len();
                            let equivalence = HumanizedExpr::seq(equivalence, cols);
                            if equivalences == 2 {
                                bracketed("", "", separated(" = ", equivalence))
                            } else {
                                bracketed("eq(", ")", separated(", ", equivalence))
                            }
                        }),
                    );
                    write!(f, "{}Join on=({})", ctx.indent, equivalences)?;
                } else {
                    write!(f, "{}CrossJoin", ctx.indent)?;
                }
                if let Some(name) = implementation.name() {
                    write!(f, " type={}", name)?;
                }

                self.fmt_attributes(f, ctx)?;

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
                            CompactScalarSeq(key).to_string()
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
                                soft_assert!(inputs.len() == tail.len() + 1);

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
                                soft_assert!(inputs.len() == half_join_chains.len());

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
                self.fmt_attributes(f, ctx)?;
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

                            if let Some(cols) = input.column_names(ctx) {
                                let group_key = HumanizedExpr::seq(group_key, Some(cols));
                                write!(f, " project=[{}]", separated(", ", group_key))?;
                            } else {
                                let group_key = CompactScalarSeq(group_key);
                                write!(f, " project=[{}]", group_key)?;
                            }
                        } else {
                            write!(f, "{}Reduce", ctx.indent)?;

                            if group_key.len() > 0 {
                                if let Some(cols) = input.column_names(ctx) {
                                    let group_key = HumanizedExpr::seq(group_key, Some(cols));
                                    write!(f, " group_by=[{}]", separated(", ", group_key))?;
                                } else {
                                    let group_key = CompactScalarSeq(group_key);
                                    write!(f, " group_by=[{}]", group_key)?;
                                }
                            }
                        }
                        if aggregates.len() > 0 {
                            let cols = input.column_names(ctx);
                            let aggregates = HumanizedExpr::seq(aggregates, cols);
                            write!(f, " aggregates=[{}]", separated(", ", aggregates))?;
                        }
                        if *monotonic {
                            write!(f, " monotonic")?;
                        }
                        if let Some(expected_group_size) = expected_group_size {
                            write!(f, " exp_group_size={}", expected_group_size)?;
                        }
                        self.fmt_attributes(f, ctx)
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
                                let group_by = HumanizedExpr::seq(group_key, cols);
                                write!(f, " group_by=[{}]", separated(", ", group_by))?;
                            } else {
                                let group_by = Indices(group_key);
                                write!(f, " group_by=[{}]", group_by)?;
                            }
                        }
                        if order_key.len() > 0 {
                            let order_by = HumanizedExpr::seq(order_key, cols);
                            write!(f, " order_by=[{}]", separated(", ", order_by))?;
                        }
                        if let Some(limit) = limit {
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
                        self.fmt_attributes(f, ctx)
                    },
                    fmt_children: |f, ctx| input.fmt_text(f, ctx),
                }
                .render(f, ctx)?;
            }
            Negate { input } => {
                FmtNode {
                    fmt_root: |f, ctx| {
                        write!(f, "{}Negate", ctx.indent)?;
                        self.fmt_attributes(f, ctx)
                    },
                    fmt_children: |f, ctx| input.fmt_text(f, ctx),
                }
                .render(f, ctx)?;
            }
            Threshold { input } => {
                FmtNode {
                    fmt_root: |f, ctx| {
                        write!(f, "{}Threshold", ctx.indent)?;
                        self.fmt_attributes(f, ctx)
                    },
                    fmt_children: |f, ctx| input.fmt_text(f, ctx),
                }
                .render(f, ctx)?;
            }
            Union { base, inputs } => {
                write!(f, "{}Union", ctx.indent)?;
                self.fmt_attributes(f, ctx)?;
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

                        if let Some(cols) = input.column_names(ctx) {
                            let keys = keys.iter().map(|key| {
                                let key = HumanizedExpr::seq(key, Some(cols));
                                separated(", ", key)
                            });
                            let keys = separated("], [", keys);
                            write!(f, " keys=[[{}]]", keys)?;
                        } else {
                            let keys = keys.iter().map(|key| CompactScalarSeq(key));
                            let keys = separated("], [", keys);
                            write!(f, " keys=[[{}]]", keys)?;
                        }

                        self.fmt_attributes(f, ctx)
                    },
                    fmt_children: |f, ctx| input.fmt_text(f, ctx),
                }
                .render(f, ctx)?;
            }
        }

        Ok(())
    }

    fn fmt_attributes(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &PlanRenderingContext<'_, MirRelationExpr>,
    ) -> fmt::Result {
        if ctx.config.requires_attributes() {
            if let Some(attrs) = ctx.annotations.get(self) {
                writeln!(f, " {}", HumanizedAttributes::new(attrs, ctx))
            } else {
                writeln!(f, " // error: no attrs for subtree in map")
            }
        } else {
            writeln!(f)
        }
    }

    fn column_names<'a>(
        &'a self,
        ctx: &'a PlanRenderingContext<'_, MirRelationExpr>,
    ) -> Option<&Vec<String>> {
        if !ctx.config.humanized_exprs {
            None
        } else if let Some(attrs) = ctx.annotations.get(self) {
            attrs.column_names.as_ref()
        } else {
            None
        }
    }

    pub fn fmt_indexed_filter<'b, C>(
        f: &mut fmt::Formatter<'_>,
        ctx: &mut C,
        coll_id: &GlobalId, // The id of the collection that the index is on
        idx_id: &GlobalId,  // The id of the index
        constants: Option<Vec<Row>>, // The values that we are looking up
        cse_id: Option<&LocalId>, // Sometimes, RelationCSE pulls out the const input
    ) -> fmt::Result
    where
        C: AsMut<mz_ore::str::Indent> + AsRef<&'b dyn ExprHumanizer>,
    {
        let humanized_coll = ctx
            .as_ref()
            .humanize_id(*coll_id)
            .unwrap_or_else(|| coll_id.to_string());
        let humanized_index = ctx
            .as_ref()
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
                    write!(f, "value={}]", constants.get(0).unwrap())?;
                } else {
                    write!(f, "values=[{}]]", separated("; ", constants))?;
                }
            }
        } else {
            // Can't happen in dataflow, only in fast path.
            write!(
                f,
                "{}ReadIndex on={} {}=[{}]",
                ctx.as_mut(),
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
        fmt::Display::fmt(&HumanizedExpr::new(self, cols), f)
    }
}

impl fmt::Display for MirScalarExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.format(f, None)
    }
}

#[derive(Debug, Clone)]
pub struct HumanizedExpr<'a, T> {
    pub(crate) expr: &'a T,
    pub(crate) cols: Option<&'a Vec<String>>,
}

impl<'a, T> HumanizedExpr<'a, T> {
    pub fn new(expr: &'a T, cols: Option<&'a Vec<String>>) -> Self {
        Self { expr, cols }
    }

    pub fn seq<'i>(
        exprs: &'i [T],
        cols: Option<&'i Vec<String>>,
    ) -> impl Iterator<Item = HumanizedExpr<'i, T>> + Clone {
        exprs.iter().map(move |expr| HumanizedExpr::new(expr, cols))
    }

    pub(crate) fn child<U>(&self, expr: &'a U) -> HumanizedExpr<'a, U> {
        HumanizedExpr {
            expr,
            cols: self.cols,
        }
    }
}

// A usize that directly represents a column reference
impl<'a> fmt::Display for HumanizedExpr<'a, usize> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.cols {
            Some(cols) if !cols[*self.expr].is_empty() => {
                // Write #c{name} if we have a name inferred for this column.
                let ident = Ident::new(cols[*self.expr].clone()); // TODO: try to avoid the `.clone()` here.
                write!(f, "#{}{{{}}}", self.expr, ident)
            }
            _ => {
                // Write #c otherwise.
                write!(f, "#{}", self.expr)
            }
        }
    }
}

impl<'a> fmt::Display for HumanizedExpr<'a, MirScalarExpr> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use MirScalarExpr::*;

        match self.expr {
            Column(i) => {
                // Delegate to the `HumanizedExpr<'a, usize>` implementation.
                self.child(i).fmt(f)
            }
            Literal(row, _) => match row {
                Ok(row) => write!(f, "{}", row.unpack_first()),
                Err(err) => write!(f, "error({})", err.to_string().quoted()),
            },
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

impl fmt::Display for AggregateExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        HumanizedExpr::new(self, None).fmt(f)
    }
}

impl<'a> fmt::Display for HumanizedExpr<'a, AggregateExpr> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.expr.is_count_asterisk() {
            return write!(f, "count(*)");
        }

        write!(
            f,
            "{}({}",
            self.expr.func.clone(),
            if self.expr.distinct { "distinct " } else { "" }
        )?;

        self.child(&self.expr.expr).fmt(f)?;
        write!(f, ")")
    }
}

/// Displays a `Row` of which the caller knows to contain exactly 1 field. This is to avoid printing
/// an extra pair of parenthesis that wrap `Row`s in the normal Display.
pub fn display_singleton_row(r: Row) -> impl fmt::Display {
    struct SingletonRow(Row);
    impl fmt::Display for SingletonRow {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let mut iter = self.0.into_iter();
            let d = iter.next().unwrap();
            assert!(matches!(iter.next(), None));
            write!(f, "{}", d)
        }
    }
    SingletonRow(r)
}
