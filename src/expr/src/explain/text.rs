// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN ... AS TEXT` support for structures defined in this crate.

use std::fmt;

use mz_ore::soft_assert;
use mz_ore::str::{bracketed, separated, Indent, IndentLike, StrExt};
use mz_repr::explain::text::{fmt_text_constant_rows, DisplayText};
use mz_repr::explain::{
    CompactScalarSeq, ExprHumanizer, Indices, PlanRenderingContext, RenderingContext,
};
use mz_repr::{GlobalId, Row};

use super::{ExplainMultiPlan, ExplainSinglePlan};
use crate::explain::ExplainSource;
use crate::{
    AggregateExpr, Id, JoinImplementation, JoinInputCharacteristics, MapFilterProject,
    MirRelationExpr, MirScalarExpr, RowSetFinishing,
};

impl<'a, T: 'a> DisplayText for ExplainSinglePlan<'a, T>
where
    T: DisplayText<PlanRenderingContext<'a, T>>,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
        let mut ctx = PlanRenderingContext::new(
            Indent::default(),
            self.context.humanizer,
            self.plan.annotations.clone(),
            self.context.config,
        );

        if let Some(finishing) = &self.context.finishing {
            finishing.fmt_text(f, &mut ctx)?;
            ctx.indented(|ctx| self.plan.plan.fmt_text(f, ctx))?;
        } else {
            self.plan.plan.fmt_text(f, &mut ctx)?;
        }

        if !self.context.used_indexes.is_empty() {
            writeln!(f, "")?;
            self.context.used_indexes.fmt_text(f, &mut ctx)?;
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
    T: DisplayText<PlanRenderingContext<'a, T>>,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
        let mut ctx = RenderingContext::new(Indent::default(), self.context.humanizer);

        // render plans
        for (no, (id, plan)) in self.plans.iter().enumerate() {
            let mut ctx = PlanRenderingContext::new(
                ctx.indent.clone(),
                ctx.humanizer,
                plan.annotations.clone(),
                self.context.config,
            );

            if no > 0 {
                writeln!(f, "")?;
            }

            writeln!(f, "{}{}:", ctx.indent, id)?;
            ctx.indented(|ctx| {
                match &self.context.finishing {
                    // if present, a RowSetFinishing always applies to the first rendered plan
                    Some(finishing) if no == 0 => {
                        finishing.fmt_text(f, ctx)?;
                        ctx.indented(|ctx| plan.plan.fmt_text(f, ctx))?;
                    }
                    // all other plans are rendered without a RowSetFinishing
                    _ => {
                        plan.plan.fmt_text(f, ctx)?;
                    }
                }
                Ok(())
            })?;
        }
        if self
            .sources
            .iter()
            .any(|ExplainSource { op, .. }| !op.is_identity())
        {
            // render one blank line between the plans and sources
            writeln!(f, "")?;
            // render sources
            for ExplainSource {
                id,
                op,
                pushdown_info,
            } in self
                .sources
                .iter()
                .filter(|ExplainSource { op, .. }| !op.is_identity())
            {
                writeln!(f, "{}Source {}", ctx.indent, id)?;
                ctx.indented(|ctx| {
                    op.fmt_text(f, ctx)?;
                    pushdown_info.fmt_text(f, ctx)?;
                    Ok(())
                })?;
            }
        }

        if !self.context.used_indexes.is_empty() {
            writeln!(f, "")?;
            self.context.used_indexes.fmt_text(f, &mut ctx)?;
        }

        if self.context.config.timing {
            writeln!(f, "")?;
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
        write!(f, "{}Finish", ctx.as_mut())?;
        // order by
        if !self.order_by.is_empty() {
            let order_by = separated(", ", &self.order_by);
            write!(f, " order_by=[{}]", order_by)?;
        }
        // limit
        if let Some(limit) = self.limit {
            write!(f, " limit={}", limit)?;
        }
        // offset
        if self.offset > 0 {
            write!(f, " offset={}", self.offset)?;
        }
        // project
        {
            let project = Indices(&self.project);
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
        let (scalars, predicates, outputs, input_arity) = (
            &self.expressions,
            &self.predicates,
            &self.projection,
            &self.input_arity,
        );

        // render `project` field iff not the identity projection
        if &outputs.len() != input_arity || outputs.iter().enumerate().any(|(i, p)| i != *p) {
            let outputs = Indices(outputs);
            writeln!(f, "{}project=({})", ctx.as_mut(), outputs)?;
        }
        // render `filter` field iff predicates are present
        if !predicates.is_empty() {
            let predicates = predicates.iter().map(|(_, p)| p);
            let predicates = separated(" AND ", predicates);
            writeln!(f, "{}filter=({})", ctx.as_mut(), predicates)?;
        }
        // render `map` field iff scalars are present
        if !scalars.is_empty() {
            let scalars = CompactScalarSeq(scalars);
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
                        writeln!(f, "{}Constant <empty>", ctx.indent)?;
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
                max_iters,
                body,
            } => {
                assert_eq!(ids.len(), values.len());
                assert_eq!(ids.len(), max_iters.len());
                let bindings = itertools::izip!(ids.iter(), values.iter(), max_iters.iter())
                    .collect::<Vec<_>>(); // CLion needs these explicit types
                let head = body.as_ref();

                // Determine whether all `max_iters` are the same number.
                // If all of them are the same, then we print it on top of the block (or not print
                // it at all if it's None). If there are differences, then we print them on the
                // ctes.
                let all_max_iters_same = max_iters
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
                    if let Some(max_iter) = all_max_iters_same {
                        write!(f, " [iteration_limit={}]", max_iter)?;
                    }
                    writeln!(f)?;
                    ctx.indented(|ctx| {
                        for (id, value, max_iter) in bindings.iter().rev() {
                            write!(f, "{}cte", ctx.indent)?;
                            if all_max_iters_same.is_none() {
                                if let Some(max_iter) = max_iter {
                                    write!(f, " [iteration_limit={}]", max_iter)?;
                                }
                            }
                            writeln!(f, " {} =", id)?;
                            ctx.indented(|ctx| value.fmt_text(f, ctx))?;
                        }
                        Ok(())
                    })?;
                }
            }
            Get { id, .. } => {
                match id {
                    Id::Local(id) => {
                        write!(f, "{}Get {}", ctx.indent, id)?;
                    }
                    Id::Global(id) => {
                        let humanized_id = ctx
                            .humanizer
                            .humanize_id(*id)
                            .unwrap_or_else(|| id.to_string());
                        write!(f, "{}Get {}", ctx.indent, humanized_id)?;
                    }
                }
                self.fmt_attributes(f, ctx)?;
            }
            Project { outputs, input } => {
                FmtNode {
                    fmt_root: |f, ctx| {
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
                    fmt_root: |f, ctx| {
                        let scalars = CompactScalarSeq(scalars);
                        write!(f, "{}Map ({})", ctx.indent, scalars)?;
                        self.fmt_attributes(f, ctx)
                    },
                    fmt_children: |f, ctx| input.fmt_text(f, ctx),
                }
                .render(f, ctx)?;
            }
            FlatMap { input, func, exprs } => {
                FmtNode {
                    fmt_root: |f, ctx| {
                        let exprs = CompactScalarSeq(exprs);
                        write!(f, "{}FlatMap {}({})", ctx.indent, func, exprs)?;
                        self.fmt_attributes(f, ctx)
                    },
                    fmt_children: |f, ctx| input.fmt_text(f, ctx),
                }
                .render(f, ctx)?;
            }
            Filter { predicates, input } => {
                FmtNode {
                    fmt_root: |f, ctx| {
                        let predicates = separated(" AND ", predicates);
                        write!(f, "{}Filter {}", ctx.indent, predicates)?;
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
                let equivalences = separated(
                    " AND ",
                    equivalences.iter().map(|equivalence| {
                        if equivalence.len() == 2 {
                            bracketed("", "", separated(" = ", equivalence))
                        } else {
                            bracketed("eq(", ")", separated(", ", equivalence))
                        }
                    }),
                );

                if has_equivalences {
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
                            JoinImplementation::IndexedFilter(_, _, _) => {
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
                implementation: JoinImplementation::IndexedFilter(id, _key, literal_constraints),
                ..
            } => {
                Self::fmt_indexed_filter(f, ctx, id, Some(literal_constraints.clone()))?;
                self.fmt_attributes(f, ctx)?;
            }
            Reduce {
                group_key,
                aggregates,
                expected_group_size,
                monotonic: _, // TODO: monotonic should be an attribute
                input,
            } => {
                FmtNode {
                    fmt_root: |f, ctx| {
                        if aggregates.len() > 0 {
                            write!(f, "{}Reduce", ctx.indent)?;
                        } else {
                            write!(f, "{}Distinct", ctx.indent)?;
                        }
                        if group_key.len() > 0 {
                            let group_key = CompactScalarSeq(group_key);
                            write!(f, " group_by=[{}]", group_key)?;
                        }
                        if aggregates.len() > 0 {
                            let aggregates = separated(", ", aggregates);
                            write!(f, " aggregates=[{}]", aggregates)?;
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
                    fmt_root: |f, ctx| {
                        write!(f, "{}TopK", ctx.indent)?;
                        if group_key.len() > 0 {
                            let group_by = Indices(group_key);
                            write!(f, " group_by=[{}]", group_by)?;
                        }
                        if order_key.len() > 0 {
                            let order_by = separated(", ", order_key);
                            write!(f, " order_by=[{}]", order_by)?;
                        }
                        if let Some(limit) = limit {
                            write!(f, " limit={}", limit)?;
                        }
                        if offset > &0 {
                            write!(f, " offset={}", offset)?
                        }
                        write!(f, " monotonic={}", monotonic)?;
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
                    fmt_root: |f, ctx| {
                        let keys = separated("], [", keys.iter().map(|key| CompactScalarSeq(key)));
                        write!(f, "{}ArrangeBy keys=[[{}]]", ctx.indent, keys)?;
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
        ctx: &mut PlanRenderingContext<'_, MirRelationExpr>,
    ) -> fmt::Result {
        if ctx.config.requires_attributes() {
            if let Some(attrs) = ctx.annotations.get(self) {
                writeln!(f, " {}", attrs)
            } else {
                writeln!(f, " # error: no attrs for subtree in map")
            }
        } else {
            writeln!(f)
        }
    }

    pub fn fmt_indexed_filter<'b, C>(
        f: &mut fmt::Formatter<'_>,
        ctx: &mut C,
        id: &GlobalId,               // The id of the index
        constants: Option<Vec<Row>>, // The values that we are looking up
    ) -> fmt::Result
    where
        C: AsMut<mz_ore::str::Indent> + AsRef<&'b dyn mz_repr::explain::ExprHumanizer>,
    {
        let humanized_index = ctx
            .as_ref()
            .humanize_id(*id)
            .unwrap_or_else(|| id.to_string());
        if let Some(constants) = constants {
            write!(
                f,
                "{}ReadExistingIndex {} lookup_",
                ctx.as_mut(),
                humanized_index
            )?;
            if constants.len() == 1 {
                write!(f, "value={}", constants.get(0).unwrap())?;
            } else {
                write!(f, "values=[{}]", separated("; ", constants))?;
            }
        } else {
            write!(f, "{}ReadExistingIndex {}", ctx.as_mut(), humanized_index)?;
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

impl fmt::Display for MirScalarExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use MirScalarExpr::*;
        match self {
            Column(i) => write!(f, "#{}", i),
            Literal(row, _) => match row {
                Ok(row) => write!(f, "{}", row.unpack_first()),
                Err(err) => write!(f, "error({})", err.to_string().quoted()),
            },
            CallUnmaterializable(func) => write!(f, "{}()", func),
            CallUnary { func, expr } => {
                if let crate::UnaryFunc::Not(_) = *func {
                    if let CallUnary { func, expr } = expr.as_ref() {
                        if let Some(is) = func.is() {
                            return write!(f, "({}) IS NOT {}", expr, is);
                        }
                    }
                }
                if let Some(is) = func.is() {
                    write!(f, "({}) IS {}", expr, is)
                } else {
                    write!(f, "{}({})", func, expr)
                }
            }
            CallBinary { func, expr1, expr2 } => {
                if func.is_infix_op() {
                    write!(f, "({} {} {})", expr1, func, expr2)
                } else {
                    write!(f, "{}({}, {})", func, expr1, expr2)
                }
            }
            CallVariadic { func, exprs } => {
                use crate::VariadicFunc::*;
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
                write!(f, "case when {} then {} else {} end", cond, then, els)
            }
        }
    }
}

impl fmt::Display for AggregateExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_count_asterisk() {
            return write!(f, "count(*)");
        }

        write!(
            f,
            "{}({}",
            self.func.clone(),
            if self.distinct { "distinct " } else { "" }
        )?;

        self.expr.fmt(f)?;
        write!(f, ")")
    }
}
