// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN ... AS TEXT` support for MIR structures.
//!
//! The format adheres to the following conventions:
//! 1. In general, every line corresponds to an [`MirRelationExpr`] node in the
//!    plan.
//! 2. Non-recursive parameters of each sub-plan are written as `$key=$val`
//!    pairs on the same line.
//! 3. A single non-recursive parameter can be written just as `$val`.
//! 4. Exceptions in (1) can be made when virtual syntax is requested (done by
//!    default, can be turned off with `WITH(raw_syntax)`).
//! 5. Exceptions in (2) can be made when join implementations are rendered
//!    explicitly `WITH(join_impls)`.

use std::fmt;

use mz_expr::{
    explain::Indices, AggregateExpr, Id, JoinImplementation, JoinInputCharacteristics,
    MapFilterProject, MirRelationExpr, MirScalarExpr,
};
use mz_ore::soft_assert;
use mz_ore::str::{bracketed, separated, IndentLike, StrExt};
use mz_repr::explain_new::{fmt_text_constant_rows, separated_text, DisplayText, ExprHumanizer};
use mz_repr::{GlobalId, Row};

use crate::explain_new::{Displayable, PlanRenderingContext};

impl<'a> DisplayText<PlanRenderingContext<'_, MirRelationExpr>>
    for Displayable<'a, MirRelationExpr>
{
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

impl<'a> Displayable<'a, MirRelationExpr> {
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

        match &self.0 {
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
                            ctx.indented(|ctx| Displayable::from(*value).fmt_text(f, ctx))?;
                        }
                        Ok(())
                    })?;
                    write!(f, "{}Return", ctx.indent)?;
                    self.fmt_attributes(f, ctx)?;
                    ctx.indented(|ctx| Displayable::from(head).fmt_text(f, ctx))?;
                } else {
                    write!(f, "{}Return", ctx.indent)?;
                    self.fmt_attributes(f, ctx)?;
                    ctx.indented(|ctx| Displayable::from(head).fmt_text(f, ctx))?;
                    writeln!(f, "{}With", ctx.indent)?;
                    ctx.indented(|ctx| {
                        for (id, value) in bindings.iter().rev() {
                            writeln!(f, "{}cte {} =", ctx.indent, *id)?;
                            ctx.indented(|ctx| Displayable::from(*value).fmt_text(f, ctx))?;
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
                        let humanized_id = ctx.humanizer.humanize_id(*id).ok_or(fmt::Error)?;
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
                    fmt_children: |f, ctx| {
                        let input = Displayable::from(input.as_ref());
                        input.fmt_text(f, ctx)
                    },
                }
                .render(f, ctx)?;
            }
            Map { scalars, input } => {
                FmtNode {
                    fmt_root: |f, ctx| {
                        let scalars = separated_text(", ", scalars.iter().map(Displayable::from));
                        write!(f, "{}Map ({})", ctx.indent, scalars)?;
                        self.fmt_attributes(f, ctx)
                    },
                    fmt_children: |f, ctx| {
                        let input = Displayable::from(input.as_ref());
                        input.fmt_text(f, ctx)
                    },
                }
                .render(f, ctx)?;
            }
            FlatMap { input, func, exprs } => {
                FmtNode {
                    fmt_root: |f, ctx| {
                        let exprs = separated_text(", ", exprs.iter().map(Displayable::from));
                        write!(f, "{}FlatMap {}({})", ctx.indent, func, exprs)?;
                        self.fmt_attributes(f, ctx)
                    },
                    fmt_children: |f, ctx| {
                        let input = Displayable::from(input.as_ref());
                        input.fmt_text(f, ctx)
                    },
                }
                .render(f, ctx)?;
            }
            Filter { predicates, input } => {
                FmtNode {
                    fmt_root: |f, ctx| {
                        let predicates =
                            separated_text(" AND ", predicates.iter().map(Displayable::from));
                        write!(f, "{}Filter {}", ctx.indent, predicates)?;
                        self.fmt_attributes(f, ctx)
                    },
                    fmt_children: |f, ctx| {
                        let input = Displayable::from(input.as_ref());
                        input.fmt_text(f, ctx)
                    },
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
                            let global_id_name = |gid: &GlobalId| -> String {
                                h.humanize_id_unqualified(*gid)
                                    .unwrap_or_else(|| format!("?{}", gid))
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
                    let join_order = |head_idx: usize,
                                      tail: &Vec<(
                        usize,
                        Vec<MirScalarExpr>,
                        Option<JoinInputCharacteristics>,
                    )>|
                     -> String {
                        format!(
                            "{} » {}",
                            input_name(head_idx),
                            separated(
                                " » ",
                                tail.iter().map(|(pos, key, characteristics)| {
                                    format!(
                                        "{}[{}]{}",
                                        input_name(*pos),
                                        if key.is_empty() {
                                            "×".to_owned()
                                        } else {
                                            separated_text(", ", key.iter().map(Displayable::from))
                                                .to_string()
                                        },
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
                            JoinImplementation::Differential((head_idx, _head_key), tail) => {
                                soft_assert!(inputs.len() == tail.len() + 1);

                                writeln!(f, "{}implementation", ctx.indent)?;
                                ctx.indented(|ctx| {
                                    writeln!(f, "{}{}", ctx.indent, join_order(*head_idx, tail))
                                })?;
                            }
                            JoinImplementation::DeltaQuery(half_join_chains) => {
                                soft_assert!(inputs.len() == half_join_chains.len());

                                writeln!(f, "{}implementation", ctx.indent)?;
                                ctx.indented(|ctx| {
                                    for (pos, chain) in half_join_chains.iter().enumerate() {
                                        writeln!(f, "{}{}", ctx.indent, join_order(pos, chain))?;
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
                        Displayable::from(input).fmt_text(f, ctx)?;
                    }
                    Ok(())
                })?;
            }
            Join {
                implementation: JoinImplementation::IndexedFilter(id, _key, literal_constraints),
                ..
            } => {
                Self::fmt_indexed_filter(f, ctx, id, Some(literal_constraints.clone()))?;
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
                            let group_key =
                                separated_text(", ", group_key.iter().map(Displayable::from));
                            write!(f, " group_by=[{}]", group_key)?;
                        }
                        if aggregates.len() > 0 {
                            let aggregates =
                                separated_text(", ", aggregates.iter().map(Displayable::from));
                            write!(f, " aggregates=[{}]", aggregates)?;
                        }
                        if let Some(expected_group_size) = expected_group_size {
                            write!(f, " exp_group_size={}", expected_group_size)?;
                        }
                        self.fmt_attributes(f, ctx)
                    },
                    fmt_children: |f, ctx| {
                        let input = Displayable::from(input.as_ref());
                        input.fmt_text(f, ctx)
                    },
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
                        self.fmt_attributes(f, ctx)
                    },
                    fmt_children: |f, ctx| {
                        let input = Displayable::from(input.as_ref());
                        input.fmt_text(f, ctx)
                    },
                }
                .render(f, ctx)?;
            }
            Negate { input } => {
                FmtNode {
                    fmt_root: |f, ctx| {
                        write!(f, "{}Negate", ctx.indent)?;
                        self.fmt_attributes(f, ctx)
                    },
                    fmt_children: |f, ctx| {
                        let input = Displayable::from(input.as_ref());
                        input.fmt_text(f, ctx)
                    },
                }
                .render(f, ctx)?;
            }
            Threshold { input } => {
                FmtNode {
                    fmt_root: |f, ctx| {
                        write!(f, "{}Threshold", ctx.indent)?;
                        self.fmt_attributes(f, ctx)
                    },
                    fmt_children: |f, ctx| {
                        let input = Displayable::from(input.as_ref());
                        input.fmt_text(f, ctx)
                    },
                }
                .render(f, ctx)?;
            }
            Union { base, inputs } => {
                write!(f, "{}Union", ctx.indent)?;
                self.fmt_attributes(f, ctx)?;
                ctx.indented(|ctx| {
                    Displayable::from(base.as_ref()).fmt_text(f, ctx)?;
                    for input in inputs.iter() {
                        Displayable::from(input).fmt_text(f, ctx)?;
                    }
                    Ok(())
                })?;
            }
            ArrangeBy { input, keys } => {
                FmtNode {
                    fmt_root: |f, ctx| {
                        let keys = separated(
                            "], [",
                            keys.iter()
                                .map(|key| separated_text(", ", key.iter().map(Displayable::from))),
                        );
                        write!(f, "{}ArrangeBy keys=[[{}]]", ctx.indent, keys)?;
                        self.fmt_attributes(f, ctx)
                    },
                    fmt_children: |f, ctx| Displayable::from(input.as_ref()).fmt_text(f, ctx),
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
            if let Some(attrs) = ctx.annotations.get(self.0) {
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
        C: AsMut<mz_ore::str::Indent> + AsRef<&'b dyn mz_repr::explain_new::ExprHumanizer>,
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
                writeln!(f, "value={}", constants.get(0).unwrap())?;
            } else {
                writeln!(f, "values=[{}]", separated("; ", constants))?;
            }
        } else {
            writeln!(f, "{}ReadExistingIndex {}", ctx.as_mut(), humanized_index)?;
        }
        Ok(())
    }
}

/// A helper struct that abstracts over the formatting behavior of a
/// single-input node.
///
/// If [`mz_repr::explain_new::ExplainConfig::linear_chains`] is set, this will
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

impl<'a> DisplayText for Displayable<'a, MirScalarExpr> {
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, ctx: &mut ()) -> fmt::Result {
        use MirScalarExpr::*;
        match self.0 {
            Column(i) => write!(f, "#{}", i),
            Literal(row, _) => match row {
                Ok(row) => write!(f, "{}", row.unpack_first()),
                Err(err) => write!(f, "error({})", err.to_string().quoted()),
            },
            CallUnmaterializable(func) => write!(f, "{}()", func),
            CallUnary { func, expr } => {
                if let mz_expr::UnaryFunc::Not(_) = *func {
                    if let CallUnary {
                        func,
                        expr: inner_expr,
                    } = expr.as_ref()
                    {
                        if let Some(is) = func.is() {
                            write!(f, "(")?;
                            Displayable::from(inner_expr.as_ref()).fmt_text(f, ctx)?;
                            write!(f, ") IS NOT {}", is)?;
                            return Ok(());
                        }
                    }
                }
                if let Some(is) = func.is() {
                    write!(f, "(")?;
                    Displayable::from(expr.as_ref()).fmt_text(f, ctx)?;
                    write!(f, ") IS {}", is)
                } else {
                    write!(f, "{}(", func)?;
                    Displayable::from(expr.as_ref()).fmt_text(f, ctx)?;
                    write!(f, ")")
                }
            }
            CallBinary { func, expr1, expr2 } => {
                if func.is_infix_op() {
                    write!(f, "(")?;
                    Displayable::from(expr1.as_ref()).fmt_text(f, ctx)?;
                    write!(f, " {} ", func)?;
                    Displayable::from(expr2.as_ref()).fmt_text(f, ctx)?;
                    write!(f, ")")
                } else {
                    write!(f, "{}", func)?;
                    write!(f, "(")?;
                    Displayable::from(expr1.as_ref()).fmt_text(f, ctx)?;
                    write!(f, ", ")?;
                    Displayable::from(expr2.as_ref()).fmt_text(f, ctx)?;
                    write!(f, ")")
                }
            }
            CallVariadic { func, exprs } => {
                use mz_expr::VariadicFunc::*;
                match func {
                    ArrayCreate { .. } => {
                        let exprs = separated_text(", ", exprs.iter().map(Displayable::from));
                        write!(f, "array[{}]", exprs)
                    }
                    ListCreate { .. } => {
                        let exprs = separated_text(", ", exprs.iter().map(Displayable::from));
                        write!(f, "list[{}]", exprs)
                    }
                    RecordCreate { .. } => {
                        let exprs = separated_text(", ", exprs.iter().map(Displayable::from));
                        write!(f, "row({})", exprs)
                    }
                    func if func.is_infix_op() && exprs.len() > 1 => {
                        let func = format!(" {} ", func);
                        let exprs = separated_text(&func, exprs.iter().map(Displayable::from));
                        write!(f, "({})", exprs)
                    }
                    func => {
                        let exprs = separated_text(", ", exprs.iter().map(Displayable::from));
                        write!(f, "{}({})", func, exprs)
                    }
                }
            }
            If { cond, then, els } => {
                write!(f, "case when ")?;
                Displayable::from(cond.as_ref()).fmt_text(f, ctx)?;
                write!(f, " then ")?;
                Displayable::from(then.as_ref()).fmt_text(f, ctx)?;
                write!(f, " else ")?;
                Displayable::from(els.as_ref()).fmt_text(f, ctx)?;
                write!(f, " end")
            }
        }
    }
}

impl<'a> DisplayText for Displayable<'a, AggregateExpr> {
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, ctx: &mut ()) -> fmt::Result {
        let func = self.0.func.clone();
        if self.0.distinct {
            write!(f, "{}(distinct ", func)?;
            Displayable::from(&self.0.expr).fmt_text(f, ctx)?;
        } else {
            write!(f, "{}(", func)?;
            Displayable::from(&self.0.expr).fmt_text(f, ctx)?;
        }
        write!(f, ")")
    }
}
