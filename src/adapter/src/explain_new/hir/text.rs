// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN ... AS TEXT` support for HIR structures.
//!
//! The format adheres to the following conventions:
//! 1. In general, every line corresponds to an [`HirRelationExpr`] node in the
//!    plan.
//! 2. Non-recursive parameters of each sub-plan are written as `$key=$val`
//!    pairs on the same line.
//! 3. A single non-recursive parameter can be written just as `$val`.
//! 4. Exceptions in (1) can be made when virtual syntax is requested (done by
//!    default, can be turned off with `WITH(raw_syntax)`).

use std::fmt;

use mz_expr::explain::Indices;
use mz_expr::virtual_syntax::{AlgExcept, Except};
use mz_expr::Id;
use mz_ore::str::{separated, IndentLike};
use mz_repr::explain_new::{fmt_text_constant_rows, separated_text, DisplayText};
use mz_sql::plan::{AggregateExpr, Hir, HirRelationExpr, HirScalarExpr, JoinKind, WindowExprType};

use crate::explain_new::{Displayable, PlanRenderingContext};

impl<'a> DisplayText<PlanRenderingContext<'_, HirRelationExpr>>
    for Displayable<'a, HirRelationExpr>
{
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, HirRelationExpr>,
    ) -> fmt::Result {
        if ctx.config.raw_syntax {
            self.fmt_raw_syntax(f, ctx)
        } else {
            self.fmt_virtual_syntax(f, ctx)
        }
    }
}

impl<'a> Displayable<'a, HirRelationExpr> {
    fn fmt_virtual_syntax(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, HirRelationExpr>,
    ) -> fmt::Result {
        if let Some(Except { all, lhs, rhs }) = Hir::un_except(&self.0) {
            if all {
                writeln!(f, "{}ExceptAll", ctx.indent)?;
            } else {
                writeln!(f, "{}Except", ctx.indent)?;
            }
            ctx.indented(|ctx| {
                Displayable::from(lhs).fmt_text(f, ctx)?;
                Displayable::from(rhs).fmt_text(f, ctx)?;
                Ok(())
            })?;
        } else {
            // fallback to raw syntax formatting as a last resort
            self.fmt_raw_syntax(f, ctx)?;
        }

        Ok(())
    }

    fn fmt_raw_syntax(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, HirRelationExpr>,
    ) -> fmt::Result {
        use HirRelationExpr::*;
        match &self.0 {
            Constant { rows, .. } => {
                writeln!(f, "{}Constant", ctx.indent)?;
                ctx.indented(|ctx| {
                    fmt_text_constant_rows(f, rows.iter().map(|row| (row, &1)), &mut ctx.indent)
                })?;
            }
            Let {
                name: _,
                id,
                value,
                body,
            } => {
                let mut bindings = vec![(id, value.as_ref())];
                let mut head = body.as_ref();

                // Render Let-blocks nested in the body an outer Let-block in one step
                // with a flattened list of bindings
                while let Let {
                    name: _,
                    id,
                    value,
                    body,
                } = head
                {
                    bindings.push((id, value.as_ref()));
                    head = body.as_ref();
                }

                // The body comes first in the text output format in order to
                // align with the format convention the dataflow is rendered
                // top to bottom
                writeln!(f, "{}Let", ctx.indent)?;
                ctx.indented(|ctx| {
                    Displayable::from(head).fmt_text(f, ctx)?;
                    writeln!(f, "{}Where", ctx.indent)?;
                    ctx.indented(|ctx| {
                        for (id, value) in bindings.iter().rev() {
                            // TODO: print the name and not the id
                            writeln!(f, "{}{} =", ctx.indent, *id)?;
                            ctx.indented(|ctx| Displayable::from(*value).fmt_text(f, ctx))?;
                        }
                        Ok(())
                    })?;
                    Ok(())
                })?;
            }
            Get { id, .. } => match id {
                Id::Local(id) => {
                    // TODO: resolve local id to the human-readable name from the context
                    writeln!(f, "{}Get {}", ctx.indent, id)?;
                }
                Id::Global(id) => {
                    let humanized_id = ctx.humanizer.humanize_id(*id).ok_or(fmt::Error)?;
                    writeln!(f, "{}Get {}", ctx.indent, humanized_id)?;
                }
            },
            Project { outputs, input } => {
                let outputs = Indices(outputs);
                writeln!(f, "{}Project ({})", ctx.indent, outputs)?;
                ctx.indented(|ctx| Displayable::from(input.as_ref()).fmt_text(f, ctx))?;
            }
            Map { scalars, input } => {
                let scalars = separated_text(", ", scalars.iter().map(Displayable::from));
                writeln!(f, "{}Map ({})", ctx.indent, scalars)?;
                ctx.indented(|ctx| Displayable::from(input.as_ref()).fmt_text(f, ctx))?;
            }
            CallTable { func, exprs } => {
                let exprs = separated_text(", ", exprs.iter().map(Displayable::from));
                writeln!(f, "{}CallTable {}({})", ctx.indent, func, exprs)?;
            }
            Filter { predicates, input } => {
                let predicates = separated_text(" AND ", predicates.iter().map(Displayable::from));
                writeln!(f, "{}Filter {}", ctx.indent, predicates)?;
                ctx.indented(|ctx| Displayable::from(input.as_ref()).fmt_text(f, ctx))?;
            }
            Join {
                left,
                right,
                on,
                kind,
            } => {
                if on.is_literal_true() && kind == &JoinKind::Inner {
                    write!(f, "{}CrossJoin", ctx.indent)?;
                } else {
                    write!(f, "{}{}Join ", ctx.indent, kind)?;
                    Displayable::from(on).fmt_text(f, &mut ())?;
                }
                writeln!(f)?;
                ctx.indented(|ctx| {
                    Displayable::from(left.as_ref()).fmt_text(f, ctx)?;
                    Displayable::from(right.as_ref()).fmt_text(f, ctx)?;
                    Ok(())
                })?;
            }
            Reduce {
                group_key,
                aggregates,
                expected_group_size,
                input,
            } => {
                write!(f, "{}Reduce", ctx.indent)?;
                if group_key.len() > 0 {
                    let group_key = Indices(group_key);
                    write!(f, " group_by=[{}]", group_key)?;
                }
                if aggregates.len() > 0 {
                    let aggregates = separated_text(", ", aggregates.iter().map(Displayable::from));
                    write!(f, " aggregates=[{}]", aggregates)?;
                }
                if let Some(expected_group_size) = expected_group_size {
                    write!(f, " exp_group_size={}", expected_group_size)?;
                }
                writeln!(f)?;
                ctx.indented(|ctx| Displayable::from(input.as_ref()).fmt_text(f, ctx))?;
            }
            Distinct { input } => {
                writeln!(f, "{}Distinct", ctx.indent)?;
                ctx.indented(|ctx| Displayable::from(input.as_ref()).fmt_text(f, ctx))?;
            }
            TopK {
                group_key,
                order_key,
                limit,
                offset,
                input,
            } => {
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
                writeln!(f)?;
                ctx.indented(|ctx| Displayable::from(input.as_ref()).fmt_text(f, ctx))?;
            }
            Negate { input } => {
                writeln!(f, "{}Negate", ctx.indent)?;
                ctx.indented(|ctx| Displayable::from(input.as_ref()).fmt_text(f, ctx))?;
            }
            Threshold { input } => {
                writeln!(f, "{}Threshold", ctx.indent)?;
                ctx.indented(|ctx| Displayable::from(input.as_ref()).fmt_text(f, ctx))?;
            }
            Union { base, inputs } => {
                writeln!(f, "{}Union", ctx.indent)?;
                ctx.indented(|ctx| {
                    Displayable::from(base.as_ref()).fmt_text(f, ctx)?;
                    for input in inputs.iter() {
                        Displayable::from(input).fmt_text(f, ctx)?;
                    }
                    Ok(())
                })?;
            }
        }

        Ok(())
    }
}

impl<'a> DisplayText for Displayable<'a, HirScalarExpr> {
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, ctx: &mut ()) -> fmt::Result {
        use HirRelationExpr::Get;
        use HirScalarExpr::*;
        match self.0 {
            Column(i) => write!(
                f,
                "#{}{}",
                (0..i.level).map(|_| '^').collect::<String>(),
                i.column
            ),
            Parameter(i) => write!(f, "${}", i),
            Literal(row, _) => write!(f, "{}", row.unpack_first()),
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
            Windowing(expr) => {
                match &expr.func {
                    WindowExprType::Scalar(scalar) => {
                        write!(f, "{}()", scalar.clone().into_expr())?
                    }
                    WindowExprType::Value(scalar) => {
                        write!(f, "{}(", scalar.clone().into_expr())?;
                        Displayable::from(scalar.expr.as_ref()).fmt_text(f, ctx)?;
                        write!(f, ")")?
                    }
                }
                write!(f, " over (")?;
                for (i, e) in expr.partition.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    Displayable::from(e).fmt_text(f, ctx)?;
                }
                write!(f, ")")?;

                if !expr.order_by.is_empty() {
                    write!(f, " order by (")?;
                    for (i, e) in expr.order_by.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        Displayable::from(e).fmt_text(f, ctx)?;
                    }
                    write!(f, ")")?;
                }
                Ok(())
            }
            Exists(expr) => match expr.as_ref() {
                Get { id, .. } => write!(f, "exists(Get {})", id), // TODO: optional humanizer
                _ => write!(f, "exists(???)"),
            },
            Select(expr) => match expr.as_ref() {
                Get { id, .. } => write!(f, "select(Get {})", id), // TODO: optional humanizer
                _ => write!(f, "select(???)"),
            },
        }
    }
}

impl<'a> DisplayText for Displayable<'a, AggregateExpr> {
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, ctx: &mut ()) -> fmt::Result {
        let func = self.0.func.clone().into_expr();
        if self.0.distinct {
            write!(f, "{}(distinct ", func)?;
            Displayable::from(self.0.expr.as_ref()).fmt_text(f, ctx)?;
        } else {
            write!(f, "{}(", func)?;
            Displayable::from(self.0.expr.as_ref()).fmt_text(f, ctx)?;
        }
        write!(f, ")")
    }
}
