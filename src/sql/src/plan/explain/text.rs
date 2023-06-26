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

use itertools::Itertools;
use std::fmt;

use mz_expr::virtual_syntax::{AlgExcept, Except};
use mz_expr::{Id, WindowFrame};
use mz_ore::str::{separated, IndentLike};
use mz_repr::explain::text::{fmt_text_constant_rows, DisplayText};
use mz_repr::explain::{CompactScalarSeq, Indices, PlanRenderingContext};

use crate::plan::{AggregateExpr, Hir, HirRelationExpr, HirScalarExpr, JoinKind, WindowExprType};

impl DisplayText<PlanRenderingContext<'_, HirRelationExpr>> for HirRelationExpr {
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

impl HirRelationExpr {
    fn fmt_virtual_syntax(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, HirRelationExpr>,
    ) -> fmt::Result {
        if let Some(Except { all, lhs, rhs }) = Hir::un_except(self) {
            if all {
                writeln!(f, "{}ExceptAll", ctx.indent)?;
            } else {
                writeln!(f, "{}Except", ctx.indent)?;
            }
            ctx.indented(|ctx| {
                lhs.fmt_text(f, ctx)?;
                rhs.fmt_text(f, ctx)?;
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
        match &self {
            Constant { rows, .. } => {
                if !rows.is_empty() {
                    writeln!(f, "{}Constant", ctx.indent)?;
                    ctx.indented(|ctx| {
                        fmt_text_constant_rows(f, rows.iter().map(|row| (row, &1)), &mut ctx.indent)
                    })?;
                } else {
                    writeln!(f, "{}Constant <empty>", ctx.indent)?;
                }
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

                writeln!(f, "{}Return", ctx.indent)?;
                ctx.indented(|ctx| head.fmt_text(f, ctx))?;
                writeln!(f, "{}With", ctx.indent)?;
                ctx.indented(|ctx| {
                    for (id, value) in bindings.iter().rev() {
                        // TODO: print the name and not the id
                        writeln!(f, "{}cte {} =", ctx.indent, *id)?;
                        ctx.indented(|ctx| value.fmt_text(f, ctx))?;
                    }
                    Ok(())
                })?;
            }
            LetRec {
                limit,
                bindings,
                body,
            } => {
                writeln!(f, "{}Return", ctx.indent)?;
                ctx.indented(|ctx| body.fmt_text(f, ctx))?;
                write!(f, "{}With Mutually Recursive", ctx.indent)?;
                if let Some(limit) = limit {
                    write!(f, " {}", limit)?;
                }
                writeln!(f)?;
                ctx.indented(|ctx| {
                    for (_name, id, value, _type) in bindings.iter().rev() {
                        // TODO: print the name and not the id
                        writeln!(f, "{}cte {} =", ctx.indent, *id)?;
                        ctx.indented(|ctx| value.fmt_text(f, ctx))?;
                    }
                    Ok(())
                })?;
            }
            Get { id, .. } => match id {
                Id::Local(id) => {
                    // TODO: resolve local id to the human-readable name from the context
                    writeln!(f, "{}Get {}", ctx.indent, id)?;
                }
                Id::Global(id) => {
                    let humanized_id = ctx
                        .humanizer
                        .humanize_id(*id)
                        .unwrap_or_else(|| id.to_string());
                    writeln!(f, "{}Get {}", ctx.indent, humanized_id)?;
                }
            },
            Project { outputs, input } => {
                let outputs = Indices(outputs);
                writeln!(f, "{}Project ({})", ctx.indent, outputs)?;
                ctx.indented(|ctx| input.fmt_text(f, ctx))?;
            }
            Map { scalars, input } => {
                let scalars = CompactScalarSeq(scalars);
                writeln!(f, "{}Map ({})", ctx.indent, scalars)?;
                ctx.indented(|ctx| input.fmt_text(f, ctx))?;
            }
            CallTable { func, exprs } => {
                let exprs = CompactScalarSeq(exprs);
                writeln!(f, "{}CallTable {}({})", ctx.indent, func, exprs)?;
            }
            Filter { predicates, input } => {
                let predicates = separated(" AND ", predicates);
                writeln!(f, "{}Filter {}", ctx.indent, predicates)?;
                ctx.indented(|ctx| input.fmt_text(f, ctx))?;
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
                    write!(f, "{}{}Join {}", ctx.indent, kind, on)?;
                }
                writeln!(f)?;
                ctx.indented(|ctx| {
                    left.fmt_text(f, ctx)?;
                    right.fmt_text(f, ctx)?;
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
                    let aggregates = separated(", ", aggregates);
                    write!(f, " aggregates=[{}]", aggregates)?;
                }
                if let Some(expected_group_size) = expected_group_size {
                    write!(f, " exp_group_size={}", expected_group_size)?;
                }
                writeln!(f)?;
                ctx.indented(|ctx| input.fmt_text(f, ctx))?;
            }
            Distinct { input } => {
                writeln!(f, "{}Distinct", ctx.indent)?;
                ctx.indented(|ctx| input.fmt_text(f, ctx))?;
            }
            TopK {
                group_key,
                order_key,
                limit,
                offset,
                input,
                expected_group_size,
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
                if let Some(expected_group_size) = expected_group_size {
                    write!(f, " exp_group_size={}", expected_group_size)?;
                }
                writeln!(f)?;
                ctx.indented(|ctx| input.fmt_text(f, ctx))?;
            }
            Negate { input } => {
                writeln!(f, "{}Negate", ctx.indent)?;
                ctx.indented(|ctx| input.fmt_text(f, ctx))?;
            }
            Threshold { input } => {
                writeln!(f, "{}Threshold", ctx.indent)?;
                ctx.indented(|ctx| input.fmt_text(f, ctx))?;
            }
            Union { base, inputs } => {
                writeln!(f, "{}Union", ctx.indent)?;
                ctx.indented(|ctx| {
                    base.fmt_text(f, ctx)?;
                    for input in inputs.iter() {
                        input.fmt_text(f, ctx)?;
                    }
                    Ok(())
                })?;
            }
        }

        Ok(())
    }
}

impl fmt::Display for HirScalarExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use HirRelationExpr::Get;
        use HirScalarExpr::*;
        match self {
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
                use mz_expr::VariadicFunc::*;
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
            Windowing(expr) => {
                // First, print
                // - the window function name
                // - the arguments.
                // Also, dig out some info from the `func`.
                let (column_orders, ignore_nulls, window_frame) = match &expr.func {
                    WindowExprType::Scalar(scalar_window_expr) => {
                        write!(f, "{}()", scalar_window_expr.func)?;
                        (&scalar_window_expr.order_by, false, None)
                    }
                    WindowExprType::Value(value_window_expr) => {
                        write!(f, "{}({})", value_window_expr.func, value_window_expr.args)?;
                        (
                            &value_window_expr.order_by,
                            value_window_expr.ignore_nulls,
                            Some(&value_window_expr.window_frame),
                        )
                    }
                };

                // Reconstruct the ORDER BY (see comment on `WindowExpr.order_by`).
                // We assume that the `column_order.column`s refer to each of the expressions in
                // `expr.order_by` in order. This is a consequence of how `plan_function_order_by`
                // works.
                assert!(column_orders
                    .iter()
                    .enumerate()
                    .all(|(i, column_order)| i == column_order.column));
                let order_by = column_orders
                    .iter()
                    .zip_eq(expr.order_by.iter())
                    .map(|(column_order, expr)| {
                        ColumnOrderWithExpr {
                            expr: expr.clone(),
                            desc: column_order.desc,
                            nulls_last: column_order.nulls_last,
                        }
                        // (We can ignore column_order.column because of the above assert.)
                    })
                    .collect_vec();

                // Print IGNORE NULLS if present.
                if ignore_nulls {
                    write!(f, " ignore nulls")?;
                }

                // Print the OVER clause.
                // This is close to the SQL syntax, but we are adding some [] to make it easier to
                // read.
                write!(f, " over (")?;
                if !expr.partition.is_empty() {
                    write!(
                        f,
                        "partition by [{}] ",
                        separated(", ", expr.partition.iter())
                    )?;
                }
                write!(f, "order by [{}]", separated(", ", order_by.iter()))?;
                if let Some(window_frame) = window_frame {
                    if *window_frame != WindowFrame::default() {
                        write!(f, " {}", window_frame)?;
                    }
                }
                write!(f, ")")?;

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

/// This is like ColumnOrder, but contains a HirScalarExpr instead of just a column reference by
/// index. (This is only used in EXPLAIN, when reconstructing an ORDER BY inside an OVER clause.)
struct ColumnOrderWithExpr {
    /// The scalar expression.
    pub expr: HirScalarExpr,
    /// Whether to sort in descending order.
    pub desc: bool,
    /// Whether to sort nulls last.
    pub nulls_last: bool,
}

impl fmt::Display for ColumnOrderWithExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // If you modify this, then please also attend to Display for ColumnOrder!
        write!(
            f,
            "{} {} {}",
            self.expr,
            if self.desc { "desc" } else { "asc" },
            if self.nulls_last {
                "nulls_last"
            } else {
                "nulls_first"
            },
        )
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
            self.func.clone().into_expr(),
            if self.distinct { "distinct " } else { "" }
        )?;

        self.expr.fmt(f)?;
        write!(f, ")")
    }
}
