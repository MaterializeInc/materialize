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

use mz_expr::Id;
use mz_expr::explain::{HumanizedExplain, HumanizerMode, fmt_text_constant_rows};
use mz_expr::virtual_syntax::{AlgExcept, Except};
use mz_ore::str::{IndentLike, separated};
use mz_repr::Datum;
use mz_repr::Diff;
use mz_repr::explain::{CompactScalarSeq, Indices, PlanRenderingContext};

use mz_repr::explain::text::DisplayText;

use crate::{Hir, HirRelationExpr, JoinKind};

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
    pub fn fmt_virtual_syntax(
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

    pub fn fmt_raw_syntax(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'_, HirRelationExpr>,
    ) -> fmt::Result {
        use HirRelationExpr::*;

        let mode = HumanizedExplain::new(ctx.config.redacted);

        match &self {
            Constant { rows, .. } => {
                if !rows.is_empty() {
                    writeln!(f, "{}Constant", ctx.indent)?;
                    ctx.indented(|ctx| {
                        fmt_text_constant_rows(
                            f,
                            rows.iter().map(|row| (row, &Diff::ONE)),
                            &mut ctx.indent,
                            ctx.config.redacted,
                        )
                    })?;
                } else {
                    writeln!(f, "{}Constant <empty>", ctx.indent)?;
                }
            }
            Let {
                name,
                id,
                value,
                body,
            } => {
                let mut bindings = vec![(id, name, value.as_ref())];
                let mut head = body.as_ref();

                // Render Let-blocks nested in the body an outer Let-block in one step
                // with a flattened list of bindings
                while let Let {
                    name,
                    id,
                    value,
                    body,
                } = head
                {
                    bindings.push((id, name, value.as_ref()));
                    head = body.as_ref();
                }

                writeln!(f, "{}With", ctx.indent)?;
                ctx.indented(|ctx| {
                    for (id, name, value) in bindings.iter() {
                        // TODO: print the name and not the id
                        writeln!(f, "{}cte [{} as {}] =", ctx.indent, *id, *name)?;
                        ctx.indented(|ctx| value.fmt_text(f, ctx))?;
                    }
                    Ok(())
                })?;
                writeln!(f, "{}Return", ctx.indent)?;
                ctx.indented(|ctx| head.fmt_text(f, ctx))?;
            }
            LetRec {
                limit,
                bindings,
                body,
            } => {
                write!(f, "{}With Mutually Recursive", ctx.indent)?;
                if let Some(limit) = limit {
                    write!(f, " {}", limit)?;
                }
                writeln!(f)?;
                ctx.indented(|ctx| {
                    for (name, id, value, _type) in bindings.iter() {
                        // TODO: print the name and not the id
                        writeln!(f, "{}cte [{} as {}] =", ctx.indent, *id, *name)?;
                        ctx.indented(|ctx| value.fmt_text(f, ctx))?;
                    }
                    Ok(())
                })?;
                writeln!(f, "{}Return", ctx.indent)?;
                ctx.indented(|ctx| body.fmt_text(f, ctx))?;
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
                    let order_by = mode.seq(order_key, None);
                    let order_by = separated(", ", order_by);
                    write!(f, " order_by=[{}]", order_by)?;
                }
                if let Some(limit) = limit {
                    write!(f, " limit={}", limit)?;
                }
                // We only print the offset if it is not trivial, i.e., not 0.
                let is_zero_offset = offset.as_literal().map_or(false, |d| d == Datum::Int64(0));
                if !is_zero_offset {
                    write!(f, " offset={}", offset)?;
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
