// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN` support for `Mir` structures.

use std::fmt;

use mz_expr::{
    explain::Indices, AggregateExpr, Id, JoinImplementation, MirRelationExpr, MirScalarExpr,
};
use mz_ore::str::{bracketed, separated, IndentLike, StrExt};
use mz_repr::explain_new::{fmt_text_constant_rows, separated_text, DisplayText};

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
                    write!(f, "{}Constant", ctx.indent)?;
                    self.fmt_attributes(f, ctx)?;
                    ctx.indented(|ctx| {
                        fmt_text_constant_rows(f, rows.iter().map(|(x, y)| (x, y)), &mut ctx.indent)
                    })?;
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

                // The body comes first in the text output format in order to
                // align with the format convention the dataflow is rendered
                // top to bottom
                write!(f, "{}Let", ctx.indent)?;
                self.fmt_attributes(f, ctx)?;
                ctx.indented(|ctx| {
                    Displayable::from(head).fmt_text(f, ctx)?;
                    writeln!(f, "{}Where", ctx.indent)?;
                    ctx.indented(|ctx| {
                        for (id, value) in bindings.iter().rev() {
                            writeln!(f, "{}{} =", ctx.indent, *id)?;
                            ctx.indented(|ctx| Displayable::from(*value).fmt_text(f, ctx))?;
                        }
                        Ok(())
                    })?;
                    Ok(())
                })?;
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
                let outputs = Indices(outputs);
                write!(f, "{}Project ({})", ctx.indent, outputs)?;
                self.fmt_attributes(f, ctx)?;
                ctx.indented(|ctx| Displayable::from(input.as_ref()).fmt_text(f, ctx))?;
            }
            Map { scalars, input } => {
                let scalars = separated_text(", ", scalars.iter().map(Displayable::from));
                write!(f, "{}Map ({})", ctx.indent, scalars)?;
                self.fmt_attributes(f, ctx)?;
                ctx.indented(|ctx| Displayable::from(input.as_ref()).fmt_text(f, ctx))?;
            }
            FlatMap { input, func, exprs } => {
                let exprs = separated_text(", ", exprs.iter().map(Displayable::from));
                write!(f, "{}FlatMap {}({})", ctx.indent, func, exprs)?;
                self.fmt_attributes(f, ctx)?;
                ctx.indented(|ctx| Displayable::from(input.as_ref()).fmt_text(f, ctx))?;
            }
            Filter { predicates, input } => {
                let predicates = separated_text(" AND ", predicates.iter().map(Displayable::from));
                write!(f, "{}Filter {}", ctx.indent, predicates)?;
                self.fmt_attributes(f, ctx)?;
                ctx.indented(|ctx| Displayable::from(input.as_ref()).fmt_text(f, ctx))?;
            }
            Join {
                inputs,
                equivalences,
                implementation,
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

                if ctx.config.join_impls && implementation.is_implemented() {
                    match implementation {
                        JoinImplementation::Differential((head_idx, _head_key), tail) => {
                            write!(f, "{}Filter {}", ctx.indent, equivalences)?;
                            self.fmt_attributes(f, ctx)?;

                            debug_assert_eq!(inputs.len(), tail.len() + 1);

                            for (idx, key) in tail.iter().rev() {
                                ctx.indent += 1;
                                let key = separated_text(", ", key.iter().map(Displayable::from));
                                writeln!(f, "{}LinearJoin using=[{}]", ctx.indent, key)?;
                                ctx.indented(|ctx| {
                                    Displayable::from(&inputs[*idx]).fmt_text(f, ctx)
                                })?;
                            }
                            ctx.indented(|ctx| {
                                Displayable::from(&inputs[*head_idx]).fmt_text(f, ctx)
                            })?;
                            ctx.indent -= tail.len();
                        }
                        JoinImplementation::DeltaQuery(half_join_chains) => {
                            write!(f, "{}Filter {}", ctx.indent, equivalences)?;
                            self.fmt_attributes(f, ctx)?;

                            debug_assert_eq!(inputs.len(), half_join_chains.len());

                            ctx.indent += 1;
                            writeln!(f, "{}Union", ctx.indent)?;
                            for (input, half_join_chain) in std::iter::zip(inputs, half_join_chains)
                            {
                                for (idx, key) in half_join_chain.iter().rev() {
                                    ctx.indent += 1;
                                    let key =
                                        separated_text(", ", key.iter().map(Displayable::from));
                                    writeln!(f, "{}HalfJoin using=[{}]", ctx.indent, key)?;
                                    ctx.indented(|ctx| {
                                        Displayable::from(&inputs[*idx]).fmt_text(f, ctx)
                                    })?;
                                }
                                ctx.indented(|ctx| Displayable::from(input).fmt_text(f, ctx))?;
                                ctx.indent -= half_join_chain.len();
                            }
                            ctx.indent -= 1;
                        }
                        JoinImplementation::IndexedFilter(_, key, row) => {
                            write!(f, "{}Lookup ", ctx.indent)?;

                            for (i, (expr, lit)) in std::iter::zip(key, row.unpack()).enumerate() {
                                Displayable::from(expr).fmt_text(f, &mut ())?;
                                write!(f, " = ")?;
                                write!(f, "{}", lit)?;
                                if i < key.len() - 1 {
                                    write!(f, " AND ")?;
                                }
                            }
                            writeln!(f, "")?;
                            ctx.indented(|ctx| Displayable::from(&inputs[0]).fmt_text(f, ctx))?;
                        }
                        JoinImplementation::Unimplemented => {
                            unreachable!();
                        }
                    }
                } else {
                    // unimplemented or impl_joins not set
                    if has_equivalences {
                        write!(f, "{}Join on=({})", ctx.indent, equivalences)?;
                    } else {
                        write!(f, "{}CrossJoin", ctx.indent)?;
                    }
                    if let Some(name) = implementation.name() {
                        write!(f, " type={}", name)?;
                    }
                    self.fmt_attributes(f, ctx)?;

                    ctx.indented(|ctx| {
                        for input in inputs {
                            Displayable::from(input).fmt_text(f, ctx)?;
                        }
                        Ok(())
                    })?;
                }
            }
            Reduce {
                group_key,
                aggregates,
                expected_group_size,
                monotonic: _, // TODO: monotonic should be an attribute
                input,
            } => {
                if aggregates.len() > 0 {
                    write!(f, "{}Reduce", ctx.indent)?;
                } else {
                    write!(f, "{}Distinct", ctx.indent)?;
                }
                if group_key.len() > 0 {
                    let group_key = separated_text(", ", group_key.iter().map(Displayable::from));
                    write!(f, " group_by=[{}]", group_key)?;
                }
                if aggregates.len() > 0 {
                    let aggregates = separated_text(", ", aggregates.iter().map(Displayable::from));
                    write!(f, " aggregates=[{}]", aggregates)?;
                }
                if let Some(expected_group_size) = expected_group_size {
                    write!(f, " exp_group_size={}", expected_group_size)?;
                }
                self.fmt_attributes(f, ctx)?;
                ctx.indented(|ctx| Displayable::from(input.as_ref()).fmt_text(f, ctx))?;
            }
            TopK {
                group_key,
                order_key,
                limit,
                offset,
                monotonic,
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
                write!(f, " monotonic={}", monotonic)?;
                self.fmt_attributes(f, ctx)?;
                ctx.indented(|ctx| Displayable::from(input.as_ref()).fmt_text(f, ctx))?;
            }
            Negate { input } => {
                write!(f, "{}Negate", ctx.indent)?;
                self.fmt_attributes(f, ctx)?;
                ctx.indented(|ctx| Displayable::from(input.as_ref()).fmt_text(f, ctx))?;
            }
            Threshold { input } => {
                write!(f, "{}Threshold", ctx.indent)?;
                self.fmt_attributes(f, ctx)?;
                ctx.indented(|ctx| Displayable::from(input.as_ref()).fmt_text(f, ctx))?;
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
                let keys = separated(
                    "], [",
                    keys.iter()
                        .map(|key| separated_text(", ", key.iter().map(Displayable::from))),
                );
                write!(f, "{}ArrangeBy keys=[[{}]]", ctx.indent, keys)?;
                self.fmt_attributes(f, ctx)?;
                ctx.indented(|ctx| Displayable::from(input.as_ref()).fmt_text(f, ctx))?;
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
