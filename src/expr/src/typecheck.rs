// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Thorough consistency checking and type synthesis for MIR expressions

use itertools::Itertools;
use mz_repr::{
    explain::{DummyHumanizer, ExprHumanizer},
    ColumnType, RelationType, Row, ScalarType,
};
use std::collections::BTreeMap;

use crate::{
    relation::non_nullable_columns, AggregateExpr, ColumnOrder, Id, JoinImplementation, LocalId,
    MirRelationExpr, MirScalarExpr,
};

/// The possible forms of inconsistency/errors discovered during typechecking.
///
/// Every variant has a `source` field identifying the MIR term that is home
///  to the error (though not necessarily the root cause of the error).
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum TypeError<'a> {
    Unbound {
        source: &'a MirRelationExpr,
        id: Id,
        typ: RelationType,
    },
    NoSuchColumn {
        source: &'a MirRelationExpr,
        expr: &'a MirScalarExpr,
        col: usize,
    },
    MismatchColumn {
        source: &'a MirRelationExpr,
        got: ColumnType,
        expected: ColumnType,
        message: String,
    },
    MismatchColumns {
        source: &'a MirRelationExpr,
        got: Vec<ColumnType>,
        expected: Vec<ColumnType>,
        message: String,
    },
    BadConstantRow {
        source: &'a MirRelationExpr,
        got: Row,
        expected: Vec<ColumnType>,
    },
    BadProject {
        source: &'a MirRelationExpr,
        got: Vec<usize>,
        input_type: Vec<ColumnType>,
    },
    BadTopKGroupKey {
        source: &'a MirRelationExpr,
        key: usize,
        input_type: Vec<ColumnType>,
    },
    BadTopKOrdering {
        source: &'a MirRelationExpr,
        order: ColumnOrder,
        input_type: Vec<ColumnType>,
    },
    BadLetRecBindings {
        source: &'a MirRelationExpr,
    },
    Shadowing {
        source: &'a MirRelationExpr,
        id: Id,
    },
}

pub type Context = BTreeMap<Id, Vec<ColumnType>>;

/// Returns true when it is safe to treat a `sub` row as an `sup` row
///
/// In particular, the core types must be equal, and if a column in `sup` is nullable, that column should also be nullable in `sub`
/// Conversely, it is okay to treat a known non-nullable column as nullable: `sub` may be nullable when `sup` is not
pub fn is_subtype_of(sub: &[ColumnType], sup: &[ColumnType]) -> bool {
    if sub.len() != sup.len() {
        return false;
    }

    sub.iter().zip_eq(sup.iter()).all(|(got, known)| {
        (!known.nullable || got.nullable) && got.scalar_type.base_eq(&known.scalar_type)
    })
}

impl MirRelationExpr {
    /// Returns the type of a relation expression or a type error.
    ///
    /// This function is careful to check validity, not just find out the type.
    ///
    /// It should be linear in the size of the AST.
    ///
    /// ??? should we also compute keys and return a `RelationType`?
    pub fn typecheck(&self, ctx: &Context) -> Result<Vec<ColumnType>, TypeError> {
        use MirRelationExpr::*;

        match self {
            Constant { typ, rows } => {
                if let Ok(rows) = rows {
                    for (row, _id) in rows {
                        let datums = row.unpack();

                        // correct length
                        if datums.len() != typ.column_types.len() {
                            return Err(TypeError::BadConstantRow {
                                source: self,
                                got: row.clone(),
                                expected: typ.column_types.clone(),
                            });
                        }

                        // correct types
                        if datums
                            .iter()
                            .zip_eq(typ.column_types.iter())
                            .any(|(d, ty)| d != &mz_repr::Datum::Dummy && !d.is_instance_of(ty))
                        {
                            return Err(TypeError::BadConstantRow {
                                source: self,
                                got: row.clone(),
                                expected: typ.column_types.clone(),
                            });
                        }
                    }
                }

                Ok(typ.column_types.clone())
            }
            Get { typ, id } => {
                if let Id::Global(_global_id) = id {
                    if !ctx.contains_key(id) {
                        // TODO(mgree) pass QueryContext through to check these types
                        return Ok(typ.column_types.clone());
                    }
                }

                let ctx_typ = ctx.get(id).ok_or_else(|| TypeError::Unbound {
                    source: self,
                    id: id.clone(),
                    typ: typ.clone(),
                })?;

                // covariant: the ascribed type must be a subtype of the actual type in the context
                if !is_subtype_of(&typ.column_types, ctx_typ) {
                    return Err(TypeError::MismatchColumns {
                        source: self,
                        got: typ.column_types.clone(),
                        expected: ctx_typ.clone(),
                        message: "annotation did not match context type".into(),
                    });
                }

                Ok(typ.column_types.clone())
            }
            Project { input, outputs } => {
                let t_in = input.typecheck(ctx)?;

                for x in outputs {
                    if *x >= t_in.len() {
                        return Err(TypeError::BadProject {
                            source: self,
                            got: outputs.clone(),
                            input_type: t_in,
                        });
                    }
                }

                Ok(outputs.iter().map(|col| t_in[*col].clone()).collect())
            }
            Map { input, scalars } => {
                let mut t_in = input.typecheck(ctx)?;

                for expr in scalars.iter() {
                    t_in.push(expr.typecheck(self, &t_in)?);
                }

                Ok(t_in)
            }
            FlatMap { input, func, exprs } => {
                let mut t_in = input.typecheck(ctx)?;

                let mut t_exprs = Vec::with_capacity(exprs.len());
                for expr in exprs {
                    t_exprs.push(expr.typecheck(self, &t_in)?);
                }
                // TODO(mgree) check t_exprs agrees with `func`'s input type

                let t_out = func.output_type().column_types;

                // FlatMap extends the existing columns
                t_in.extend(t_out);
                Ok(t_in)
            }
            Filter { input, predicates } => {
                let mut t_in = input.typecheck(ctx)?;

                // Set as nonnull any columns where null values would cause
                // any predicate to evaluate to null.
                for column in non_nullable_columns(predicates) {
                    t_in[column].nullable = false;
                }

                Ok(t_in)
            }
            Join {
                inputs,
                equivalences,
                implementation,
            } => {
                let mut t_in = Vec::new();

                for input in inputs.iter() {
                    t_in.extend(input.typecheck(ctx)?);
                }

                for eq_class in equivalences {
                    let mut t_exprs: Vec<ColumnType> = Vec::with_capacity(eq_class.len());

                    for expr in eq_class {
                        let t_expr = expr.typecheck(self, &t_in)?;

                        if let Some(t_first) = t_exprs.get(0) {
                            // ??? do we care about matching nullability?
                            if !t_expr.scalar_type.base_eq(&t_first.scalar_type) {
                                return Err(TypeError::MismatchColumn {
                                    source: self,
                                    got: t_expr,
                                    expected: t_first.clone(),
                                    message: "equivalence class members do not match".into(),
                                });
                            }
                        }
                        t_exprs.push(t_expr);
                    }
                }

                // check that the join implementation is consistent
                match implementation {
                    JoinImplementation::Differential((_, first_keys, _), others) => {
                        if let Some(keys) = first_keys {
                            for expr in keys {
                                let _ = expr.typecheck(self, &t_in)?;
                            }
                        }

                        for (_, keys, _) in others {
                            for expr in keys {
                                let _ = expr.typecheck(self, &t_in)?;
                            }
                        }
                    }
                    JoinImplementation::DeltaQuery(plans) => {
                        for plan in plans {
                            for (_, keys, _) in plan {
                                for expr in keys {
                                    let _ = expr.typecheck(self, &t_in)?;
                                }
                            }
                        }
                    }
                    JoinImplementation::IndexedFilter(_global_id, keys, consts) => {
                        let typ: Vec<ColumnType> = keys
                            .iter()
                            .map(|expr| expr.typecheck(self, &t_in))
                            .collect::<Result<Vec<ColumnType>, TypeError>>()?;

                        for row in consts {
                            let datums = row.unpack();

                            // correct length
                            if datums.len() != typ.len() {
                                return Err(TypeError::BadConstantRow {
                                    source: self,
                                    got: row.clone(),
                                    expected: typ,
                                });
                            }

                            // correct types
                            if datums
                                .iter()
                                .zip_eq(typ.iter())
                                .any(|(d, ty)| d != &mz_repr::Datum::Dummy && !d.is_instance_of(ty))
                            {
                                return Err(TypeError::BadConstantRow {
                                    source: self,
                                    got: row.clone(),
                                    expected: typ,
                                });
                            }
                        }
                    }
                    JoinImplementation::Unimplemented => (),
                }

                Ok(t_in)
            }
            Reduce {
                input,
                group_key,
                aggregates,
                monotonic: _,
                expected_group_size: _,
            } => {
                let t_in = input.typecheck(ctx)?;

                let mut t_out = group_key
                    .iter()
                    .map(|expr| expr.typecheck(self, &t_in))
                    .collect::<Result<Vec<_>, _>>()?;

                for agg in aggregates {
                    t_out.push(agg.typecheck(self, &t_in)?);
                }

                Ok(t_out)
            }
            TopK {
                input,
                group_key,
                order_key,
                limit: _,
                offset: _,
                monotonic: _,
                expected_group_size: _,
            } => {
                let t_in = input.typecheck(ctx)?;

                for &key in group_key {
                    if key >= t_in.len() {
                        return Err(TypeError::BadTopKGroupKey {
                            source: self,
                            key,
                            input_type: t_in,
                        });
                    }
                }

                for order in order_key {
                    if order.column >= t_in.len() {
                        return Err(TypeError::BadTopKOrdering {
                            source: self,
                            order: order.clone(),
                            input_type: t_in,
                        });
                    }
                }

                Ok(t_in)
            }
            Negate { input } => input.typecheck(ctx),
            Threshold { input } => input.typecheck(ctx),
            Union { base, inputs } => {
                let mut t_base = base.typecheck(ctx)?;

                for input in inputs {
                    let t_input = input.typecheck(ctx)?;

                    if t_base.len() != t_input.len() {
                        return Err(TypeError::MismatchColumns {
                            source: self,
                            got: t_base.clone(),
                            expected: t_input,
                            message: "union branches have different numbers of columns".into(),
                        });
                    }

                    for (base_col, input_col) in t_base.iter_mut().zip_eq(t_input) {
                        *base_col =
                            base_col
                                .union(&input_col)
                                .map_err(|e| TypeError::MismatchColumn {
                                    source: self,
                                    got: input_col,
                                    expected: base_col.clone(),
                                    message: format!(
                                        "couldn't compute union of column types in union: {e}"
                                    ),
                                })?;
                    }
                }

                Ok(t_base)
            }
            Let { id, value, body } => {
                let t_value = value.typecheck(ctx)?;

                let binding = Id::Local(*id);
                if ctx.contains_key(&binding) {
                    return Err(TypeError::Shadowing {
                        source: self,
                        id: binding,
                    });
                }

                let mut body_ctx = ctx.clone();
                body_ctx.insert(Id::Local(*id), t_value);

                body.typecheck(&body_ctx)
            }
            LetRec { ids, values, body } => {
                if ids.len() != values.len() {
                    return Err(TypeError::BadLetRecBindings { source: self });
                }

                // temporary hack: steal info from the Gets inside to learn the expected types
                let mut ctx = ctx.clone();
                // calling self.collect_recursive_variable_types() triggers a panic due to nested letrecs with shadowing IDs
                for expr in values.iter().chain(std::iter::once(body.as_ref())) {
                    expr.collect_recursive_variable_types(ids, &mut ctx)?;
                }

                for (id, value) in ids.iter().zip_eq(values.iter()) {
                    let typ = value.typecheck(&ctx)?;

                    let id = Id::Local(id.clone());
                    if let Some(ctx_typ) = ctx.get_mut(&id) {
                        for (base_col, input_col) in ctx_typ.iter_mut().zip_eq(typ) {
                            *base_col = base_col.union(&input_col).map_err(|e| {
                                TypeError::MismatchColumn {
                                    source: self,
                                    got: input_col,
                                    expected: base_col.clone(),
                                    message: format!(
                                        "couldn't compute union of column types in let rec: {e}"
                                    ),
                                }
                            })?;
                        }
                    } else {
                        ctx.insert(id, typ);
                    }
                }

                body.typecheck(&ctx)
            }
            ArrangeBy { input, keys } => {
                let t_in = input.typecheck(ctx)?;

                for cols in keys {
                    for col in cols {
                        let _ = col.typecheck(self, &t_in)?;
                    }
                }

                Ok(t_in)
            }
        }
    }

    /// Traverses a term to collect the types of given ids.
    ///
    /// LetRec doesn't have type info stored in it. Until we change the MIR to track that information explicitly, we have to rebuild it from looking at the term.
    fn collect_recursive_variable_types(
        &self,
        ids: &[LocalId],
        ctx: &mut Context,
    ) -> Result<(), TypeError> {
        match self {
            MirRelationExpr::Get {
                id: Id::Local(id),
                typ,
            } => {
                if !ids.contains(id) {
                    return Ok(());
                }

                let id = Id::Local(id.clone());
                if let Some(ctx_typ) = ctx.get_mut(&id) {
                    for (base_col, input_col) in ctx_typ.iter_mut().zip_eq(typ.column_types.iter())
                    {
                        *base_col =
                            base_col
                                .union(input_col)
                                .map_err(|e| TypeError::MismatchColumn {
                                    source: self,
                                    got: input_col.clone(),
                                    expected: base_col.clone(),
                                    message: format!(
                                        "couldn't compute union of collected column types: {}",
                                        e
                                    ),
                                })?;
                    }
                } else {
                    ctx.insert(id, typ.column_types.clone());
                }
            }
            MirRelationExpr::Get {
                id: Id::Global(..), ..
            }
            | MirRelationExpr::Constant { .. } => (),
            MirRelationExpr::Let { id, value, body } => {
                value.collect_recursive_variable_types(ids, ctx)?;

                // we've shadowed the id
                if ids.contains(id) {
                    return Err(TypeError::Shadowing {
                        source: self,
                        id: Id::Local(*id),
                    });
                }

                body.collect_recursive_variable_types(ids, ctx)?;
            }
            MirRelationExpr::LetRec {
                ids: inner_ids,
                values,
                body,
            } => {
                for inner_id in inner_ids {
                    if ids.contains(inner_id) {
                        return Err(TypeError::Shadowing {
                            source: self,
                            id: Id::Local(*inner_id),
                        });
                    }
                }

                for value in values {
                    value.collect_recursive_variable_types(ids, ctx)?;
                }

                body.collect_recursive_variable_types(ids, ctx)?;
            }
            MirRelationExpr::Project { input, .. }
            | MirRelationExpr::Map { input, .. }
            | MirRelationExpr::FlatMap { input, .. }
            | MirRelationExpr::Filter { input, .. }
            | MirRelationExpr::Reduce { input, .. }
            | MirRelationExpr::TopK { input, .. }
            | MirRelationExpr::Negate { input }
            | MirRelationExpr::Threshold { input }
            | MirRelationExpr::ArrangeBy { input, .. } => {
                input.collect_recursive_variable_types(ids, ctx)?;
            }
            MirRelationExpr::Join { inputs, .. } => {
                for input in inputs {
                    input.collect_recursive_variable_types(ids, ctx)?;
                }
            }
            MirRelationExpr::Union { base, inputs } => {
                base.collect_recursive_variable_types(ids, ctx)?;

                for input in inputs {
                    input.collect_recursive_variable_types(ids, ctx)?;
                }
            }
        }

        Ok(())
    }
}

impl MirScalarExpr {
    fn typecheck<'a>(
        &'a self,
        source: &'a MirRelationExpr,
        column_types: &[ColumnType],
    ) -> Result<ColumnType, TypeError<'a>> {
        match self {
            MirScalarExpr::Column(i) => match column_types.get(*i) {
                Some(ty) => Ok(ty.clone()),
                None => Err(TypeError::NoSuchColumn {
                    source,
                    expr: self,
                    col: *i,
                }),
            },
            MirScalarExpr::Literal(row, typ) => {
                if let Ok(row) = row {
                    let datums = row.unpack();

                    if datums.len() != 1
                        || (datums[0] != mz_repr::Datum::Dummy && !datums[0].is_instance_of(typ))
                    {
                        return Err(TypeError::BadConstantRow {
                            source,
                            got: row.clone(),
                            expected: vec![typ.clone()],
                        });
                    }
                }

                Ok(typ.clone())
            }
            MirScalarExpr::CallUnmaterializable(func) => Ok(func.output_type()),
            MirScalarExpr::CallUnary { expr, func } => {
                Ok(func.output_type(expr.typecheck(source, column_types)?))
            }
            MirScalarExpr::CallBinary { expr1, expr2, func } => Ok(func.output_type(
                expr1.typecheck(source, column_types)?,
                expr2.typecheck(source, column_types)?,
            )),
            MirScalarExpr::CallVariadic { exprs, func } => Ok(func.output_type(
                exprs
                    .iter()
                    .map(|e| e.typecheck(source, column_types))
                    .collect::<Result<Vec<_>, TypeError>>()?,
            )),
            MirScalarExpr::If { cond, then, els } => {
                let cond_type = cond.typecheck(source, column_types)?;

                // condition must be boolean
                // ignoring nullability: null is treated as false
                // NB this behavior is slightly different from columns_match (for which we would set nullable to false in the expected type)
                if cond_type.scalar_type != ScalarType::Bool {
                    return Err(TypeError::MismatchColumn {
                        source,
                        got: cond_type,
                        expected: ColumnType {
                            scalar_type: ScalarType::Bool,
                            nullable: true,
                        },
                        message: "expected boolean condition".into(),
                    });
                }

                let then_type = then.typecheck(source, column_types)?;
                let else_type = els.typecheck(source, column_types)?;
                then_type
                    .union(&else_type)
                    .map_err(|e| TypeError::MismatchColumn {
                        source,
                        got: then_type,
                        expected: else_type,
                        message: format!("couldn't compute union of column types for if: {e}"),
                    })
            }
        }
    }
}

impl AggregateExpr {
    pub fn typecheck<'a>(
        &'a self,
        source: &'a MirRelationExpr,
        column_types: &[ColumnType],
    ) -> Result<ColumnType, TypeError<'a>> {
        let t_in = self.expr.typecheck(source, column_types)?;

        // TODO check that t_in is actually acceptable for `func`

        Ok(self.func.output_type(t_in))
    }
}

impl<'a> TypeError<'a> {
    pub fn source(&self) -> &'a MirRelationExpr {
        use TypeError::*;
        match self {
            Unbound { source, .. }
            | NoSuchColumn { source, .. }
            | MismatchColumn { source, .. }
            | MismatchColumns { source, .. }
            | BadConstantRow { source, .. }
            | BadProject { source, .. }
            | BadTopKGroupKey { source, .. }
            | BadTopKOrdering { source, .. }
            | BadLetRecBindings { source }
            | Shadowing { source, .. } => source,
        }
    }

    fn humanize<H>(&self, humanizer: &H, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    where
        H: ExprHumanizer,
    {
        writeln!(f, "In the MIR term:\n{}\n", self.source().pretty())?;

        use TypeError::*;
        match self {
            Unbound { source: _, id, typ } => {
                let typ = columns_pretty(&typ.column_types, humanizer);
                writeln!(f, "{id} is unbound\ndeclared type {typ}")?
            }
            NoSuchColumn {
                source: _,
                expr,
                col,
            } => writeln!(f, "{expr} references non-existent column {col}")?,
            MismatchColumn {
                source: _,
                got,
                expected,
                message,
            } => {
                let got = humanizer.humanize_column_type(got);
                let expected = humanizer.humanize_column_type(expected);
                writeln!(
                    f,
                    "mismatched column types: {message}\ngot {got}\nexpected {expected}"
                )?
            }
            MismatchColumns {
                source: _,
                got,
                expected,
                message,
            } => {
                let got = columns_pretty(got, humanizer);
                let expected = columns_pretty(expected, humanizer);

                writeln!(
                    f,
                    "mismatched relation types: {message}\ngot {got}\nexpected {expected}"
                )?
            }
            BadConstantRow {
                source: _,
                got,
                expected,
            } => {
                let expected = columns_pretty(expected, humanizer);

                writeln!(
                    f,
                    "bad constant row\ngot {got}\nexpected row of type {expected}"
                )?
            }
            BadProject {
                source: _,
                got,
                input_type,
            } => {
                let input_type = columns_pretty(input_type, humanizer);

                writeln!(
                    f,
                    "projection of non-existant columns {got:?} from type {input_type}"
                )?
            }
            BadTopKGroupKey {
                source: _,
                key,
                input_type,
            } => {
                let input_type = columns_pretty(input_type, humanizer);

                writeln!(
                    f,
                    "TopK group key {key} references invalid column\ncolumns: {input_type}"
                )?
            }
            BadTopKOrdering {
                source: _,
                order,
                input_type,
            } => {
                let col = order.column;
                let input_type = columns_pretty(input_type, humanizer);

                writeln!(
                    f,
                    "TopK ordering {order} references invalid column {col} orderings\ncolumns: {input_type}")?
            }
            BadLetRecBindings { source: _ } => {
                writeln!(f, "LetRec ids and definitions don't line up")?
            }
            Shadowing { source: _, id } => writeln!(f, "id {id} is shadowed")?,
        }

        Ok(())
    }
}

pub fn columns_pretty<H>(cols: &[ColumnType], humanizer: &H) -> String
where
    H: ExprHumanizer,
{
    let mut s = String::with_capacity(2 + 3 * cols.len());

    s.push('(');

    let mut it = cols.iter().peekable();
    while let Some(col) = it.next() {
        s.push_str(&humanizer.humanize_column_type(col));

        if it.peek().is_some() {
            s.push_str(", ");
        }
    }

    s.push(')');

    s
}

impl<'a> std::fmt::Display for TypeError<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.humanize(&DummyHumanizer, f)
    }
}
