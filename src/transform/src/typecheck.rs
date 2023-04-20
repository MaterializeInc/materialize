// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Check that the visible type of each query has not been changed

use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use itertools::Itertools;
use mz_expr::{
    non_nullable_columns, AggregateExpr, ColumnOrder, Id, JoinImplementation, LocalId,
    MirRelationExpr, MirScalarExpr, RECURSION_LIMIT,
};
use mz_ore::stack::{CheckedRecursion, RecursionGuard, RecursionLimitError};
use mz_repr::{
    explain::{DummyHumanizer, ExprHumanizer},
    ColumnType, RelationType, Row, ScalarType,
};

use crate::TransformError;

/// Typechecking contexts as shared by various typechecking passes.
///
/// We use a `RefCell` to ensure that contexts are shared by multiple typechecker passes.
/// Shared contexts help catch consistency issues.
pub type SharedContext = Rc<RefCell<Context>>;

/// Generates an empty context
pub fn empty_context() -> SharedContext {
    Rc::new(RefCell::new(BTreeMap::new()))
}

/// The possible forms of inconsistency/errors discovered during typechecking.
///
/// Every variant has a `source` field identifying the MIR term that is home
/// to the error (though not necessarily the root cause of the error).
#[derive(Clone, Debug)]
#[allow(missing_docs)]
pub enum TypeError<'a> {
    /// Unbound identifiers (local or global)
    Unbound {
        source: &'a MirRelationExpr,
        id: Id,
        typ: RelationType,
    },
    /// Dereference of a non-existent column
    NoSuchColumn {
        source: &'a MirRelationExpr,
        expr: &'a MirScalarExpr,
        col: usize,
    },
    /// A single column type does not match
    MismatchColumn {
        source: &'a MirRelationExpr,
        got: ColumnType,
        expected: ColumnType,
        message: String,
    },
    /// Relation column types do not match
    MismatchColumns {
        source: &'a MirRelationExpr,
        got: Vec<ColumnType>,
        expected: Vec<ColumnType>,
        message: String,
    },
    /// A constant row does not have the correct type
    BadConstantRow {
        source: &'a MirRelationExpr,
        got: Row,
        expected: Vec<ColumnType>,
    },
    /// Projection of a non-existent column
    BadProject {
        source: &'a MirRelationExpr,
        got: Vec<usize>,
        input_type: Vec<ColumnType>,
    },
    /// An equivalence class in a join was malformed
    BadJoinEquivalence {
        source: &'a MirRelationExpr,
        got: Vec<ColumnType>,
        message: String,
    },
    /// TopK grouping by non-existent column
    BadTopKGroupKey {
        source: &'a MirRelationExpr,
        key: usize,
        input_type: Vec<ColumnType>,
    },
    /// TopK ordering by non-existent column
    BadTopKOrdering {
        source: &'a MirRelationExpr,
        order: ColumnOrder,
        input_type: Vec<ColumnType>,
    },
    /// LetRec bindings are malformed
    BadLetRecBindings { source: &'a MirRelationExpr },
    /// Local identifiers are shadowed
    Shadowing { source: &'a MirRelationExpr, id: Id },
    /// Recursion depth exceeded
    Recursion { error: RecursionLimitError },
}

impl<'a> From<RecursionLimitError> for TypeError<'a> {
    fn from(error: RecursionLimitError) -> Self {
        TypeError::Recursion { error }
    }
}

type Context = BTreeMap<Id, Vec<ColumnType>>;

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

/// Check that the visible type of each query has not been changed
#[derive(Debug)]
pub struct Typecheck {
    /// The known types of the queries so far
    ctx: SharedContext,
    /// Whether or not this is the first run of the transform
    disallow_new_globals: bool,
    /// Whether or not to be strict about join equivalences having the same nullability
    strict_join_equivalences: bool,
    /// Recursion guard for checked recursion
    recursion_guard: RecursionGuard,
}

impl CheckedRecursion for Typecheck {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl Typecheck {
    /// Creates a typechecking consistency checking pass using a given shared context
    pub fn new(ctx: SharedContext) -> Self {
        Self {
            ctx,
            disallow_new_globals: false,
            strict_join_equivalences: false,
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }

    /// New non-transient global IDs will be treated as an error
    ///
    /// Only turn this on after the context has been appropraitely populated by, e.g., an earlier run
    pub fn disallow_new_globals(mut self) -> Self {
        self.disallow_new_globals = true;
        self
    }

    /// Equivalence classes in joins must not only agree on scalar type, but also on nullability
    ///
    /// Only turn this on before `JoinImplementation`
    pub fn strict_join_equivalences(mut self) -> Self {
        self.strict_join_equivalences = true;
        self
    }

    /// Returns the type of a relation expression or a type error.
    ///
    /// This function is careful to check validity, not just find out the type.
    ///
    /// It should be linear in the size of the AST.
    ///
    /// ??? should we also compute keys and return a `RelationType`?
    pub fn typecheck<'a>(
        &self,
        expr: &'a MirRelationExpr,
        ctx: &Context,
    ) -> Result<Vec<ColumnType>, TypeError<'a>> {
        use MirRelationExpr::*;

        self.checked_recur(|tc| match expr {
            Constant { typ, rows } => {
                if let Ok(rows) = rows {
                    for (row, _id) in rows {
                        let datums = row.unpack();

                        // correct length
                        if datums.len() != typ.column_types.len() {
                            return Err(TypeError::BadConstantRow {
                                source: expr,
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
                                source: expr,
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
                    source: expr,
                    id: id.clone(),
                    typ: typ.clone(),
                })?;

                // covariant: the ascribed type must be a subtype of the actual type in the context
                if !is_subtype_of(&typ.column_types, ctx_typ) {
                    return Err(TypeError::MismatchColumns {
                        source: expr,
                        got: typ.column_types.clone(),
                        expected: ctx_typ.clone(),
                        message: "annotation did not match context type".into(),
                    });
                }

                Ok(typ.column_types.clone())
            }
            Project { input, outputs } => {
                let t_in = tc.typecheck(input, ctx)?;

                for x in outputs {
                    if *x >= t_in.len() {
                        return Err(TypeError::BadProject {
                            source: expr,
                            got: outputs.clone(),
                            input_type: t_in,
                        });
                    }
                }

                Ok(outputs.iter().map(|col| t_in[*col].clone()).collect())
            }
            Map { input, scalars } => {
                let mut t_in = tc.typecheck(input, ctx)?;

                for scalar_expr in scalars.iter() {
                    t_in.push(tc.typecheck_scalar(scalar_expr, expr, &t_in)?);
                }

                Ok(t_in)
            }
            FlatMap { input, func, exprs } => {
                let mut t_in = tc.typecheck(input, ctx)?;

                let mut t_exprs = Vec::with_capacity(exprs.len());
                for scalar_expr in exprs {
                    t_exprs.push(tc.typecheck_scalar(scalar_expr, expr, &t_in)?);
                }
                // TODO(mgree) check t_exprs agrees with `func`'s input type

                let t_out = func.output_type().column_types;

                // FlatMap extends the existing columns
                t_in.extend(t_out);
                Ok(t_in)
            }
            Filter { input, predicates } => {
                let mut t_in = tc.typecheck(input, ctx)?;

                // Set as nonnull any columns where null values would cause
                // any predicate to evaluate to null.
                for column in non_nullable_columns(predicates) {
                    t_in[column].nullable = false;
                }

                for scalar_expr in predicates {
                    let t = tc.typecheck_scalar(scalar_expr, expr, &t_in)?;

                    // filter condition must be boolean
                    // ignoring nullability: null is treated as false
                    // NB this behavior is slightly different from columns_match (for which we would set nullable to false in the expected type)
                    if t.scalar_type != ScalarType::Bool {
                        return Err(TypeError::MismatchColumn {
                            source: expr,
                            got: t,
                            expected: ColumnType {
                                scalar_type: ScalarType::Bool,
                                nullable: true,
                            },
                            message: "expected boolean condition".into(),
                        });
                    }
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
                    t_in.extend(tc.typecheck(input, ctx)?);
                }

                for eq_class in equivalences {
                    let mut t_exprs: Vec<ColumnType> = Vec::with_capacity(eq_class.len());

                    let mut found_non_null = false;

                    for scalar_expr in eq_class {
                        let t_expr = tc.typecheck_scalar(scalar_expr, expr, &t_in)?;

                        if let Some(t_first) = t_exprs.get(0) {
                            if !t_expr.scalar_type.base_eq(&t_first.scalar_type) {
                                return Err(TypeError::MismatchColumn {
                                    source: expr,
                                    got: t_expr,
                                    expected: t_first.clone(),
                                    message: "equivalence class members have different scalar types".into(),
                                });
                            }

                            // equivalences may or may not match on nullability
                            // before JoinImplementation runs, nullability should match.
                            // but afterwards, some nulls may appear that are actually being filtered out elsewhere
                            //
                            // TODO(mgree) find a more consistent way to handle this invariant
                            if self.strict_join_equivalences {
                                if !t_expr.nullable {
                                    found_non_null = true;
                                }

                                if t_expr.nullable != t_first.nullable {
                                    return Err(TypeError::MismatchColumn {
                                        source: expr,
                                        got: t_expr,
                                        expected: t_first.clone(),
                                        message: "equivalence class members have different nullability (and join equivalence checking is strict)".to_string(),
                                    });
                                }
                            }
                        }
                        t_exprs.push(t_expr);
                    }

                    if self.strict_join_equivalences && !found_non_null {
                        return Err(TypeError::BadJoinEquivalence {
                            source: expr,
                            got: t_exprs,
                            message: "all expressions were nullable (and join equivalence checking is strict)".to_string(),
                        });
                    }
                }

                // check that the join implementation is consistent
                match implementation {
                    JoinImplementation::Differential((_, first_keys, _), others) => {
                        if let Some(keys) = first_keys {
                            for scalar_expr in keys {
                                let _ = tc.typecheck_scalar(scalar_expr, expr, &t_in)?;
                            }
                        }

                        for (_, keys, _) in others {
                            for scalar_expr in keys {
                                let _ = tc.typecheck_scalar(scalar_expr, expr, &t_in)?;
                            }
                        }
                    }
                    JoinImplementation::DeltaQuery(plans) => {
                        for plan in plans {
                            for (_, keys, _) in plan {
                                for scalar_expr in keys {
                                    let _ = tc.typecheck_scalar(scalar_expr, expr, &t_in)?;
                                }
                            }
                        }
                    }
                    JoinImplementation::IndexedFilter(_global_id, keys, consts) => {
                        let typ: Vec<ColumnType> = keys
                            .iter()
                            .map(|scalar_expr| tc.typecheck_scalar(scalar_expr, expr, &t_in))
                            .collect::<Result<Vec<ColumnType>, TypeError>>()?;

                        for row in consts {
                            let datums = row.unpack();

                            // correct length
                            if datums.len() != typ.len() {
                                return Err(TypeError::BadConstantRow {
                                    source: expr,
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
                                    source: expr,
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
                let t_in = tc.typecheck(input, ctx)?;

                let mut t_out = group_key
                    .iter()
                    .map(|scalar_expr| tc.typecheck_scalar(scalar_expr, expr, &t_in))
                    .collect::<Result<Vec<_>, _>>()?;

                for agg in aggregates {
                    t_out.push(tc.typecheck_aggregate(agg, expr, &t_in)?);
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
                let t_in = tc.typecheck(input, ctx)?;

                for &key in group_key {
                    if key >= t_in.len() {
                        return Err(TypeError::BadTopKGroupKey {
                            source: expr,
                            key,
                            input_type: t_in,
                        });
                    }
                }

                for order in order_key {
                    if order.column >= t_in.len() {
                        return Err(TypeError::BadTopKOrdering {
                            source: expr,
                            order: order.clone(),
                            input_type: t_in,
                        });
                    }
                }

                Ok(t_in)
            }
            Negate { input } => tc.typecheck(input, ctx),
            Threshold { input } => tc.typecheck(input, ctx),
            Union { base, inputs } => {
                let mut t_base = tc.typecheck(base, ctx)?;

                for input in inputs {
                    let t_input = tc.typecheck(input, ctx)?;

                    if t_base.len() != t_input.len() {
                        return Err(TypeError::MismatchColumns {
                            source: expr,
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
                                    source: expr,
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
                let t_value = tc.typecheck(value, ctx)?;

                let binding = Id::Local(*id);
                if ctx.contains_key(&binding) {
                    return Err(TypeError::Shadowing {
                        source: expr,
                        id: binding,
                    });
                }

                let mut body_ctx = ctx.clone();
                body_ctx.insert(Id::Local(*id), t_value);

                tc.typecheck(body, &body_ctx)
            }
            LetRec { ids, values, body } => {
                if ids.len() != values.len() {
                    return Err(TypeError::BadLetRecBindings { source: expr });
                }

                // temporary hack: steal info from the Gets inside to learn the expected types
                // if no get occurs in any definition or the body, that means that relation is dead code (which is okay)
                let mut ctx = ctx.clone();
                // calling tc.collect_recursive_variable_types(expr, ...) triggers a panic due to nested letrecs with shadowing IDs
                for inner_expr in values.iter().chain(std::iter::once(body.as_ref())) {
                    tc.collect_recursive_variable_types(inner_expr, ids, &mut ctx)?;
                }

                for (id, value) in ids.iter().zip_eq(values.iter()) {
                    let typ = tc.typecheck(value, &ctx)?;

                    let id = Id::Local(id.clone());
                    if let Some(ctx_typ) = ctx.get_mut(&id) {
                        for (base_col, input_col) in ctx_typ.iter_mut().zip_eq(typ) {
                            *base_col = base_col.union(&input_col).map_err(|e| {
                                TypeError::MismatchColumn {
                                    source: expr,
                                    got: input_col,
                                    expected: base_col.clone(),
                                    message: format!(
                                        "couldn't compute union of column types in let rec: {e}"
                                    ),
                                }
                            })?;
                        }
                    } else {
                        // dead code: no `Get` references this relation anywhere. we record the type anyway
                        ctx.insert(id, typ);
                    }
                }

                tc.typecheck(body, &ctx)
            }
            ArrangeBy { input, keys } => {
                let t_in = tc.typecheck(input, ctx)?;

                for cols in keys {
                    for col in cols {
                        let _ = tc.typecheck_scalar(col, expr, &t_in)?;
                    }
                }

                Ok(t_in)
            }
        })
    }

    /// Traverses a term to collect the types of given ids.
    ///
    /// LetRec doesn't have type info stored in it. Until we change the MIR to track that information explicitly, we have to rebuild it from looking at the term.
    fn collect_recursive_variable_types<'a>(
        &self,
        expr: &'a MirRelationExpr,
        ids: &[LocalId],
        ctx: &mut Context,
    ) -> Result<(), TypeError<'a>> {
        use MirRelationExpr::*;

        self.checked_recur(|tc| {
            match expr {
                Get {
                    id: Id::Local(id),
                    typ,
                } => {
                    if !ids.contains(id) {
                        return Ok(());
                    }

                    let id = Id::Local(id.clone());
                    if let Some(ctx_typ) = ctx.get_mut(&id) {
                        for (base_col, input_col) in
                            ctx_typ.iter_mut().zip_eq(typ.column_types.iter())
                        {
                            *base_col = base_col.union(input_col).map_err(|e| {
                                TypeError::MismatchColumn {
                                    source: expr,
                                    got: input_col.clone(),
                                    expected: base_col.clone(),
                                    message: format!(
                                        "couldn't compute union of collected column types: {}",
                                        e
                                    ),
                                }
                            })?;
                        }
                    } else {
                        ctx.insert(id, typ.column_types.clone());
                    }
                }
                Get {
                    id: Id::Global(..), ..
                }
                | Constant { .. } => (),
                Let { id, value, body } => {
                    tc.collect_recursive_variable_types(value, ids, ctx)?;

                    // we've shadowed the id
                    if ids.contains(id) {
                        return Err(TypeError::Shadowing {
                            source: expr,
                            id: Id::Local(*id),
                        });
                    }

                    tc.collect_recursive_variable_types(body, ids, ctx)?;
                }
                LetRec {
                    ids: inner_ids,
                    values,
                    body,
                } => {
                    for inner_id in inner_ids {
                        if ids.contains(inner_id) {
                            return Err(TypeError::Shadowing {
                                source: expr,
                                id: Id::Local(*inner_id),
                            });
                        }
                    }

                    for value in values {
                        tc.collect_recursive_variable_types(value, ids, ctx)?;
                    }

                    tc.collect_recursive_variable_types(body, ids, ctx)?;
                }
                Project { input, .. }
                | Map { input, .. }
                | FlatMap { input, .. }
                | Filter { input, .. }
                | Reduce { input, .. }
                | TopK { input, .. }
                | Negate { input }
                | Threshold { input }
                | ArrangeBy { input, .. } => {
                    tc.collect_recursive_variable_types(input, ids, ctx)?;
                }
                Join { inputs, .. } => {
                    for input in inputs {
                        tc.collect_recursive_variable_types(input, ids, ctx)?;
                    }
                }
                Union { base, inputs } => {
                    tc.collect_recursive_variable_types(base, ids, ctx)?;

                    for input in inputs {
                        tc.collect_recursive_variable_types(input, ids, ctx)?;
                    }
                }
            }

            Ok(())
        })
    }

    fn typecheck_scalar<'a>(
        &self,
        expr: &'a MirScalarExpr,
        source: &'a MirRelationExpr,
        column_types: &[ColumnType],
    ) -> Result<ColumnType, TypeError<'a>> {
        use MirScalarExpr::*;

        self.checked_recur(|tc| match expr {
            Column(i) => match column_types.get(*i) {
                Some(ty) => Ok(ty.clone()),
                None => Err(TypeError::NoSuchColumn {
                    source,
                    expr,
                    col: *i,
                }),
            },
            Literal(row, typ) => {
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
            CallUnmaterializable(func) => Ok(func.output_type()),
            CallUnary { expr, func } => {
                Ok(func.output_type(tc.typecheck_scalar(expr, source, column_types)?))
            }
            CallBinary { expr1, expr2, func } => Ok(func.output_type(
                tc.typecheck_scalar(expr1, source, column_types)?,
                tc.typecheck_scalar(expr2, source, column_types)?,
            )),
            CallVariadic { exprs, func } => Ok(func.output_type(
                exprs
                    .iter()
                    .map(|e| tc.typecheck_scalar(e, source, column_types))
                    .collect::<Result<Vec<_>, TypeError>>()?,
            )),
            If { cond, then, els } => {
                let cond_type = tc.typecheck_scalar(cond, source, column_types)?;

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

                let then_type = tc.typecheck_scalar(then, source, column_types)?;
                let else_type = tc.typecheck_scalar(els, source, column_types)?;
                then_type
                    .union(&else_type)
                    .map_err(|e| TypeError::MismatchColumn {
                        source,
                        got: then_type,
                        expected: else_type,
                        message: format!("couldn't compute union of column types for if: {e}"),
                    })
            }
        })
    }

    /// Typecheck an `AggregateExpr`
    pub fn typecheck_aggregate<'a>(
        &self,
        expr: &'a AggregateExpr,
        source: &'a MirRelationExpr,
        column_types: &[ColumnType],
    ) -> Result<ColumnType, TypeError<'a>> {
        self.checked_recur(|tc| {
            let t_in = tc.typecheck_scalar(&expr.expr, source, column_types)?;

            // TODO check that t_in is actually acceptable for `func`

            Ok(expr.func.output_type(t_in))
        })
    }
}

// Detailed type error logging as a warning, with failures in CI (SOFT_ASSERTIONS) and a logged error in production
macro_rules! type_error {
    ($($arg:tt)+) => {{
        ::tracing::warn!($($arg)+);

        if mz_ore::assert::SOFT_ASSERTIONS.load(::std::sync::atomic::Ordering::Relaxed) {
            return Err(TransformError::Internal("type error in MIR optimization (details in warning)".to_string()));
        } else {
            ::tracing::error!("type error in MIR optimization (details in warning)");
        }
    }}
}

impl crate::Transform for Typecheck {
    fn transform(
        &self,
        relation: &mut mz_expr::MirRelationExpr,
        args: crate::TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let mut ctx = self.ctx.borrow_mut();

        let expected = args
            .global_id
            .map_or_else(|| None, |id| ctx.get(&Id::Global(*id)));

        if let Some(id) = args.global_id {
            if self.disallow_new_globals
                && expected.is_none()
                && args.global_id.is_some()
                && !id.is_transient()
            {
                type_error!(
                    "FOUND NEW NON-TRANSIENT TOP LEVEL QUERY BOUND TO {id}:\n{}",
                    relation.pretty()
                );
            }
        }

        let got = self.typecheck(relation, &ctx);

        let humanizer = mz_repr::explain::DummyHumanizer;

        if let Err(err) = self.typecheck(relation, &ctx) {
            type_error!(
                "TYPE ERROR: {err}\nIN UNKNOWN QUERY:\n{}",
                relation.pretty()
            );
        }

        match (got, expected) {
            (Ok(got), Some(expected)) => {
                let id = args.global_id.unwrap();

                // contravariant: global types can be updated
                if !is_subtype_of(expected, &got) {
                    let got = columns_pretty(&got, &humanizer);
                    let expected = columns_pretty(expected, &humanizer);

                    type_error!(
                        "TYPE ERROR: GLOBAL ID TYPE CHANGED\n     got {got}\nexpected {expected} \nIN KNOWN QUERY BOUND TO {id}:\n{}",
                        relation.pretty()
                    );
                }
            }
            (Ok(got), None) => {
                if let Some(id) = args.global_id {
                    ctx.insert(Id::Global(*id), got);
                }
            }
            (Err(err), _) => {
                let binding = match args.global_id {
                    Some(id) => format!("QUERY BOUND TO {id}"),
                    None => "TRANSIENT QUERY".to_string(),
                };

                let (expected, known) = match expected {
                    Some(expected) => (
                        format!("expected type {}", columns_pretty(expected, &humanizer)),
                        "KNOWN".to_string(),
                    ),
                    None => ("no expected type".to_string(), "NEW".to_string()),
                };

                type_error!(
                    "TYPE ERROR:\n{err}\n{expected}\nIN {known} {binding}:\n{}",
                    relation.pretty()
                );
            }
        }

        Ok(())
    }
}

fn columns_pretty<H>(cols: &[ColumnType], humanizer: &H) -> String
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

impl<'a> TypeError<'a> {
    /// The source of the type error
    pub fn source(&self) -> Option<&'a MirRelationExpr> {
        use TypeError::*;
        match self {
            Unbound { source, .. }
            | NoSuchColumn { source, .. }
            | MismatchColumn { source, .. }
            | MismatchColumns { source, .. }
            | BadConstantRow { source, .. }
            | BadProject { source, .. }
            | BadJoinEquivalence { source, .. }
            | BadTopKGroupKey { source, .. }
            | BadTopKOrdering { source, .. }
            | BadLetRecBindings { source }
            | Shadowing { source, .. } => Some(source),
            Recursion { .. } => None,
        }
    }

    fn humanize<H>(&self, humanizer: &H, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    where
        H: ExprHumanizer,
    {
        if let Some(source) = self.source() {
            writeln!(f, "In the MIR term:\n{}\n", source.pretty())?;
        }

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
                    "mismatched column types: {message}\n      got {got}\nexpected {expected}"
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
                    "mismatched relation types: {message}\n      got {got}\nexpected {expected}"
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
                    "bad constant row\n      got {got}\nexpected row of type {expected}"
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
            BadJoinEquivalence {
                source: _,
                got,
                message,
            } => {
                let got = columns_pretty(got, humanizer);

                writeln!(f, "bad join equivalence {got}: {message}")?
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
            Recursion { error } => writeln!(f, "{error}")?,
        }

        Ok(())
    }
}
