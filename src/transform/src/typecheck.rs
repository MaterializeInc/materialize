// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Check that the visible type of each query has not been changed

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt::Write;
use std::rc::Rc;

use itertools::Itertools;
use mz_expr::{
    non_nullable_columns, AggregateExpr, ColumnOrder, Id, JoinImplementation, LocalId,
    MirRelationExpr, MirScalarExpr, RECURSION_LIMIT,
};
use mz_ore::stack::{CheckedRecursion, RecursionGuard, RecursionLimitError};
use mz_repr::explain::{DummyHumanizer, ExprHumanizer};
use mz_repr::{ColumnName, ColumnType, RelationType, Row, ScalarBaseType, ScalarType};

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
pub enum TypeError<'a> {
    /// Unbound identifiers (local or global)
    Unbound {
        /// Expression with the bug
        source: &'a MirRelationExpr,
        /// The (unbound) identifier referenced
        id: Id,
        /// The type `id` was expected to have
        typ: RelationType,
    },
    /// Dereference of a non-existent column
    NoSuchColumn {
        /// Expression with the bug
        source: &'a MirRelationExpr,
        /// Scalar expression that references an invalid column
        expr: &'a MirScalarExpr,
        /// The invalid column referenced
        col: usize,
    },
    /// A single column type does not match
    MismatchColumn {
        /// Expression with the bug
        source: &'a MirRelationExpr,
        /// The column type we found (`sub` type)
        got: ColumnType,
        /// The column type we expected (`sup` type)
        expected: ColumnType,
        /// The difference between these types
        diffs: Vec<ColumnTypeDifference>,
        /// An explanatory message
        message: String,
    },
    /// Relation column types do not match
    MismatchColumns {
        /// Expression with the bug
        source: &'a MirRelationExpr,
        /// The column types we found (`sub` type)
        got: Vec<ColumnType>,
        /// The solumn types we expected (`sup` type)
        expected: Vec<ColumnType>,
        /// The difference between these types
        diffs: Vec<RelationTypeDifference>,
        /// An explanatory message
        message: String,
    },
    /// A constant row does not have the correct type
    BadConstantRow {
        /// Expression with the bug
        source: &'a MirRelationExpr,
        /// A constant row
        got: Row,
        /// The expected type (which that row does not have)
        expected: Vec<ColumnType>,
        // TODO(mgree) with a good way to get the type of a Datum, we could give a diff here
    },
    /// Projection of a non-existent column
    BadProject {
        /// Expression with the bug
        source: &'a MirRelationExpr,
        /// The column projected
        got: Vec<usize>,
        /// The input columns (which don't have that column)
        input_type: Vec<ColumnType>,
    },
    /// An equivalence class in a join was malformed
    BadJoinEquivalence {
        /// Expression with the bug
        source: &'a MirRelationExpr,
        /// The join equivalences
        got: Vec<ColumnType>,
        /// The problem with the join equivalences
        message: String,
    },
    /// TopK grouping by non-existent column
    BadTopKGroupKey {
        /// Expression with the bug
        source: &'a MirRelationExpr,
        /// The bad column reference in the group key
        k: usize,
        /// The input columns (which don't have that column)
        input_type: Vec<ColumnType>,
    },
    /// TopK ordering by non-existent column
    BadTopKOrdering {
        /// Expression with the bug
        source: &'a MirRelationExpr,
        /// The ordering used
        order: ColumnOrder,
        /// The input columns (which don't work for that ordering)
        input_type: Vec<ColumnType>,
    },
    /// LetRec bindings are malformed
    BadLetRecBindings {
        /// Expression with the bug
        source: &'a MirRelationExpr,
    },
    /// Local identifiers are shadowed
    Shadowing {
        /// Expression with the bug
        source: &'a MirRelationExpr,
        /// The id that was shadowed
        id: Id,
    },
    /// Recursion depth exceeded
    Recursion {
        /// The error that aborted recursion
        error: RecursionLimitError,
    },
}

impl<'a> From<RecursionLimitError> for TypeError<'a> {
    fn from(error: RecursionLimitError) -> Self {
        TypeError::Recursion { error }
    }
}

type Context = BTreeMap<Id, Vec<ColumnType>>;

/// Characterizes differences between relation types
///
/// Each constructor indicates a reason why some type `sub` was not a subtype of another type `sup`
#[derive(Clone, Debug, Hash)]
pub enum RelationTypeDifference {
    /// `sub` and `sup` don't have the same number of columns
    Length {
        /// Length of `sub`
        len_sub: usize,
        /// Length of `sup`
        len_sup: usize,
    },
    /// `sub` and `sup` differ at the indicated column
    Column {
        /// The column at which `sub` and `sup` differ
        col: usize,
        /// The difference between `sub` and `sup`
        diff: ColumnTypeDifference,
    },
}

/// Characterizes differences between individual column types
///
/// Each constructor indicates a reason why some type `sub` was not a subtype of another type `sup`
/// There may be multiple reasons, e.g., `sub` may be missing fields and have fields of different types
#[derive(Clone, Debug, Hash)]
pub enum ColumnTypeDifference {
    /// The `ScalarBaseType` of `sub` doesn't match that of `sup`
    NotSubtype {
        /// Would-be subtype
        sub: ScalarType,
        /// Would-be supertype
        sup: ScalarType,
    },
    /// `sub` was nullable but `sup` was not
    Nullability {
        /// Would-be subtype
        sub: ColumnType,
        /// Would-be supertype
        sup: ColumnType,
    },
    /// Both `sub` and `sup` are a list, map, array, or range, but `sub`'s element type differed from `sup`s
    ElementType {
        /// The type constructor (list, array, etc.)
        ctor: String,
        /// The difference in the element type
        element_type: Box<ColumnTypeDifference>,
    },
    /// `sub` and `sup` are both records, but `sub` is missing fields present in `sup`
    RecordMissingFields {
        /// The missing fields
        missing: Vec<ColumnName>,
    },
    /// `sub` and `sup` are both records, but some fields in `sub` are not subtypes of fields in `sup`
    RecordFields {
        /// The differences, by field
        fields: Vec<(ColumnName, ColumnTypeDifference)>,
    },
}

impl RelationTypeDifference {
    /// Returns the same type difference, but ignoring nullability
    ///
    /// Returns `None` when _all_ of the differences are due to nullability
    pub fn ignore_nullability(self) -> Option<Self> {
        use RelationTypeDifference::*;

        match self {
            Length { .. } => Some(self),
            Column { col, diff } => diff.ignore_nullability().map(|diff| Column { col, diff }),
        }
    }
}

impl ColumnTypeDifference {
    /// Returns the same type difference, but ignoring nullability
    ///
    /// Returns `None` when _all_ of the differences are due to nullability
    pub fn ignore_nullability(self) -> Option<Self> {
        use ColumnTypeDifference::*;

        match self {
            Nullability { .. } => None,
            NotSubtype { .. } | RecordMissingFields { .. } => Some(self),
            ElementType { ctor, element_type } => {
                element_type
                    .ignore_nullability()
                    .map(|element_type| ElementType {
                        ctor,
                        element_type: Box::new(element_type),
                    })
            }
            RecordFields { fields } => {
                let fields = fields
                    .into_iter()
                    .flat_map(|(col, diff)| diff.ignore_nullability().map(|diff| (col, diff)))
                    .collect::<Vec<_>>();

                if fields.is_empty() {
                    None
                } else {
                    Some(RecordFields { fields })
                }
            }
        }
    }
}

/// Returns a list of differences that make `sub` not a subtype of `sup`
///
/// This function returns an empty list when `sub` is a subtype of `sup`
pub fn relation_subtype_difference(
    sub: &[ColumnType],
    sup: &[ColumnType],
) -> Vec<RelationTypeDifference> {
    let mut diffs = Vec::new();

    if sub.len() != sup.len() {
        diffs.push(RelationTypeDifference::Length {
            len_sub: sub.len(),
            len_sup: sup.len(),
        });

        // TODO(mgree) we could do an edit-distance computation to report more errors
        return diffs;
    }

    diffs.extend(
        sub.iter()
            .zip_eq(sup.iter())
            .enumerate()
            .flat_map(|(col, (sub_ty, sup_ty))| {
                column_subtype_difference(sub_ty, sup_ty)
                    .into_iter()
                    .map(move |diff| RelationTypeDifference::Column { col, diff })
            }),
    );

    diffs
}

/// Returns a list of differences that make `sub` not a subtype of `sup`
///
/// This function returns an empty list when `sub` is a subtype of `sup`
pub fn column_subtype_difference(sub: &ColumnType, sup: &ColumnType) -> Vec<ColumnTypeDifference> {
    let mut diffs = scalar_subtype_difference(&sub.scalar_type, &sup.scalar_type);

    if sub.nullable && !sup.nullable {
        diffs.push(ColumnTypeDifference::Nullability {
            sub: sub.clone(),
            sup: sup.clone(),
        });
    }

    diffs
}

/// Returns a list of differences that make `sub` not a subtype of `sup`
///
/// This function returns an empty list when `sub` is a subtype of `sup`
pub fn scalar_subtype_difference(sub: &ScalarType, sup: &ScalarType) -> Vec<ColumnTypeDifference> {
    use ScalarType::*;

    let mut diffs = Vec::new();

    match (sub, sup) {
        (
            List {
                element_type: sub_elt,
                ..
            },
            List {
                element_type: sup_elt,
                ..
            },
        )
        | (
            Map {
                value_type: sub_elt,
                ..
            },
            Map {
                value_type: sup_elt,
                ..
            },
        )
        | (
            Range {
                element_type: sub_elt,
                ..
            },
            Range {
                element_type: sup_elt,
                ..
            },
        )
        | (Array(sub_elt), Array(sup_elt)) => {
            let ctor = format!("{:?}", ScalarBaseType::from(sub));
            diffs.extend(
                scalar_subtype_difference(sub_elt, sup_elt)
                    .into_iter()
                    .map(|diff| ColumnTypeDifference::ElementType {
                        ctor: ctor.clone(),
                        element_type: Box::new(diff),
                    }),
            );
        }
        (
            Record {
                fields: sub_fields, ..
            },
            Record {
                fields: sup_fields, ..
            },
        ) => {
            let sub = sub_fields
                .iter()
                .map(|(sub_field, sub_ty)| (sub_field.clone(), sub_ty))
                .collect::<BTreeMap<_, _>>();

            let mut missing = Vec::new();
            let mut field_diffs = Vec::new();
            for (sup_field, sup_ty) in sup_fields {
                if let Some(sub_ty) = sub.get(sup_field) {
                    let diff = column_subtype_difference(sub_ty, sup_ty);

                    if !diff.is_empty() {
                        field_diffs.push((sup_field.clone(), diff));
                    }
                } else {
                    missing.push(sup_field.clone());
                }
            }
        }
        (_, _) => {
            // TODO(mgree) confirm that we don't want to allow numeric subtyping
            if ScalarBaseType::from(sub) != ScalarBaseType::from(sup) {
                diffs.push(ColumnTypeDifference::NotSubtype {
                    sub: sub.clone(),
                    sup: sup.clone(),
                })
            }
        }
    };

    diffs
}

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
    ///   ggevay: Checking keys would have the same problem as checking nullability: key inference
    ///   is very heuristic (even more so than nullability inference), so it's almost impossible to
    ///   reliably keep it stable across transformations.
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
                let diffs = relation_subtype_difference(&typ.column_types, ctx_typ).into_iter().flat_map(|diff| diff.ignore_nullability()).collect::<Vec<_>>();

                if !diffs.is_empty() {
                    return Err(TypeError::MismatchColumns {
                        source: expr,
                        got: typ.column_types.clone(),
                        expected: ctx_typ.clone(),
                        diffs,
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
                        let sub = t.scalar_type.clone();

                        return Err(TypeError::MismatchColumn {
                            source: expr,
                            got: t,
                            expected: ColumnType {
                                scalar_type: ScalarType::Bool,
                                nullable: true,
                            },
                            diffs: vec![ColumnTypeDifference::NotSubtype { sub, sup: ScalarType::Bool }],
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
                let mut t_in_global = Vec::new();
                let mut t_in_local = vec![Vec::new(); inputs.len()];

                for (i, input) in inputs.iter().enumerate() {
                    let input_t = tc.typecheck(input, ctx)?;
                    t_in_global.extend(input_t.clone());
                    t_in_local[i] = input_t;
                }

                for eq_class in equivalences {
                    let mut t_exprs: Vec<ColumnType> = Vec::with_capacity(eq_class.len());

                    let mut all_nullable = true;

                    for scalar_expr in eq_class {
                        // Note: the equivalences have global column references
                        let t_expr = tc.typecheck_scalar(scalar_expr, expr, &t_in_global)?;

                        if !t_expr.nullable {
                            all_nullable = false;
                        }

                        if let Some(t_first) = t_exprs.get(0) {
                            let diffs = scalar_subtype_difference(&t_expr.scalar_type, &t_first.scalar_type);
                            if !diffs.is_empty() {
                                return Err(TypeError::MismatchColumn {
                                    source: expr,
                                    got: t_expr,
                                    expected: t_first.clone(),
                                    diffs,
                                    message: "equivalence class members have different scalar types".into(),
                                });
                            }

                            // equivalences may or may not match on nullability
                            // before JoinImplementation runs, nullability should match.
                            // but afterwards, some nulls may appear that are actually being filtered out elsewhere
                            if self.strict_join_equivalences {
                                if t_expr.nullable != t_first.nullable {
                                    let sub = t_expr.clone();
                                    let sup = t_first.clone();

                                    let err = TypeError::MismatchColumn {
                                        source: expr,
                                        got: t_expr.clone(),
                                        expected: t_first.clone(),
                                        diffs: vec![ColumnTypeDifference::Nullability { sub, sup }],
                                        message: "equivalence class members have different nullability (and join equivalence checking is strict)".to_string(),
                                    };

                                    // TODO(mgree) this imprecision should be resolved, but we need to fix the optimizer
                                    ::tracing::debug!("{err}");
                                }
                            }
                        }

                        t_exprs.push(t_expr);
                    }

                    if self.strict_join_equivalences && all_nullable {
                        let err = TypeError::BadJoinEquivalence {
                            source: expr,
                            got: t_exprs,
                            message: "all expressions were nullable (and join equivalence checking is strict)".to_string(),
                        };

                        // TODO(mgree) this imprecision should be resolved, but we need to fix the optimizer
                        ::tracing::debug!("{err}");
                    }
                }

                // check that the join implementation is consistent
                match implementation {
                    JoinImplementation::Differential((start_idx, first_key, _), others) => {
                        if let Some(key) = first_key {
                            for k in key {
                                let _ = tc.typecheck_scalar(k, expr, &t_in_local[*start_idx])?;
                            }
                        }

                        for (idx, key, _) in others {
                            for k in key {
                                let _ = tc.typecheck_scalar(k, expr, &t_in_local[*idx])?;
                            }
                        }
                    }
                    JoinImplementation::DeltaQuery(plans) => {
                        for plan in plans {
                            for (idx, key, _) in plan {
                                for k in key {
                                    let _ = tc.typecheck_scalar(k, expr, &t_in_local[*idx])?;
                                }
                            }
                        }
                    }
                    JoinImplementation::IndexedFilter(_global_id, key, consts) => {
                        let typ: Vec<ColumnType> = key
                            .iter()
                            .map(|k| tc.typecheck_scalar(k, expr, &t_in_global))
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

                Ok(t_in_global)
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

                for &k in group_key {
                    if k >= t_in.len() {
                        return Err(TypeError::BadTopKGroupKey {
                            source: expr,
                            k,
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

                    let len_sub = t_base.len();
                    let len_sup = t_input.len();
                    if len_sub != len_sup {
                        return Err(TypeError::MismatchColumns {
                            source: expr,
                            got: t_base.clone(),
                            expected: t_input,
                            diffs: vec![RelationTypeDifference::Length {
                                len_sub,
                                len_sup,
                            }],
                            message: "union branches have different numbers of columns".into(),
                        });
                    }

                    for (base_col, input_col) in t_base.iter_mut().zip_eq(t_input) {
                        *base_col =
                            base_col
                                .union(&input_col)
                                .map_err(|e| {
                                    let base_col = base_col.clone();
                                    let diffs = column_subtype_difference(&base_col, &input_col);

                                    TypeError::MismatchColumn {
                                    source: expr,
                                    got: input_col,
                                    expected: base_col,
                                    diffs,
                                    message: format!(
                                        "couldn't compute union of column types in union: {e}"
                                    ),
                                }
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
            LetRec { ids, values, body, limits: _ } => {
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
                                let base_col = base_col.clone();
                                let diffs = column_subtype_difference(&base_col, &input_col);

                                TypeError::MismatchColumn {
                                    source: expr,
                                    got: input_col,
                                    expected: base_col,
                                    diffs,
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

                for key in keys {
                    for k in key {
                        let _ = tc.typecheck_scalar(k, expr, &t_in)?;
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
                                let base_col = base_col.clone();
                                let diffs = column_subtype_difference(&base_col, input_col);

                                TypeError::MismatchColumn {
                                    source: expr,
                                    got: input_col.clone(),
                                    expected: base_col,
                                    diffs,
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
                    limits: _,
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
                    let sub = cond_type.scalar_type.clone();

                    return Err(TypeError::MismatchColumn {
                        source,
                        got: cond_type,
                        expected: ColumnType {
                            scalar_type: ScalarType::Bool,
                            nullable: true,
                        },
                        diffs: vec![ColumnTypeDifference::NotSubtype {
                            sub,
                            sup: ScalarType::Bool,
                        }],
                        message: "expected boolean condition".into(),
                    });
                }

                let then_type = tc.typecheck_scalar(then, source, column_types)?;
                let else_type = tc.typecheck_scalar(els, source, column_types)?;
                then_type.union(&else_type).map_err(|e| {
                    let diffs = column_subtype_difference(&then_type, &else_type);

                    TypeError::MismatchColumn {
                        source,
                        got: then_type,
                        expected: else_type,
                        diffs,
                        message: format!("couldn't compute union of column types for if: {e}"),
                    }
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

/// Detailed type error logging as a warning, with failures in CI (SOFT_ASSERTIONS) and a logged error in production
///
/// type_error(severity, ...) logs a type warning; if `severity` is `true`, it will also log an error (visible in Sentry)
macro_rules! type_error {
    ($severity:expr, $($arg:tt)+) => {{
        if $severity {
          ::tracing::warn!($($arg)+);
          ::tracing::error!("type error in MIR optimization (details in warning; see 'Type error omnibus' issue #19101 <https://github.com/MaterializeInc/materialize/issues/19101>)");
        } else {
          ::tracing::debug!($($arg)+);
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
                    false, // not severe
                    "TYPE WARNING: NEW NON-TRANSIENT GLOBAL ID {id}\n{}",
                    relation.pretty()
                );
            }
        }

        let got = self.typecheck(relation, &ctx);

        let humanizer = mz_repr::explain::DummyHumanizer;

        match (got, expected) {
            (Ok(got), Some(expected)) => {
                let id = args.global_id.unwrap();

                // contravariant: global types can be updated
                let diffs = relation_subtype_difference(expected, &got);
                if !diffs.is_empty() {
                    // SEVERE only if got and expected have true differences, not just nullability
                    let severity = diffs
                        .iter()
                        .any(|diff| diff.clone().ignore_nullability().is_some());

                    let err = TypeError::MismatchColumns {
                        source: relation,
                        got,
                        expected: expected.clone(),
                        diffs,
                        message: format!("a global id {id}'s type changed (was `expected` which should be a subtype of `got`) "),
                    };

                    type_error!(severity, "TYPE ERROR IN KNOWN GLOBAL ID {id}:\n{err}");
                }
            }
            (Ok(got), None) => {
                if let Some(id) = args.global_id {
                    ctx.insert(Id::Global(*id), got);
                }
            }
            (Err(err), _) => {
                let (expected, binding) = match expected {
                    Some(expected) => {
                        let id = args.global_id.unwrap();
                        (
                            format!("expected type {}\n", columns_pretty(expected, &humanizer)),
                            format!("KNOWN GLOBAL ID {id}"),
                        )
                    }
                    None => ("".to_string(), "TRANSIENT QUERY".to_string()),
                };

                type_error!(
                    true, // SEVERE: the transformed code is inconsistent
                    "TYPE ERROR IN {binding}:\n{err}\n{expected}{}",
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

impl RelationTypeDifference {
    /// Pretty prints a type difference
    ///
    /// Always indents two spaces
    pub fn humanize<H>(&self, h: &H, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    where
        H: ExprHumanizer,
    {
        use RelationTypeDifference::*;
        match self {
            Length { len_sub, len_sup } => {
                writeln!(
                    f,
                    "  number of columns do not match ({len_sub} != {len_sup})"
                )
            }
            Column { col, diff } => {
                writeln!(f, "  column {col} differs:")?;
                diff.humanize(4, h, f)
            }
        }
    }
}

impl ColumnTypeDifference {
    /// Pretty prints a type difference at a given indentation level
    pub fn humanize<H>(
        &self,
        indent: usize,
        h: &H,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result
    where
        H: ExprHumanizer,
    {
        use ColumnTypeDifference::*;

        // indent
        write!(f, "{:indent$}", "")?;

        match self {
            NotSubtype { sub, sup } => {
                let sub = h.humanize_scalar_type(sub);
                let sup = h.humanize_scalar_type(sup);

                writeln!(f, "{sub} is a not a subtype of {sup}")
            }
            Nullability { sub, sup } => {
                let sub = h.humanize_column_type(sub);
                let sup = h.humanize_column_type(sup);

                writeln!(f, "{sub} is nullable but {sup} is not")
            }
            ElementType { ctor, element_type } => {
                writeln!(f, "{ctor} element types differ:")?;

                element_type.humanize(indent + 2, h, f)
            }
            RecordMissingFields { missing } => {
                write!(f, "missing column fields:")?;
                for col in missing {
                    write!(f, " {col}")?;
                }
                f.write_char('\n')
            }
            RecordFields { fields } => {
                writeln!(f, "{} record fields differ:", fields.len())?;

                for (col, diff) in fields {
                    writeln!(f, "{:indent$}  field '{col}':", "")?;
                    diff.humanize(indent + 4, h, f)?;
                }
                Ok(())
            }
        }
    }
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
                diffs,
                message,
            } => {
                let got = humanizer.humanize_column_type(got);
                let expected = humanizer.humanize_column_type(expected);
                writeln!(
                    f,
                    "mismatched column types: {message}\n      got {got}\nexpected {expected}"
                )?;

                for diff in diffs {
                    diff.humanize(2, humanizer, f)?;
                }
            }
            MismatchColumns {
                source: _,
                got,
                expected,
                diffs,
                message,
            } => {
                let got = columns_pretty(got, humanizer);
                let expected = columns_pretty(expected, humanizer);

                writeln!(
                    f,
                    "mismatched relation types: {message}\n      got {got}\nexpected {expected}"
                )?;

                for diff in diffs {
                    diff.humanize(humanizer, f)?;
                }
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
                k,
                input_type,
            } => {
                let input_type = columns_pretty(input_type, humanizer);

                writeln!(
                    f,
                    "TopK group key component references invalid column {k} in columns: {input_type}"
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
