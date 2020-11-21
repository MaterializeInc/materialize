// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This file houses a representation of a SQL plan that is parallel to that found in
//! src/expr/relation/mod.rs, but represents an earlier phase of planning. It's structurally very
//! similar to that file, with some differences which are noted below. It gets turned into that
//! representation via a call to decorrelate().

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::mem;

use anyhow::bail;
use itertools::Itertools;

use ore::collections::CollectionExt;
use repr::*;

use crate::plan::query::ExprContext;
use crate::plan::typeconv::{self, CastContext};
use crate::plan::Params;

// these happen to be unchanged at the moment, but there might be additions later
pub use expr::{
    AggregateFunc, BinaryFunc, ColumnOrder, NullaryFunc, TableFunc, UnaryFunc, VariadicFunc,
};
use repr::adt::array::ArrayDimension;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Just like expr::RelationExpr, except where otherwise noted below.
///
/// - There is no equivalent to `expr::RelationExpr::Let`.
pub enum RelationExpr {
    Constant {
        rows: Vec<Row>,
        typ: RelationType,
    },
    Get {
        id: expr::Id,
        typ: RelationType,
    },
    Project {
        input: Box<RelationExpr>,
        outputs: Vec<usize>,
    },
    Map {
        input: Box<RelationExpr>,
        scalars: Vec<ScalarExpr>,
    },
    CallTable {
        func: TableFunc,
        exprs: Vec<ScalarExpr>,
    },
    Filter {
        input: Box<RelationExpr>,
        predicates: Vec<ScalarExpr>,
    },
    /// Unlike expr::RelationExpr, we haven't yet compiled LeftOuter/RightOuter/FullOuter
    /// joins away into more primitive exprs
    Join {
        left: Box<RelationExpr>,
        right: Box<RelationExpr>,
        on: ScalarExpr,
        kind: JoinKind,
    },
    /// Unlike expr::RelationExpr, when `key` is empty AND `input` is empty this returns
    /// a single row with the aggregates evaluated over empty groups, rather than returning zero
    /// rows
    Reduce {
        input: Box<RelationExpr>,
        group_key: Vec<usize>,
        aggregates: Vec<AggregateExpr>,
    },
    Distinct {
        input: Box<RelationExpr>,
    },
    /// Groups and orders within each group, limiting output.
    TopK {
        /// The source collection.
        input: Box<RelationExpr>,
        /// Column indices used to form groups.
        group_key: Vec<usize>,
        /// Column indices used to order rows within groups.
        order_key: Vec<ColumnOrder>,
        /// Number of records to retain
        limit: Option<usize>,
        /// Number of records to skip
        offset: usize,
    },
    Negate {
        input: Box<RelationExpr>,
    },
    Threshold {
        input: Box<RelationExpr>,
    },
    Union {
        base: Box<RelationExpr>,
        inputs: Vec<RelationExpr>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Just like expr::ScalarExpr, except where otherwise noted below.
pub enum ScalarExpr {
    /// Unlike expr::ScalarExpr, we can nest RelationExprs via eg Exists. This means that a
    /// variable could refer to a column of the current input, or to a column of an outer relation.
    /// We use ColumnRef to denote the difference.
    Column(ColumnRef),
    Parameter(usize),
    Literal(Row, ColumnType),
    CallNullary(NullaryFunc),
    CallUnary {
        func: UnaryFunc,
        expr: Box<ScalarExpr>,
    },
    CallBinary {
        func: BinaryFunc,
        expr1: Box<ScalarExpr>,
        expr2: Box<ScalarExpr>,
    },
    CallVariadic {
        func: VariadicFunc,
        exprs: Vec<ScalarExpr>,
    },
    If {
        cond: Box<ScalarExpr>,
        then: Box<ScalarExpr>,
        els: Box<ScalarExpr>,
    },
    /// Returns true if `expr` returns any rows
    Exists(Box<RelationExpr>),
    /// Given `expr` with arity 1. If expr returns:
    /// * 0 rows, return NULL
    /// * 1 row, return the value of that row
    /// * >1 rows, the sql spec says we should throw an error but we can't
    ///   (see https://github.com/MaterializeInc/materialize/issues/489)
    ///   so instead we return all the rows.
    ///   If there are multiple `Select` expressions in a single SQL query, the result is that we take the product of all of them.
    ///   This is counter to the spec, but is consistent with eg postgres' treatment of multiple set-returning-functions
    ///   (see https://tapoueh.org/blog/2017/10/set-returning-functions-and-postgresql-10/).
    Select(Box<RelationExpr>),
}

/// A `CoercibleScalarExpr` is a [`ScalarExpr`] whose type is not fully
/// determined. Several SQL expressions can be freely coerced based upon where
/// in the expression tree they appear. For example, the string literal '42'
/// will be automatically coerced to the integer 42 if used in a numeric
/// context:
///
/// ```sql
/// SELECT '42' + 42
/// ```
///
/// This separate type gives the code that needs to interact with coercions very
/// fine-grained control over what coercions happen and when.
///
/// The primary driver of coercion is function and operator selection, as
/// choosing the correct function or operator implementation depends on the type
/// of the provided arguments. Coercion also occurs at the very root of the
/// scalar expression tree. For example in
///
/// ```sql
/// SELECT ... WHERE $1
/// ```
///
/// the `WHERE` clause will coerce the contained unconstrained type parameter
/// `$1` to have type bool.
#[derive(Clone, Debug)]
pub enum CoercibleScalarExpr {
    Coerced(ScalarExpr),
    Parameter(usize),
    LiteralNull,
    LiteralString(String),
    LiteralRecord(Vec<CoercibleScalarExpr>),
}

impl CoercibleScalarExpr {
    pub fn type_as(self, ecx: &ExprContext, ty: &ScalarType) -> Result<ScalarExpr, anyhow::Error> {
        let expr = typeconv::plan_coerce(ecx, self, ty)?;
        let expr_ty = ecx.scalar_type(&expr);
        if ty != &expr_ty {
            bail!("{} must have type {}, not type {}", ecx.name, ty, expr_ty);
        }
        Ok(expr)
    }

    pub fn type_as_any(self, ecx: &ExprContext) -> Result<ScalarExpr, anyhow::Error> {
        typeconv::plan_coerce(ecx, self, &ScalarType::String)
    }

    pub fn cast_to(
        self,
        op: &str,
        ecx: &ExprContext,
        ccx: CastContext,
        ty: &ScalarType,
    ) -> Result<ScalarExpr, anyhow::Error> {
        let expr = typeconv::plan_coerce(ecx, self, ty)?;
        typeconv::plan_cast(op, ecx, ccx, expr, ty)
    }
}

/// An expression whose type can be ascertained.
///
/// Abstracts over `ScalarExpr` and `CoercibleScalarExpr`.
pub trait AbstractExpr {
    type Type: AbstractColumnType;

    /// Computes the type of the expression.
    fn typ(
        &self,
        outers: &[RelationType],
        inner: &RelationType,
        params: &BTreeMap<usize, ScalarType>,
    ) -> Self::Type;
}

impl AbstractExpr for CoercibleScalarExpr {
    type Type = Option<ColumnType>;

    fn typ(
        &self,
        outers: &[RelationType],
        inner: &RelationType,
        params: &BTreeMap<usize, ScalarType>,
    ) -> Self::Type {
        match self {
            CoercibleScalarExpr::Coerced(expr) => Some(expr.typ(outers, inner, params)),
            _ => None,
        }
    }
}

/// A column type-like object whose underlying scalar type-like object can be
/// ascertained.
///
/// Abstracts over `ColumnType` and `Option<ColumnType>`.
pub trait AbstractColumnType {
    type AbstractScalarType;

    /// Converts the column type-like object into its inner scalar type-like
    /// object.
    fn scalar_type(self) -> Self::AbstractScalarType;
}

impl AbstractColumnType for ColumnType {
    type AbstractScalarType = ScalarType;

    fn scalar_type(self) -> Self::AbstractScalarType {
        self.scalar_type
    }
}

impl AbstractColumnType for Option<ColumnType> {
    type AbstractScalarType = Option<ScalarType>;

    fn scalar_type(self) -> Self::AbstractScalarType {
        self.map(|t| t.scalar_type)
    }
}

impl From<ScalarExpr> for CoercibleScalarExpr {
    fn from(expr: ScalarExpr) -> CoercibleScalarExpr {
        CoercibleScalarExpr::Coerced(expr)
    }
}

/// A leveled column reference.
///
/// In the course of decorrelation, multiple levels of nested subqueries are
/// traversed, and references to columns may correspond to different levels
/// of containing outer subqueries.
///
/// A `ColumnRef` allows expressions to refer to columns while being clear
/// about which level the column references without manually performing the
/// bookkeeping tracking their actual column locations.
///
/// Specifically, a `ColumnRef` refers to a column `level` subquery level *out*
/// from the reference, using `column` as a unique identifier in that subquery level.
/// A `level` of zero corresponds to the current scope, and levels increase to
/// indicate subqueries further "outwards".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct ColumnRef {
    // scope level, where 0 is the current scope and 1+ are outer scopes.
    pub level: usize,
    // level-local column identifier used.
    pub column: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinKind {
    Inner { lateral: bool },
    LeftOuter { lateral: bool },
    RightOuter,
    FullOuter,
}

impl JoinKind {
    pub fn is_lateral(&self) -> bool {
        match self {
            JoinKind::Inner { lateral } | JoinKind::LeftOuter { lateral } => *lateral,
            JoinKind::RightOuter | JoinKind::FullOuter => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateExpr {
    pub func: AggregateFunc,
    pub expr: Box<ScalarExpr>,
    pub distinct: bool,
}

impl RelationExpr {
    pub fn typ(
        &self,
        outers: &[RelationType],
        params: &BTreeMap<usize, ScalarType>,
    ) -> RelationType {
        match self {
            RelationExpr::Constant { typ, .. } => typ.clone(),
            RelationExpr::Get { typ, .. } => typ.clone(),
            RelationExpr::Project { input, outputs } => {
                let input_typ = input.typ(outers, params);
                RelationType::new(
                    outputs
                        .iter()
                        .map(|&i| input_typ.column_types[i].clone())
                        .collect(),
                )
            }
            RelationExpr::Map { input, scalars } => {
                let mut typ = input.typ(outers, params);
                for scalar in scalars {
                    typ.column_types.push(scalar.typ(outers, &typ, params));
                }
                typ
            }
            RelationExpr::CallTable { func, exprs: _ } => func.output_type(),
            RelationExpr::Filter { input, .. } | RelationExpr::TopK { input, .. } => {
                input.typ(outers, params)
            }
            RelationExpr::Join {
                left, right, kind, ..
            } => {
                let left_nullable = matches!(kind, JoinKind::RightOuter | JoinKind::FullOuter);
                let right_nullable =
                    matches!(kind, JoinKind::LeftOuter { .. } | JoinKind::FullOuter);
                let lt = left.typ(outers, params).column_types.into_iter().map(|t| {
                    let nullable = t.nullable || left_nullable;
                    t.nullable(nullable)
                });
                let outers = if kind.is_lateral() {
                    let mut outers = outers.to_vec();
                    outers.push(RelationType::new(lt.clone().collect()));
                    Cow::Owned(outers)
                } else {
                    Cow::Borrowed(outers)
                };
                let rt = right
                    .typ(&outers, params)
                    .column_types
                    .into_iter()
                    .map(|t| {
                        let nullable = t.nullable || right_nullable;
                        t.nullable(nullable)
                    });
                RelationType::new(lt.chain(rt).collect())
            }
            RelationExpr::Reduce {
                input,
                group_key,
                aggregates,
            } => {
                let input_typ = input.typ(outers, params);
                let mut column_types = group_key
                    .iter()
                    .map(|&i| input_typ.column_types[i].clone())
                    .collect::<Vec<_>>();
                for agg in aggregates {
                    column_types.push(agg.typ(outers, &input_typ, params));
                }
                // TODO(frank): add primary key information.
                RelationType::new(column_types)
            }
            // TODO(frank): check for removal; add primary key information.
            RelationExpr::Distinct { input }
            | RelationExpr::Negate { input }
            | RelationExpr::Threshold { input } => input.typ(outers, params),
            RelationExpr::Union { base, inputs } => {
                let mut base_cols = base.typ(outers, params).column_types;
                for input in inputs {
                    for (base_col, col) in base_cols
                        .iter_mut()
                        .zip_eq(input.typ(outers, params).column_types)
                    {
                        *base_col = base_col.union(&col).unwrap();
                    }
                }
                RelationType::new(base_cols)
            }
        }
    }

    pub fn arity(&self) -> usize {
        match self {
            RelationExpr::Constant { typ, .. } => typ.column_types.len(),
            RelationExpr::Get { typ, .. } => typ.column_types.len(),
            RelationExpr::Project { outputs, .. } => outputs.len(),
            RelationExpr::Map { input, scalars } => input.arity() + scalars.len(),
            RelationExpr::CallTable { func, .. } => func.output_arity(),
            RelationExpr::Filter { input, .. }
            | RelationExpr::TopK { input, .. }
            | RelationExpr::Distinct { input }
            | RelationExpr::Negate { input }
            | RelationExpr::Threshold { input } => input.arity(),
            RelationExpr::Join { left, right, .. } => left.arity() + right.arity(),
            RelationExpr::Union { base, .. } => base.arity(),
            RelationExpr::Reduce {
                group_key,
                aggregates,
                ..
            } => group_key.len() + aggregates.len(),
        }
    }

    pub fn is_join_identity(&self) -> bool {
        match self {
            RelationExpr::Constant { rows, .. } => rows.len() == 1 && self.arity() == 0,
            _ => false,
        }
    }

    pub fn project(self, outputs: Vec<usize>) -> Self {
        if outputs.iter().copied().eq(0..self.arity()) {
            // The projection is trivial. Suppress it.
            self
        } else {
            RelationExpr::Project {
                input: Box::new(self),
                outputs,
            }
        }
    }

    pub fn map(mut self, scalars: Vec<ScalarExpr>) -> Self {
        if scalars.is_empty() {
            // The map is trivial. Suppress it.
            self
        } else if let RelationExpr::Map {
            scalars: old_scalars,
            input: _,
        } = &mut self
        {
            // Map applied to a map. Fuse the maps.
            old_scalars.extend(scalars);
            self
        } else {
            RelationExpr::Map {
                input: Box::new(self),
                scalars,
            }
        }
    }

    pub fn filter(self, predicates: Vec<ScalarExpr>) -> Self {
        RelationExpr::Filter {
            input: Box::new(self),
            predicates,
        }
    }

    pub fn reduce(self, group_key: Vec<usize>, aggregates: Vec<AggregateExpr>) -> Self {
        RelationExpr::Reduce {
            input: Box::new(self),
            group_key,
            aggregates,
        }
    }

    #[allow(dead_code)]
    pub fn top_k(
        self,
        group_key: Vec<usize>,
        order_key: Vec<ColumnOrder>,
        limit: Option<usize>,
        offset: usize,
    ) -> Self {
        RelationExpr::TopK {
            input: Box::new(self),
            group_key,
            order_key,
            limit,
            offset,
        }
    }

    pub fn negate(self) -> Self {
        RelationExpr::Negate {
            input: Box::new(self),
        }
    }

    pub fn distinct(self) -> Self {
        RelationExpr::Distinct {
            input: Box::new(self),
        }
    }

    pub fn threshold(self) -> Self {
        RelationExpr::Threshold {
            input: Box::new(self),
        }
    }

    pub fn union(self, other: Self) -> Self {
        RelationExpr::Union {
            base: Box::new(self),
            inputs: vec![other],
        }
    }

    pub fn exists(self) -> ScalarExpr {
        ScalarExpr::Exists(Box::new(self))
    }

    pub fn select(self) -> ScalarExpr {
        ScalarExpr::Select(Box::new(self))
    }

    pub fn take(&mut self) -> RelationExpr {
        mem::replace(
            self,
            RelationExpr::Constant {
                rows: vec![],
                typ: RelationType::new(Vec::new()),
            },
        )
    }

    // TODO(benesch): these visit methods are too duplicative. Figure out how
    // to deduplicate.

    pub fn visit<'a, F>(&'a self, f: &mut F)
    where
        F: FnMut(&'a Self),
    {
        self.visit1(|e: &RelationExpr| e.visit(f));
        f(self);
    }

    pub fn visit1<'a, F>(&'a self, mut f: F)
    where
        F: FnMut(&'a Self),
    {
        match self {
            RelationExpr::Constant { .. }
            | RelationExpr::Get { .. }
            | RelationExpr::CallTable { .. } => (),
            RelationExpr::Project { input, .. } => {
                f(input);
            }
            RelationExpr::Map { input, .. } => {
                f(input);
            }
            RelationExpr::Filter { input, .. } => {
                f(input);
            }
            RelationExpr::Join { left, right, .. } => {
                f(left);
                f(right);
            }
            RelationExpr::Reduce { input, .. } => {
                f(input);
            }
            RelationExpr::Distinct { input } => {
                f(input);
            }
            RelationExpr::TopK { input, .. } => {
                f(input);
            }
            RelationExpr::Negate { input } => {
                f(input);
            }
            RelationExpr::Threshold { input } => {
                f(input);
            }
            RelationExpr::Union { base, inputs } => {
                f(base);
                for input in inputs {
                    f(input);
                }
            }
        }
    }

    pub fn visit_mut<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        self.visit1_mut(|e: &mut RelationExpr| e.visit_mut(f));
        f(self);
    }

    pub fn visit1_mut<'a, F>(&'a mut self, mut f: F)
    where
        F: FnMut(&'a mut Self),
    {
        match self {
            RelationExpr::Constant { .. }
            | RelationExpr::Get { .. }
            | RelationExpr::CallTable { .. } => (),
            RelationExpr::Project { input, .. } => {
                f(input);
            }
            RelationExpr::Map { input, .. } => {
                f(input);
            }
            RelationExpr::Filter { input, .. } => {
                f(input);
            }
            RelationExpr::Join { left, right, .. } => {
                f(left);
                f(right);
            }
            RelationExpr::Reduce { input, .. } => {
                f(input);
            }
            RelationExpr::Distinct { input } => {
                f(input);
            }
            RelationExpr::TopK { input, .. } => {
                f(input);
            }
            RelationExpr::Negate { input } => {
                f(input);
            }
            RelationExpr::Threshold { input } => {
                f(input);
            }
            RelationExpr::Union { base, inputs } => {
                f(base);
                for input in inputs {
                    f(input);
                }
            }
        }
    }

    /// Visits the column references in this relation expression.
    ///
    /// The `depth` argument should indicate the subquery nesting depth of the expression,
    /// which will be incremented with each subquery entered and presented to the supplied
    /// function `f`.
    pub fn visit_columns<F>(&mut self, depth: usize, f: &mut F)
    where
        F: FnMut(usize, &mut ColumnRef),
    {
        match self {
            RelationExpr::Join {
                kind,
                on,
                left,
                right,
            } => {
                left.visit_columns(depth, f);
                let depth = if kind.is_lateral() { depth + 1 } else { depth };
                right.visit_columns(depth, f);
                on.visit_columns(depth, f);
            }
            RelationExpr::Map { scalars, input } => {
                for scalar in scalars {
                    scalar.visit_columns(depth, f);
                }
                input.visit_columns(depth, f);
            }
            RelationExpr::CallTable { exprs, .. } => {
                for expr in exprs {
                    expr.visit_columns(depth, f);
                }
            }
            RelationExpr::Filter { predicates, input } => {
                for predicate in predicates {
                    predicate.visit_columns(depth, f);
                }
                input.visit_columns(depth, f);
            }
            RelationExpr::Reduce {
                aggregates, input, ..
            } => {
                for aggregate in aggregates {
                    aggregate.visit_columns(depth, f);
                }
                input.visit_columns(depth, f);
            }
            RelationExpr::Union { base, inputs } => {
                base.visit_columns(depth, f);
                for input in inputs {
                    input.visit_columns(depth, f);
                }
            }
            RelationExpr::Project { input, .. }
            | RelationExpr::Distinct { input }
            | RelationExpr::TopK { input, .. }
            | RelationExpr::Negate { input }
            | RelationExpr::Threshold { input } => {
                input.visit_columns(depth, f);
            }
            RelationExpr::Constant { .. } | RelationExpr::Get { .. } => (),
        }
    }

    /// Replaces any parameter references in the expression with the
    /// corresponding datum from `params`.
    pub fn bind_parameters(&mut self, params: &Params) -> Result<(), anyhow::Error> {
        match self {
            RelationExpr::Join {
                on, left, right, ..
            } => {
                on.bind_parameters(params)?;
                left.bind_parameters(params)?;
                right.bind_parameters(params)
            }
            RelationExpr::Map { scalars, input } => {
                for scalar in scalars {
                    scalar.bind_parameters(params)?;
                }
                input.bind_parameters(params)
            }
            RelationExpr::CallTable { exprs, .. } => {
                for expr in exprs {
                    expr.bind_parameters(params)?;
                }
                Ok(())
            }
            RelationExpr::Filter { predicates, input } => {
                for predicate in predicates {
                    predicate.bind_parameters(params)?;
                }
                input.bind_parameters(params)
            }
            RelationExpr::Reduce {
                aggregates, input, ..
            } => {
                for aggregate in aggregates {
                    aggregate.bind_parameters(params)?;
                }
                input.bind_parameters(params)
            }
            RelationExpr::Union { base, inputs } => {
                for input in inputs {
                    input.bind_parameters(params)?;
                }
                base.bind_parameters(params)
            }
            RelationExpr::Project { input, .. }
            | RelationExpr::Distinct { input, .. }
            | RelationExpr::TopK { input, .. }
            | RelationExpr::Negate { input, .. }
            | RelationExpr::Threshold { input, .. } => input.bind_parameters(params),
            RelationExpr::Constant { .. } | RelationExpr::Get { .. } => Ok(()),
        }
    }

    /// See the documentation for [`ScalarExpr::splice_parameters`].
    pub fn splice_parameters(&mut self, params: &[ScalarExpr], depth: usize) {
        match self {
            RelationExpr::Join {
                kind,
                on,
                left,
                right,
            } => {
                left.splice_parameters(params, depth);
                let depth = if kind.is_lateral() { depth + 1 } else { depth };
                right.splice_parameters(params, depth);
                on.splice_parameters(params, depth);
            }
            RelationExpr::Map { scalars, input } => {
                for scalar in scalars {
                    scalar.splice_parameters(params, depth);
                }
                input.splice_parameters(params, depth);
            }
            RelationExpr::CallTable { exprs, .. } => {
                for expr in exprs {
                    expr.splice_parameters(params, depth);
                }
            }
            RelationExpr::Filter { predicates, input } => {
                for predicate in predicates {
                    predicate.splice_parameters(params, depth);
                }
                input.splice_parameters(params, depth);
            }
            RelationExpr::Reduce {
                aggregates, input, ..
            } => {
                for aggregate in aggregates {
                    aggregate.expr.splice_parameters(params, depth);
                }
                input.splice_parameters(params, depth);
            }
            RelationExpr::Union { base, inputs } => {
                base.splice_parameters(params, depth);
                for input in inputs {
                    input.splice_parameters(params, depth);
                }
            }
            RelationExpr::Project { input, .. }
            | RelationExpr::Distinct { input }
            | RelationExpr::TopK { input, .. }
            | RelationExpr::Negate { input }
            | RelationExpr::Threshold { input } => {
                input.splice_parameters(params, depth);
            }
            RelationExpr::Constant { .. } | RelationExpr::Get { .. } => (),
        }
    }

    /// Constructs a constant collection from specific rows and schema.
    pub fn constant(rows: Vec<Vec<Datum>>, typ: RelationType) -> Self {
        let mut row_packer = repr::RowPacker::new();
        let rows = rows
            .into_iter()
            .map(move |datums| row_packer.pack(datums))
            .collect();
        RelationExpr::Constant { rows, typ }
    }

    pub fn finish(&mut self, finishing: expr::RowSetFinishing) {
        if !finishing.is_trivial(self.arity()) {
            *self = RelationExpr::Project {
                input: Box::new(RelationExpr::TopK {
                    input: Box::new(std::mem::replace(
                        self,
                        RelationExpr::Constant {
                            rows: vec![],
                            typ: RelationType::new(Vec::new()),
                        },
                    )),
                    group_key: vec![],
                    order_key: finishing.order_by,
                    limit: finishing.limit,
                    offset: finishing.offset,
                }),
                outputs: finishing.project,
            }
        }
    }
}

impl ScalarExpr {
    /// Replaces any parameter references in the expression with the
    /// corresponding datum in `params`.
    pub fn bind_parameters(&mut self, params: &Params) -> Result<(), anyhow::Error> {
        match self {
            ScalarExpr::Literal(_, _) | ScalarExpr::Column(_) | ScalarExpr::CallNullary(_) => {
                Ok(())
            }
            ScalarExpr::Parameter(n) => {
                let datum = match params.datums.iter().nth(*n - 1) {
                    None => bail!("there is no parameter ${}", n),
                    Some(datum) => datum,
                };
                let scalar_type = &params.types[*n - 1];
                let row = Row::pack(&[datum]);
                let column_type = scalar_type.clone().nullable(datum.is_null());
                *self = ScalarExpr::Literal(row, column_type);
                Ok(())
            }
            ScalarExpr::CallUnary { expr, .. } => expr.bind_parameters(params),
            ScalarExpr::CallBinary { expr1, expr2, .. } => {
                expr1.bind_parameters(params)?;
                expr2.bind_parameters(params)
            }
            ScalarExpr::CallVariadic { exprs, .. } => {
                for expr in exprs {
                    expr.bind_parameters(params)?;
                }
                Ok(())
            }
            ScalarExpr::If { cond, then, els } => {
                cond.bind_parameters(params)?;
                then.bind_parameters(params)?;
                els.bind_parameters(params)
            }
            ScalarExpr::Exists(expr) | ScalarExpr::Select(expr) => expr.bind_parameters(params),
        }
    }

    // Like [`ScalarExpr::bind_parameters`]`, except that parameters are
    // replaced with the corresponding expression fragment from `params` rather
    // than a datum.
    ///
    /// Specifically, the parameter `$1` will be replaced with `params[0]`, the
    /// parameter `$2` will be replaced with `params[1]`, and so on. Parameters
    /// in `self` that refer to invalid indices of `params` will cause a panic.
    ///
    /// Column references in parameters will be corrected to account for the
    /// depth at which they are spliced.
    pub fn splice_parameters(&mut self, params: &[ScalarExpr], depth: usize) {
        self.visit_mut(&mut |e| match e {
            ScalarExpr::Parameter(i) => {
                *e = params[*i - 1].clone();
                // Correct any column references in the parameter expression for
                // its new depth.
                e.visit_columns(0, &mut |_, col| col.level += depth);
            }
            ScalarExpr::Exists(e) | ScalarExpr::Select(e) => e.splice_parameters(params, depth + 1),
            _ => (),
        })
    }

    pub fn literal(datum: Datum, scalar_type: ScalarType) -> ScalarExpr {
        let row = Row::pack(&[datum]);
        ScalarExpr::Literal(row, scalar_type.nullable(datum.is_null()))
    }

    pub fn literal_true() -> ScalarExpr {
        ScalarExpr::literal(Datum::True, ScalarType::Bool)
    }

    pub fn literal_null(scalar_type: ScalarType) -> ScalarExpr {
        ScalarExpr::literal(Datum::Null, scalar_type)
    }

    pub fn literal_1d_array(
        datums: Vec<Datum>,
        element_scalar_type: ScalarType,
    ) -> Result<ScalarExpr, anyhow::Error> {
        let scalar_type = match element_scalar_type {
            ScalarType::Array(_) => {
                return Err(anyhow::anyhow!("cannot build array from array type"))
            }
            typ => ScalarType::Array(Box::new(typ)).nullable(false),
        };

        let mut packer = RowPacker::new();
        packer.push_array(
            &[ArrayDimension {
                lower_bound: 1,
                length: datums.len(),
            }],
            datums,
        )?;
        let row = packer.finish();

        Ok(ScalarExpr::Literal(row, scalar_type))
    }

    pub fn call_unary(self, func: UnaryFunc) -> Self {
        ScalarExpr::CallUnary {
            func,
            expr: Box::new(self),
        }
    }

    pub fn call_binary(self, other: Self, func: BinaryFunc) -> Self {
        ScalarExpr::CallBinary {
            func,
            expr1: Box::new(self),
            expr2: Box::new(other),
        }
    }

    pub fn take(&mut self) -> Self {
        mem::replace(self, ScalarExpr::literal_null(ScalarType::String))
    }

    pub fn visit<'a, F>(&'a self, f: &mut F)
    where
        F: FnMut(&'a Self),
    {
        self.visit1(|e: &ScalarExpr| e.visit(f));
        f(self);
    }

    pub fn visit1<'a, F>(&'a self, mut f: F)
    where
        F: FnMut(&'a Self),
    {
        use ScalarExpr::*;
        match self {
            Column(..) | Parameter(..) | Literal(..) | CallNullary(..) => (),
            CallUnary { expr, .. } => f(expr),
            CallBinary { expr1, expr2, .. } => {
                f(expr1);
                f(expr2);
            }
            CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr);
                }
            }
            If { cond, then, els } => {
                f(cond);
                f(then);
                f(els);
            }
            Exists(..) | Select(..) => (),
        }
    }

    pub fn visit_mut<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        self.visit1_mut(|e: &mut ScalarExpr| e.visit_mut(f));
        f(self);
    }

    pub fn visit_mut_pre<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        f(self);
        self.visit1_mut(|e: &mut ScalarExpr| e.visit_mut(f));
    }

    pub fn visit1_mut<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut Self),
    {
        use ScalarExpr::*;
        match self {
            Column(..) | Parameter(..) | Literal(..) | CallNullary(..) => (),
            CallUnary { expr, .. } => f(expr),
            CallBinary { expr1, expr2, .. } => {
                f(expr1);
                f(expr2);
            }
            CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr);
                }
            }
            If { cond, then, els } => {
                f(cond);
                f(then);
                f(els);
            }
            Exists(..) | Select(..) => (),
        }
    }

    /// Visits the column references in this scalar expression.
    ///
    /// The `depth` argument should indicate the subquery nesting depth of the expression,
    /// which will be incremented with each subquery entered and presented to the supplied
    /// function `f`.
    pub fn visit_columns<F>(&mut self, depth: usize, f: &mut F)
    where
        F: FnMut(usize, &mut ColumnRef),
    {
        match self {
            ScalarExpr::Literal(_, _) | ScalarExpr::Parameter(_) | ScalarExpr::CallNullary(_) => (),
            ScalarExpr::Column(col_ref) => f(depth, col_ref),
            ScalarExpr::CallUnary { expr, .. } => expr.visit_columns(depth, f),
            ScalarExpr::CallBinary { expr1, expr2, .. } => {
                expr1.visit_columns(depth, f);
                expr2.visit_columns(depth, f);
            }
            ScalarExpr::CallVariadic { exprs, .. } => {
                for expr in exprs {
                    expr.visit_columns(depth, f);
                }
            }
            ScalarExpr::If { cond, then, els } => {
                cond.visit_columns(depth, f);
                then.visit_columns(depth, f);
                els.visit_columns(depth, f);
            }
            ScalarExpr::Exists(expr) | ScalarExpr::Select(expr) => {
                expr.visit_columns(depth + 1, f);
            }
        }
    }

    fn simplify_to_literal(self) -> Option<Row> {
        let mut expr = self.lower_uncorrelated().ok()?;
        expr.reduce(&repr::RelationType::empty());
        match expr {
            expr::ScalarExpr::Literal(Ok(row), _) => Some(row),
            _ => None,
        }
    }

    /// Attempts to simplify this expression to a literal 64-bit integer.
    ///
    /// Returns `None` if this expression cannot be simplified, e.g. because it
    /// contains non-literal values.
    ///
    /// # Panics
    ///
    /// Panics if this expression does not have type [`ScalarType::Int64`].
    pub fn into_literal_int64(self) -> Option<i64> {
        self.simplify_to_literal().and_then(|row| {
            let datum = row.unpack_first();
            if datum.is_null() {
                None
            } else {
                Some(datum.unwrap_int64())
            }
        })
    }

    /// Attempts to simplify this expression to a literal string.
    ///
    /// Returns `None` if this expression cannot be simplified, e.g. because it
    /// contains non-literal values.
    ///
    /// # Panics
    ///
    /// Panics if this expression does not have type [`ScalarType::String`].
    pub fn into_literal_string(self) -> Option<String> {
        self.simplify_to_literal().and_then(|row| {
            let datum = row.unpack_first();
            if datum.is_null() {
                None
            } else {
                Some(datum.unwrap_str().to_owned())
            }
        })
    }
}

impl AbstractExpr for ScalarExpr {
    type Type = ColumnType;

    fn typ(
        &self,
        outers: &[RelationType],
        inner: &RelationType,
        params: &BTreeMap<usize, ScalarType>,
    ) -> Self::Type {
        match self {
            ScalarExpr::Column(ColumnRef { level, column }) => {
                if *level == 0 {
                    inner.column_types[*column].clone()
                } else {
                    outers[outers.len() - *level].column_types[*column].clone()
                }
            }
            ScalarExpr::Parameter(n) => params[&n].clone().nullable(true),
            ScalarExpr::Literal(_, typ) => typ.clone(),
            ScalarExpr::CallNullary(func) => func.output_type(),
            ScalarExpr::CallUnary { expr, func } => {
                func.output_type(expr.typ(outers, inner, params))
            }
            ScalarExpr::CallBinary { expr1, expr2, func } => func.output_type(
                expr1.typ(outers, inner, params),
                expr2.typ(outers, inner, params),
            ),
            ScalarExpr::CallVariadic { exprs, func } => {
                func.output_type(exprs.iter().map(|e| e.typ(outers, inner, params)).collect())
            }
            ScalarExpr::If { cond: _, then, els } => {
                let then_type = then.typ(outers, inner, params);
                let else_type = els.typ(outers, inner, params);
                then_type.union(&else_type).unwrap()
            }
            ScalarExpr::Exists(_) => ScalarType::Bool.nullable(true),
            ScalarExpr::Select(expr) => {
                let mut outers = outers.to_vec();
                outers.push(inner.clone());
                expr.typ(&outers, params)
                    .column_types
                    .into_element()
                    .nullable(true)
            }
        }
    }
}

impl AggregateExpr {
    /// Replaces any parameter references in the expression with the
    /// corresponding datum from `parameters`.
    pub fn bind_parameters(&mut self, params: &Params) -> Result<(), anyhow::Error> {
        self.expr.bind_parameters(params)
    }

    pub fn typ(
        &self,
        outers: &[RelationType],
        inner: &RelationType,
        params: &BTreeMap<usize, ScalarType>,
    ) -> ColumnType {
        self.func.output_type(self.expr.typ(outers, inner, params))
    }

    /// Visits the column references in this aggregate expression.
    ///
    /// The `depth` argument should indicate the subquery nesting depth of the expression,
    /// which will be incremented with each subquery entered and presented to the supplied
    /// function `f`.
    pub fn visit_columns<F>(&mut self, depth: usize, f: &mut F)
    where
        F: FnMut(usize, &mut ColumnRef),
    {
        self.expr.visit_columns(depth, f);
    }
}
