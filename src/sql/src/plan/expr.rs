// Copyright Materialize, Inc. and contributors. All rights reserved.
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
//! representation via a call to lower().

use std::collections::BTreeMap;
use std::fmt;
use std::mem;

use itertools::Itertools;
use mz_expr::visit::Visit;
use mz_expr::visit::VisitChildren;
use mz_ore::stack::RecursionLimitError;
use serde::{Deserialize, Serialize};

use mz_ore::collections::CollectionExt;
use mz_ore::stack;
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::numeric::NumericMaxScale;
use mz_repr::explain_new::DummyHumanizer;
use mz_repr::*;

use crate::plan::error::PlanError;
use crate::plan::query::ExprContext;
use crate::plan::typeconv::{self, CastContext};
use crate::plan::Params;

// these happen to be unchanged at the moment, but there might be additions later
pub use mz_expr::{
    BinaryFunc, ColumnOrder, TableFunc, UnaryFunc, UnmaterializableFunc, VariadicFunc, WindowFrame,
    WindowFrameBound, WindowFrameUnits,
};

use super::Explanation;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Just like MirRelationExpr, except where otherwise noted below.
pub enum HirRelationExpr {
    Constant {
        rows: Vec<Row>,
        typ: RelationType,
    },
    Get {
        id: mz_expr::Id,
        typ: RelationType,
    },
    /// CTE
    Let {
        name: String,
        /// The identifier to be used in `Get` variants to retrieve `value`.
        id: mz_expr::LocalId,
        /// The collection to be bound to `name`.
        value: Box<HirRelationExpr>,
        /// The result of the `Let`, evaluated with `name` bound to `value`.
        body: Box<HirRelationExpr>,
    },
    Project {
        input: Box<HirRelationExpr>,
        outputs: Vec<usize>,
    },
    Map {
        input: Box<HirRelationExpr>,
        scalars: Vec<HirScalarExpr>,
    },
    CallTable {
        func: TableFunc,
        exprs: Vec<HirScalarExpr>,
    },
    Filter {
        input: Box<HirRelationExpr>,
        predicates: Vec<HirScalarExpr>,
    },
    /// Unlike MirRelationExpr, we haven't yet compiled LeftOuter/RightOuter/FullOuter
    /// joins away into more primitive exprs
    Join {
        left: Box<HirRelationExpr>,
        right: Box<HirRelationExpr>,
        on: HirScalarExpr,
        kind: JoinKind,
    },
    /// Unlike MirRelationExpr, when `key` is empty AND `input` is empty this returns
    /// a single row with the aggregates evaluated over empty groups, rather than returning zero
    /// rows
    Reduce {
        input: Box<HirRelationExpr>,
        group_key: Vec<usize>,
        aggregates: Vec<AggregateExpr>,
        expected_group_size: Option<usize>,
    },
    Distinct {
        input: Box<HirRelationExpr>,
    },
    /// Groups and orders within each group, limiting output.
    TopK {
        /// The source collection.
        input: Box<HirRelationExpr>,
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
        input: Box<HirRelationExpr>,
    },
    /// Keep rows from a dataflow where the row counts are positive.
    Threshold {
        input: Box<HirRelationExpr>,
    },
    Union {
        base: Box<HirRelationExpr>,
        inputs: Vec<HirRelationExpr>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Just like mz_expr::MirScalarExpr, except where otherwise noted below.
pub enum HirScalarExpr {
    /// Unlike mz_expr::MirScalarExpr, we can nest HirRelationExprs via eg Exists. This means that a
    /// variable could refer to a column of the current input, or to a column of an outer relation.
    /// We use ColumnRef to denote the difference.
    Column(ColumnRef),
    Parameter(usize),
    Literal(Row, ColumnType),
    CallUnmaterializable(UnmaterializableFunc),
    CallUnary {
        func: UnaryFunc,
        expr: Box<HirScalarExpr>,
    },
    CallBinary {
        func: BinaryFunc,
        expr1: Box<HirScalarExpr>,
        expr2: Box<HirScalarExpr>,
    },
    CallVariadic {
        func: VariadicFunc,
        exprs: Vec<HirScalarExpr>,
    },
    If {
        cond: Box<HirScalarExpr>,
        then: Box<HirScalarExpr>,
        els: Box<HirScalarExpr>,
    },
    /// Returns true if `expr` returns any rows
    Exists(Box<HirRelationExpr>),
    /// Given `expr` with arity 1. If expr returns:
    /// * 0 rows, return NULL
    /// * 1 row, return the value of that row
    /// * >1 rows, the sql spec says we should throw an error but we can't
    ///   (see <https://github.com/MaterializeInc/materialize/issues/489>)
    ///   so instead we return all the rows.
    ///   If there are multiple `Select` expressions in a single SQL query, the result is that we take the product of all of them.
    ///   This is counter to the spec, but is consistent with eg postgres' treatment of multiple set-returning-functions
    ///   (see <https://tapoueh.org/blog/2017/10/set-returning-functions-and-postgresql-10/>).
    Select(Box<HirRelationExpr>),
    Windowing(WindowExpr),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Represents the invocation of a window function over a partition with an optional
/// order.
pub struct WindowExpr {
    pub func: WindowExprType,
    pub partition: Vec<HirScalarExpr>,
    pub order_by: Vec<HirScalarExpr>,
}

impl WindowExpr {
    pub fn visit_expressions<'a, F, E>(&'a self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&'a HirScalarExpr) -> Result<(), E>,
    {
        self.func.visit_expressions(f)?;
        for expr in self.partition.iter() {
            f(expr)?;
        }
        for expr in self.order_by.iter() {
            f(expr)?;
        }
        Ok(())
    }

    pub fn visit_expressions_mut<'a, F, E>(&'a mut self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&'a mut HirScalarExpr) -> Result<(), E>,
    {
        self.func.visit_expressions_mut(f)?;
        for expr in self.partition.iter_mut() {
            f(expr)?;
        }
        for expr in self.order_by.iter_mut() {
            f(expr)?;
        }
        Ok(())
    }
}

impl VisitChildren<HirScalarExpr> for WindowExpr {
    fn visit_children<F>(&self, mut f: F)
    where
        F: FnMut(&HirScalarExpr),
    {
        self.func.visit_children(&mut f);
        for expr in self.partition.iter() {
            f(expr);
        }
        for expr in self.order_by.iter() {
            f(expr);
        }
    }

    fn visit_mut_children<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut HirScalarExpr),
    {
        self.func.visit_mut_children(&mut f);
        for expr in self.partition.iter_mut() {
            f(expr);
        }
        for expr in self.order_by.iter_mut() {
            f(expr);
        }
    }

    fn try_visit_children<F, E>(&self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&HirScalarExpr) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        self.func.try_visit_children(&mut f)?;
        for expr in self.partition.iter() {
            f(expr)?;
        }
        for expr in self.order_by.iter() {
            f(expr)?;
        }
        Ok(())
    }

    fn try_visit_mut_children<F, E>(&mut self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&mut HirScalarExpr) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        self.func.try_visit_mut_children(&mut f)?;
        for expr in self.partition.iter_mut() {
            f(expr)?;
        }
        for expr in self.order_by.iter_mut() {
            f(expr)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// A window function with its parameters.
///
/// There are three types of window functions:
/// - scalar window functions, that return a different scalar value for each
/// row within a partition that depends exclusively on the position of the row
/// within the partition;
/// - value window functions, that return a scalar value for each row within a
/// partition that might be computed based on a single previous, current or
/// following row;
/// - aggregate window functions, that return a computed value for the row that
/// depends on multiple other rows within the same partition. Aggregate window
/// functions can be in some cases be computed by joining the input relation
/// with a reduction over the same relation that computes the aggregation using
/// the partition key as its grouping key.
pub enum WindowExprType {
    Scalar(ScalarWindowExpr),
    Value(ValueWindowExpr),
}

impl WindowExprType {
    pub fn visit_expressions<'a, F, E>(&'a self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&'a HirScalarExpr) -> Result<(), E>,
    {
        match self {
            Self::Scalar(expr) => expr.visit_expressions(f),
            Self::Value(expr) => expr.visit_expressions(f),
        }
    }

    pub fn visit_expressions_mut<'a, F, E>(&'a mut self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&'a mut HirScalarExpr) -> Result<(), E>,
    {
        match self {
            Self::Scalar(expr) => expr.visit_expressions_mut(f),
            Self::Value(expr) => expr.visit_expressions_mut(f),
        }
    }

    fn typ(
        &self,
        outers: &[RelationType],
        inner: &RelationType,
        params: &BTreeMap<usize, ScalarType>,
    ) -> ColumnType {
        match self {
            Self::Scalar(expr) => expr.typ(outers, inner, params),
            Self::Value(expr) => expr.typ(outers, inner, params),
        }
    }
}

impl VisitChildren<HirScalarExpr> for WindowExprType {
    fn visit_children<F>(&self, f: F)
    where
        F: FnMut(&HirScalarExpr),
    {
        match self {
            Self::Scalar(_) => (),
            Self::Value(expr) => expr.visit_children(f),
        }
    }

    fn visit_mut_children<F>(&mut self, f: F)
    where
        F: FnMut(&mut HirScalarExpr),
    {
        match self {
            Self::Scalar(_) => (),
            Self::Value(expr) => expr.visit_mut_children(f),
        }
    }

    fn try_visit_children<F, E>(&self, f: F) -> Result<(), E>
    where
        F: FnMut(&HirScalarExpr) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        match self {
            Self::Scalar(_) => Ok(()),
            Self::Value(expr) => expr.try_visit_children(f),
        }
    }

    fn try_visit_mut_children<F, E>(&mut self, f: F) -> Result<(), E>
    where
        F: FnMut(&mut HirScalarExpr) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        match self {
            Self::Scalar(_) => Ok(()),
            Self::Value(expr) => expr.try_visit_mut_children(f),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ScalarWindowExpr {
    pub func: ScalarWindowFunc,
    pub order_by: Vec<ColumnOrder>,
}

impl ScalarWindowExpr {
    pub fn visit_expressions<'a, F, E>(&'a self, _f: &mut F) -> Result<(), E>
    where
        F: FnMut(&'a HirScalarExpr) -> Result<(), E>,
    {
        match self.func {
            ScalarWindowFunc::RowNumber => {}
            ScalarWindowFunc::DenseRank => {}
        }
        Ok(())
    }

    pub fn visit_expressions_mut<'a, F, E>(&'a mut self, _f: &mut F) -> Result<(), E>
    where
        F: FnMut(&'a mut HirScalarExpr) -> Result<(), E>,
    {
        match self.func {
            ScalarWindowFunc::RowNumber => {}
            ScalarWindowFunc::DenseRank => {}
        }
        Ok(())
    }

    fn typ(
        &self,
        _outers: &[RelationType],
        _inner: &RelationType,
        _params: &BTreeMap<usize, ScalarType>,
    ) -> ColumnType {
        self.func.output_type()
    }

    pub fn into_expr(self) -> mz_expr::AggregateFunc {
        match self.func {
            ScalarWindowFunc::RowNumber => mz_expr::AggregateFunc::RowNumber {
                order_by: self.order_by,
            },
            ScalarWindowFunc::DenseRank => mz_expr::AggregateFunc::DenseRank {
                order_by: self.order_by,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Scalar Window functions
pub enum ScalarWindowFunc {
    RowNumber,
    DenseRank,
}

impl ScalarWindowFunc {
    pub fn output_type(&self) -> ColumnType {
        match self {
            ScalarWindowFunc::RowNumber => ScalarType::Int64.nullable(false),
            ScalarWindowFunc::DenseRank => ScalarType::Int64.nullable(false),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ValueWindowExpr {
    pub func: ValueWindowFunc,
    pub expr: Box<HirScalarExpr>,
    pub order_by: Vec<ColumnOrder>,
    pub window_frame: WindowFrame,
}

impl ValueWindowExpr {
    pub fn visit_expressions<'a, F, E>(&'a self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&'a HirScalarExpr) -> Result<(), E>,
    {
        f(&self.expr)
    }

    pub fn visit_expressions_mut<'a, F, E>(&'a mut self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&'a mut HirScalarExpr) -> Result<(), E>,
    {
        f(&mut self.expr)
    }

    fn typ(
        &self,
        outers: &[RelationType],
        inner: &RelationType,
        params: &BTreeMap<usize, ScalarType>,
    ) -> ColumnType {
        self.func.output_type(self.expr.typ(outers, inner, params))
    }

    pub fn into_expr(self) -> mz_expr::AggregateFunc {
        match self.func {
            // Lag and Lead are fundamentally the same function, just with opposite directions
            ValueWindowFunc::Lag => mz_expr::AggregateFunc::LagLead {
                order_by: self.order_by,
                lag_lead: mz_expr::LagLeadType::Lag,
            },
            ValueWindowFunc::Lead => mz_expr::AggregateFunc::LagLead {
                order_by: self.order_by,
                lag_lead: mz_expr::LagLeadType::Lead,
            },
            ValueWindowFunc::FirstValue => mz_expr::AggregateFunc::FirstValue {
                order_by: self.order_by,
                window_frame: self.window_frame,
            },
            ValueWindowFunc::LastValue => mz_expr::AggregateFunc::LastValue {
                order_by: self.order_by,
                window_frame: self.window_frame,
            },
        }
    }
}

impl VisitChildren<HirScalarExpr> for ValueWindowExpr {
    fn visit_children<F>(&self, mut f: F)
    where
        F: FnMut(&HirScalarExpr),
    {
        f(&self.expr)
    }

    fn visit_mut_children<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut HirScalarExpr),
    {
        f(&mut self.expr)
    }

    fn try_visit_children<F, E>(&self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&HirScalarExpr) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        f(&self.expr)
    }

    fn try_visit_mut_children<F, E>(&mut self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&mut HirScalarExpr) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        f(&mut self.expr)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Value Window functions
pub enum ValueWindowFunc {
    Lag,
    Lead,
    FirstValue,
    LastValue,
}

impl ValueWindowFunc {
    pub fn output_type(&self, input_type: ColumnType) -> ColumnType {
        match self {
            ValueWindowFunc::Lag | ValueWindowFunc::Lead => {
                // The input is a (value, offset, default) record, so extract the type of the first arg
                input_type.scalar_type.unwrap_record_element_type()[0]
                    .clone()
                    .nullable(true)
            }
            ValueWindowFunc::FirstValue | ValueWindowFunc::LastValue => {
                input_type.scalar_type.nullable(true)
            }
        }
    }
}

/// A `CoercibleScalarExpr` is a [`HirScalarExpr`] whose type is not fully
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
    Coerced(HirScalarExpr),
    Parameter(usize),
    LiteralNull,
    LiteralString(String),
    LiteralRecord(Vec<CoercibleScalarExpr>),
}

impl CoercibleScalarExpr {
    pub fn type_as(self, ecx: &ExprContext, ty: &ScalarType) -> Result<HirScalarExpr, PlanError> {
        let expr = typeconv::plan_coerce(ecx, self, ty)?;
        let expr_ty = ecx.scalar_type(&expr);
        if ty != &expr_ty {
            sql_bail!(
                "{} must have type {}, not type {}",
                ecx.name,
                ecx.humanize_scalar_type(ty),
                ecx.humanize_scalar_type(&expr_ty),
            );
        }
        Ok(expr)
    }

    pub fn type_as_any(self, ecx: &ExprContext) -> Result<HirScalarExpr, PlanError> {
        typeconv::plan_coerce(ecx, self, &ScalarType::String)
    }

    pub fn cast_to(
        self,
        ecx: &ExprContext,
        ccx: CastContext,
        ty: &ScalarType,
    ) -> Result<HirScalarExpr, PlanError> {
        let expr = typeconv::plan_coerce(ecx, self, ty)?;
        typeconv::plan_cast(ecx, ccx, expr, ty)
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
            CoercibleScalarExpr::LiteralRecord(scalars) => {
                let mut fields = vec![];
                for (i, scalar) in scalars.iter().enumerate() {
                    fields.push((
                        format!("f{}", i + 1).into(),
                        scalar.typ(outers, inner, params)?,
                    ));
                }
                Some(ColumnType {
                    scalar_type: ScalarType::Record {
                        fields,
                        custom_id: None,
                    },
                    nullable: false,
                })
            }
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

impl From<HirScalarExpr> for CoercibleScalarExpr {
    fn from(expr: HirScalarExpr) -> CoercibleScalarExpr {
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct ColumnRef {
    // scope level, where 0 is the current scope and 1+ are outer scopes.
    pub level: usize,
    // level-local column identifier used.
    pub column: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinKind {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

impl fmt::Display for JoinKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                JoinKind::Inner => "Inner",
                JoinKind::LeftOuter => "LeftOuter",
                JoinKind::RightOuter => "RightOuter",
                JoinKind::FullOuter => "FullOuter",
            }
        )
    }
}

impl JoinKind {
    pub fn can_be_correlated(&self) -> bool {
        match self {
            JoinKind::Inner | JoinKind::LeftOuter => true,
            JoinKind::RightOuter | JoinKind::FullOuter => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AggregateExpr {
    pub func: AggregateFunc,
    pub expr: Box<HirScalarExpr>,
    pub distinct: bool,
}

/// Aggregate functions analogous to `mz_expr::AggregateFunc`, but whose
/// types may be different.
///
/// Specifically, the nullability of the aggregate columns is more common
/// here than in `expr`, as these aggregates may be applied over empty
/// result sets and should be null in those cases, whereas `expr` variants
/// only return null values when supplied nulls as input.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum AggregateFunc {
    MaxNumeric,
    MaxInt16,
    MaxInt32,
    MaxInt64,
    MaxFloat32,
    MaxFloat64,
    MaxBool,
    MaxString,
    MaxDate,
    MaxTimestamp,
    MaxTimestampTz,
    MinNumeric,
    MinInt16,
    MinInt32,
    MinInt64,
    MinFloat32,
    MinFloat64,
    MinBool,
    MinString,
    MinDate,
    MinTimestamp,
    MinTimestampTz,
    SumInt16,
    SumInt32,
    SumInt64,
    SumFloat32,
    SumFloat64,
    SumNumeric,
    Count,
    Any,
    All,
    /// Accumulates `Datum::List`s whose first element is a JSON-typed `Datum`s
    /// into a JSON list. The other elements are columns used by `order_by`.
    ///
    /// WARNING: Unlike the `jsonb_agg` function that is exposed by the SQL
    /// layer, this function filters out `Datum::Null`, for consistency with
    /// the other aggregate functions.
    JsonbAgg {
        order_by: Vec<ColumnOrder>,
    },
    /// Zips `Datum::List`s whose first element is a JSON-typed `Datum`s into a
    /// JSON map. The other elements are columns used by `order_by`.
    JsonbObjectAgg {
        order_by: Vec<ColumnOrder>,
    },
    /// Accumulates `Datum::List`s whose first element is a `Datum::Array` into a
    /// single `Datum::Array`. The other elements are columns used by `order_by`.
    ArrayConcat {
        order_by: Vec<ColumnOrder>,
    },
    /// Accumulates `Datum::List`s whose first element is a `Datum::List` into a
    /// single `Datum::List`. The other elements are columns used by `order_by`.
    ListConcat {
        order_by: Vec<ColumnOrder>,
    },
    StringAgg {
        order_by: Vec<ColumnOrder>,
    },
    /// Accumulates any number of `Datum::Dummy`s into `Datum::Dummy`.
    ///
    /// Useful for removing an expensive aggregation while maintaining the shape
    /// of a reduce operator.
    Dummy,
}

impl AggregateFunc {
    /// Converts the `sql::AggregateFunc` to a corresponding `mz_expr::AggregateFunc`.
    pub fn into_expr(self) -> mz_expr::AggregateFunc {
        match self {
            AggregateFunc::MaxNumeric => mz_expr::AggregateFunc::MaxNumeric,
            AggregateFunc::MaxInt64 => mz_expr::AggregateFunc::MaxInt64,
            AggregateFunc::MaxInt16 => mz_expr::AggregateFunc::MaxInt16,
            AggregateFunc::MaxInt32 => mz_expr::AggregateFunc::MaxInt32,
            AggregateFunc::MaxFloat32 => mz_expr::AggregateFunc::MaxFloat32,
            AggregateFunc::MaxFloat64 => mz_expr::AggregateFunc::MaxFloat64,
            AggregateFunc::MaxBool => mz_expr::AggregateFunc::MaxBool,
            AggregateFunc::MaxString => mz_expr::AggregateFunc::MaxString,
            AggregateFunc::MaxDate => mz_expr::AggregateFunc::MaxDate,
            AggregateFunc::MaxTimestamp => mz_expr::AggregateFunc::MaxTimestamp,
            AggregateFunc::MaxTimestampTz => mz_expr::AggregateFunc::MaxTimestampTz,
            AggregateFunc::MinNumeric => mz_expr::AggregateFunc::MinNumeric,
            AggregateFunc::MinInt16 => mz_expr::AggregateFunc::MinInt16,
            AggregateFunc::MinInt32 => mz_expr::AggregateFunc::MinInt32,
            AggregateFunc::MinInt64 => mz_expr::AggregateFunc::MinInt64,
            AggregateFunc::MinFloat32 => mz_expr::AggregateFunc::MinFloat32,
            AggregateFunc::MinFloat64 => mz_expr::AggregateFunc::MinFloat64,
            AggregateFunc::MinBool => mz_expr::AggregateFunc::MinBool,
            AggregateFunc::MinString => mz_expr::AggregateFunc::MinString,
            AggregateFunc::MinDate => mz_expr::AggregateFunc::MinDate,
            AggregateFunc::MinTimestamp => mz_expr::AggregateFunc::MinTimestamp,
            AggregateFunc::MinTimestampTz => mz_expr::AggregateFunc::MinTimestampTz,
            AggregateFunc::SumInt16 => mz_expr::AggregateFunc::SumInt16,
            AggregateFunc::SumInt32 => mz_expr::AggregateFunc::SumInt32,
            AggregateFunc::SumInt64 => mz_expr::AggregateFunc::SumInt64,
            AggregateFunc::SumFloat32 => mz_expr::AggregateFunc::SumFloat32,
            AggregateFunc::SumFloat64 => mz_expr::AggregateFunc::SumFloat64,
            AggregateFunc::SumNumeric => mz_expr::AggregateFunc::SumNumeric,
            AggregateFunc::Count => mz_expr::AggregateFunc::Count,
            AggregateFunc::Any => mz_expr::AggregateFunc::Any,
            AggregateFunc::All => mz_expr::AggregateFunc::All,
            AggregateFunc::JsonbAgg { order_by } => mz_expr::AggregateFunc::JsonbAgg { order_by },
            AggregateFunc::JsonbObjectAgg { order_by } => {
                mz_expr::AggregateFunc::JsonbObjectAgg { order_by }
            }
            AggregateFunc::ArrayConcat { order_by } => {
                mz_expr::AggregateFunc::ArrayConcat { order_by }
            }
            AggregateFunc::ListConcat { order_by } => {
                mz_expr::AggregateFunc::ListConcat { order_by }
            }
            AggregateFunc::StringAgg { order_by } => mz_expr::AggregateFunc::StringAgg { order_by },
            AggregateFunc::Dummy => mz_expr::AggregateFunc::Dummy,
        }
    }

    /// Returns a datum whose inclusion in the aggregation will not change its
    /// result.
    pub fn identity_datum(&self) -> Datum<'static> {
        match self {
            AggregateFunc::Any => Datum::False,
            AggregateFunc::All => Datum::True,
            AggregateFunc::Dummy => Datum::Dummy,
            AggregateFunc::ArrayConcat { .. } => Datum::empty_array(),
            AggregateFunc::ListConcat { .. } => Datum::empty_list(),
            _ => Datum::Null,
        }
    }

    /// The output column type for the result of an aggregation.
    ///
    /// The output column type also contains nullability information, which
    /// is (without further information) true for aggregations that are not
    /// counts.
    pub fn output_type(&self, input_type: ColumnType) -> ColumnType {
        let scalar_type = match self {
            AggregateFunc::Count => ScalarType::Int64,
            AggregateFunc::Any => ScalarType::Bool,
            AggregateFunc::All => ScalarType::Bool,
            AggregateFunc::JsonbAgg { .. } => ScalarType::Jsonb,
            AggregateFunc::JsonbObjectAgg { .. } => ScalarType::Jsonb,
            AggregateFunc::StringAgg { .. } => ScalarType::String,
            AggregateFunc::SumInt16 | AggregateFunc::SumInt32 => ScalarType::Int64,
            AggregateFunc::SumInt64 => ScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            },
            AggregateFunc::ArrayConcat { .. } | AggregateFunc::ListConcat { .. } => {
                match input_type.scalar_type {
                    // The input is wrapped in a Record if there's an ORDER BY, so extract it out.
                    ScalarType::Record { fields, .. } => fields[0].1.scalar_type.clone(),
                    _ => unreachable!(),
                }
            }
            _ => input_type.scalar_type,
        };
        // max/min/sum return null on empty sets
        let nullable = !matches!(self, AggregateFunc::Count);
        scalar_type.nullable(nullable)
    }

    pub fn is_order_sensitive(&self) -> bool {
        use AggregateFunc::*;
        matches!(
            self,
            JsonbAgg { .. }
                | JsonbObjectAgg { .. }
                | ArrayConcat { .. }
                | ListConcat { .. }
                | StringAgg { .. }
        )
    }
}

impl HirRelationExpr {
    pub fn typ(
        &self,
        outers: &[RelationType],
        params: &BTreeMap<usize, ScalarType>,
    ) -> RelationType {
        stack::maybe_grow(|| match self {
            HirRelationExpr::Constant { typ, .. } => typ.clone(),
            HirRelationExpr::Get { typ, .. } => typ.clone(),
            HirRelationExpr::Let { body, .. } => body.typ(outers, params),
            HirRelationExpr::Project { input, outputs } => {
                let input_typ = input.typ(outers, params);
                RelationType::new(
                    outputs
                        .iter()
                        .map(|&i| input_typ.column_types[i].clone())
                        .collect(),
                )
            }
            HirRelationExpr::Map { input, scalars } => {
                let mut typ = input.typ(outers, params);
                for scalar in scalars {
                    typ.column_types.push(scalar.typ(outers, &typ, params));
                }
                typ
            }
            HirRelationExpr::CallTable { func, exprs: _ } => func.output_type(),
            HirRelationExpr::Filter { input, .. } | HirRelationExpr::TopK { input, .. } => {
                input.typ(outers, params)
            }
            HirRelationExpr::Join {
                left, right, kind, ..
            } => {
                let left_nullable = matches!(kind, JoinKind::RightOuter | JoinKind::FullOuter);
                let right_nullable =
                    matches!(kind, JoinKind::LeftOuter { .. } | JoinKind::FullOuter);
                let lt = left.typ(outers, params).column_types.into_iter().map(|t| {
                    let nullable = t.nullable || left_nullable;
                    t.nullable(nullable)
                });
                let mut outers = outers.to_vec();
                outers.insert(0, RelationType::new(lt.clone().collect()));
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
            HirRelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                expected_group_size: _,
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
            HirRelationExpr::Distinct { input }
            | HirRelationExpr::Negate { input }
            | HirRelationExpr::Threshold { input } => input.typ(outers, params),
            HirRelationExpr::Union { base, inputs } => {
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
        })
    }

    pub fn arity(&self) -> usize {
        match self {
            HirRelationExpr::Constant { typ, .. } => typ.column_types.len(),
            HirRelationExpr::Get { typ, .. } => typ.column_types.len(),
            HirRelationExpr::Let { body, .. } => body.arity(),
            HirRelationExpr::Project { outputs, .. } => outputs.len(),
            HirRelationExpr::Map { input, scalars } => input.arity() + scalars.len(),
            HirRelationExpr::CallTable { func, .. } => func.output_arity(),
            HirRelationExpr::Filter { input, .. }
            | HirRelationExpr::TopK { input, .. }
            | HirRelationExpr::Distinct { input }
            | HirRelationExpr::Negate { input }
            | HirRelationExpr::Threshold { input } => input.arity(),
            HirRelationExpr::Join { left, right, .. } => left.arity() + right.arity(),
            HirRelationExpr::Union { base, .. } => base.arity(),
            HirRelationExpr::Reduce {
                group_key,
                aggregates,
                ..
            } => group_key.len() + aggregates.len(),
        }
    }

    /// Pretty-print this HirRelationExpr to a string.
    pub fn pretty(&self) -> String {
        Explanation::new(self, &DummyHumanizer).to_string()
    }

    /// Reports whether this expression contains a column reference to its
    /// direct parent scope.
    pub fn is_correlated(&self) -> bool {
        let mut correlated = false;
        self.visit_columns(0, &mut |depth, col| {
            if col.level > depth && col.level - depth == 1 {
                correlated = true;
            }
        });
        correlated
    }

    pub fn is_join_identity(&self) -> bool {
        match self {
            HirRelationExpr::Constant { rows, .. } => rows.len() == 1 && self.arity() == 0,
            _ => false,
        }
    }

    pub fn project(self, outputs: Vec<usize>) -> Self {
        if outputs.iter().copied().eq(0..self.arity()) {
            // The projection is trivial. Suppress it.
            self
        } else {
            HirRelationExpr::Project {
                input: Box::new(self),
                outputs,
            }
        }
    }

    pub fn map(mut self, scalars: Vec<HirScalarExpr>) -> Self {
        if scalars.is_empty() {
            // The map is trivial. Suppress it.
            self
        } else if let HirRelationExpr::Map {
            scalars: old_scalars,
            input: _,
        } = &mut self
        {
            // Map applied to a map. Fuse the maps.
            old_scalars.extend(scalars);
            self
        } else {
            HirRelationExpr::Map {
                input: Box::new(self),
                scalars,
            }
        }
    }

    pub fn filter(self, predicates: Vec<HirScalarExpr>) -> Self {
        HirRelationExpr::Filter {
            input: Box::new(self),
            predicates,
        }
    }

    pub fn reduce(
        self,
        group_key: Vec<usize>,
        aggregates: Vec<AggregateExpr>,
        expected_group_size: Option<usize>,
    ) -> Self {
        HirRelationExpr::Reduce {
            input: Box::new(self),
            group_key,
            aggregates,
            expected_group_size,
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
        HirRelationExpr::TopK {
            input: Box::new(self),
            group_key,
            order_key,
            limit,
            offset,
        }
    }

    pub fn negate(self) -> Self {
        HirRelationExpr::Negate {
            input: Box::new(self),
        }
    }

    pub fn distinct(self) -> Self {
        HirRelationExpr::Distinct {
            input: Box::new(self),
        }
    }

    pub fn threshold(self) -> Self {
        HirRelationExpr::Threshold {
            input: Box::new(self),
        }
    }

    pub fn union(self, other: Self) -> Self {
        HirRelationExpr::Union {
            base: Box::new(self),
            inputs: vec![other],
        }
    }

    pub fn exists(self) -> HirScalarExpr {
        HirScalarExpr::Exists(Box::new(self))
    }

    pub fn select(self) -> HirScalarExpr {
        HirScalarExpr::Select(Box::new(self))
    }

    pub fn join(
        self,
        mut right: HirRelationExpr,
        on: HirScalarExpr,
        kind: JoinKind,
    ) -> HirRelationExpr {
        if self.is_join_identity() && !right.is_correlated() && on == HirScalarExpr::literal_true()
        {
            // The join can be elided, but we need to adjust column references
            // on the right-hand side to account for the removal of the scope
            // introduced by the join.
            right.visit_columns_mut(0, &mut |depth, col| {
                if col.level > depth {
                    col.level -= 1;
                }
            });
            right
        } else if right.is_join_identity() && on == HirScalarExpr::literal_true() {
            self
        } else {
            HirRelationExpr::Join {
                left: Box::new(self),
                right: Box::new(right),
                on,
                kind,
            }
        }
    }

    pub fn take(&mut self) -> HirRelationExpr {
        mem::replace(
            self,
            HirRelationExpr::Constant {
                rows: vec![],
                typ: RelationType::new(Vec::new()),
            },
        )
    }

    pub fn visit<'a, F>(&'a self, depth: usize, f: &mut F)
    where
        F: FnMut(&'a Self, usize),
    {
        let _ = self.visit_fallible(depth, &mut |e: &HirRelationExpr,
                                                 depth: usize|
         -> Result<(), ()> {
            f(e, depth);
            Ok(())
        });
    }

    pub fn visit_fallible<'a, F, E>(&'a self, depth: usize, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&'a Self, usize) -> Result<(), E>,
    {
        self.visit1(depth, |e: &HirRelationExpr, depth: usize| {
            e.visit_fallible(depth, f)
        })?;
        f(self, depth)
    }

    pub fn visit1<'a, F, E>(&'a self, depth: usize, mut f: F) -> Result<(), E>
    where
        F: FnMut(&'a Self, usize) -> Result<(), E>,
    {
        match self {
            HirRelationExpr::Constant { .. }
            | HirRelationExpr::Get { .. }
            | HirRelationExpr::CallTable { .. } => (),
            HirRelationExpr::Let { body, value, .. } => {
                f(value, depth)?;
                f(body, depth)?;
            }
            HirRelationExpr::Project { input, .. } => {
                f(input, depth)?;
            }
            HirRelationExpr::Map { input, .. } => {
                f(input, depth)?;
            }
            HirRelationExpr::Filter { input, .. } => {
                f(input, depth)?;
            }
            HirRelationExpr::Join { left, right, .. } => {
                f(left, depth)?;
                f(right, depth + 1)?;
            }
            HirRelationExpr::Reduce { input, .. } => {
                f(input, depth)?;
            }
            HirRelationExpr::Distinct { input } => {
                f(input, depth)?;
            }
            HirRelationExpr::TopK { input, .. } => {
                f(input, depth)?;
            }
            HirRelationExpr::Negate { input } => {
                f(input, depth)?;
            }
            HirRelationExpr::Threshold { input } => {
                f(input, depth)?;
            }
            HirRelationExpr::Union { base, inputs } => {
                f(base, depth)?;
                for input in inputs {
                    f(input, depth)?;
                }
            }
        }
        Ok(())
    }

    pub fn visit_mut<F>(&mut self, depth: usize, f: &mut F)
    where
        F: FnMut(&mut Self, usize),
    {
        let _ = self.visit_mut_fallible(depth, &mut |e: &mut HirRelationExpr,
                                                     depth: usize|
         -> Result<(), ()> {
            f(e, depth);
            Ok(())
        });
    }

    pub fn visit_mut_fallible<F, E>(&mut self, depth: usize, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut Self, usize) -> Result<(), E>,
    {
        self.visit1_mut(depth, |e: &mut HirRelationExpr, depth: usize| {
            e.visit_mut_fallible(depth, f)
        })?;
        f(self, depth)
    }

    pub fn visit1_mut<'a, F, E>(&'a mut self, depth: usize, mut f: F) -> Result<(), E>
    where
        F: FnMut(&'a mut Self, usize) -> Result<(), E>,
    {
        match self {
            HirRelationExpr::Constant { .. }
            | HirRelationExpr::Get { .. }
            | HirRelationExpr::CallTable { .. } => (),
            HirRelationExpr::Let { body, value, .. } => {
                f(value, depth)?;
                f(body, depth)?;
            }
            HirRelationExpr::Project { input, .. } => {
                f(input, depth)?;
            }
            HirRelationExpr::Map { input, .. } => {
                f(input, depth)?;
            }
            HirRelationExpr::Filter { input, .. } => {
                f(input, depth)?;
            }
            HirRelationExpr::Join { left, right, .. } => {
                f(left, depth)?;
                f(right, depth + 1)?;
            }
            HirRelationExpr::Reduce { input, .. } => {
                f(input, depth)?;
            }
            HirRelationExpr::Distinct { input } => {
                f(input, depth)?;
            }
            HirRelationExpr::TopK { input, .. } => {
                f(input, depth)?;
            }
            HirRelationExpr::Negate { input } => {
                f(input, depth)?;
            }
            HirRelationExpr::Threshold { input } => {
                f(input, depth)?;
            }
            HirRelationExpr::Union { base, inputs } => {
                f(base, depth)?;
                for input in inputs {
                    f(input, depth)?;
                }
            }
        }
        Ok(())
    }

    /// Visits all scalar expressions within the sub-tree of the given relation.
    ///
    /// The `depth` argument should indicate the subquery nesting depth of the expression,
    /// which will be incremented when entering the RHS of a join or a subquery and
    /// presented to the supplied function `f`.
    pub fn visit_scalar_expressions<F, E>(&self, depth: usize, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&HirScalarExpr, usize) -> Result<(), E>,
    {
        self.visit_fallible(depth, &mut |e: &HirRelationExpr,
                                         depth: usize|
         -> Result<(), E> {
            match e {
                HirRelationExpr::Join { on, .. } => {
                    f(on, depth)?;
                }
                HirRelationExpr::Map { scalars, .. } => {
                    for scalar in scalars {
                        f(scalar, depth)?;
                    }
                }
                HirRelationExpr::CallTable { exprs, .. } => {
                    for expr in exprs {
                        f(expr, depth)?;
                    }
                }
                HirRelationExpr::Filter { predicates, .. } => {
                    for predicate in predicates {
                        f(predicate, depth)?;
                    }
                }
                HirRelationExpr::Reduce { aggregates, .. } => {
                    for aggregate in aggregates {
                        f(&aggregate.expr, depth)?;
                    }
                }
                HirRelationExpr::Union { .. }
                | HirRelationExpr::Let { .. }
                | HirRelationExpr::Project { .. }
                | HirRelationExpr::Distinct { .. }
                | HirRelationExpr::TopK { .. }
                | HirRelationExpr::Negate { .. }
                | HirRelationExpr::Threshold { .. }
                | HirRelationExpr::Constant { .. }
                | HirRelationExpr::Get { .. } => (),
            }
            Ok(())
        })
    }

    /// Like `visit_scalar_expressions`, but permits mutating the expressions.
    pub fn visit_scalar_expressions_mut<F, E>(&mut self, depth: usize, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut HirScalarExpr, usize) -> Result<(), E>,
    {
        self.visit_mut_fallible(depth, &mut |e: &mut HirRelationExpr,
                                             depth: usize|
         -> Result<(), E> {
            match e {
                HirRelationExpr::Join { on, .. } => {
                    f(on, depth)?;
                }
                HirRelationExpr::Map { scalars, .. } => {
                    for scalar in scalars.iter_mut() {
                        f(scalar, depth)?;
                    }
                }
                HirRelationExpr::CallTable { exprs, .. } => {
                    for expr in exprs.iter_mut() {
                        f(expr, depth)?;
                    }
                }
                HirRelationExpr::Filter { predicates, .. } => {
                    for predicate in predicates.iter_mut() {
                        f(predicate, depth)?;
                    }
                }
                HirRelationExpr::Reduce { aggregates, .. } => {
                    for aggregate in aggregates.iter_mut() {
                        f(&mut aggregate.expr, depth)?;
                    }
                }
                HirRelationExpr::Union { .. }
                | HirRelationExpr::Let { .. }
                | HirRelationExpr::Project { .. }
                | HirRelationExpr::Distinct { .. }
                | HirRelationExpr::TopK { .. }
                | HirRelationExpr::Negate { .. }
                | HirRelationExpr::Threshold { .. }
                | HirRelationExpr::Constant { .. }
                | HirRelationExpr::Get { .. } => (),
            }
            Ok(())
        })
    }

    /// Visits the column references in this relation expression.
    ///
    /// The `depth` argument should indicate the subquery nesting depth of the expression,
    /// which will be incremented when entering the RHS of a join or a subquery and
    /// presented to the supplied function `f`.
    pub fn visit_columns<F>(&self, depth: usize, f: &mut F)
    where
        F: FnMut(usize, &ColumnRef),
    {
        let _ = self.visit_scalar_expressions(depth, &mut |e: &HirScalarExpr,
                                                           depth: usize|
         -> Result<(), ()> {
            e.visit_columns(depth, f);
            Ok(())
        });
    }

    /// Like `visit_columns`, but permits mutating the column references.
    pub fn visit_columns_mut<F>(&mut self, depth: usize, f: &mut F)
    where
        F: FnMut(usize, &mut ColumnRef),
    {
        let _ = self.visit_scalar_expressions_mut(depth, &mut |e: &mut HirScalarExpr,
                                                               depth: usize|
         -> Result<(), ()> {
            e.visit_columns_mut(depth, f);
            Ok(())
        });
    }

    /// Replaces any parameter references in the expression with the
    /// corresponding datum from `params`.
    pub fn bind_parameters(&mut self, params: &Params) -> Result<(), PlanError> {
        self.visit_scalar_expressions_mut(0, &mut |e: &mut HirScalarExpr, _: usize| {
            e.bind_parameters(params)
        })
    }

    /// See the documentation for [`HirScalarExpr::splice_parameters`].
    pub fn splice_parameters(&mut self, params: &[HirScalarExpr], depth: usize) {
        let _ = self.visit_scalar_expressions_mut(depth, &mut |e: &mut HirScalarExpr,
                                                               depth: usize|
         -> Result<(), ()> {
            e.splice_parameters(params, depth);
            Ok(())
        });
    }

    /// Constructs a constant collection from specific rows and schema.
    pub fn constant(rows: Vec<Vec<Datum>>, typ: RelationType) -> Self {
        let rows = rows
            .into_iter()
            .map(move |datums| Row::pack_slice(&datums))
            .collect();
        HirRelationExpr::Constant { rows, typ }
    }

    pub fn finish(&mut self, finishing: mz_expr::RowSetFinishing) {
        if !finishing.is_trivial(self.arity()) {
            *self = HirRelationExpr::Project {
                input: Box::new(HirRelationExpr::TopK {
                    input: Box::new(std::mem::replace(
                        self,
                        HirRelationExpr::Constant {
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

impl VisitChildren<Self> for HirRelationExpr {
    fn visit_children<F>(&self, mut f: F)
    where
        F: FnMut(&Self),
    {
        // subqueries of type HirRelationExpr might be wrapped in
        // Exists or Select variants within HirScalarExpr trees
        // attached at the current node, and we want to visit them as well
        VisitChildren::visit_children(self, |expr: &HirScalarExpr| {
            #[allow(deprecated)]
            Visit::visit_post_nolimit(expr, &mut |expr| match expr {
                HirScalarExpr::Exists(expr) | HirScalarExpr::Select(expr) => f(expr.as_ref()),
                _ => (),
            });
        });

        use HirRelationExpr::*;
        match self {
            Constant { rows: _, typ: _ } | Get { id: _, typ: _ } => (),
            Let {
                name: _,
                id: _,
                value,
                body,
            } => {
                f(value);
                f(body);
            }
            Project { input, outputs: _ } => f(input),
            Map { input, scalars: _ } => {
                f(input);
            }
            CallTable { func: _, exprs: _ } => (),
            Filter {
                input,
                predicates: _,
            } => {
                f(input);
            }
            Join {
                left,
                right,
                on: _,
                kind: _,
            } => {
                f(left);
                f(right);
            }
            Reduce {
                input,
                group_key: _,
                aggregates: _,
                expected_group_size: _,
            } => {
                f(input);
            }
            Distinct { input }
            | TopK {
                input,
                group_key: _,
                order_key: _,
                limit: _,
                offset: _,
            }
            | Negate { input }
            | Threshold { input } => {
                f(input);
            }
            Union { base, inputs } => {
                f(base);
                for input in inputs {
                    f(input);
                }
            }
        }
    }

    fn visit_mut_children<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut Self),
    {
        // subqueries of type HirRelationExpr might be wrapped in
        // Exists or Select variants within HirScalarExpr trees
        // attached at the current node, and we want to visit them as well
        VisitChildren::visit_mut_children(self, |expr: &mut HirScalarExpr| {
            #[allow(deprecated)]
            Visit::visit_mut_post_nolimit(expr, &mut |expr| match expr {
                HirScalarExpr::Exists(expr) | HirScalarExpr::Select(expr) => f(expr.as_mut()),
                _ => (),
            });
        });

        use HirRelationExpr::*;
        match self {
            Constant { rows: _, typ: _ } | Get { id: _, typ: _ } => (),
            Let {
                name: _,
                id: _,
                value,
                body,
            } => {
                f(value);
                f(body);
            }
            Project { input, outputs: _ } => f(input),
            Map { input, scalars: _ } => {
                f(input);
            }
            CallTable { func: _, exprs: _ } => (),
            Filter {
                input,
                predicates: _,
            } => {
                f(input);
            }
            Join {
                left,
                right,
                on: _,
                kind: _,
            } => {
                f(left);
                f(right);
            }
            Reduce {
                input,
                group_key: _,
                aggregates: _,
                expected_group_size: _,
            } => {
                f(input);
            }
            Distinct { input }
            | TopK {
                input,
                group_key: _,
                order_key: _,
                limit: _,
                offset: _,
            }
            | Negate { input }
            | Threshold { input } => {
                f(input);
            }
            Union { base, inputs } => {
                f(base);
                for input in inputs {
                    f(input);
                }
            }
        }
    }

    fn try_visit_children<F, E>(&self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&Self) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        // subqueries of type HirRelationExpr might be wrapped in
        // Exists or Select variants within HirScalarExpr trees
        // attached at the current node, and we want to visit them as well
        VisitChildren::try_visit_children(self, |expr: &HirScalarExpr| {
            Visit::try_visit_post(expr, &mut |expr| match expr {
                HirScalarExpr::Exists(expr) | HirScalarExpr::Select(expr) => f(expr.as_ref()),
                _ => Ok(()),
            })
        })?;

        use HirRelationExpr::*;
        match self {
            Constant { rows: _, typ: _ } | Get { id: _, typ: _ } => (),
            Let {
                name: _,
                id: _,
                value,
                body,
            } => {
                f(value)?;
                f(body)?;
            }
            Project { input, outputs: _ } => f(input)?,
            Map { input, scalars: _ } => {
                f(input)?;
            }
            CallTable { func: _, exprs: _ } => (),
            Filter {
                input,
                predicates: _,
            } => {
                f(input)?;
            }
            Join {
                left,
                right,
                on: _,
                kind: _,
            } => {
                f(left)?;
                f(right)?;
            }
            Reduce {
                input,
                group_key: _,
                aggregates: _,
                expected_group_size: _,
            } => {
                f(input)?;
            }
            Distinct { input }
            | TopK {
                input,
                group_key: _,
                order_key: _,
                limit: _,
                offset: _,
            }
            | Negate { input }
            | Threshold { input } => {
                f(input)?;
            }
            Union { base, inputs } => {
                f(base)?;
                for input in inputs {
                    f(input)?;
                }
            }
        }
        Ok(())
    }

    fn try_visit_mut_children<F, E>(&mut self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&mut Self) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        // subqueries of type HirRelationExpr might be wrapped in
        // Exists or Select variants within HirScalarExpr trees
        // attached at the current node, and we want to visit them as well
        VisitChildren::try_visit_mut_children(self, |expr: &mut HirScalarExpr| {
            Visit::try_visit_mut_post(expr, &mut |expr| match expr {
                HirScalarExpr::Exists(expr) | HirScalarExpr::Select(expr) => f(expr.as_mut()),
                _ => Ok(()),
            })
        })?;

        use HirRelationExpr::*;
        match self {
            Constant { rows: _, typ: _ } | Get { id: _, typ: _ } => (),
            Let {
                name: _,
                id: _,
                value,
                body,
            } => {
                f(value)?;
                f(body)?;
            }
            Project { input, outputs: _ } => f(input)?,
            Map { input, scalars: _ } => {
                f(input)?;
            }
            CallTable { func: _, exprs: _ } => (),
            Filter {
                input,
                predicates: _,
            } => {
                f(input)?;
            }
            Join {
                left,
                right,
                on: _,
                kind: _,
            } => {
                f(left)?;
                f(right)?;
            }
            Reduce {
                input,
                group_key: _,
                aggregates: _,
                expected_group_size: _,
            } => {
                f(input)?;
            }
            Distinct { input }
            | TopK {
                input,
                group_key: _,
                order_key: _,
                limit: _,
                offset: _,
            }
            | Negate { input }
            | Threshold { input } => {
                f(input)?;
            }
            Union { base, inputs } => {
                f(base)?;
                for input in inputs {
                    f(input)?;
                }
            }
        }
        Ok(())
    }
}

impl VisitChildren<HirScalarExpr> for HirRelationExpr {
    fn visit_children<F>(&self, mut f: F)
    where
        F: FnMut(&HirScalarExpr),
    {
        use HirRelationExpr::*;
        match self {
            Constant { rows: _, typ: _ }
            | Get { id: _, typ: _ }
            | Let {
                name: _,
                id: _,
                value: _,
                body: _,
            }
            | Project {
                input: _,
                outputs: _,
            } => (),
            Map { input: _, scalars } => {
                for scalar in scalars {
                    f(scalar);
                }
            }
            CallTable { func: _, exprs } => {
                for expr in exprs {
                    f(expr);
                }
            }
            Filter {
                input: _,
                predicates,
            } => {
                for predicate in predicates {
                    f(predicate);
                }
            }
            Join {
                left: _,
                right: _,
                on,
                kind: _,
            } => f(on),
            Reduce {
                input: _,
                group_key: _,
                aggregates,
                expected_group_size: _,
            } => {
                for aggregate in aggregates {
                    f(aggregate.expr.as_ref());
                }
            }
            Distinct { input: _ }
            | TopK {
                input: _,
                group_key: _,
                order_key: _,
                limit: _,
                offset: _,
            }
            | Negate { input: _ }
            | Threshold { input: _ }
            | Union { base: _, inputs: _ } => (),
        }
    }

    fn visit_mut_children<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut HirScalarExpr),
    {
        use HirRelationExpr::*;
        match self {
            Constant { rows: _, typ: _ }
            | Get { id: _, typ: _ }
            | Let {
                name: _,
                id: _,
                value: _,
                body: _,
            }
            | Project {
                input: _,
                outputs: _,
            } => (),
            Map { input: _, scalars } => {
                for scalar in scalars {
                    f(scalar);
                }
            }
            CallTable { func: _, exprs } => {
                for expr in exprs {
                    f(expr);
                }
            }
            Filter {
                input: _,
                predicates,
            } => {
                for predicate in predicates {
                    f(predicate);
                }
            }
            Join {
                left: _,
                right: _,
                on,
                kind: _,
            } => f(on),
            Reduce {
                input: _,
                group_key: _,
                aggregates,
                expected_group_size: _,
            } => {
                for aggregate in aggregates {
                    f(aggregate.expr.as_mut());
                }
            }
            Distinct { input: _ }
            | TopK {
                input: _,
                group_key: _,
                order_key: _,
                limit: _,
                offset: _,
            }
            | Negate { input: _ }
            | Threshold { input: _ }
            | Union { base: _, inputs: _ } => (),
        }
    }

    fn try_visit_children<F, E>(&self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&HirScalarExpr) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        use HirRelationExpr::*;
        match self {
            Constant { rows: _, typ: _ }
            | Get { id: _, typ: _ }
            | Let {
                name: _,
                id: _,
                value: _,
                body: _,
            }
            | Project {
                input: _,
                outputs: _,
            } => (),
            Map { input: _, scalars } => {
                for scalar in scalars {
                    f(scalar)?;
                }
            }
            CallTable { func: _, exprs } => {
                for expr in exprs {
                    f(expr)?;
                }
            }
            Filter {
                input: _,
                predicates,
            } => {
                for predicate in predicates {
                    f(predicate)?;
                }
            }
            Join {
                left: _,
                right: _,
                on,
                kind: _,
            } => f(on)?,
            Reduce {
                input: _,
                group_key: _,
                aggregates,
                expected_group_size: _,
            } => {
                for aggregate in aggregates {
                    f(aggregate.expr.as_ref())?;
                }
            }
            Distinct { input: _ }
            | TopK {
                input: _,
                group_key: _,
                order_key: _,
                limit: _,
                offset: _,
            }
            | Negate { input: _ }
            | Threshold { input: _ }
            | Union { base: _, inputs: _ } => (),
        }
        Ok(())
    }

    fn try_visit_mut_children<F, E>(&mut self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&mut HirScalarExpr) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        use HirRelationExpr::*;
        match self {
            Constant { rows: _, typ: _ }
            | Get { id: _, typ: _ }
            | Let {
                name: _,
                id: _,
                value: _,
                body: _,
            }
            | Project {
                input: _,
                outputs: _,
            } => (),
            Map { input: _, scalars } => {
                for scalar in scalars {
                    f(scalar)?;
                }
            }
            CallTable { func: _, exprs } => {
                for expr in exprs {
                    f(expr)?;
                }
            }
            Filter {
                input: _,
                predicates,
            } => {
                for predicate in predicates {
                    f(predicate)?;
                }
            }
            Join {
                left: _,
                right: _,
                on,
                kind: _,
            } => f(on)?,
            Reduce {
                input: _,
                group_key: _,
                aggregates,
                expected_group_size: _,
            } => {
                for aggregate in aggregates {
                    f(aggregate.expr.as_mut())?;
                }
            }
            Distinct { input: _ }
            | TopK {
                input: _,
                group_key: _,
                order_key: _,
                limit: _,
                offset: _,
            }
            | Negate { input: _ }
            | Threshold { input: _ }
            | Union { base: _, inputs: _ } => (),
        }
        Ok(())
    }
}

impl HirScalarExpr {
    /// Replaces any parameter references in the expression with the
    /// corresponding datum in `params`.
    pub fn bind_parameters(&mut self, params: &Params) -> Result<(), PlanError> {
        self.visit_recursively_mut(0, &mut |_: usize, e: &mut HirScalarExpr| {
            if let HirScalarExpr::Parameter(n) = e {
                let datum = match params.datums.iter().nth(*n - 1) {
                    None => sql_bail!("there is no parameter ${}", n),
                    Some(datum) => datum,
                };
                let scalar_type = &params.types[*n - 1];
                let row = Row::pack(&[datum]);
                let column_type = scalar_type.clone().nullable(datum.is_null());
                *e = HirScalarExpr::Literal(row, column_type);
            }
            Ok(())
        })
    }

    /// Like [`HirScalarExpr::bind_parameters`], except that parameters are
    /// replaced with the corresponding expression fragment from `params` rather
    /// than a datum.
    ///
    /// Specifically, the parameter `$1` will be replaced with `params[0]`, the
    /// parameter `$2` will be replaced with `params[1]`, and so on. Parameters
    /// in `self` that refer to invalid indices of `params` will cause a panic.
    ///
    /// Column references in parameters will be corrected to account for the
    /// depth at which they are spliced.
    pub fn splice_parameters(&mut self, params: &[HirScalarExpr], depth: usize) {
        let _ = self.visit_recursively_mut(depth, &mut |depth: usize,
                                                        e: &mut HirScalarExpr|
         -> Result<(), ()> {
            if let HirScalarExpr::Parameter(i) = e {
                *e = params[*i - 1].clone();
                // Correct any column references in the parameter expression for
                // its new depth.
                e.visit_columns_mut(0, &mut |d: usize, col: &mut ColumnRef| {
                    if col.level >= d {
                        col.level += depth
                    }
                });
            }
            Ok(())
        });
    }

    /// Constructs a column reference in the current scope.
    pub fn column(index: usize) -> HirScalarExpr {
        HirScalarExpr::Column(ColumnRef {
            level: 0,
            column: index,
        })
    }

    pub fn literal(datum: Datum, scalar_type: ScalarType) -> HirScalarExpr {
        let row = Row::pack(&[datum]);
        HirScalarExpr::Literal(row, scalar_type.nullable(datum.is_null()))
    }

    pub fn literal_true() -> HirScalarExpr {
        HirScalarExpr::literal(Datum::True, ScalarType::Bool)
    }

    pub fn literal_null(scalar_type: ScalarType) -> HirScalarExpr {
        HirScalarExpr::literal(Datum::Null, scalar_type)
    }

    pub fn literal_1d_array(
        datums: Vec<Datum>,
        element_scalar_type: ScalarType,
    ) -> Result<HirScalarExpr, PlanError> {
        let scalar_type = match element_scalar_type {
            ScalarType::Array(_) => {
                sql_bail!("cannot build array from array type");
            }
            typ => ScalarType::Array(Box::new(typ)).nullable(false),
        };

        let mut row = Row::default();
        row.packer()
            .push_array(
                &[ArrayDimension {
                    lower_bound: 1,
                    length: datums.len(),
                }],
                datums,
            )
            .expect("array constructed to be valid");

        Ok(HirScalarExpr::Literal(row, scalar_type))
    }

    pub fn as_literal(&self) -> Option<Datum> {
        if let HirScalarExpr::Literal(row, _column_type) = self {
            Some(row.unpack_first())
        } else {
            None
        }
    }

    pub fn is_literal_true(&self) -> bool {
        Some(Datum::True) == self.as_literal()
    }

    pub fn is_literal_false(&self) -> bool {
        Some(Datum::False) == self.as_literal()
    }

    pub fn is_literal_null(&self) -> bool {
        Some(Datum::Null) == self.as_literal()
    }

    pub fn call_unary(self, func: UnaryFunc) -> Self {
        HirScalarExpr::CallUnary {
            func,
            expr: Box::new(self),
        }
    }

    pub fn call_binary(self, other: Self, func: BinaryFunc) -> Self {
        HirScalarExpr::CallBinary {
            func,
            expr1: Box::new(self),
            expr2: Box::new(other),
        }
    }

    pub fn take(&mut self) -> Self {
        mem::replace(self, HirScalarExpr::literal_null(ScalarType::String))
    }

    pub fn visit<'a, F>(&'a self, f: &mut F)
    where
        F: FnMut(&'a Self),
    {
        self.visit1(|e: &HirScalarExpr| e.visit(f));
        f(self);
    }

    pub fn visit1<'a, F>(&'a self, mut f: F)
    where
        F: FnMut(&'a Self),
    {
        use HirScalarExpr::*;
        match self {
            Column(..) | Parameter(..) | Literal(..) | CallUnmaterializable(..) => (),
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
            Windowing(expr) => {
                let _ = expr.visit_expressions(&mut |e| -> Result<(), ()> {
                    f(e);
                    Ok(())
                });
            }
        }
    }

    pub fn visit_mut<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        self.visit1_mut(|e: &mut HirScalarExpr| e.visit_mut(f));
        f(self);
    }

    pub fn visit_mut_pre<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        f(self);
        self.visit1_mut(|e: &mut HirScalarExpr| e.visit_mut(f));
    }

    pub fn visit1_mut<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut Self),
    {
        use HirScalarExpr::*;
        match self {
            Column(..) | Parameter(..) | Literal(..) | CallUnmaterializable(..) => (),
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
            Windowing(expr) => {
                let _ = expr.visit_expressions_mut(&mut |e| -> Result<(), ()> {
                    f(e);
                    Ok(())
                });
            }
        }
    }

    /// A generalization of `visit`. The function `pre` runs on a
    /// `HirScalarExpr` before it runs on any of the child `HirScalarExpr`s.
    /// The function `post` runs on child `HirScalarExpr`s first before the
    /// parent. Optionally, `pre` can return which child `HirScalarExpr`s, if
    /// any, should be visited (default is to visit all children).
    pub fn visit_pre_post<F1, F2>(&self, pre: &mut F1, post: &mut F2)
    where
        F1: FnMut(&Self) -> Option<Vec<&Self>>,
        F2: FnMut(&Self),
    {
        let to_visit = pre(self);
        if let Some(to_visit) = to_visit {
            for e in to_visit {
                e.visit_pre_post(pre, post);
            }
        } else {
            self.visit1(|e| e.visit_pre_post(pre, post));
        }
        post(self);
    }

    /// Visits the column references in this scalar expression.
    ///
    /// The `depth` argument should indicate the subquery nesting depth of the expression,
    /// which will be incremented with each subquery entered and presented to the supplied
    /// function `f`.
    pub fn visit_columns<F>(&self, depth: usize, f: &mut F)
    where
        F: FnMut(usize, &ColumnRef),
    {
        let _ = self.visit_recursively(depth, &mut |depth: usize,
                                                    e: &HirScalarExpr|
         -> Result<(), ()> {
            if let HirScalarExpr::Column(col) = e {
                f(depth, col)
            }
            Ok(())
        });
    }

    /// Like `visit_columns`, but permits mutating the column references.
    pub fn visit_columns_mut<F>(&mut self, depth: usize, f: &mut F)
    where
        F: FnMut(usize, &mut ColumnRef),
    {
        let _ = self.visit_recursively_mut(depth, &mut |depth: usize,
                                                        e: &mut HirScalarExpr|
         -> Result<(), ()> {
            if let HirScalarExpr::Column(col) = e {
                f(depth, col)
            }
            Ok(())
        });
    }

    /// Like `visit` but it enters the subqueries visiting the scalar expressions contained
    /// in them. It takes the current depth of the expression and increases it when
    /// entering a subquery.
    pub fn visit_recursively<F, E>(&self, depth: usize, f: &mut F) -> Result<(), E>
    where
        F: FnMut(usize, &HirScalarExpr) -> Result<(), E>,
    {
        match self {
            HirScalarExpr::Literal(_, _)
            | HirScalarExpr::Parameter(_)
            | HirScalarExpr::CallUnmaterializable(_)
            | HirScalarExpr::Column(_) => (),
            HirScalarExpr::CallUnary { expr, .. } => expr.visit_recursively(depth, f)?,
            HirScalarExpr::CallBinary { expr1, expr2, .. } => {
                expr1.visit_recursively(depth, f)?;
                expr2.visit_recursively(depth, f)?;
            }
            HirScalarExpr::CallVariadic { exprs, .. } => {
                for expr in exprs {
                    expr.visit_recursively(depth, f)?;
                }
            }
            HirScalarExpr::If { cond, then, els } => {
                cond.visit_recursively(depth, f)?;
                then.visit_recursively(depth, f)?;
                els.visit_recursively(depth, f)?;
            }
            HirScalarExpr::Exists(expr) | HirScalarExpr::Select(expr) => {
                expr.visit_scalar_expressions(depth + 1, &mut |e, depth| {
                    e.visit_recursively(depth, f)
                })?;
            }
            HirScalarExpr::Windowing(expr) => {
                expr.visit_expressions(&mut |e| e.visit_recursively(depth, f))?;
            }
        }
        f(depth, self)
    }

    /// Like `visit_recursively`, but permits mutating the scalar expressions.
    pub fn visit_recursively_mut<F, E>(&mut self, depth: usize, f: &mut F) -> Result<(), E>
    where
        F: FnMut(usize, &mut HirScalarExpr) -> Result<(), E>,
    {
        match self {
            HirScalarExpr::Literal(_, _)
            | HirScalarExpr::Parameter(_)
            | HirScalarExpr::CallUnmaterializable(_)
            | HirScalarExpr::Column(_) => (),
            HirScalarExpr::CallUnary { expr, .. } => expr.visit_recursively_mut(depth, f)?,
            HirScalarExpr::CallBinary { expr1, expr2, .. } => {
                expr1.visit_recursively_mut(depth, f)?;
                expr2.visit_recursively_mut(depth, f)?;
            }
            HirScalarExpr::CallVariadic { exprs, .. } => {
                for expr in exprs {
                    expr.visit_recursively_mut(depth, f)?;
                }
            }
            HirScalarExpr::If { cond, then, els } => {
                cond.visit_recursively_mut(depth, f)?;
                then.visit_recursively_mut(depth, f)?;
                els.visit_recursively_mut(depth, f)?;
            }
            HirScalarExpr::Exists(expr) | HirScalarExpr::Select(expr) => {
                expr.visit_scalar_expressions_mut(depth + 1, &mut |e, depth| {
                    e.visit_recursively_mut(depth, f)
                })?;
            }
            HirScalarExpr::Windowing(expr) => {
                expr.visit_expressions_mut(&mut |e| e.visit_recursively_mut(depth, f))?;
            }
        }
        f(depth, self)
    }

    fn simplify_to_literal(self) -> Option<Row> {
        let mut expr = self.lower_uncorrelated().ok()?;
        expr.reduce(&mz_repr::RelationType::empty());
        match expr {
            mz_expr::MirScalarExpr::Literal(Ok(row), _) => Some(row),
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

impl VisitChildren<Self> for HirScalarExpr {
    fn visit_children<F>(&self, mut f: F)
    where
        F: FnMut(&Self),
    {
        use HirScalarExpr::*;
        match self {
            Column(..) | Parameter(..) | Literal(..) | CallUnmaterializable(..) => (),
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
            Windowing(expr) => expr.visit_children(f),
        }
    }

    fn visit_mut_children<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut Self),
    {
        use HirScalarExpr::*;
        match self {
            Column(..) | Parameter(..) | Literal(..) | CallUnmaterializable(..) => (),
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
            Windowing(expr) => expr.visit_mut_children(f),
        }
    }

    fn try_visit_children<F, E>(&self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&Self) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        use HirScalarExpr::*;
        match self {
            Column(..) | Parameter(..) | Literal(..) | CallUnmaterializable(..) => (),
            CallUnary { expr, .. } => f(expr)?,
            CallBinary { expr1, expr2, .. } => {
                f(expr1)?;
                f(expr2)?;
            }
            CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr)?;
                }
            }
            If { cond, then, els } => {
                f(cond)?;
                f(then)?;
                f(els)?;
            }
            Exists(..) | Select(..) => (),
            Windowing(expr) => expr.try_visit_children(f)?,
        }
        Ok(())
    }

    fn try_visit_mut_children<F, E>(&mut self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&mut Self) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        use HirScalarExpr::*;
        match self {
            Column(..) | Parameter(..) | Literal(..) | CallUnmaterializable(..) => (),
            CallUnary { expr, .. } => f(expr)?,
            CallBinary { expr1, expr2, .. } => {
                f(expr1)?;
                f(expr2)?;
            }
            CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr)?;
                }
            }
            If { cond, then, els } => {
                f(cond)?;
                f(then)?;
                f(els)?;
            }
            Exists(..) | Select(..) => (),
            Windowing(expr) => expr.try_visit_mut_children(f)?,
        }
        Ok(())
    }
}

impl AbstractExpr for HirScalarExpr {
    type Type = ColumnType;

    fn typ(
        &self,
        outers: &[RelationType],
        inner: &RelationType,
        params: &BTreeMap<usize, ScalarType>,
    ) -> Self::Type {
        stack::maybe_grow(|| match self {
            HirScalarExpr::Column(ColumnRef { level, column }) => {
                if *level == 0 {
                    inner.column_types[*column].clone()
                } else {
                    outers[*level - 1].column_types[*column].clone()
                }
            }
            HirScalarExpr::Parameter(n) => params[&n].clone().nullable(true),
            HirScalarExpr::Literal(_, typ) => typ.clone(),
            HirScalarExpr::CallUnmaterializable(func) => func.output_type(),
            HirScalarExpr::CallUnary { expr, func } => {
                func.output_type(expr.typ(outers, inner, params))
            }
            HirScalarExpr::CallBinary { expr1, expr2, func } => func.output_type(
                expr1.typ(outers, inner, params),
                expr2.typ(outers, inner, params),
            ),
            HirScalarExpr::CallVariadic { exprs, func } => {
                func.output_type(exprs.iter().map(|e| e.typ(outers, inner, params)).collect())
            }
            HirScalarExpr::If { cond: _, then, els } => {
                let then_type = then.typ(outers, inner, params);
                let else_type = els.typ(outers, inner, params);
                then_type.union(&else_type).unwrap()
            }
            HirScalarExpr::Exists(_) => ScalarType::Bool.nullable(true),
            HirScalarExpr::Select(expr) => {
                let mut outers = outers.to_vec();
                outers.insert(0, inner.clone());
                expr.typ(&outers, params)
                    .column_types
                    .into_element()
                    .nullable(true)
            }
            HirScalarExpr::Windowing(expr) => expr.func.typ(outers, inner, params),
        })
    }
}

impl AggregateExpr {
    /// Replaces any parameter references in the expression with the
    /// corresponding datum from `parameters`.
    pub fn bind_parameters(&mut self, params: &Params) -> Result<(), PlanError> {
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
}
