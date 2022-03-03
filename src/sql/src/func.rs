// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! TBD: Currently, `sql::func` handles matching arguments to their respective
//! built-in functions (for most built-in functions, at least).

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;

use itertools::Itertools;
use lazy_static::lazy_static;

use mz_expr::func;
use mz_ore::collections::CollectionExt;
use mz_pgrepr::oid;
use mz_repr::{ColumnName, ColumnType, Datum, RelationType, Row, ScalarBaseType, ScalarType};

use crate::ast::{SelectStatement, Statement};
use crate::names::{resolve_names, resolve_names_expr, PartialName};
use crate::plan::error::PlanError;
use crate::plan::expr::{
    AggregateFunc, BinaryFunc, CoercibleScalarExpr, ColumnOrder, HirRelationExpr, HirScalarExpr,
    NullaryFunc, ScalarWindowFunc, TableFunc, UnaryFunc, VariadicFunc,
};
use crate::plan::query::{self, ExprContext, QueryContext, QueryLifetime};
use crate::plan::scope::Scope;
use crate::plan::transform_ast;
use crate::plan::typeconv::{self, CastContext};
use crate::plan::StatementContext;

/// A specifier for a function or an operator.
#[derive(Clone, Copy, Debug)]
pub enum FuncSpec<'a> {
    /// A function name.
    Func(&'a PartialName),
    /// An operator name.
    Op(&'a str),
}

impl<'a> fmt::Display for FuncSpec<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FuncSpec::Func(n) => n.fmt(f),
            FuncSpec::Op(o) => o.fmt(f),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Mirrored from [PostgreSQL's `typcategory`][typcategory].
///
/// Note that Materialize also uses a number of pseudotypes when planning, but
/// we have yet to need to integrate them with `TypeCategory`.
///
/// [typcategory]:
/// https://www.postgresql.org/docs/9.6/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE
pub enum TypeCategory {
    Array,
    Bool,
    DateTime,
    List,
    Numeric,
    Pseudo,
    String,
    Timespan,
    UserDefined,
}

impl TypeCategory {
    /// Extracted from PostgreSQL 9.6.
    /// ```sql,ignore
    /// SELECT array_agg(typname), typcategory
    /// FROM pg_catalog.pg_type
    /// WHERE typname IN (
    ///  'bool', 'bytea', 'date', 'float4', 'float8', 'int4', 'int8', 'interval', 'jsonb',
    ///  'numeric', 'text', 'time', 'timestamp', 'timestamptz'
    /// )
    /// GROUP BY typcategory
    /// ORDER BY typcategory;
    /// ```
    pub fn from_type(typ: &ScalarType) -> Self {
        match typ {
            ScalarType::Array(..) | ScalarType::Int2Vector => Self::Array,
            ScalarType::Bool => Self::Bool,
            ScalarType::Bytes | ScalarType::Jsonb | ScalarType::Uuid => Self::UserDefined,
            ScalarType::Date
            | ScalarType::Time
            | ScalarType::Timestamp
            | ScalarType::TimestampTz => Self::DateTime,
            ScalarType::Float32
            | ScalarType::Float64
            | ScalarType::Int16
            | ScalarType::Int32
            | ScalarType::Int64
            | ScalarType::Oid
            | ScalarType::RegClass
            | ScalarType::RegProc
            | ScalarType::RegType
            | ScalarType::Numeric { .. } => Self::Numeric,
            ScalarType::Interval => Self::Timespan,
            ScalarType::List { .. } => Self::List,
            ScalarType::String | ScalarType::Char { .. } | ScalarType::VarChar { .. } => {
                Self::String
            }
            ScalarType::Record { .. } => Self::Pseudo,
            ScalarType::Map { .. } => Self::Pseudo,
        }
    }

    pub fn from_param(param: &ParamType) -> Self {
        match param {
            ParamType::Any
            | ParamType::ArrayAny
            | ParamType::ArrayElementAny
            | ParamType::ListAny
            | ParamType::ListElementAny
            | ParamType::NonVecAny
            | ParamType::MapAny
            | ParamType::RecordAny => Self::Pseudo,
            ParamType::Plain(t) => Self::from_type(t),
        }
    }

    /// Extracted from PostgreSQL 9.6.
    /// ```ignore
    /// SELECT typcategory, typname, typispreferred
    /// FROM pg_catalog.pg_type
    /// WHERE typispreferred = true
    /// ORDER BY typcategory;
    /// ```
    pub fn preferred_type(&self) -> Option<ScalarType> {
        match self {
            Self::Array | Self::List | Self::Pseudo | Self::UserDefined => None,
            Self::Bool => Some(ScalarType::Bool),
            Self::DateTime => Some(ScalarType::TimestampTz),
            Self::Numeric => Some(ScalarType::Float64),
            Self::String => Some(ScalarType::String),
            Self::Timespan => Some(ScalarType::Interval),
        }
    }
}

/// Builds an expression that evaluates a scalar function on the provided
/// input expressions.
struct Operation<R>(
    Box<
        dyn Fn(
                &ExprContext,
                Vec<CoercibleScalarExpr>,
                &ParamList,
                Vec<ColumnOrder>,
            ) -> Result<R, PlanError>
            + Send
            + Sync,
    >,
);

impl Operation<HirScalarExpr> {
    /// Builds a unary operation that simply returns its input.
    fn identity() -> Operation<HirScalarExpr> {
        Operation::unary(|_ecx, e| Ok(e))
    }
}

impl<R: GetReturnType> Operation<R> {
    fn new<F>(f: F) -> Operation<R>
    where
        F: Fn(
                &ExprContext,
                Vec<CoercibleScalarExpr>,
                &ParamList,
                Vec<ColumnOrder>,
            ) -> Result<R, PlanError>
            + Send
            + Sync
            + 'static,
    {
        Operation(Box::new(f))
    }

    /// Builds an operation that takes no arguments.
    fn nullary<F>(f: F) -> Operation<R>
    where
        F: Fn(&ExprContext) -> Result<R, PlanError> + Send + Sync + 'static,
    {
        Self::variadic(move |ecx, exprs| {
            assert!(exprs.is_empty());
            f(ecx)
        })
    }

    /// Builds an operation that takes one argument.
    fn unary<F>(f: F) -> Operation<R>
    where
        F: Fn(&ExprContext, HirScalarExpr) -> Result<R, PlanError> + Send + Sync + 'static,
    {
        Self::variadic(move |ecx, exprs| f(ecx, exprs.into_element()))
    }

    /// Builds an operation that takes one argument and an order_by.
    fn unary_ordered<F>(f: F) -> Operation<R>
    where
        F: Fn(&ExprContext, HirScalarExpr, Vec<ColumnOrder>) -> Result<R, PlanError>
            + Send
            + Sync
            + 'static,
    {
        Self::new(move |ecx, cexprs, params, order_by| {
            let exprs = coerce_args_to_types(ecx, cexprs, params)?;
            f(ecx, exprs.into_element(), order_by)
        })
    }

    /// Builds an operation that takes two arguments.
    fn binary<F>(f: F) -> Operation<R>
    where
        F: Fn(&ExprContext, HirScalarExpr, HirScalarExpr) -> Result<R, PlanError>
            + Send
            + Sync
            + 'static,
    {
        Self::variadic(move |ecx, exprs| {
            assert_eq!(exprs.len(), 2);
            let mut exprs = exprs.into_iter();
            let left = exprs.next().unwrap();
            let right = exprs.next().unwrap();
            f(ecx, left, right)
        })
    }

    /// Builds an operation that takes two arguments and an order_by.
    fn binary_ordered<F>(f: F) -> Operation<R>
    where
        F: Fn(&ExprContext, HirScalarExpr, HirScalarExpr, Vec<ColumnOrder>) -> Result<R, PlanError>
            + Send
            + Sync
            + 'static,
    {
        Self::new(move |ecx, cexprs, params, order_by| {
            let exprs = coerce_args_to_types(ecx, cexprs, params)?;
            assert_eq!(exprs.len(), 2);
            let mut exprs = exprs.into_iter();
            let left = exprs.next().unwrap();
            let right = exprs.next().unwrap();
            f(ecx, left, right, order_by)
        })
    }

    /// Builds an operation that takes any number of arguments.
    fn variadic<F>(f: F) -> Operation<R>
    where
        F: Fn(&ExprContext, Vec<HirScalarExpr>) -> Result<R, PlanError> + Send + Sync + 'static,
    {
        Self::new(move |ecx, cexprs, params, _order_by| {
            let exprs = coerce_args_to_types(ecx, cexprs, params)?;
            f(ecx, exprs)
        })
    }
}

/// Backing implementation for sql_impl_func and sql_impl_cast. See those
/// functions for details.
pub fn sql_impl(
    expr: &'static str,
) -> impl Fn(&QueryContext, Vec<ScalarType>) -> Result<HirScalarExpr, PlanError> {
    let expr = mz_sql_parser::parser::parse_expr(expr)
        .expect("static function definition failed to parse");
    move |qcx, types| {
        // Reconstruct an expression context where the parameter types are
        // bound to the types of the expressions in `args`.
        let mut scx = qcx.scx.clone();
        scx.param_types = RefCell::new(
            types
                .into_iter()
                .enumerate()
                .map(|(i, ty)| (i + 1, ty))
                .collect(),
        );
        let mut qcx = QueryContext::root(&scx, qcx.lifetime);

        // Desugar the expression
        let mut expr = expr.clone();
        transform_ast::transform_expr(&scx, &mut expr)?;

        let expr = resolve_names_expr(&mut qcx, expr)?;

        let ecx = ExprContext {
            qcx: &qcx,
            name: "static function definition",
            scope: &Scope::empty(),
            relation_type: &RelationType::empty(),
            allow_aggregates: false,
            allow_subqueries: true,
            allow_windows: false,
        };

        // Plan the expression.
        query::plan_expr(&ecx, &expr)?.type_as_any(&ecx)
    }
}

// Constructs a definition for a built-in function out of a static SQL
// expression.
//
// The SQL expression should use the standard parameter syntax (`$1`, `$2`, ...)
// to refer to the inputs to the function. For example, a built-in function that
// takes two arguments and concatenates them with an arrow in between could be
// defined like so:
//
//     sql_impl_func("$1 || '<->' || $2")
//
// The number of parameters in the SQL expression must exactly match the number
// of parameters in the built-in's declaration. There is no support for variadic
// functions.
fn sql_impl_func(expr: &'static str) -> Operation<HirScalarExpr> {
    let invoke = sql_impl(expr);
    Operation::variadic(move |ecx, args| {
        let types = args.iter().map(|arg| ecx.scalar_type(arg)).collect();
        let mut out = invoke(&ecx.qcx, types)?;
        out.splice_parameters(&args, 0);
        Ok(out)
    })
}

// Defines a built-in table function from a static SQL SELECT statement.
//
// The SQL statement should use the standard parameter syntax (`$1`, `$2`, ...)
// to refer to the inputs to the function; see sql_impl_func for an example.
//
// The number of parameters in the SQL expression must exactly match the number
// of parameters in the built-in's declaration. There is no support for variadic
// functions.
//
// As this is a full SQL statement, it returns a set of rows, similar to a
// table function. The SELECT's projection's names are used and should be
// aliased if needed.
fn sql_impl_table_func_inner(
    sql: &'static str,
    experimental: Option<&'static str>,
) -> Operation<TableFuncPlan> {
    let query = match mz_sql_parser::parser::parse_statements(sql)
        .expect("static function definition failed to parse")
        .expect_element("static function definition must have exactly one statement")
    {
        Statement::Select(SelectStatement { query, as_of: None }) => query,
        _ => panic!("static function definition expected SELECT statement"),
    };
    let invoke = move |qcx: &QueryContext, types: Vec<ScalarType>| {
        // Reconstruct an expression context where the parameter types are
        // bound to the types of the expressions in `args`.
        let mut scx = qcx.scx.clone();
        scx.param_types = RefCell::new(
            types
                .into_iter()
                .enumerate()
                .map(|(i, ty)| (i + 1, ty))
                .collect(),
        );
        let mut qcx = QueryContext::root(&scx, qcx.lifetime);

        let mut query = query.clone();
        transform_ast::transform_query(&scx, &mut query)?;

        let query = resolve_names(&mut qcx, query)?;

        query::plan_nested_query(&mut qcx, &query)
    };

    Operation::variadic(move |ecx, args| {
        if let Some(feature_name) = experimental {
            ecx.require_experimental_mode(feature_name)?;
        }
        let types = args.iter().map(|arg| ecx.scalar_type(arg)).collect();
        let (mut expr, scope) = invoke(&ecx.qcx, types)?;
        expr.splice_parameters(&args, 0);
        Ok(TableFuncPlan {
            expr,
            column_names: scope.column_names().cloned().collect(),
        })
    })
}

fn sql_impl_table_func(sql: &'static str) -> Operation<TableFuncPlan> {
    sql_impl_table_func_inner(sql, None)
}

fn experimental_sql_impl_table_func(
    feature: &'static str,
    sql: &'static str,
) -> Operation<TableFuncPlan> {
    sql_impl_table_func_inner(sql, Some(feature))
}

/// Describes a single function's implementation.
pub struct FuncImpl<R> {
    oid: u32,
    params: ParamList,
    return_type: ReturnType,
    op: Operation<R>,
}

/// Describes how each implementation should be represented in the catalog.
#[derive(Debug)]
pub struct FuncImplCatalogDetails {
    pub oid: u32,
    pub arg_oids: Vec<u32>,
    pub variadic_oid: Option<u32>,
    pub return_oid: Option<u32>,
    pub return_is_set: bool,
}

impl<R: GetReturnType> FuncImpl<R> {
    fn details(&self) -> FuncImplCatalogDetails {
        FuncImplCatalogDetails {
            oid: self.oid,
            arg_oids: self.params.arg_oids(),
            variadic_oid: self.params.variadic_oid(),
            return_oid: self.return_type.typ.as_ref().map(|t| t.oid()),
            return_is_set: self.return_type.is_set_of,
        }
    }
}

impl<R> fmt::Debug for FuncImpl<R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FuncImpl")
            .field("oid", &self.oid)
            .field("params", &self.params)
            .field("ret", &self.return_type)
            .field("op", &"<omitted>")
            .finish()
    }
}

impl From<NullaryFunc> for Operation<HirScalarExpr> {
    fn from(n: NullaryFunc) -> Operation<HirScalarExpr> {
        Operation::nullary(move |_ecx| Ok(HirScalarExpr::CallNullary(n.clone())))
    }
}

impl From<UnaryFunc> for Operation<HirScalarExpr> {
    fn from(u: UnaryFunc) -> Operation<HirScalarExpr> {
        Operation::unary(move |_ecx, e| Ok(e.call_unary(u.clone())))
    }
}

impl From<BinaryFunc> for Operation<HirScalarExpr> {
    fn from(b: BinaryFunc) -> Operation<HirScalarExpr> {
        Operation::binary(move |_ecx, left, right| Ok(left.call_binary(right, b.clone())))
    }
}

impl From<VariadicFunc> for Operation<HirScalarExpr> {
    fn from(v: VariadicFunc) -> Operation<HirScalarExpr> {
        Operation::variadic(move |_ecx, exprs| {
            Ok(HirScalarExpr::CallVariadic {
                func: v.clone(),
                exprs,
            })
        })
    }
}

impl From<AggregateFunc> for Operation<(HirScalarExpr, AggregateFunc)> {
    fn from(a: AggregateFunc) -> Operation<(HirScalarExpr, AggregateFunc)> {
        Operation::unary(move |_ecx, e| Ok((e, a.clone())))
    }
}

impl From<ScalarWindowFunc> for Operation<ScalarWindowFunc> {
    fn from(a: ScalarWindowFunc) -> Operation<ScalarWindowFunc> {
        Operation::nullary(move |_ecx| Ok(a.clone()))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
/// Describes possible types of function parameters.
///
/// Note that this is not exhaustive and will likely require additions.
pub enum ParamList {
    Exact(Vec<ParamType>),
    Variadic(ParamType),
}

impl ParamList {
    /// Determines whether `typs` are compatible with `self`.
    fn matches_argtypes(&self, ecx: &ExprContext, typs: &[Option<ScalarType>]) -> bool {
        if !self.validate_arg_len(typs.len()) {
            return false;
        }

        for (i, typ) in typs.iter().enumerate() {
            let param = &self[i];
            if let Some(typ) = typ {
                // Ensures either `typ` can at least be implicitly cast to a
                // type `param` accepts. Implicit in this check is that unknown
                // type arguments can be cast to any type.
                //
                // N.B. this will require more fallthrough checks once we
                // support RECORD types in functions.
                if !param.accepts_type(ecx, typ) {
                    return false;
                }
            }
        }

        !self.has_polymorphic() || self.resolve_polymorphic_types(typs).is_some()
    }

    /// Validates that the number of input elements are viable for `self`.
    fn validate_arg_len(&self, input_len: usize) -> bool {
        match self {
            Self::Exact(p) => p.len() == input_len,
            Self::Variadic(_) => input_len > 0,
        }
    }

    /// Reports whether the parameter list contains any polymorphic parameters.
    fn has_polymorphic(&self) -> bool {
        match self {
            ParamList::Exact(p) => p.iter().any(|p| p.is_polymorphic()),
            ParamList::Variadic(p) => p.is_polymorphic(),
        }
    }

    /// Enforces polymorphic type consistency by finding the concrete type that
    /// satisfies the constraints expressed by the polymorphic types in the
    /// parameter list.
    ///
    /// Polymorphic type consistency constraints include:
    /// - All arguments passed to `ArrayAny` must be equivalent
    ///   `ScalarType::Array`s with the same types of elements. All arguments
    ///   passed to `ArrayElementAny` must also be of these elements' type.
    ///   Note that equivalent includes types like `Int2Vector`.
    /// - All arguments passed to `ListAny` must be `ScalarType::List`s with
    ///   the same types of elements. All arguments passed to `ListElementAny`
    ///   must also be of these elements' type.
    /// - All arguments passed to `MapAny` must be `ScalarType::Map`s with the
    ///   same type of value in each key, value pair.
    ///
    /// Returns `Some` if the constraints were successfully resolved, or `None`
    /// otherwise.
    ///
    /// ## Custom types
    ///
    /// Materialize supports two classes of types:
    /// - Custom types, which are defined by `CREATE TYPE` or contain a
    ///   reference to a type that was.
    /// - Built-in types, which are all other types, e.g. `int4`, `int4 list`.
    ///
    ///   Among built-in types there are:
    ///   - Complex types, which contain references to other types
    ///   - Simple types, which do not contain referneces to other types
    ///
    /// To support accepting custom type values passed to polymorphic
    /// parameters, we must handle polymorphism for custom types. To understand
    /// how we assess custom types' polymorphism, it's useful to categorize
    /// polymorphic parameters in MZ.
    ///
    /// - **Complex parameters** include complex built-in types' polymorphic
    ///    parameters, e.g. `ListAny` and `MapAny`.
    ///
    ///   Valid `ScalarType`s passed to these parameters have a `custom_oid`
    ///   field and some embedded type, which we'll refer to as its element.
    ///
    /// - **Element parameters** which include `ArrayElementAny`,
    ///   `ListElementAny` and `NonVecAny`.
    ///
    /// Note that:
    /// - Custom types can be used as values for either complex or element
    ///   parameters; we'll refer to these as custom complex values and custom
    ///   element values, or collectively as custom values.
    /// - `ArrayAny` is slightly different from either case, but is uncommonly
    ///   used and not addressed further.
    ///
    /// ### Resolution
    ///
    /// - Upon encountering the first custom complex value:
    ///   - All other custom complex types must exactly match both its
    ///     `custom_oid` and embedded element.
    ///   - All custom element types must exactly match its embedded element
    ///     type.
    ///
    ///   One of the complexities here is that the custom complex value's
    ///   element can be built-in type, meaning any custom element values will
    ///   cause polymorphic resolution to fail.
    ///
    /// - Upon encountering the first custom element value:
    ///   - All other custom element values must exactly match its type.
    ///   - All custom complex types' embedded elements must exactly match its
    ///     type.
    ///
    /// ### Custom + built-in types
    ///
    /// If you use both custom and built-in types, the resultant type will be
    /// the least-custom custom type that fulfills the above requirements.
    ///
    /// For example if you `list_append(int4 list list, custom_int4_list)`, the
    /// resulant type will be complex: its `custom_oid` will be `None`, but its
    /// embedded element will be the custom element type, i.e.
    /// `custom_int4_list list`).
    ///
    /// However, it's also important to note that a complex value whose
    /// `custom_oid` is `None` are still considered complex if its embedded
    /// element is complex. Consider the following scenario:
    ///
    /// ```sql
    /// CREATE TYPE int4_list_custom AS LIST (element_type=int4);
    /// CREATE TYPE int4_list_list_custom AS LIST (element_type=int4_list_custom);
    /// /* Errors because we won't coerce int4_list_custom list to
    ///    int4_list_list_custom */
    /// SELECT '{{1}}'::int4_list_list_custom || '{{2}}'::int4_list_custom list;
    /// ```
    ///
    /// We will not coerce `int4_list_custom list` to
    /// `int4_list_list_custom`––only built-in types are ever coerced into
    /// custom types. It's also trivial for users to add a cast to ensure
    /// custom type consistency.
    fn resolve_polymorphic_types(&self, typs: &[Option<ScalarType>]) -> Option<ScalarType> {
        // Determines if types have the same [`ScalarBaseType`], and if complex
        // types' elements do, as well.
        // TODO: This shouldn't test equality; it should find the best common type.
        fn complex_base_eq(l: &ScalarType, r: &ScalarType) -> bool {
            match (l, r) {
                (ScalarType::Array(l), ScalarType::Array(r))
                | (
                    ScalarType::List {
                        element_type: l, ..
                    },
                    ScalarType::List {
                        element_type: r, ..
                    },
                )
                | (ScalarType::Map { value_type: l, .. }, ScalarType::Map { value_type: r, .. }) => {
                    complex_base_eq(l, r)
                }
                (ScalarType::Int2Vector, ScalarType::Array(el))
                | (ScalarType::Array(el), ScalarType::Int2Vector) => **el == ScalarType::Int16,
                (l, r) => ScalarBaseType::from(l) == ScalarBaseType::from(r),
            }
        }

        let mut custom_oid_lock = false;
        let mut element_lock = false;
        let mut constrained_type: Option<ScalarType> = None;

        // Determine the element on which to constrain the parameters.
        for (i, typ) in typs.iter().enumerate() {
            let param = &self[i];
            match (param, typ, &mut constrained_type) {
                (ParamType::ArrayAny, Some(typ), None) => {
                    constrained_type = Some(match typ {
                        // All polymorphic inputs get cast to the constrained
                        // type. Because you can cast `int2vector` to `int2[]`
                        // but not the other way around, `int2vector` must be
                        // rewritten to `int2[]`.
                        ScalarType::Int2Vector => ScalarType::Array(Box::new(ScalarType::Int16)),
                        other => other.clone(),
                    });
                }
                (ParamType::ArrayAny, Some(typ), Some(constrained)) => {
                    if !complex_base_eq(typ, constrained) {
                        return None;
                    }
                }
                (ParamType::ListAny, Some(typ), None) | (ParamType::MapAny, Some(typ), None) => {
                    constrained_type = Some(typ.clone());
                    custom_oid_lock = typ.is_custom_type();
                    element_lock = typ.is_custom_type();
                }
                (ParamType::ListAny, Some(typ), Some(constrained))
                | (ParamType::MapAny, Some(typ), Some(constrained)) => {
                    let element_accessor = match typ {
                        ScalarType::List { .. } => ScalarType::unwrap_list_element_type,
                        ScalarType::Map { .. } => ScalarType::unwrap_map_value_type,
                        _ => unreachable!(),
                    };

                    if (custom_oid_lock && typ.is_custom_type() && typ != constrained)
                        || (element_lock
                            && typ.is_custom_type()
                            && element_accessor(typ) != element_accessor(constrained))
                        || !complex_base_eq(typ, constrained)
                    {
                        return None;
                    }

                    if typ.is_custom_type() && !custom_oid_lock {
                        constrained_type = Some(typ.clone());
                        custom_oid_lock = true;
                        element_lock = true;
                    }
                }
                (ParamType::ArrayElementAny, Some(t), None) => {
                    constrained_type = Some(ScalarType::Array(Box::new(t.clone())));
                    element_lock = t.is_custom_type();
                }
                (ParamType::ArrayElementAny, Some(t), Some(constrained)) => {
                    let constrained_element_type = constrained.unwrap_array_element_type();
                    if (element_lock && t.is_custom_type() && t != constrained_element_type)
                        || !complex_base_eq(t, &constrained_element_type)
                    {
                        return None;
                    }
                    if t.is_custom_type() && !element_lock {
                        constrained_type = Some(ScalarType::Array(Box::new(t.clone())));
                        element_lock = true;
                    }
                }
                (ParamType::ListElementAny, Some(t), None) => {
                    constrained_type = Some(ScalarType::List {
                        custom_oid: None,
                        element_type: Box::new(t.clone()),
                    });
                    element_lock = t.is_custom_type();
                }
                (ParamType::ListElementAny, Some(t), Some(constrained_list)) => {
                    let constrained_element_type = constrained_list.unwrap_list_element_type();
                    if (element_lock && t.is_custom_type() && t != constrained_element_type)
                        || !complex_base_eq(t, &constrained_element_type)
                    {
                        return None;
                    }
                    if t.is_custom_type() && !element_lock {
                        constrained_type = Some(ScalarType::List {
                            custom_oid: None,
                            element_type: Box::new(t.clone()),
                        });
                        element_lock = true;
                    }
                }
                (ParamType::NonVecAny, Some(t), None) => {
                    constrained_type = Some(t.clone());
                }
                (ParamType::NonVecAny, Some(t), Some(constrained)) => {
                    if !complex_base_eq(t, &constrained) {
                        return None;
                    }
                }
                (ParamType::RecordAny, Some(t @ ScalarType::Record { .. }), None) => {
                    constrained_type = Some(t.clone());
                }
                (
                    ParamType::RecordAny,
                    Some(ScalarType::Record { .. }),
                    Some(t @ ScalarType::Record { .. }),
                ) => {
                    // We can directly return the scalar type here without further checks.
                    // Record type functions will attempt to cast the second param into
                    // the type of the first. If the record cannot be cast, it will error
                    // out then
                    return Some(t.clone());
                }
                // These checks don't need to be more exhaustive (e.g. failing
                // if arguments passed to `ListAny` are not `ScalarType::List`)
                // because we've already done general type checking in
                // `matches_argtypes`.
                _ => {}
            }
        }

        constrained_type
    }

    /// Matches a `&[ScalarType]` derived from the user's function argument
    /// against this `ParamList`'s permitted arguments.
    fn exact_match(&self, types: &[&ScalarType]) -> bool {
        types.iter().enumerate().all(|(i, t)| self[i] == **t)
    }

    /// Generates values underlying data for for `mz_catalog.mz_functions.arg_ids`.
    fn arg_oids(&self) -> Vec<u32> {
        match self {
            ParamList::Exact(p) => p.iter().map(|p| p.oid()).collect::<Vec<_>>(),
            ParamList::Variadic(p) => vec![p.oid()],
        }
    }

    /// Generates values for `mz_catalog.mz_functions.variadic_id`.
    fn variadic_oid(&self) -> Option<u32> {
        match self {
            ParamList::Exact(_) => None,
            ParamList::Variadic(p) => Some(p.oid()),
        }
    }

    /// Returns a set of `CoercibleScalarExpr`s whose values are literal nulls,
    /// typed such that they're compatible with `self`.
    ///
    /// # Panics
    ///
    /// Panics if called on a [`ParamList`] that contains any polymorphic
    /// [`ParamType`]s.
    fn contrive_coercible_exprs(&self) -> Vec<CoercibleScalarExpr> {
        let i = match self {
            ParamList::Exact(p) => p.clone(),
            ParamList::Variadic(p) => {
                vec![p.clone()]
            }
        };

        i.iter()
            .map(|p| {
                CoercibleScalarExpr::Coerced(HirScalarExpr::literal_null(match p {
                    ParamType::Plain(t) => t.clone(),
                    o => unreachable!("o {:?} is polymorphic and doesn't have a ScalarType", o),
                }))
            })
            .collect()
    }
}

impl std::ops::Index<usize> for ParamList {
    type Output = ParamType;

    fn index(&self, i: usize) -> &Self::Output {
        match self {
            Self::Exact(p) => &p[i],
            Self::Variadic(p) => &p,
        }
    }
}

/// Provides a shorthand function for writing `ParamList::Exact`.
impl From<Vec<ParamType>> for ParamList {
    fn from(p: Vec<ParamType>) -> ParamList {
        ParamList::Exact(p)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
/// Describes parameter types; these are essentially just `ScalarType` with some
/// added flexibility.
pub enum ParamType {
    /// A pseudotype permitting any type.
    Any,
    /// A polymorphic pseudotype permitting any array type.  For more details,
    /// see `ParamList::resolve_polymorphic_types`.
    ArrayAny,
    /// A polymorphic pseudotype permitting all types, with more constraints
    /// than `Any`, i.e. it is subject to polymorphic constraints. For more
    /// details, see `ParamList::resolve_polymorphic_types`.
    ArrayElementAny,
    /// A polymorphic pseudotype permitting a `ScalarType::List` of any element
    /// type. For more details, see `ParamList::resolve_polymorphic_types`.
    ListAny,
    /// A polymorphic pseudotype permitting all types, with more constraints
    /// than `Any`, i.e. it is subject to polymorphic constraints. For more
    /// details, see `ParamList::resolve_polymorphic_types`.
    ListElementAny,
    /// A polymorphic pseudotype with the same behavior as `ListElementAny`,
    /// except it does not permit either `ScalarType::Array` or
    /// `ScalarType::List`.
    NonVecAny,
    /// A polymorphic pseudotype permitting a `ScalarType::Map` of any non-nested
    /// value type. For more details, see `ParamList::resolve_polymorphic_types`.
    MapAny,
    /// A standard parameter that accepts arguments that match its embedded
    /// `ScalarType`.
    Plain(ScalarType),
    /// A polymorphic pseudotype permitting a `ScalarType::Record` of any type.
    /// Currently only used to express return values.
    RecordAny,
}

impl ParamType {
    /// Does `self` accept arguments of type `t`?
    fn accepts_type(&self, ecx: &ExprContext, t: &ScalarType) -> bool {
        use ParamType::*;
        use ScalarType::*;

        match self {
            ArrayAny => matches!(t, Array(..) | Int2Vector),
            ListAny => matches!(t, List { .. }),
            Any | ArrayElementAny | ListElementAny => true,
            NonVecAny => !t.is_vec(),
            MapAny => matches!(t, Map { .. }),
            Plain(to) => typeconv::can_cast(ecx, CastContext::Implicit, t, to),
            RecordAny => matches!(t, Record { .. }),
        }
    }

    /// Does `t`'s [`TypeCategory`] prefer `self`? This question can make
    /// more sense with the understanding that pseudotypes are never preferred.
    fn is_preferred_by(&self, t: &ScalarType) -> bool {
        if let Some(pt) = TypeCategory::from_type(t).preferred_type() {
            *self == pt
        } else {
            false
        }
    }

    /// Is `self` the preferred parameter type for its `TypeCategory`?
    fn prefers_self(&self) -> bool {
        if let Some(pt) = TypeCategory::from_param(self).preferred_type() {
            *self == pt
        } else {
            false
        }
    }

    fn is_polymorphic(&self) -> bool {
        use ParamType::*;
        match self {
            ArrayAny | ArrayElementAny | ListAny | MapAny | ListElementAny | NonVecAny
            | RecordAny => true,
            Any | Plain(_) => false,
        }
    }

    fn oid(&self) -> u32 {
        match self {
            ParamType::Plain(t) => match t {
                ScalarType::List { custom_oid, .. } | ScalarType::Map { custom_oid, .. }
                    if custom_oid.is_some() =>
                {
                    custom_oid.unwrap()
                }
                t => {
                    let t: mz_pgrepr::Type = t.into();
                    t.oid()
                }
            },
            ParamType::Any => postgres_types::Type::ANY.oid(),
            ParamType::ArrayAny => postgres_types::Type::ANYARRAY.oid(),
            ParamType::ArrayElementAny => postgres_types::Type::ANYELEMENT.oid(),
            ParamType::ListAny => mz_pgrepr::LIST.oid(),
            ParamType::ListElementAny => postgres_types::Type::ANYELEMENT.oid(),
            ParamType::MapAny => mz_pgrepr::MAP.oid(),
            ParamType::NonVecAny => postgres_types::Type::ANYNONARRAY.oid(),
            ParamType::RecordAny => postgres_types::Type::RECORD.oid(),
        }
    }
}

impl PartialEq<ScalarType> for ParamType {
    fn eq(&self, other: &ScalarType) -> bool {
        match self {
            ParamType::Plain(s) => s.base_eq(other),
            // Pseudotypes never equal concrete types
            _ => false,
        }
    }
}

impl PartialEq<ParamType> for ScalarType {
    fn eq(&self, other: &ParamType) -> bool {
        other == self
    }
}

impl From<ScalarType> for ParamType {
    fn from(s: ScalarType) -> ParamType {
        ParamType::Plain(s)
    }
}

impl From<ScalarBaseType> for ParamType {
    fn from(s: ScalarBaseType) -> ParamType {
        use ScalarBaseType::*;
        let s = match s {
            Array => return ParamType::ArrayAny,
            List => return ParamType::ListAny,
            Map => return ParamType::MapAny,
            Record => return ParamType::RecordAny,
            Bool => ScalarType::Bool,
            Int16 => ScalarType::Int16,
            Int32 => ScalarType::Int32,
            Int64 => ScalarType::Int64,
            Float32 => ScalarType::Float32,
            Float64 => ScalarType::Float64,
            Numeric => ScalarType::Numeric { max_scale: None },
            Date => ScalarType::Date,
            Time => ScalarType::Time,
            Timestamp => ScalarType::Timestamp,
            TimestampTz => ScalarType::TimestampTz,
            Interval => ScalarType::Interval,
            Bytes => ScalarType::Bytes,
            String => ScalarType::String,
            Char => ScalarType::Char { length: None },
            VarChar => ScalarType::VarChar { max_length: None },
            Jsonb => ScalarType::Jsonb,
            Uuid => ScalarType::Uuid,
            Oid => ScalarType::Oid,
            RegClass => ScalarType::RegClass,
            RegProc => ScalarType::RegProc,
            RegType => ScalarType::RegType,
            Int2Vector => ScalarType::Int2Vector,
        };
        ParamType::Plain(s)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ReturnType {
    typ: Option<ParamType>,
    is_set_of: bool,
}

impl ReturnType {
    /// Expresses that a function's return type is a scalar value.
    fn scalar(typ: ParamType) -> ReturnType {
        ReturnType {
            typ: Some(typ),
            is_set_of: false,
        }
    }

    /// Expresses that a function's return type is a set of values, e.g. a table
    /// function.
    fn set_of(typ: ParamType) -> ReturnType {
        ReturnType {
            typ: Some(typ),
            is_set_of: true,
        }
    }
}

impl From<ParamType> for ReturnType {
    fn from(typ: ParamType) -> ReturnType {
        ReturnType::scalar(typ)
    }
}

impl From<ScalarBaseType> for ReturnType {
    fn from(s: ScalarBaseType) -> ReturnType {
        ParamType::from(s).into()
    }
}

impl From<ScalarType> for ReturnType {
    fn from(s: ScalarType) -> ReturnType {
        ParamType::Plain(s).into()
    }
}

pub trait GetReturnType {
    fn return_type(&self, ecx: &ExprContext, param_list: &ParamList) -> ReturnType;
}

impl GetReturnType for HirScalarExpr {
    fn return_type(&self, ecx: &ExprContext, param_list: &ParamList) -> ReturnType {
        fn assert_oti_len(oti: &[ColumnType], len: usize, name: &str) {
            assert_eq!(
                oti.len(),
                len,
                "{} requires exactly {} contrived input to automatically determine return type",
                name,
                len,
            );
        }

        let mut output_type_inputs: Vec<ColumnType> = param_list
            .contrive_coercible_exprs()
            .into_iter()
            .map(|c| {
                let expr = c.type_as_any(&ecx).expect("c is typed NULL");
                ecx.column_type(&expr)
            })
            .collect();

        let c = match self {
            HirScalarExpr::Literal(_row, column_type) => column_type.clone(),
            HirScalarExpr::CallNullary(func) => {
                assert_oti_len(&output_type_inputs, 0, "HirScalarExpr::CallNullary");
                func.output_type()
            }
            HirScalarExpr::CallUnary { func, .. } => {
                assert_oti_len(&output_type_inputs, 1, "HirScalarExpr::CallUnary");
                func.output_type(output_type_inputs.remove(0))
            }
            HirScalarExpr::CallBinary { func, .. } => {
                assert_oti_len(&output_type_inputs, 2, "HirScalarExpr::CallBinary");
                func.output_type(output_type_inputs.remove(0), output_type_inputs.remove(0))
            }
            HirScalarExpr::CallVariadic { func, .. } => func.output_type(output_type_inputs),
            other => unreachable!(
                "unexepected HirScalarExpr in Operation<HirScalarExpr>::return_type: {:?}",
                other
            ),
        };

        ReturnType::scalar(c.scalar_type.into())
    }
}

impl GetReturnType for (HirScalarExpr, AggregateFunc) {
    fn return_type(&self, ecx: &ExprContext, _param_list: &ParamList) -> ReturnType {
        let c = ecx.column_type(&self.0);
        let s = self.1.output_type(c).scalar_type;
        ReturnType::scalar(s.into())
    }
}

impl GetReturnType for ScalarWindowFunc {
    fn return_type(&self, _ecx: &ExprContext, _param_list: &ParamList) -> ReturnType {
        ReturnType::scalar(self.output_type().scalar_type.into())
    }
}

impl GetReturnType for TableFuncPlan {
    fn return_type(&self, _ecx: &ExprContext, _param_list: &ParamList) -> ReturnType {
        let mut cols: Vec<ScalarType> = match &self.expr {
            HirRelationExpr::CallTable { func, .. } => func
                .output_type()
                .column_types
                .into_iter()
                .map(|col| col.scalar_type)
                .collect(),
            other => unreachable!(
                "unexepected HirRelationExpr in Operation<TableFuncPlan>::return_type: {:?}",
                other
            ),
        };

        match cols.len() {
            0 => ReturnType {
                typ: None,
                is_set_of: true,
            },
            1 => ReturnType::set_of(cols.remove(0).into()),
            // Returned relation types with > 1 column are treated as records,
            // irrespective of the return type we currently assess e.g.
            // ```sql
            //  SELECT jsonb_each('{"a": 1}');
            //  jsonb_each
            //  ------------
            //  (a,1)
            //
            // SELECT pg_typeof(jsonb_each('{"a": 1}'));
            // pg_typeof
            // -----------
            //  record
            // ```
            _ => ReturnType::set_of(ParamType::RecordAny),
        }
    }
}

#[derive(Clone, Debug)]
/// Tracks candidate implementations.
pub struct Candidate<'a, R> {
    /// The implementation under consideration.
    fimpl: &'a FuncImpl<R>,
    exact_matches: usize,
    preferred_types: usize,
}

/// Selects the best implementation given the provided `args` using a
/// process similar to [PostgreSQL's parser][pgparser], and returns the
/// `ScalarExpr` to invoke that function.
///
/// Inline comments prefixed with number are taken from the "Function Type
/// Resolution" section of the aforelinked page.
///
/// # Errors
/// - When the provided arguments are not valid for any implementation, e.g.
///   cannot be converted to the appropriate types.
/// - When all implementations are equally valid.
///
/// [pgparser]: https://www.postgresql.org/docs/current/typeconv-oper.html
pub fn select_impl<R>(
    ecx: &ExprContext,
    spec: FuncSpec,
    impls: &[FuncImpl<R>],
    args: Vec<CoercibleScalarExpr>,
    order_by: Vec<ColumnOrder>,
) -> Result<R, PlanError>
where
    R: fmt::Debug,
{
    let name = spec.to_string();
    let ecx = &ecx.with_name(&name);
    let types: Vec<_> = args.iter().map(|e| ecx.scalar_type(e)).collect();
    select_impl_inner(ecx, impls, args, &types, order_by).map_err(|e| {
        let types: Vec<_> = types
            .into_iter()
            .map(|ty| match ty {
                Some(ty) => ecx.humanize_scalar_type(&ty),
                None => "unknown".to_string(),
            })
            .collect();
        let context = match (spec, types.as_slice()) {
            (FuncSpec::Func(name), _) => {
                format!("Cannot call function {}({})", name, types.join(", "))
            }
            (FuncSpec::Op(name), [typ]) => format!("no overload for {} {}", name, typ),
            (FuncSpec::Op(name), [ltyp, rtyp]) => {
                format!("no overload for {} {} {}", ltyp, name, rtyp)
            }
            (FuncSpec::Op(_), [..]) => unreachable!("non-unary non-binary operator"),
        };
        PlanError::Unstructured(format!("{}: {}", context, e))
    })
}

fn select_impl_inner<R>(
    ecx: &ExprContext,
    impls: &[FuncImpl<R>],
    cexprs: Vec<CoercibleScalarExpr>,
    types: &[Option<ScalarType>],
    order_by: Vec<ColumnOrder>,
) -> Result<R, PlanError>
where
    R: fmt::Debug,
{
    // 4.a. Discard candidate functions for which the input types do not
    // match and cannot be converted (using an implicit conversion) to
    // match. unknown literals are assumed to be convertible to anything for
    // this purpose.
    let impls: Vec<_> = impls
        .iter()
        .filter(|i| i.params.matches_argtypes(ecx, types))
        .collect();

    let f = find_match(ecx, types, impls)?;

    (f.op.0)(ecx, cexprs, &f.params, order_by)
}

/// Finds an exact match based on the arguments, or, if no exact match, finds
/// the best match available. Patterned after [PostgreSQL's type conversion
/// matching algorithm][pgparser].
///
/// [pgparser]: https://www.postgresql.org/docs/current/typeconv-func.html
fn find_match<'a, R: std::fmt::Debug>(
    ecx: &ExprContext,
    types: &[Option<ScalarType>],
    impls: Vec<&'a FuncImpl<R>>,
) -> Result<&'a FuncImpl<R>, PlanError> {
    let all_types_known = types.iter().all(|t| t.is_some());

    // Check for exact match.
    if all_types_known {
        let known_types: Vec<_> = types.iter().filter_map(|t| t.as_ref()).collect();
        let matching_impls: Vec<&FuncImpl<_>> = impls
            .iter()
            .filter(|i| i.params.exact_match(&known_types))
            .cloned()
            .collect();

        if matching_impls.len() == 1 {
            return Ok(&matching_impls[0]);
        }
    }

    // No exact match. Apply PostgreSQL's best match algorithm. Generate
    // candidates by assessing their compatibility with each implementation's
    // parameters.
    let mut candidates: Vec<Candidate<_>> = Vec::new();
    macro_rules! maybe_get_last_candidate {
        () => {
            if candidates.len() == 1 {
                return Ok(&candidates[0].fimpl);
            }
        };
    }
    let mut max_exact_matches = 0;
    for fimpl in impls {
        let mut exact_matches = 0;
        let mut preferred_types = 0;

        for (i, arg_type) in types.iter().enumerate() {
            let param_type = &fimpl.params[i];

            match arg_type {
                Some(arg_type) => {
                    if param_type == arg_type {
                        exact_matches += 1;
                    }
                    if param_type.is_preferred_by(arg_type) {
                        preferred_types += 1;
                    }
                }
                None => {
                    if param_type.prefers_self() {
                        preferred_types += 1;
                    }
                }
            }
        }

        // 4.a. Discard candidate functions for which the input types do not
        // match and cannot be converted (using an implicit conversion) to
        // match. unknown literals are assumed to be convertible to anything for
        // this purpose.
        max_exact_matches = std::cmp::max(max_exact_matches, exact_matches);
        candidates.push(Candidate {
            fimpl,
            exact_matches,
            preferred_types,
        });
    }

    if candidates.is_empty() {
        sql_bail!(
            "arguments cannot be implicitly cast to any implementation's parameters; \
            try providing explicit casts"
        )
    }

    maybe_get_last_candidate!();

    // 4.c. Run through all candidates and keep those with the most exact
    // matches on input types. Keep all candidates if none have exact matches.
    candidates.retain(|c| c.exact_matches >= max_exact_matches);

    maybe_get_last_candidate!();

    // 4.d. Run through all candidates and keep those that accept preferred
    // types (of the input data type's type category) at the most positions
    // where type conversion will be required.
    let mut max_preferred_types = 0;
    for c in &candidates {
        max_preferred_types = std::cmp::max(max_preferred_types, c.preferred_types);
    }
    candidates.retain(|c| c.preferred_types >= max_preferred_types);

    maybe_get_last_candidate!();

    if all_types_known {
        sql_bail!(
            "unable to determine which implementation to use; try providing \
            explicit casts to match parameter types"
        )
    }

    let mut found_known = false;
    let mut types_match = true;
    let mut common_type: Option<ScalarType> = None;

    for (i, arg_type) in types.iter().enumerate() {
        let mut selected_category: Option<TypeCategory> = None;
        let mut categories_match = true;

        match arg_type {
            // 4.e. If any input arguments are unknown, check the type
            // categories accepted at those argument positions by the remaining
            // candidates.
            None => {
                for c in candidates.iter() {
                    let this_category = TypeCategory::from_param(&c.fimpl.params[i]);
                    // 4.e. cont: Select the string category if any candidate
                    // accepts that category. (This bias towards string is
                    // appropriate since an unknown-type literal looks like a
                    // string.)
                    if this_category == TypeCategory::String {
                        selected_category = Some(TypeCategory::String);
                        break;
                    }
                    match selected_category {
                        Some(ref mut selected_category) => {
                            // 4.e. cont: [...otherwise,] if all the remaining candidates
                            // accept the same type category, select that category.
                            categories_match =
                                selected_category == &this_category && categories_match;
                        }
                        None => selected_category = Some(this_category.clone()),
                    }
                }

                // 4.e. cont: Otherwise fail because the correct choice cannot
                // be deduced without more clues.
                // (ed: this doesn't mean fail entirely, simply moving onto 4.f)
                if selected_category != Some(TypeCategory::String) && !categories_match {
                    break;
                }

                // 4.e. cont: Now discard candidates that do not accept the
                // selected type category. Furthermore, if any candidate accepts
                // a preferred type in that category, discard candidates that
                // accept non-preferred types for that argument.
                let selected_category = selected_category.unwrap();

                let preferred_type = selected_category.preferred_type();
                let mut found_preferred_type_candidate = false;
                candidates.retain(|c| {
                    if let Some(typ) = &preferred_type {
                        found_preferred_type_candidate = c.fimpl.params[i].accepts_type(ecx, typ)
                            || found_preferred_type_candidate;
                    }
                    selected_category == TypeCategory::from_param(&c.fimpl.params[i])
                });

                if found_preferred_type_candidate {
                    let preferred_type = preferred_type.unwrap();
                    candidates.retain(|c| c.fimpl.params[i].accepts_type(ecx, &preferred_type));
                }
            }
            Some(typ) => {
                found_known = true;
                // Track if all known types are of the same type; use this info
                // in 4.f.
                match common_type {
                    Some(ref common_type) => types_match = common_type == typ && types_match,
                    None => common_type = Some(typ.clone()),
                }
            }
        }
    }

    maybe_get_last_candidate!();

    // 4.f. If there are both unknown and known-type arguments, and all the
    // known-type arguments have the same type, assume that the unknown
    // arguments are also of that type, and check which candidates can accept
    // that type at the unknown-argument positions.
    // (ed: We know unknown argument exists if we're in this part of the code.)
    if found_known && types_match {
        let common_type = common_type.unwrap();
        let common_typed: Vec<_> = types
            .iter()
            .map(|t| match t {
                Some(t) => Some(t.clone()),
                None => Some(common_type.clone()),
            })
            .collect();

        candidates.retain(|c| c.fimpl.params.matches_argtypes(ecx, &common_typed));

        maybe_get_last_candidate!();
    }

    sql_bail!(
        "unable to determine which implementation to use; try providing \
        explicit casts to match parameter types"
    )
}

/// Coerces concrete arguments for a function according to the abstract
/// parameters specified in the function definition.
///
/// You must only call this function if `ParamList::matches_argtypes` has
/// verified that the `args` are valid for `params`.
fn coerce_args_to_types(
    ecx: &ExprContext,
    args: Vec<CoercibleScalarExpr>,
    params: &ParamList,
) -> Result<Vec<HirScalarExpr>, PlanError> {
    let types: Vec<_> = args.iter().map(|e| ecx.scalar_type(e)).collect();
    let get_constrained_ty = || {
        params
            .resolve_polymorphic_types(&types)
            .expect("function selection verifies that polymorphic types successfully resolved")
    };

    let do_convert =
        |arg: CoercibleScalarExpr, ty: &ScalarType| arg.cast_to(ecx, CastContext::Implicit, ty);

    let mut exprs = Vec::new();
    for (i, arg) in args.into_iter().enumerate() {
        let expr = match &params[i] {
            // Concrete type. Direct conversion.
            ParamType::Plain(ty) => do_convert(arg, ty)?,

            // Polymorphic pseudotypes. Convert based on constrained type.
            ParamType::ArrayAny | ParamType::ListAny | ParamType::MapAny => {
                do_convert(arg, &get_constrained_ty())?
            }
            ParamType::ArrayElementAny => {
                let constrained_array = get_constrained_ty();
                do_convert(arg, &constrained_array.unwrap_array_element_type())?
            }
            ParamType::ListElementAny => {
                let constrained_list = get_constrained_ty();
                do_convert(arg, &constrained_list.unwrap_list_element_type())?
            }
            ParamType::NonVecAny => {
                let ty = get_constrained_ty();
                assert!(!ty.is_vec());
                do_convert(arg, &ty)?
            }

            // Special "any" psuedotype. Per PostgreSQL, uncoerced literals
            // are accepted, but uncoerced parameters are rejected.
            ParamType::Any => match arg {
                CoercibleScalarExpr::Parameter(n) => {
                    sql_bail!("could not determine data type of parameter ${}", n)
                }
                _ => arg.type_as_any(ecx)?,
            },

            ParamType::RecordAny => {
                let constrained = get_constrained_ty();
                do_convert(arg, &constrained)?
            }
        };
        exprs.push(expr);
    }
    Ok(exprs)
}

/// Provides shorthand for converting `Vec<ScalarType>` into `Vec<ParamType>`.
macro_rules! params {
    ($p:ident...) => { ParamList::Variadic($p.into()) };
    ($($p:expr),*) => { ParamList::Exact(vec![$($p.into(),)*]) };
}

macro_rules! impl_def {
    // Return type explicitly specified. This must be the case in situations
    // such as:
    // - Polymorphic functions: We have no way of understanding if the input
    //   type affects the return type, so you must tell us what the return type
    //   is.
    // - Explicitly defined Operations whose returned expression does not
    //   appropriately correlate to the function itself, e.g. returning a
    //   UnaryFunc from a FuncImpl that takes two parameters.
    // - Unimplemented/catalog-only functions
    ($params:expr, $op:expr, $return_type:expr, $oid:expr) => {{
        FuncImpl {
            oid: $oid,
            params: $params.into(),
            op: $op.into(),
            return_type: $return_type.into(),
        }
    }};
    // Return type can be automatically determined as a function of the
    // parameters.
    ($params:expr, $op:expr, $oid:expr) => {{
        let pcx = crate::plan::PlanContext::new(chrono::MIN_DATETIME, false);
        let scx = StatementContext::new(None, &crate::catalog::DummyCatalog);
        // This lifetime is compatible with more functions.
        let qcx = QueryContext::root(&scx, QueryLifetime::OneShot(&pcx));
        let ecx = ExprContext {
            qcx: &qcx,
            name: "dummy for builtin func return type eval",
            scope: &Scope::empty(),
            relation_type: &RelationType::empty(),
            allow_aggregates: true,
            allow_subqueries: false,
            allow_windows: true,
        };

        let op = Operation::from($op);
        let params = ParamList::from($params);
        assert!(
            !params.has_polymorphic(),
            "loading builtin functions failed: polymorphic functions must have return types explicitly defined"
        );

        let cexprs = params.contrive_coercible_exprs();
        let r = (op.0)(&ecx, cexprs, &params, vec![]).unwrap();
        let return_type = r.return_type(&ecx, &params);

        FuncImpl {
            oid: $oid,
            params,
            op,
            return_type,
        }
    }};
}

/// Constructs builtin function map.
macro_rules! builtins {
    {
        $(
            $name:expr => $ty:ident {
                $($params:expr => $op:expr $(=> $return_type:expr)?, $oid:expr;)+
            }
        ),+
    } => {{

        let mut builtins = HashMap::new();
        $(
            let impls = vec![$(impl_def!($params, $op $(,$return_type)?, $oid)),+];
            let old = builtins.insert($name, Func::$ty(impls));
            assert!(old.is_none(), "duplicate entry in builtins list");
        )+
        builtins
    }};
}

#[derive(Debug)]
pub struct TableFuncPlan {
    pub expr: HirRelationExpr,
    pub column_names: Vec<ColumnName>,
}

#[derive(Debug)]
pub enum Func {
    Scalar(Vec<FuncImpl<HirScalarExpr>>),
    Aggregate(Vec<FuncImpl<(HirScalarExpr, AggregateFunc)>>),
    Table(Vec<FuncImpl<TableFuncPlan>>),
    ScalarWindow(Vec<FuncImpl<ScalarWindowFunc>>),
}

impl Func {
    pub fn func_impls(&self) -> Vec<FuncImplCatalogDetails> {
        match self {
            Func::Scalar(impls) => impls.iter().map(|f| f.details()).collect::<Vec<_>>(),
            Func::Aggregate(impls) => impls.iter().map(|f| f.details()).collect::<Vec<_>>(),
            Func::Table(impls) => impls.iter().map(|f| f.details()).collect::<Vec<_>>(),
            Func::ScalarWindow(impls) => impls.iter().map(|f| f.details()).collect::<Vec<_>>(),
        }
    }
}

/// Functions using this macro should be transformed/planned away before
/// reaching function selection code, but still need to be present in the
/// catalog during planning.
macro_rules! catalog_name_only {
    ($name:expr) => {
        panic!(
            "{} should be planned away before reaching function selection",
            $name
        )
    };
}

lazy_static! {
    /// Correlates a built-in function name to its implementations.
    pub static ref PG_CATALOG_BUILTINS: HashMap<&'static str, Func> = {
        use ParamType::*;
        use ScalarBaseType::*;
        builtins! {
            // Literal OIDs collected from PG 13 using a version of this query
            // ```sql
            // SELECT oid, proname, proargtypes::regtype[]
            // FROM pg_proc
            // WHERE proname IN (
            //      'ascii', 'array_upper', 'jsonb_build_object'
            // );
            // ```
            // Values are also available through
            // https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_proc.dat

            // Scalars.
            "abs" => Scalar {
                params!(Int16) => UnaryFunc::AbsInt16(func::AbsInt16), 1398;
                params!(Int32) => UnaryFunc::AbsInt32(func::AbsInt32), 1397;
                params!(Int64) => UnaryFunc::AbsInt64(func::AbsInt64), 1396;
                params!(Numeric) => UnaryFunc::AbsNumeric(func::AbsNumeric), 1705;
                params!(Float32) => UnaryFunc::AbsFloat32(func::AbsFloat32), 1394;
                params!(Float64) => UnaryFunc::AbsFloat64(func::AbsFloat64), 1395;
            },
            "array_cat" => Scalar {
                params!(ArrayAny, ArrayAny) => Operation::binary(|ecx, lhs, rhs| {
                    ecx.require_experimental_mode("array_cat")?;
                    Ok(lhs.call_binary(rhs, BinaryFunc::ArrayArrayConcat))
                }) => ArrayAny, 383;
            },
            "array_in" => Scalar {
                params!(String, Oid, Int32) =>
                    Operation::unary(|_ecx, _e| bail_unsupported!("array_in")) => ArrayAny, 750;
            },
            "array_length" => Scalar {
                params![ArrayAny, Int64] => BinaryFunc::ArrayLength => Int32, 2176;
            },
            "array_lower" => Scalar {
                params!(ArrayAny, Int64) => BinaryFunc::ArrayLower => Int32, 2091;
            },
            "array_remove" => Scalar {
                params!(ArrayAny, ArrayElementAny) => BinaryFunc::ArrayRemove => ArrayAny, 3167;
            },
            "array_to_string" => Scalar {
                params!(ArrayAny, String) => Operation::variadic(array_to_string) => String, 395;
                params!(ArrayAny, String, String) => Operation::variadic(array_to_string) => String, 384;
            },
            "array_upper" => Scalar {
                params!(ArrayAny, Int64) => BinaryFunc::ArrayUpper => Int32, 2092;
            },
            "ascii" => Scalar {
                params!(String) => UnaryFunc::Ascii, 1620;
            },
            "avg" => Scalar {
                params!(Int64) => Operation::nullary(|_ecx| catalog_name_only!("avg")) => Numeric, 2100;
                params!(Int32) => Operation::nullary(|_ecx| catalog_name_only!("avg")) => Numeric, 2101;
                params!(Int16) => Operation::nullary(|_ecx| catalog_name_only!("avg")) => Numeric, 2102;
                params!(Float32) => Operation::nullary(|_ecx| catalog_name_only!("avg")) => Float64, 2104;
                params!(Float64) => Operation::nullary(|_ecx| catalog_name_only!("avg")) => Float64, 2105;
                params!(Interval) => Operation::nullary(|_ecx| catalog_name_only!("avg")) => Interval, 2106;
            },
            "bit_length" => Scalar {
                params!(Bytes) => UnaryFunc::BitLengthBytes, 1810;
                params!(String) => UnaryFunc::BitLengthString, 1811;
            },
            "btrim" => Scalar {
                params!(String) => UnaryFunc::TrimWhitespace, 885;
                params!(String, String) => BinaryFunc::Trim, 884;
            },
            "cbrt" => Scalar {
                params!(Float64) => UnaryFunc::CbrtFloat64(func::CbrtFloat64), 1345;
            },
            "ceil" => Scalar {
                params!(Float32) => UnaryFunc::CeilFloat32(func::CeilFloat32), oid::FUNC_CEIL_F32_OID;
                params!(Float64) => UnaryFunc::CeilFloat64(func::CeilFloat64), 2308;
                params!(Numeric) => UnaryFunc::CeilNumeric(func::CeilNumeric), 1711;
            },
            "char_length" => Scalar {
                params!(String) => UnaryFunc::CharLength, 1381;
            },
            "concat" => Scalar {
                params!(Any...) => Operation::variadic(|ecx, cexprs| {
                    if cexprs.is_empty() {
                        sql_bail!("No function matches the given name and argument types. \
                        You might need to add explicit type casts.")
                    }
                    let mut exprs = vec![];
                    for expr in cexprs {
                        exprs.push(match ecx.scalar_type(&expr) {
                            // concat uses nonstandard bool -> string casts
                            // to match historical baggage in PostgreSQL.
                            ScalarType::Bool => expr.call_unary(UnaryFunc::CastBoolToStringNonstandard(func::CastBoolToStringNonstandard)),
                            // TODO(#7572): remove call to PadChar
                            ScalarType::Char { length } => expr.call_unary(UnaryFunc::PadChar(func::PadChar { length })),
                            _ => typeconv::to_string(ecx, expr)
                        });
                    }
                    Ok(HirScalarExpr::CallVariadic { func: VariadicFunc::Concat, exprs })
                }) => String, 3058;
            },
            "convert_from" => Scalar {
                params!(Bytes, String) => BinaryFunc::ConvertFrom, 1714;
            },
            "cos" => Scalar {
                params!(Float64) => UnaryFunc::Cos(func::Cos), 1605;
            },
            "acos" => Scalar {
                params!(Float64) => UnaryFunc::Acos(func::Acos), 1601;
            },
            "cosh" => Scalar {
                params!(Float64) => UnaryFunc::Cosh(func::Cosh), 2463;
            },
            "acosh" => Scalar {
                params!(Float64) => UnaryFunc::Acosh(func::Acosh), 2466;
            },
            "cot" => Scalar {
                params!(Float64) => UnaryFunc::Cot(func::Cot), 1607;
            },
            "current_schema" => Scalar {
                // TODO: this should be name
                params!() => sql_impl_func("current_schemas(false)[1]") => String, 1402;
            },
            "current_schemas" => Scalar {
                params!(Bool) => Operation::unary(|_ecx, e| {
                    Ok(HirScalarExpr::If {
                        cond: Box::new(e),
                        then: Box::new(HirScalarExpr::CallNullary(NullaryFunc::CurrentSchemasWithSystem)),
                        els: Box::new(HirScalarExpr::CallNullary(NullaryFunc::CurrentSchemasWithoutSystem)),
                    })
                    // TODO: this should be name[]
                }) => ScalarType::Array(Box::new(ScalarType::String)), 1403;
            },
            "current_database" => Scalar {
                params!() => NullaryFunc::CurrentDatabase, 861;
            },
            "current_user" => Scalar {
                params!() => NullaryFunc::CurrentUser, 745;
            },
            "session_user" => Scalar {
                params!() => NullaryFunc::CurrentUser, 746;
            },
            "chr" => Scalar {
                params!(Int32) => UnaryFunc::Chr(func::Chr), 1621;
            },
            "date_bin" => Scalar {
                params!(Interval, Timestamp) => Operation::binary(|ecx, stride, source| {
                    ecx.require_experimental_mode("binary date_bin")?;
                    Ok(stride.call_binary(source, BinaryFunc::DateBinTimestamp))
                }), oid::FUNC_MZ_DATE_BIN_UNIX_EPOCH_TS_OID;
                params!(Interval, TimestampTz) => Operation::binary(|ecx, stride, source| {
                    ecx.require_experimental_mode("binary date_bin")?;
                    Ok(stride.call_binary(source, BinaryFunc::DateBinTimestampTz))
                }), oid::FUNC_MZ_DATE_BIN_UNIX_EPOCH_TSTZ_OID;
                params!(Interval, Timestamp, Timestamp) => VariadicFunc::DateBinTimestamp, 6177;
                params!(Interval, TimestampTz, TimestampTz) => VariadicFunc::DateBinTimestampTz, 6178;
            },
            "extract" => Scalar {
                params!(String, Interval) => BinaryFunc::ExtractInterval, 6204;
                params!(String, Time) => BinaryFunc::ExtractTime, 6200;
                params!(String, Timestamp) => BinaryFunc::ExtractTimestamp, 6202;
                params!(String, TimestampTz) => BinaryFunc::ExtractTimestampTz, 6203;
                params!(String, Date) => BinaryFunc::ExtractDate, 6199;
            },
            "date_part" => Scalar {
                params!(String, Interval) => BinaryFunc::DatePartInterval, 1172;
                params!(String, Time) => BinaryFunc::DatePartTime, 1385;
                params!(String, Timestamp) => BinaryFunc::DatePartTimestamp, 2021;
                params!(String, TimestampTz) => BinaryFunc::DatePartTimestampTz, 1171;
            },
            "date_trunc" => Scalar {
                params!(String, Timestamp) => BinaryFunc::DateTruncTimestamp, 2020;
                params!(String, TimestampTz) => BinaryFunc::DateTruncTimestampTz, 1217;
            },
            "degrees" => Scalar {
                params!(Float64) => UnaryFunc::Degrees(func::Degrees), 1608;
            },
            "digest" => Scalar {
                params!(String, String) => BinaryFunc::DigestString, 44154;
                params!(Bytes, String) => BinaryFunc::DigestBytes, 44155;
            },
            "exp" => Scalar {
                params!(Float64) => UnaryFunc::Exp(func::Exp), 1347;
                params!(Numeric) => UnaryFunc::ExpNumeric(func::ExpNumeric), 1732;
            },
            "floor" => Scalar {
                params!(Float32) => UnaryFunc::FloorFloat32(func::FloorFloat32), oid::FUNC_FLOOR_F32_OID;
                params!(Float64) => UnaryFunc::FloorFloat64(func::FloorFloat64), 2309;
                params!(Numeric) => UnaryFunc::FloorNumeric(func::FloorNumeric), 1712;
            },
            "format_type" => Scalar {
                params!(Oid, Int32) => sql_impl_func(
                    "CASE
                        WHEN $1 IS NULL THEN NULL
                        ELSE coalesce((SELECT concat(coalesce(mz_internal.mz_type_name($1), name), mz_internal.mz_render_typmod($1, $2)) FROM mz_catalog.mz_types WHERE oid = $1), '???')
                    END"
                ) => String, 1081;
            },
            "hmac" => Scalar {
                params!(String, String, String) => VariadicFunc::HmacString, 44156;
                params!(Bytes, Bytes, String) => VariadicFunc::HmacBytes, 44157;
            },
            "jsonb_array_length" => Scalar {
                params!(Jsonb) => UnaryFunc::JsonbArrayLength, 3207;
            },
            "jsonb_build_array" => Scalar {
                params!() => VariadicFunc::JsonbBuildArray, 3272;
                params!(Any...) => Operation::variadic(|ecx, exprs| Ok(HirScalarExpr::CallVariadic {
                    func: VariadicFunc::JsonbBuildArray,
                    exprs: exprs.into_iter().map(|e| typeconv::to_jsonb(ecx, e)).collect(),
                })) => Jsonb, 3271;
            },
            "jsonb_build_object" => Scalar {
                params!() => VariadicFunc::JsonbBuildObject, 3274;
                params!(Any...) => Operation::variadic(|ecx, exprs| {
                    if exprs.len() % 2 != 0 {
                        sql_bail!("argument list must have even number of elements")
                    }
                    Ok(HirScalarExpr::CallVariadic {
                        func: VariadicFunc::JsonbBuildObject,
                        exprs: exprs.into_iter().tuples().map(|(key, val)| {
                            let key = typeconv::to_string(ecx, key);
                            let val = typeconv::to_jsonb(ecx, val);
                            vec![key, val]
                        }).flatten().collect(),
                    })
                }) => Jsonb, 3273;
            },
            "jsonb_pretty" => Scalar {
                params!(Jsonb) => UnaryFunc::JsonbPretty, 3306;
            },
            "jsonb_strip_nulls" => Scalar {
                params!(Jsonb) => UnaryFunc::JsonbStripNulls, 3262;
            },
            "jsonb_typeof" => Scalar {
                params!(Jsonb) => UnaryFunc::JsonbTypeof, 3210;
            },
            "justify_days" => Scalar {
                params!(Interval) => UnaryFunc::JustifyDays(func::JustifyDays), 1295;
            },
            "justify_hours" => Scalar {
                params!(Interval) => UnaryFunc::JustifyHours(func::JustifyHours), 1175;
            },
            "justify_interval" => Scalar {
                params!(Interval) => UnaryFunc::JustifyInterval(func::JustifyInterval), 2711;
            },
            "left" => Scalar {
                params!(String, Int32) => BinaryFunc::Left, 3060;
            },
            "length" => Scalar {
                params!(Bytes) => UnaryFunc::ByteLengthBytes, 2010;
                // bpcharlen is redundant with automatic coercion to string, 1318.
                params!(String) => UnaryFunc::CharLength, 1317;
                params!(Bytes, String) => BinaryFunc::EncodedBytesCharLength, 1713;
            },
            "like_escape" => Scalar {
                params!(String, String) => BinaryFunc::LikeEscape, 1637;
            },
            "ln" => Scalar {
                params!(Float64) => UnaryFunc::Ln(func::Ln), 1341;
                params!(Numeric) => UnaryFunc::LnNumeric(func::LnNumeric), 1734;
            },
            "log10" => Scalar {
                params!(Float64) => UnaryFunc::Log10(func::Log10), 1194;
                params!(Numeric) => UnaryFunc::Log10Numeric(func::Log10Numeric), 1481;
            },
            "log" => Scalar {
                params!(Float64) => UnaryFunc::Log10(func::Log10), 1340;
                params!(Numeric) => UnaryFunc::Log10Numeric(func::Log10Numeric), 1741;
                params!(Numeric, Numeric) => BinaryFunc::LogNumeric, 1736;
            },
            "lower" => Scalar {
                params!(String) => UnaryFunc::Lower, 870;
            },
            "lpad" => Scalar {
                params!(String, Int64) => VariadicFunc::PadLeading, 879;
                params!(String, Int64, String) => VariadicFunc::PadLeading, 873;
            },
            "ltrim" => Scalar {
                params!(String) => UnaryFunc::TrimLeadingWhitespace, 881;
                params!(String, String) => BinaryFunc::TrimLeading, 875;
            },
            "make_timestamp" => Scalar {
                params!(Int64, Int64, Int64, Int64, Int64, Float64) => VariadicFunc::MakeTimestamp, 3461;
            },
            "md5" => Scalar {
                params!(String) => Operation::unary(move |_ecx, input| {
                    let algorithm = HirScalarExpr::literal(Datum::String("md5"), ScalarType::String);
                    let encoding = HirScalarExpr::literal(Datum::String("hex"), ScalarType::String);
                    Ok(input.call_binary(algorithm, BinaryFunc::DigestString).call_binary(encoding, BinaryFunc::Encode))
                }) => String, 2311;
                params!(Bytes) => Operation::unary(move |_ecx, input| {
                    let algorithm = HirScalarExpr::literal(Datum::String("md5"), ScalarType::String);
                    let encoding = HirScalarExpr::literal(Datum::String("hex"), ScalarType::String);
                    Ok(input.call_binary(algorithm, BinaryFunc::DigestBytes).call_binary(encoding, BinaryFunc::Encode))
                }) => String, 2321;
            },
            "mod" => Scalar {
                params!(Numeric, Numeric) => Operation::nullary(|_ecx| catalog_name_only!("mod")) => Numeric, 1728;
                params!(Int16, Int16) => Operation::nullary(|_ecx| catalog_name_only!("mod")) => Int16, 940;
                params!(Int32, Int32) => Operation::nullary(|_ecx| catalog_name_only!("mod")) => Int32, 941;
                params!(Int64, Int64) => Operation::nullary(|_ecx| catalog_name_only!("mod")) => Int64, 947;
            },
            "now" => Scalar {
                params!() => NullaryFunc::CurrentTimestamp, 1299;
            },
            "octet_length" => Scalar {
                params!(Bytes) => UnaryFunc::ByteLengthBytes, 720;
                params!(String) => UnaryFunc::ByteLengthString, 1374;
                params!(Char) => Operation::unary(|ecx, e| {
                    let length = ecx.scalar_type(&e).unwrap_char_length();
                    Ok(e.call_unary(UnaryFunc::PadChar(func::PadChar { length }))
                        .call_unary(UnaryFunc::ByteLengthString)
                    )
                }), 1375;
            },
            "obj_description" => Scalar {
                params!(Oid, String) => Operation::binary(|_ecx, _oid, _catalog| {
                    // This function is meant to return the comment on a
                    // database object, but we don't presently support comments,
                    // so stubbed out out to always return NULL.
                    Ok(HirScalarExpr::literal_null(ScalarType::String))
                }), 1215;
            },
            "pg_column_size" => Scalar {
                params!(Any) => UnaryFunc::PgColumnSize(func::PgColumnSize) => Int32, 1269;
            },
            "mz_row_size" => Scalar {
                params!(Any) => Operation::unary(|ecx, e| {
                    let s = ecx.scalar_type(&e);
                    if !matches!(s, ScalarType::Record{..}) {
                        sql_bail!("mz_row_size requires a record type");
                    }
                    Ok(e.call_unary(UnaryFunc::MzRowSize(func::MzRowSize)))
                }) => Int32, oid::FUNC_MZ_ROW_SIZE;
            },
            "pg_encoding_to_char" => Scalar {
                // Materialize only supports UT8-encoded databases. Return 'UTF8' if Postgres'
                // encoding id for UTF8 (6) is provided, otherwise return 'NULL'.
                params!(Int64) => sql_impl_func("CASE WHEN $1 = 6 THEN 'UTF8' ELSE NULL END") => String, 1597;
            },
            "pg_backend_pid" => Scalar {
                params!() => NullaryFunc::PgBackendPid, 2026;
            },
            // pg_get_constraintdef gives more info about a constraint with in the `pg_constraint`
            // view. It currently returns no information as the `pg_constraint` view is empty in
            // materialize
            "pg_get_constraintdef" => Scalar {
                params!(Oid) => UnaryFunc::PgGetConstraintdef(func::PgGetConstraintdef), 1387;
                params!(Oid, Bool) => BinaryFunc::PgGetConstraintdef, 2508;
            },
            // pg_get_expr is meant to convert the textual version of
            // pg_node_tree data into parseable expressions. However, we don't
            // use the pg_get_expr structure anywhere and the equivalent columns
            // in Materialize (e.g. index expressions) are already stored as
            // parseable expressions. So, we offer this function in the catalog
            // for ORM support, but make no effort to provide its semantics,
            // e.g. this also means we drop the Oid argument on the floor.
            "pg_get_expr" => Scalar {
                params!(String, Oid) => Operation::binary(|_ecx, l, _r| Ok(l)), 1716;
                params!(String, Oid, Bool) => Operation::variadic(move |_ecx, mut args| Ok(args.remove(0))), 2509;
            },
            "pg_get_userbyid" => Scalar {
                params!(Oid) => sql_impl_func("'unknown (OID=' || $1 || ')'") => String, 1642;
            },
            "pg_postmaster_start_time" => Scalar {
                params!() => NullaryFunc::PgPostmasterStartTime, 2560;
            },
            "pg_table_is_visible" => Scalar {
                params!(Oid) => sql_impl_func(
                    "(SELECT s.name = ANY(current_schemas(true))
                     FROM mz_catalog.mz_objects o JOIN mz_catalog.mz_schemas s ON o.schema_id = s.id
                     WHERE o.oid = $1)"
                ) => Bool, 2079;
            },
            "pg_type_is_visible" => Scalar {
                params!(Oid) => sql_impl_func(
                    "(SELECT s.name = ANY(current_schemas(true))
                     FROM mz_catalog.mz_types t JOIN mz_catalog.mz_schemas s ON t.schema_id = s.id
                     WHERE t.oid = $1)"
                ) => Bool, 2080;
            },
            "pg_typeof" => Scalar {
                params!(Any) => Operation::new(|ecx, exprs, params, _order_by| {
                    // pg_typeof reports the type *before* coercion.
                    let name = match ecx.scalar_type(&exprs[0]) {
                        None => "unknown".to_string(),
                        Some(ty) => ecx.humanize_scalar_type(&ty),
                    };

                    // For consistency with other functions, verify that
                    // coercion is possible, though we don't actually care about
                    // the coerced results.
                    coerce_args_to_types(ecx, exprs, params)?;

                    // TODO(benesch): make this function have return type
                    // regtype, when we support that type. Document the function
                    // at that point. For now, it's useful enough to have this
                    // halfway version that returns a string.
                    Ok(HirScalarExpr::literal(Datum::String(&name), ScalarType::String))
                }) => String, 1619;
            },
            "position" => Scalar {
                params!(String, String) => BinaryFunc::Position, 849;
            },
            "pow" => Scalar {
                params!(Float64, Float64) => Operation::nullary(|_ecx| catalog_name_only!("pow")) => Float64, 1346;
            },
            "power" => Scalar {
                params!(Float64, Float64) => BinaryFunc::Power, 1368;
                params!(Numeric, Numeric) => BinaryFunc::PowerNumeric, 2169;
            },
            "radians" => Scalar {
                params!(Float64) => UnaryFunc::Radians(func::Radians), 1609;
            },
            "repeat" => Scalar {
                params!(String, Int32) => BinaryFunc::RepeatString, 1622;
            },
            "regexp_match" => Scalar {
                params!(String, String) => VariadicFunc::RegexpMatch => ScalarType::Array(Box::new(ScalarType::String)), 3396;
                params!(String, String, String) => VariadicFunc::RegexpMatch => ScalarType::Array(Box::new(ScalarType::String)), 3397;
            },
            "replace" => Scalar {
                params!(String, String, String) => VariadicFunc::Replace, 2087;
            },
            "right" => Scalar {
                params!(String, Int32) => BinaryFunc::Right, 3061;
            },
            "round" => Scalar {
                params!(Float32) => UnaryFunc::RoundFloat32(func::RoundFloat32), oid::FUNC_ROUND_F32_OID;
                params!(Float64) => UnaryFunc::RoundFloat64(func::RoundFloat64), 1342;
                params!(Numeric) => UnaryFunc::RoundNumeric(func::RoundNumeric), 1708;
                params!(Numeric, Int32) => BinaryFunc::RoundNumeric, 1707;
            },
            "rtrim" => Scalar {
                params!(String) => UnaryFunc::TrimTrailingWhitespace, 882;
                params!(String, String) => BinaryFunc::TrimTrailing, 876;
            },
            "sha224" => Scalar {
                params!(Bytes) => digest("sha224") => Bytes, 3419;
            },
            "sha256" => Scalar {
                params!(Bytes) => digest("sha256") => Bytes, 3420;
            },
            "sha384" => Scalar {
                params!(Bytes) => digest("sha384") => Bytes, 3421;
            },
            "sha512" => Scalar {
                params!(Bytes) => digest("sha512") => Bytes, 3422;
            },
            "sin" => Scalar {
                params!(Float64) => UnaryFunc::Sin(func::Sin), 1604;
            },
            "asin" => Scalar {
                params!(Float64) => UnaryFunc::Asin(func::Asin), 1600;
            },
            "sinh" => Scalar {
                params!(Float64) => UnaryFunc::Sinh(func::Sinh), 2462;
            },
            "asinh" => Scalar {
                params!(Float64) => UnaryFunc::Asinh(func::Asinh), 2465;
            },
            "split_part" => Scalar {
                params!(String, String, Int64) => VariadicFunc::SplitPart, 2088;
            },
            "stddev" => Scalar {
                params!(Float32) => Operation::nullary(|_ecx| catalog_name_only!("stddev")) => Float64, 2157;
                params!(Float64) => Operation::nullary(|_ecx| catalog_name_only!("stddev")) => Float64, 2158;
                params!(Int16) => Operation::nullary(|_ecx| catalog_name_only!("stddev")) => Numeric, 2156;
                params!(Int32) => Operation::nullary(|_ecx| catalog_name_only!("stddev")) => Numeric, 2155;
                params!(Int64) => Operation::nullary(|_ecx| catalog_name_only!("stddev")) => Numeric, 2154;
            },
            "stddev_pop" => Scalar {
                params!(Float32) => Operation::nullary(|_ecx| catalog_name_only!("stddev_pop")) => Float64, 2727;
                params!(Float64) => Operation::nullary(|_ecx| catalog_name_only!("stddev_pop")) => Float64, 2728;
                params!(Int16) => Operation::nullary(|_ecx| catalog_name_only!("stddev_pop")) => Numeric, 2726;
                params!(Int32) => Operation::nullary(|_ecx| catalog_name_only!("stddev_pop")) => Numeric, 2725;
                params!(Int64) => Operation::nullary(|_ecx| catalog_name_only!("stddev_pop")) => Numeric , 2724;
            },
            "stddev_samp" => Scalar {
                params!(Float32) => Operation::nullary(|_ecx| catalog_name_only!("stddev_samp")) => Float64, 2715;
                params!(Float64) => Operation::nullary(|_ecx| catalog_name_only!("stddev_samp")) => Float64, 2716;
                params!(Int16) => Operation::nullary(|_ecx| catalog_name_only!("stddev_samp")) => Numeric, 2714;
                params!(Int32) => Operation::nullary(|_ecx| catalog_name_only!("stddev_samp")) => Numeric, 2713;
                params!(Int64) => Operation::nullary(|_ecx| catalog_name_only!("stddev_samp")) => Numeric, 2712;
            },
            "substr" => Scalar {
                params!(String, Int64) => VariadicFunc::Substr, 883;
                params!(String, Int64, Int64) => VariadicFunc::Substr, 877;
            },
            "substring" => Scalar {
                params!(String, Int64) => VariadicFunc::Substr, 937;
                params!(String, Int64, Int64) => VariadicFunc::Substr, 936;
            },
            "sqrt" => Scalar {
                params!(Float64) => UnaryFunc::SqrtFloat64(func::SqrtFloat64), 1344;
                params!(Numeric) => UnaryFunc::SqrtNumeric(func::SqrtNumeric), 1730;
            },
            "tan" => Scalar {
                params!(Float64) => UnaryFunc::Tan(func::Tan), 1606;
            },
            "atan" => Scalar {
                params!(Float64) => UnaryFunc::Atan(func::Atan), 1602;
            },
            "tanh" => Scalar {
                params!(Float64) => UnaryFunc::Tanh(func::Tanh), 2464;
            },
            "atanh" => Scalar {
                params!(Float64) => UnaryFunc::Atanh(func::Atanh), 2467;
            },
            "timezone" => Scalar {
                params!(String, Timestamp) => BinaryFunc::TimezoneTimestamp, 2069;
                params!(String, TimestampTz) => BinaryFunc::TimezoneTimestampTz, 1159;
                // PG defines this as `text timetz`
                params!(String, Time) => Operation::binary(|ecx, lhs, rhs| {
                    match ecx.qcx.lifetime {
                        QueryLifetime::OneShot(pcx) => {
                            let wall_time = pcx.wall_time.naive_utc();
                            Ok(lhs.call_binary(rhs, BinaryFunc::TimezoneTime{wall_time}))
                        },
                        QueryLifetime::Static => sql_bail!("timezone cannot be used in static queries"),
                    }
                }), 2037;
                params!(Interval, Timestamp) => BinaryFunc::TimezoneIntervalTimestamp, 2070;
                params!(Interval, TimestampTz) => BinaryFunc::TimezoneIntervalTimestampTz, 1026;
                // PG defines this as `interval timetz`
                params!(Interval, Time) => BinaryFunc::TimezoneIntervalTime, 2038;
            },
            "to_char" => Scalar {
                params!(Timestamp, String) => BinaryFunc::ToCharTimestamp, 2049;
                params!(TimestampTz, String) => BinaryFunc::ToCharTimestampTz, 1770;
            },
            // > Returns the value as json or jsonb. Arrays and composites
            // > are converted (recursively) to arrays and objects;
            // > otherwise, if there is a cast from the type to json, the
            // > cast function will be used to perform the conversion;
            // > otherwise, a scalar value is produced. For any scalar type
            // > other than a number, a Boolean, or a null value, the text
            // > representation will be used, in such a fashion that it is a
            // > valid json or jsonb value.
            //
            // https://www.postgresql.org/docs/current/functions-json.html
            "to_jsonb" => Scalar {
                params!(Any) => Operation::unary(|ecx, e| {
                    // TODO(#7572): remove this
                    let e = match ecx.scalar_type(&e) {
                        ScalarType::Char { length } => e.call_unary(UnaryFunc::PadChar(func::PadChar { length })),
                        _ => e,
                    };
                    Ok(typeconv::to_jsonb(ecx, e))
                }) => Jsonb, 3787;
            },
            "to_timestamp" => Scalar {
                params!(Float64) => UnaryFunc::ToTimestamp(func::ToTimestamp), 1158;
            },
            "upper" => Scalar {
                params!(String) => UnaryFunc::Upper, 871;
            },
            "variance" => Scalar {
                params!(Float32) => Operation::nullary(|_ecx| catalog_name_only!("variance")) => Float64, 2151;
                params!(Float64) => Operation::nullary(|_ecx| catalog_name_only!("variance")) => Float64, 2152;
                params!(Int16) => Operation::nullary(|_ecx| catalog_name_only!("variance")) => Numeric, 2150;
                params!(Int32) => Operation::nullary(|_ecx| catalog_name_only!("variance")) => Numeric, 2149;
                params!(Int64) => Operation::nullary(|_ecx| catalog_name_only!("variance")) => Numeric, 2148;
            },
            "var_pop" => Scalar {
                params!(Float32) => Operation::nullary(|_ecx| catalog_name_only!("var_pop")) => Float64, 2721;
                params!(Float64) => Operation::nullary(|_ecx| catalog_name_only!("var_pop")) => Float64, 2722;
                params!(Int16) => Operation::nullary(|_ecx| catalog_name_only!("var_pop")) => Numeric, 2720;
                params!(Int32) => Operation::nullary(|_ecx| catalog_name_only!("var_pop")) => Numeric, 2719;
                params!(Int64) => Operation::nullary(|_ecx| catalog_name_only!("var_pop")) => Numeric, 2718;
            },
            "var_samp" => Scalar {
                params!(Float32) => Operation::nullary(|_ecx| catalog_name_only!("var_samp")) => Float64, 2644;
                params!(Float64) => Operation::nullary(|_ecx| catalog_name_only!("var_samp")) => Float64, 2645;
                params!(Int16) => Operation::nullary(|_ecx| catalog_name_only!("var_samp")) => Numeric, 2643;
                params!(Int32) => Operation::nullary(|_ecx| catalog_name_only!("var_samp")) => Numeric, 2642;
                params!(Int64) => Operation::nullary(|_ecx| catalog_name_only!("var_samp")) => Numeric, 2641;
            },
            "version" => Scalar {
                params!() => NullaryFunc::Version, 89;
            },

            // Aggregates.
            "array_agg" => Aggregate {
                params!(NonVecAny) => Operation::unary_ordered(|ecx, e, order_by| {
                    let elem_type = ecx.scalar_type(&e);
                    if let ScalarType::Char {.. } | ScalarType::Map { .. } = elem_type {
                        bail_unsupported!(format!("array_agg on {}", ecx.humanize_scalar_type(&elem_type)));
                    };
                    // ArrayConcat excepts all inputs to be arrays, so wrap all input datums into
                    // arrays.
                    let e_arr = HirScalarExpr::CallVariadic{
                        func: VariadicFunc::ArrayCreate { elem_type: ecx.scalar_type(&e) },
                        exprs: vec![e],
                    };
                    Ok((e_arr, AggregateFunc::ArrayConcat { order_by }))
                }) => ArrayAny, 2335;
                params!(ArrayAny) => Operation::unary(|_ecx, _e| bail_unsupported!("array_agg on arrays")) => ArrayAny, 4053;
            },
            "bool_and" => Aggregate {
                params!(Any) => Operation::unary(|_ecx, _e| bail_unsupported!("bool_and")) => Bool, 2517;
            },
            "bool_or" => Aggregate {
                params!(Any) => Operation::unary(|_ecx, _e| bail_unsupported!("bool_or")) => Bool, 2518;
            },
            "count" => Aggregate {
                params!() => Operation::nullary(|_ecx| {
                    // COUNT(*) is equivalent to COUNT(true).
                    Ok((HirScalarExpr::literal_true(), AggregateFunc::Count))
                }), 2803;
                params!(Any) => AggregateFunc::Count => Int32, 2147;
            },
            "max" => Aggregate {
                params!(Bool) => AggregateFunc::MaxBool, oid::FUNC_MAX_BOOL_OID;
                params!(Int16) => AggregateFunc::MaxInt16, 2117;
                params!(Int32) => AggregateFunc::MaxInt32, 2116;
                params!(Int64) => AggregateFunc::MaxInt64, 2115;
                params!(Float32) => AggregateFunc::MaxFloat32, 2119;
                params!(Float64) => AggregateFunc::MaxFloat64, 2120;
                params!(String) => AggregateFunc::MaxString, 2129;
                // TODO(#7572): make this its own function
                params!(Char) => AggregateFunc::MaxString, 2244;
                params!(Date) => AggregateFunc::MaxDate, 2122;
                params!(Timestamp) => AggregateFunc::MaxTimestamp, 2126;
                params!(TimestampTz) => AggregateFunc::MaxTimestampTz, 2127;
                params!(Numeric) => AggregateFunc::MaxNumeric, oid::FUNC_MAX_NUMERIC_OID;
            },
            "min" => Aggregate {
                params!(Bool) => AggregateFunc::MinBool, oid::FUNC_MIN_BOOL_OID;
                params!(Int16) => AggregateFunc::MinInt32, 2133;
                params!(Int32) => AggregateFunc::MinInt32, 2132;
                params!(Int64) => AggregateFunc::MinInt64, 2131;
                params!(Float32) => AggregateFunc::MinFloat32, 2135;
                params!(Float64) => AggregateFunc::MinFloat64, 2136;
                params!(String) => AggregateFunc::MinString, 2145;
                // TODO(#7572): make this its own function
                params!(Char) => AggregateFunc::MinString, 2245;
                params!(Date) => AggregateFunc::MinDate, 2138;
                params!(Timestamp) => AggregateFunc::MinTimestamp, 2142;
                params!(TimestampTz) => AggregateFunc::MinTimestampTz, 2143;
                params!(Numeric) => AggregateFunc::MinNumeric, oid::FUNC_MIN_NUMERIC_OID;
            },
            "json_agg" => Aggregate {
                params!(Any) => Operation::unary(|_ecx, _e| bail_unsupported!("json_agg")) => Jsonb, 3175;
            },
            "jsonb_agg" => Aggregate {
                params!(Any) => Operation::unary_ordered(|ecx, e, order_by| {
                    // TODO(#7572): remove this
                    let e = match ecx.scalar_type(&e) {
                        ScalarType::Char { length } => e.call_unary(UnaryFunc::PadChar(func::PadChar { length })),
                        _ => e,
                    };
                    // `AggregateFunc::JsonbAgg` filters out `Datum::Null` (it
                    // needs to have *some* identity input), but the semantics
                    // of the SQL function require that `Datum::Null` is treated
                    // as `Datum::JsonbNull`. This call to `coalesce` converts
                    // between the two semantics.
                    let json_null = HirScalarExpr::literal(Datum::JsonNull, ScalarType::Jsonb);
                    let e = HirScalarExpr::CallVariadic {
                        func: VariadicFunc::Coalesce,
                        exprs: vec![typeconv::to_jsonb(ecx, e), json_null],
                    };
                    Ok((e, AggregateFunc::JsonbAgg { order_by }))
                }) => Jsonb, 3267;
            },
            "jsonb_object_agg" => Aggregate {
                params!(Any, Any) => Operation::binary_ordered(|ecx, key, val, order_by| {
                    // TODO(#7572): remove this
                    let key = match ecx.scalar_type(&key) {
                        ScalarType::Char { length } => key.call_unary(UnaryFunc::PadChar(func::PadChar { length })),
                        _ => key,
                    };
                    let val = match ecx.scalar_type(&val) {
                        ScalarType::Char { length } => val.call_unary(UnaryFunc::PadChar(func::PadChar { length })),
                        _ => val,
                    };

                    let key = typeconv::to_string(ecx, key);
                    let val = typeconv::to_jsonb(ecx, val);
                    let e = HirScalarExpr::CallVariadic {
                        func: VariadicFunc::RecordCreate {
                            field_names: vec![ColumnName::from("key"), ColumnName::from("val")],
                        },
                        exprs: vec![key, val],
                    };
                    Ok((e, AggregateFunc::JsonbObjectAgg { order_by }))
                }) => Jsonb, 3270;
            },
            "string_agg" => Aggregate {
                params!(String, String) => Operation::binary_ordered(|_ecx, value, sep, order_by| {
                    let e = HirScalarExpr::CallVariadic {
                        func: VariadicFunc::RecordCreate {
                            field_names: vec![ColumnName::from("value"), ColumnName::from("sep")],
                        },
                        exprs: vec![value, sep],
                    };
                    Ok((e, AggregateFunc::StringAgg { order_by }))
                }), 3538;
                params!(Bytes, Bytes) => Operation::binary(|_ecx, _l, _r| bail_unsupported!("string_agg")) => Bytes, 3545;
            },
            "sum" => Aggregate {
                params!(Int16) => AggregateFunc::SumInt16, 2109;
                params!(Int32) => AggregateFunc::SumInt32, 2108;
                params!(Int64) => AggregateFunc::SumInt64, 2107;
                params!(Float32) => AggregateFunc::SumFloat32, 2110;
                params!(Float64) => AggregateFunc::SumFloat64, 2111;
                params!(Numeric) => AggregateFunc::SumNumeric, 2114;
                params!(Interval) => Operation::unary(|_ecx, _e| {
                    // Explicitly providing this unsupported overload
                    // prevents `sum(NULL)` from choosing the `Float64`
                    // implementation, so that we match PostgreSQL's behavior.
                    // Plus we will one day want to support this overload.
                    bail_unsupported!("sum(interval)");
                }) => Interval, 2113;
            },

            // Scalar window functions.
            "row_number" => ScalarWindow {
                params!() => ScalarWindowFunc::RowNumber, 3100;
            },

            // Table functions.
            "generate_series" => Table {
                params!(Int32, Int32, Int32) => Operation::variadic(move |_ecx, exprs| {
                    Ok(TableFuncPlan {
                        expr: HirRelationExpr::CallTable {
                            func: TableFunc::GenerateSeriesInt32,
                            exprs,
                        },
                        column_names: vec!["generate_series".into()],
                    })
                }), 1066;
                params!(Int32, Int32) => Operation::binary(move |_ecx, start, stop| {
                    Ok(TableFuncPlan {
                        expr: HirRelationExpr::CallTable {
                            func: TableFunc::GenerateSeriesInt32,
                            exprs: vec![start, stop, HirScalarExpr::literal(Datum::Int32(1), ScalarType::Int32)],
                        },
                        column_names: vec!["generate_series".into()],
                    })
                }), 1067;
                params!(Int64, Int64, Int64) => Operation::variadic(move |_ecx, exprs| {
                    Ok(TableFuncPlan {
                        expr: HirRelationExpr::CallTable {
                            func: TableFunc::GenerateSeriesInt64,
                            exprs,
                        },
                        column_names: vec!["generate_series".into()],
                    })
                }), 1068;
                params!(Int64, Int64) => Operation::binary(move |_ecx, start, stop| {
                    let row = Row::pack(&[Datum::Int64(1)]);
                    let column_type = ColumnType { scalar_type: ScalarType::Int64, nullable: false };
                    Ok(TableFuncPlan {
                        expr: HirRelationExpr::CallTable {
                            func: TableFunc::GenerateSeriesInt64,
                            exprs: vec![start, stop, HirScalarExpr::Literal(row, column_type)],
                        },
                        column_names: vec!["generate_series".into()],
                    })
                }), 1069;
                params!(Timestamp, Timestamp, Interval) => Operation::variadic(move |_ecx, exprs| {
                    Ok(TableFuncPlan {
                        expr: HirRelationExpr::CallTable {
                            func: TableFunc::GenerateSeriesTimestamp,
                            exprs,
                        },
                        column_names: vec!["generate_series".into()],
                    })
                }), 938;
                params!(TimestampTz, TimestampTz, Interval) => Operation::variadic(move |_ecx, exprs| {
                    Ok(TableFuncPlan {
                        expr: HirRelationExpr::CallTable {
                            func: TableFunc::GenerateSeriesTimestampTz,
                            exprs,
                        },
                        column_names: vec!["generate_series".into()],
                    })
                }), 939;
            },

            "generate_subscripts" => Table {
                params!(ArrayAny, Int32) => Operation::variadic(move |_ecx, exprs| {
                    Ok(TableFuncPlan {
                        expr: HirRelationExpr::CallTable {
                            func: TableFunc::GenerateSubscriptsArray,
                            exprs,
                        },
                        column_names: vec!["generate_subscripts".into()],
                    })
                }) => ReturnType::set_of(Int32.into()), 1192;
            },

            "jsonb_array_elements" => Table {
                params!(Jsonb) => Operation::unary(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        expr: HirRelationExpr::CallTable {
                            func: TableFunc::JsonbArrayElements { stringify: false },
                            exprs: vec![jsonb],
                        },
                        column_names: vec!["value".into()],
                    })
                }), 3219;
            },
            "jsonb_array_elements_text" => Table {
                params!(Jsonb) => Operation::unary(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        expr: HirRelationExpr::CallTable {
                            func: TableFunc::JsonbArrayElements { stringify: true },
                            exprs: vec![jsonb],
                        },
                        column_names: vec!["value".into()],
                    })
                }), 3465;
            },
            "jsonb_each" => Table {
                params!(Jsonb) => Operation::unary(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        expr: HirRelationExpr::CallTable {
                            func: TableFunc::JsonbEach { stringify: false },
                            exprs: vec![jsonb],
                        },
                        column_names: vec!["key".into(), "value".into()],
                    })
                }), 3208;
            },
            "jsonb_each_text" => Table {
                params!(Jsonb) => Operation::unary(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        expr: HirRelationExpr::CallTable {
                            func: TableFunc::JsonbEach { stringify: true },
                            exprs: vec![jsonb],
                        },
                        column_names: vec!["key".into(), "value".into()],
                    })
                }), 3932;
            },
            "jsonb_object_keys" => Table {
                params!(Jsonb) => Operation::unary(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        expr: HirRelationExpr::CallTable {
                            func: TableFunc::JsonbObjectKeys,
                            exprs: vec![jsonb],
                        },
                        column_names: vec!["jsonb_object_keys".into()],
                    })
                }), 3931;
            },
            // Note that these implementations' input to `generate_series` is
            // contrived to match Flink's expected values. There are other,
            // equally valid windows we could generate.
            "date_bin_hopping" => Table {
                // (hop, width, timestamp)
                params!(Interval, Interval, Timestamp) => experimental_sql_impl_table_func("date_bin_hopping", "
                    SELECT *
                    FROM pg_catalog.generate_series(
                        pg_catalog.date_bin($1, $3 + $1, '1970-01-01') - $2, $3, $1
                    ) AS dbh(date_bin_hopping)
                ") => ReturnType::set_of(Timestamp.into()), oid::FUNC_MZ_DATE_BIN_HOPPING_UNIX_EPOCH_TS_OID;
                // (hop, width, timestamp)
                params!(Interval, Interval, TimestampTz) => experimental_sql_impl_table_func("date_bin_hopping", "
                    SELECT *
                    FROM pg_catalog.generate_series(
                        pg_catalog.date_bin($1, $3 + $1, '1970-01-01') - $2, $3, $1
                    ) AS dbh(date_bin_hopping)
                ") => ReturnType::set_of(TimestampTz.into()), oid::FUNC_MZ_DATE_BIN_HOPPING_UNIX_EPOCH_TSTZ_OID;
                // (hop, width, timestamp, origin)
                params!(Interval, Interval, Timestamp, Timestamp) => experimental_sql_impl_table_func("date_bin_hopping", "
                    SELECT *
                    FROM pg_catalog.generate_series(
                        pg_catalog.date_bin($1, $3 + $1, $4) - $2, $3, $1
                    ) AS dbh(date_bin_hopping)
                ") => ReturnType::set_of(Timestamp.into()), oid::FUNC_MZ_DATE_BIN_HOPPING_TS_OID;
                // (hop, width, timestamp, origin)
                params!(Interval, Interval, TimestampTz, TimestampTz) => experimental_sql_impl_table_func("date_bin_hopping", "
                    SELECT *
                    FROM pg_catalog.generate_series(
                        pg_catalog.date_bin($1, $3 + $1, $4) - $2, $3, $1
                    ) AS dbh(date_bin_hopping)
                ") => ReturnType::set_of(TimestampTz.into()), oid::FUNC_MZ_DATE_BIN_HOPPING_TSTZ_OID;
            },
            "encode" => Scalar {
                params!(Bytes, String) => BinaryFunc::Encode, 1946;
            },
            "decode" => Scalar {
                params!(String, String) => BinaryFunc::Decode, 1947;
            }
        }
    };

    pub static ref INFORMATION_SCHEMA_BUILTINS: HashMap<&'static str, Func> = {
        use ParamType::*;
        builtins! {
            "_pg_expandarray" => Table {
                // See: https://github.com/postgres/postgres/blob/16e3ad5d143795b05a21dc887c2ab384cce4bcb8/src/backend/catalog/information_schema.sql#L43
                params!(ArrayAny) => sql_impl_table_func("
                    SELECT
                        $1[s] AS x,
                        s - pg_catalog.array_lower($1, 1) + 1 AS n
                    FROM pg_catalog.generate_series(
                        pg_catalog.array_lower($1, 1),
                        pg_catalog.array_upper($1, 1),
                        1) as g(s)
                ") => ReturnType::set_of(RecordAny), oid::FUNC_PG_EXPAND_ARRAY;
            }
        }
    };

    pub static ref MZ_CATALOG_BUILTINS: HashMap<&'static str, Func> = {
        use ScalarType::*;
        use ParamType::*;
        builtins! {
            "csv_extract" => Table {
                params!(Int64, String) => Operation::binary(move |_ecx, ncols, input| {
                    let ncols = match ncols.into_literal_int64() {
                        None | Some(i64::MIN..=0) => {
                            sql_bail!("csv_extract number of columns must be a positive integer literal");
                        },
                        Some(ncols) => ncols,
                    };
                    let ncols = usize::try_from(ncols).expect("known to be greater than zero");
                    Ok(TableFuncPlan {
                        expr: HirRelationExpr::CallTable {
                            func: TableFunc::CsvExtract(ncols),
                            exprs: vec![input],
                        },
                        column_names: (1..=ncols).map(|i| format!("column{}", i).into()).collect(),
                    })
                }) => ReturnType::set_of(RecordAny), oid::FUNC_CSV_EXTRACT_OID;
            },
            "concat_agg" => Aggregate {
                params!(Any) => Operation::unary(|_ecx, _e| bail_unsupported!("concat_agg")) => String, oid::FUNC_CONCAT_AGG_OID;
            },
            "current_timestamp" => Scalar {
                params!() => NullaryFunc::CurrentTimestamp, oid::FUNC_CURRENT_TIMESTAMP_OID;
            },
            "list_agg" => Aggregate {
                params!(Any) => Operation::unary_ordered(|ecx, e, order_by| {
                    if let ScalarType::Char {.. }  = ecx.scalar_type(&e) {
                        bail_unsupported!("list_agg on char");
                    };
                    // ListConcat excepts all inputs to be lists, so wrap all input datums into
                    // lists.
                    let e_arr = HirScalarExpr::CallVariadic{
                        func: VariadicFunc::ListCreate { elem_type: ecx.scalar_type(&e) },
                        exprs: vec![e],
                    };
                    Ok((e_arr, AggregateFunc::ListConcat { order_by }))
                }) => ListAny,  oid::FUNC_LIST_AGG_OID;
            },
            "list_append" => Scalar {
                vec![ListAny, ListElementAny] => BinaryFunc::ListElementConcat => ListAny, oid::FUNC_LIST_APPEND_OID;
            },
            "list_cat" => Scalar {
                vec![ListAny, ListAny] => BinaryFunc::ListListConcat => ListAny, oid::FUNC_LIST_CAT_OID;
            },
            "list_n_layers" => Scalar {
                vec![ListAny] => Operation::unary(|ecx, e| {
                    ecx.require_experimental_mode("list_n_layers")?;
                    let d = ecx.scalar_type(&e).unwrap_list_n_layers();
                    Ok(HirScalarExpr::literal(Datum::Int32(d as i32), ScalarType::Int32))
                }) => Int32, oid::FUNC_LIST_N_LAYERS_OID;
            },
            "list_length" => Scalar {
                vec![ListAny] => UnaryFunc::ListLength => Int32, oid::FUNC_LIST_LENGTH_OID;
            },
            "list_length_max" => Scalar {
                vec![ListAny, Plain(Int64)] => Operation::binary(|ecx, lhs, rhs| {
                    ecx.require_experimental_mode("list_length_max")?;
                    let max_layer = ecx.scalar_type(&lhs).unwrap_list_n_layers();
                    Ok(lhs.call_binary(rhs, BinaryFunc::ListLengthMax { max_layer }))
                }) => Int32, oid::FUNC_LIST_LENGTH_MAX_OID;
            },
            "list_prepend" => Scalar {
                vec![ListElementAny, ListAny] => BinaryFunc::ElementListConcat => ListAny, oid::FUNC_LIST_PREPEND_OID;
            },
            "list_remove" => Scalar {
                vec![ListAny, ListElementAny] => Operation::binary(|ecx, lhs, rhs| {
                    ecx.require_experimental_mode("list_remove")?;
                    Ok(lhs.call_binary(rhs, BinaryFunc::ListRemove))
                }) => ListAny, oid::FUNC_LIST_REMOVE_OID;
            },
            "mz_cluster_id" => Scalar {
                params!() => NullaryFunc::MzClusterId, oid::FUNC_MZ_CLUSTER_ID_OID;
            },
            "mz_logical_timestamp" => Scalar {
                params!() => NullaryFunc::MzLogicalTimestamp, oid::FUNC_MZ_LOGICAL_TIMESTAMP_OID;
            },
            "mz_uptime" => Scalar {
                params!() => NullaryFunc::MzUptime, oid::FUNC_MZ_UPTIME_OID;
            },
            "mz_version" => Scalar {
                params!() => NullaryFunc::MzVersion, oid::FUNC_MZ_VERSION_OID;
            },
            "regexp_extract" => Table {
                params!(String, String) => Operation::binary(move |_ecx, regex, haystack| {
                    let regex = match regex.into_literal_string() {
                        None => sql_bail!("regex_extract requires a string literal as its first argument"),
                        Some(regex) => mz_expr::AnalyzedRegex::new(&regex).map_err(|e| PlanError::Unstructured(format!("analyzing regex: {}", e)))?,
                    };
                    let column_names = regex
                        .capture_groups_iter()
                        .map(|cg| {
                            cg.name.clone().unwrap_or_else(|| format!("column{}", cg.index)).into()
                        })
                        .collect();
                    Ok(TableFuncPlan {
                        expr: HirRelationExpr::CallTable {
                            func: TableFunc::RegexpExtract(regex),
                            exprs: vec![haystack],
                        },
                        column_names,
                    })
                }) => ReturnType::set_of(RecordAny), oid::FUNC_REGEXP_EXTRACT_OID;
            },
            "repeat_row" => Table {
                params!(Int64) => Operation::unary(move |ecx, n| {
                    ecx.require_experimental_mode("repeat_row")?;
                    Ok(TableFuncPlan {
                        expr: HirRelationExpr::CallTable {
                            func: TableFunc::Repeat,
                            exprs: vec![n],
                        },
                        column_names: vec![]
                    })
                }), oid::FUNC_REPEAT_OID;
            },
            "unnest" => Table {
                vec![ArrayAny] => Operation::unary(move |ecx, e| {
                    let el_typ = ecx.scalar_type(&e).unwrap_array_element_type().clone();
                    Ok(TableFuncPlan {
                        expr: HirRelationExpr::CallTable {
                            func: TableFunc::UnnestArray { el_typ },
                            exprs: vec![e],
                        },
                        column_names: vec!["unnest".into()],
                    })
                }) => ReturnType::set_of(ListElementAny), 2331;
                vec![ListAny] => Operation::unary(move |ecx, e| {
                    let el_typ = ecx.scalar_type(&e).unwrap_list_element_type().clone();
                    Ok(TableFuncPlan {
                        expr: HirRelationExpr::CallTable {
                            func: TableFunc::UnnestList { el_typ },
                            exprs: vec![e],
                        },
                        column_names: vec!["unnest".into()],
                    })
                }) => ReturnType::set_of(ListElementAny), oid::FUNC_UNNEST_LIST_OID;
            }
        }
    };


    pub static ref MZ_INTERNAL_BUILTINS: HashMap<&'static str, Func> = {
        use ParamType::*;
        use ScalarType::*;
        builtins! {
            "mz_all" => Aggregate {
                params!(Any) => AggregateFunc::All => Bool, oid::FUNC_MZ_ALL_OID;
            },
            "mz_any" => Aggregate {
                params!(Any) => AggregateFunc::Any => Bool, oid::FUNC_MZ_ANY_OID;
            },
            "mz_avg_promotion" => Scalar {
                // Promotes a numeric type to the smallest fractional type that
                // can represent it. This is primarily useful for the avg
                // aggregate function, so that the avg of an integer column does
                // not get truncated to an integer, which would be surprising to
                // users (#549).
                params!(Float32) => Operation::identity(), oid::FUNC_MZ_AVG_PROMOTION_F32_OID;
                params!(Float64) => Operation::identity(), oid::FUNC_MZ_AVG_PROMOTION_F64_OID;
                params!(Int16) => Operation::unary(|ecx, e| {
                    typeconv::plan_cast(
                        ecx, CastContext::Explicit, e, &ScalarType::Numeric {max_scale: None},
                    )
                }), oid::FUNC_MZ_AVG_PROMOTION_I16_OID;
                params!(Int32) => Operation::unary(|ecx, e| {
                    typeconv::plan_cast(
                        ecx, CastContext::Explicit, e, &ScalarType::Numeric {max_scale: None},
                    )
                }), oid::FUNC_MZ_AVG_PROMOTION_I32_OID;
            },
            "mz_classify_object_id" => Scalar {
                params!(String) => sql_impl_func(
                    "CASE
                        WHEN $1 LIKE 'u%' THEN 'user'
                        WHEN $1 LIKE 's%' THEN 'system'
                        WHEN $1 like 't%' THEN 'temp'
                    END"
                ) => String, oid::FUNC_MZ_CLASSIFY_OBJECT_ID_OID;
            },
            "mz_error_if_null" => Scalar {
                // If the first argument is NULL, returns an EvalError::Internal whose error
                // message is the second argument.
                params!(Any, String) => VariadicFunc::ErrorIfNull => Any, oid::FUNC_MZ_ERROR_IF_NULL_OID;
            },
            "mz_is_materialized" => Scalar {
                params!(String) => sql_impl_func("EXISTS (SELECT 1 FROM mz_indexes WHERE on_id = $1 AND enabled)") => Bool,
                    oid::FUNC_MZ_IS_MATERIALIZED_OID;
            },
            "mz_render_typmod" => Scalar {
                params!(Oid, Int32) => BinaryFunc::MzRenderTypmod, oid::FUNC_MZ_RENDER_TYPMOD_OID;
            },
            // This ought to be exposed in `mz_catalog`, but its name is rather
            // confusing. It does not identify the SQL session, but the
            // invocation of this `materialized` process.
            "mz_session_id" => Scalar {
                params!() => NullaryFunc::MzSessionId, oid::FUNC_MZ_SESSION_ID_OID;
            },
            "mz_sleep" => Scalar {
                params!(Float64) => UnaryFunc::Sleep(func::Sleep), oid::FUNC_MZ_SLEEP_OID;
            },
            "mz_type_name" => Scalar {
                params!(Oid) => UnaryFunc::MzTypeName(func::MzTypeName), oid::FUNC_MZ_TYPE_NAME;
            }
        }
    };
}

fn digest(algorithm: &'static str) -> Operation<HirScalarExpr> {
    Operation::unary(move |_ecx, input| {
        let algorithm = HirScalarExpr::literal(Datum::String(algorithm), ScalarType::String);
        Ok(input.call_binary(algorithm, BinaryFunc::DigestBytes))
    })
}

fn array_to_string(
    ecx: &ExprContext,
    exprs: Vec<HirScalarExpr>,
) -> Result<HirScalarExpr, PlanError> {
    let elem_type = match ecx.scalar_type(&exprs[0]) {
        ScalarType::Array(elem_type) => *elem_type,
        _ => unreachable!("array_to_string is guaranteed to receive array as first argument"),
    };
    Ok(HirScalarExpr::CallVariadic {
        func: VariadicFunc::ArrayToString { elem_type },
        exprs,
    })
}

lazy_static! {
    /// Correlates an operator with all of its implementations.
    static ref OP_IMPLS: HashMap<&'static str, Func> = {
        use ScalarBaseType::*;
        use BinaryFunc::*;
        use ParamType::*;
        builtins! {
            // Literal OIDs collected from PG 13 using a version of this query
            // ```sql
            // SELECT
            //     oid,
            //     oprname,
            //     oprleft::regtype,
            //     oprright::regtype
            // FROM
            //     pg_operator
            // WHERE
            //     oprname IN (
            //         '+', '-', '*', '/', '%',
            //         '|', '&', '#', '~', '<<', '>>',
            //         '~~', '!~~'
            //     )
            // ORDER BY
            //     oprname;
            // ```
            // Values are also available through
            // https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_operator.dat

            // ARITHMETIC
            "+" => Scalar {
                params!(Any) => Operation::new(|ecx, exprs, _params, _order_by| {
                    // Unary plus has unusual compatibility requirements.
                    //
                    // In PostgreSQL, it is only defined for numeric types, so
                    // `+$1` and `+'1'` get coerced to `Float64` per the usual
                    // rules, but `+'1'::text` is rejected.
                    //
                    // In SQLite, unary plus can be applied to *any* type, and
                    // is always the identity function.
                    //
                    // To try to be compatible with both PostgreSQL and SQlite,
                    // we accept explicitly-typed arguments of any type, but try
                    // to coerce unknown-type arguments as `Float64`.
                    typeconv::plan_coerce(ecx, exprs.into_element(), &ScalarType::Float64)
                }) => Any, oid::OP_UNARY_PLUS_OID;
                params!(Int16, Int16) => AddInt16, 550;
                params!(Int32, Int32) => AddInt32, 551;
                params!(Int64, Int64) => AddInt64, 684;
                params!(Float32, Float32) => AddFloat32, 586;
                params!(Float64, Float64) => AddFloat64, 591;
                params!(Interval, Interval) => AddInterval, 1337;
                params!(Timestamp, Interval) => AddTimestampInterval, 2066;
                params!(Interval, Timestamp) => {
                    Operation::binary(|_ecx, lhs, rhs| Ok(rhs.call_binary(lhs, AddTimestampInterval)))
                }, 2066;
                params!(TimestampTz, Interval) => AddTimestampTzInterval, 1327;
                params!(Interval, TimestampTz) => {
                    Operation::binary(|_ecx, lhs, rhs| Ok(rhs.call_binary(lhs, AddTimestampTzInterval)))
                }, 2554;
                params!(Date, Interval) => AddDateInterval, 1076;
                params!(Interval, Date) => {
                    Operation::binary(|_ecx, lhs, rhs| Ok(rhs.call_binary(lhs, AddDateInterval)))
                }, 2551;
                params!(Date, Time) => AddDateTime, 1360;
                params!(Time, Date) => {
                    Operation::binary(|_ecx, lhs, rhs| Ok(rhs.call_binary(lhs, AddDateTime)))
                }, 1363;
                params!(Time, Interval) => AddTimeInterval, 1800;
                params!(Interval, Time) => {
                    Operation::binary(|_ecx, lhs, rhs| Ok(rhs.call_binary(lhs, AddTimeInterval)))
                }, 1849;
                params!(Numeric, Numeric) => AddNumeric, 1758;
            },
            "-" => Scalar {
                params!(Int16) => UnaryFunc::NegInt16(func::NegInt16), 559;
                params!(Int32) => UnaryFunc::NegInt32(func::NegInt32), 558;
                params!(Int64) => UnaryFunc::NegInt64(func::NegInt64), 484;
                params!(Float32) => UnaryFunc::NegFloat32(func::NegFloat32), 584;
                params!(Float64) => UnaryFunc::NegFloat64(func::NegFloat64), 585;
                params!(Numeric) => UnaryFunc::NegNumeric(func::NegNumeric), 17510;
                params!(Interval) => UnaryFunc::NegInterval(func::NegInterval), 1336;
                params!(Int32, Int32) => SubInt32, 555;
                params!(Int64, Int64) => SubInt64, 685;
                params!(Float32, Float32) => SubFloat32, 587;
                params!(Float64, Float64) => SubFloat64, 592;
                params!(Numeric, Numeric) => SubNumeric, 17590;
                params!(Interval, Interval) => SubInterval, 1338;
                params!(Timestamp, Timestamp) => SubTimestamp, 2067;
                params!(TimestampTz, TimestampTz) => SubTimestampTz, 1328;
                params!(Timestamp, Interval) => SubTimestampInterval, 2068;
                params!(TimestampTz, Interval) => SubTimestampTzInterval, 1329;
                params!(Date, Date) => SubDate, 1099;
                params!(Date, Interval) => SubDateInterval, 1077;
                params!(Time, Time) => SubTime, 1399;
                params!(Time, Interval) => SubTimeInterval, 1801;
                params!(Jsonb, Int64) => JsonbDeleteInt64, 3286;
                params!(Jsonb, String) => JsonbDeleteString, 3285;
                // TODO(jamii) there should be corresponding overloads for
                // Array(Int64) and Array(String)
            },
            "*" => Scalar {
                params!(Int16, Int16) => MulInt16, 526;
                params!(Int32, Int32) => MulInt32, 514;
                params!(Int64, Int64) => MulInt64, 686;
                params!(Float32, Float32) => MulFloat32, 589;
                params!(Float64, Float64) => MulFloat64, 594;
                params!(Interval, Float64) => MulInterval, 1583;
                params!(Float64, Interval) => {
                    Operation::binary(|_ecx, lhs, rhs| Ok(rhs.call_binary(lhs, MulInterval)))
                }, 1584;
                params!(Numeric, Numeric) => MulNumeric, 1760;
            },
            "/" => Scalar {
                params!(Int16, Int16) => DivInt16, 527;
                params!(Int32, Int32) => DivInt32, 528;
                params!(Int64, Int64) => DivInt64, 687;
                params!(Float32, Float32) => DivFloat32, 588;
                params!(Float64, Float64) => DivFloat64, 593;
                params!(Interval, Float64) => DivInterval, 1585;
                params!(Numeric, Numeric) => DivNumeric, 1761;
            },
            "%" => Scalar {
                params!(Int16, Int16) => ModInt16, 529;
                params!(Int32, Int32) => ModInt32, 530;
                params!(Int64, Int64) => ModInt64, 439;
                params!(Float32, Float32) => ModFloat32, oid::OP_MOD_F32_OID;
                params!(Float64, Float64) => ModFloat64, oid::OP_MOD_F64_OID;
                params!(Numeric, Numeric) => ModNumeric, 1762;
            },
            "&" => Scalar {
                params!(Int16, Int16) => BitAndInt16, 1874;
                params!(Int32, Int32) => BitAndInt32, 1880;
                params!(Int64, Int64) => BitAndInt64, 1886;
            },
            "|" => Scalar {
                params!(Int16, Int16) => BitOrInt16, 1875;
                params!(Int32, Int32) => BitOrInt32, 1881;
                params!(Int64, Int64) => BitOrInt64, 1887;
            },
            "#" => Scalar {
                params!(Int16, Int16) => BitXorInt16, 1876;
                params!(Int32, Int32) => BitXorInt32, 1882;
                params!(Int64, Int64) => BitXorInt64, 1888;
            },
            "<<" => Scalar {
                params!(Int16, Int32) => BitShiftLeftInt16, 1878;
                params!(Int32, Int32) => BitShiftLeftInt32, 1884;
                params!(Int64, Int32) => BitShiftLeftInt64, 1890;
            },
            ">>" => Scalar {
                params!(Int16, Int32) => BitShiftRightInt16, 1879;
                params!(Int32, Int32) => BitShiftRightInt32, 1885;
                params!(Int64, Int32) => BitShiftRightInt64, 1891;
            },

            // ILIKE
            "~~*" => Scalar {
                params!(String, String) => IsLikeMatch { case_insensitive: true }, 1627;
                params!(Char, String) => Operation::binary(|ecx, lhs, rhs| {
                    let length = ecx.scalar_type(&lhs).unwrap_char_length();
                    Ok(lhs.call_unary(UnaryFunc::PadChar(func::PadChar { length }))
                        .call_binary(rhs, IsLikeMatch { case_insensitive: true })
                    )
                }), 1629;
            },
            "!~~*" => Scalar {
                params!(String, String) => Operation::binary(|_ecx, lhs, rhs| {
                    Ok(lhs
                        .call_binary(rhs, IsLikeMatch { case_insensitive: true })
                        .call_unary(UnaryFunc::Not(func::Not)))
                }) => Bool, 1628;
                params!(Char, String) => Operation::binary(|ecx, lhs, rhs| {
                    let length = ecx.scalar_type(&lhs).unwrap_char_length();
                    Ok(lhs.call_unary(UnaryFunc::PadChar(func::PadChar { length }))
                        .call_binary(rhs, IsLikeMatch { case_insensitive: false })
                        .call_unary(UnaryFunc::Not(func::Not))
                    )
                }) => Bool, 1630;
            },


            // LIKE
            "~~" => Scalar {
                params!(String, String) => IsLikeMatch { case_insensitive: false }, 1209;
                params!(Char, String) => Operation::binary(|ecx, lhs, rhs| {
                    let length = ecx.scalar_type(&lhs).unwrap_char_length();
                    Ok(lhs.call_unary(UnaryFunc::PadChar(func::PadChar { length }))
                        .call_binary(rhs, IsLikeMatch { case_insensitive: false })
                    )
                }), 1211;
            },
            "!~~" => Scalar {
                params!(String, String) => Operation::binary(|_ecx, lhs, rhs| {
                    Ok(lhs
                        .call_binary(rhs, IsLikeMatch { case_insensitive: false })
                        .call_unary(UnaryFunc::Not(func::Not)))
                }) => Bool, 1210;
                params!(Char, String) => Operation::binary(|ecx, lhs, rhs| {
                    let length = ecx.scalar_type(&lhs).unwrap_char_length();
                    Ok(lhs.call_unary(UnaryFunc::PadChar(func::PadChar { length }))
                        .call_binary(rhs, IsLikeMatch { case_insensitive: false })
                        .call_unary(UnaryFunc::Not(func::Not))
                    )
                }) => Bool, 1212;
            },

            // REGEX
            "~" => Scalar {
                params!(Int16) => UnaryFunc::BitNotInt16(func::BitNotInt16), 1877;
                params!(Int32) => UnaryFunc::BitNotInt32(func::BitNotInt32), 1883;
                params!(Int64) => UnaryFunc::BitNotInt64(func::BitNotInt64), 1889;
                params!(String, String) => IsRegexpMatch { case_insensitive: false }, 641;
                params!(Char, String) => Operation::binary(|ecx, lhs, rhs| {
                    let length = ecx.scalar_type(&lhs).unwrap_char_length();
                    Ok(lhs.call_unary(UnaryFunc::PadChar(func::PadChar { length }))
                        .call_binary(rhs, IsRegexpMatch { case_insensitive: false })
                    )
                }), 1055;
            },
            "~*" => Scalar {
                params!(String, String) => Operation::binary(|_ecx, lhs, rhs| {
                    Ok(lhs.call_binary(rhs, IsRegexpMatch { case_insensitive: true }))
                }), 1228;
                params!(Char, String) => Operation::binary(|ecx, lhs, rhs| {
                    let length = ecx.scalar_type(&lhs).unwrap_char_length();
                    Ok(lhs.call_unary(UnaryFunc::PadChar(func::PadChar { length }))
                        .call_binary(rhs, IsRegexpMatch { case_insensitive: true })
                    )
                }), 1234;
            },
            "!~" => Scalar {
                params!(String, String) => Operation::binary(|_ecx, lhs, rhs| {
                    Ok(lhs
                        .call_binary(rhs, IsRegexpMatch { case_insensitive: false })
                        .call_unary(UnaryFunc::Not(func::Not)))
                }) => Bool, 642;
                params!(Char, String) => Operation::binary(|ecx, lhs, rhs| {
                    let length = ecx.scalar_type(&lhs).unwrap_char_length();
                    Ok(lhs.call_unary(UnaryFunc::PadChar(func::PadChar { length }))
                        .call_binary(rhs, IsRegexpMatch { case_insensitive: true })
                        .call_unary(UnaryFunc::Not(func::Not))
                    )
                }) => Bool, 1056;
            },
            "!~*" => Scalar {
                params!(String, String) => Operation::binary(|_ecx, lhs, rhs| {
                    Ok(lhs
                        .call_binary(rhs, IsRegexpMatch { case_insensitive: true })
                        .call_unary(UnaryFunc::Not(func::Not)))
                }) => Bool, 1229;
                params!(Char, String) => Operation::binary(|ecx, lhs, rhs| {
                    let length = ecx.scalar_type(&lhs).unwrap_char_length();
                    Ok(lhs.call_unary(UnaryFunc::PadChar(func::PadChar { length }))
                        .call_binary(rhs, IsRegexpMatch { case_insensitive: true })
                        .call_unary(UnaryFunc::Not(func::Not))
                    )
                }) => Bool, 1235;
            },

            // CONCAT
            "||" => Scalar {
                params!(String, NonVecAny) => Operation::binary(|ecx, lhs, rhs| {
                    let rhs = typeconv::plan_cast(
                        ecx,
                        CastContext::Explicit,
                        rhs,
                        &ScalarType::String,
                    )?;
                    Ok(lhs.call_binary(rhs, TextConcat))
                }) => String, 2779;
                params!(NonVecAny, String) => Operation::binary(|ecx, lhs, rhs| {
                    let lhs = typeconv::plan_cast(
                        ecx,
                        CastContext::Explicit,
                        lhs,
                        &ScalarType::String,
                    )?;
                    Ok(lhs.call_binary(rhs, TextConcat))
                }) => String, 2780;
                params!(String, String) => TextConcat, 654;
                params!(Jsonb, Jsonb) => JsonbConcat, 3284;
                params!(ArrayAny, ArrayAny) => ArrayArrayConcat => ArrayAny, 375;
                params!(ListAny, ListAny) => ListListConcat => ListAny, oid::OP_CONCAT_LIST_LIST_OID;
                params!(ListAny, ListElementAny) => ListElementConcat => ListAny, oid::OP_CONCAT_LIST_ELEMENT_OID;
                params!(ListElementAny, ListAny) => ElementListConcat => ListAny, oid::OP_CONCAT_ELEMENY_LIST_OID;
            },

            //JSON and MAP
            "->" => Scalar {
                params!(Jsonb, Int64) => JsonbGetInt64 { stringify: false }, 3212;
                params!(Jsonb, String) => JsonbGetString { stringify: false }, 3211;
                params!(MapAny, String) => MapGetValue => Any, oid::OP_GET_VALUE_MAP_OID;
                params!(MapAny, ScalarType::Array(Box::new(ScalarType::String))) => MapGetValues => ArrayAny, oid::OP_GET_VALUES_MAP_OID;
            },
            "->>" => Scalar {
                params!(Jsonb, Int64) => JsonbGetInt64 { stringify: true }, 3481;
                params!(Jsonb, String) => JsonbGetString { stringify: true }, 3477;
            },
            "#>" => Scalar {
                params!(Jsonb, ScalarType::Array(Box::new(ScalarType::String))) => JsonbGetPath { stringify: false }, 3213;
            },
            "#>>" => Scalar {
                params!(Jsonb, ScalarType::Array(Box::new(ScalarType::String))) => JsonbGetPath { stringify: true }, 3206;
            },
            "@>" => Scalar {
                params!(Jsonb, Jsonb) => JsonbContainsJsonb, 3246;
                params!(Jsonb, String) => Operation::binary(|_ecx, lhs, rhs| {
                    Ok(lhs.call_binary(
                        rhs.call_unary(UnaryFunc::CastStringToJsonb),
                        JsonbContainsJsonb,
                    ))
                }), oid::OP_CONTAINS_JSONB_STRING_OID;
                params!(String, Jsonb) => Operation::binary(|_ecx, lhs, rhs| {
                    Ok(lhs.call_unary(UnaryFunc::CastStringToJsonb)
                          .call_binary(rhs, JsonbContainsJsonb))
                }), oid::OP_CONTAINS_STRING_JSONB_OID;
                params!(MapAny, MapAny) => MapContainsMap => Bool, oid::OP_CONTAINS_MAP_MAP_OID;
            },
            "<@" => Scalar {
                params!(Jsonb, Jsonb) => Operation::binary(|_ecx, lhs, rhs| {
                    Ok(rhs.call_binary(
                        lhs,
                        JsonbContainsJsonb
                    ))
                }), 3246;
                params!(Jsonb, String) => Operation::binary(|_ecx, lhs, rhs| {
                    Ok(rhs.call_unary(UnaryFunc::CastStringToJsonb)
                          .call_binary(lhs, BinaryFunc::JsonbContainsJsonb))
                }), oid::OP_CONTAINED_JSONB_STRING_OID;
                params!(String, Jsonb) => Operation::binary(|_ecx, lhs, rhs| {
                    Ok(rhs.call_binary(
                        lhs.call_unary(UnaryFunc::CastStringToJsonb),
                        BinaryFunc::JsonbContainsJsonb,
                    ))
                }), oid::OP_CONTAINED_STRING_JSONB_OID;
                params!(MapAny, MapAny) => Operation::binary(|_ecx, lhs, rhs| {
                    Ok(rhs.call_binary(lhs, MapContainsMap))
                }) => Bool, oid::OP_CONTAINED_MAP_MAP_OID;
            },
            "?" => Scalar {
                params!(Jsonb, String) => JsonbContainsString, 3247;
                params!(MapAny, String) => MapContainsKey => Bool, oid::OP_CONTAINS_KEY_MAP_OID;
            },
            "?&" => Scalar {
                params!(MapAny, ScalarType::Array(Box::new(ScalarType::String))) => MapContainsAllKeys => Bool, oid::OP_CONTAINS_ALL_KEYS_MAP_OID;
            },
            "?|" => Scalar {
                params!(MapAny, ScalarType::Array(Box::new(ScalarType::String))) => MapContainsAnyKeys => Bool, oid::OP_CONTAINS_ANY_KEYS_MAP_OID;
            },
            // COMPARISON OPS
            "<" => Scalar {
                params!(Numeric, Numeric) => BinaryFunc::Lt, 1754;
                params!(Bool, Bool) => BinaryFunc::Lt, 58;
                params!(Int16, Int16) => BinaryFunc::Lt, 94;
                params!(Int32, Int32) => BinaryFunc::Lt, 97;
                params!(Int64, Int64) => BinaryFunc::Lt, 412;
                params!(Float32, Float32) => BinaryFunc::Lt, 622;
                params!(Float64, Float64) => BinaryFunc::Lt, 672;
                params!(Oid, Oid) => BinaryFunc::Lt, 609;
                params!(Date, Date) => BinaryFunc::Lt, 1095;
                params!(Time, Time) => BinaryFunc::Lt, 1110;
                params!(Timestamp, Timestamp) => BinaryFunc::Lt, 2062;
                params!(TimestampTz, TimestampTz) => BinaryFunc::Lt, 1322;
                params!(Uuid, Uuid) => BinaryFunc::Lt, 2974;
                params!(Interval, Interval) => BinaryFunc::Lt, 1332;
                params!(Bytes, Bytes) => BinaryFunc::Lt, 1957;
                params!(String, String) => BinaryFunc::Lt, 664;
                params!(Char, Char) => BinaryFunc::Lt, 1058;
                params!(Jsonb, Jsonb) => BinaryFunc::Lt, 3242;
                params!(ArrayAny, ArrayAny) => BinaryFunc::Lt => Bool, 1072;
                params!(RecordAny, RecordAny) => BinaryFunc::Lt => Bool, 2990;
            },
            "<=" => Scalar {
                params!(Numeric, Numeric) => BinaryFunc::Lte, 1755;
                params!(Bool, Bool) => BinaryFunc::Lte, 1694;
                params!(Int16, Int16) => BinaryFunc::Lte, 522;
                params!(Int32, Int32) => BinaryFunc::Lte, 523;
                params!(Int64, Int64) => BinaryFunc::Lte, 414;
                params!(Float32, Float32) => BinaryFunc::Lte, 624;
                params!(Float64, Float64) => BinaryFunc::Lte, 673;
                params!(Oid, Oid) => BinaryFunc::Lte, 611;
                params!(Date, Date) => BinaryFunc::Lte, 1096;
                params!(Time, Time) => BinaryFunc::Lte, 1111;
                params!(Timestamp, Timestamp) => BinaryFunc::Lte, 2063;
                params!(TimestampTz, TimestampTz) => BinaryFunc::Lte, 1323;
                params!(Uuid, Uuid) => BinaryFunc::Lte, 2976;
                params!(Interval, Interval) => BinaryFunc::Lte, 1333;
                params!(Bytes, Bytes) => BinaryFunc::Lte, 1958;
                params!(String, String) => BinaryFunc::Lte, 665;
                params!(Char, Char) => BinaryFunc::Lte, 1059;
                params!(Jsonb, Jsonb) => BinaryFunc::Lte, 3244;
                params!(ArrayAny, ArrayAny) => BinaryFunc::Lte => Bool, 1074;
                params!(RecordAny, RecordAny) => BinaryFunc::Lte => Bool, 2992;
            },
            ">" => Scalar {
                params!(Numeric, Numeric) => BinaryFunc::Gt, 1756;
                params!(Bool, Bool) => BinaryFunc::Gt, 59;
                params!(Int16, Int16) => BinaryFunc::Gt, 520;
                params!(Int32, Int32) => BinaryFunc::Gt, 521;
                params!(Int64, Int64) => BinaryFunc::Gt, 413;
                params!(Float32, Float32) => BinaryFunc::Gt, 623;
                params!(Float64, Float64) => BinaryFunc::Gt, 674;
                params!(Oid, Oid) => BinaryFunc::Gt, 610;
                params!(Date, Date) => BinaryFunc::Gt, 1097;
                params!(Time, Time) => BinaryFunc::Gt, 1112;
                params!(Timestamp, Timestamp) => BinaryFunc::Gt, 2064;
                params!(TimestampTz, TimestampTz) => BinaryFunc::Gt, 1324;
                params!(Uuid, Uuid) => BinaryFunc::Gt, 2975;
                params!(Interval, Interval) => BinaryFunc::Gt, 1334;
                params!(Bytes, Bytes) => BinaryFunc::Gt, 1959;
                params!(String, String) => BinaryFunc::Gt, 666;
                params!(Char, Char) => BinaryFunc::Gt, 1060;
                params!(Jsonb, Jsonb) => BinaryFunc::Gt, 3243;
                params!(ArrayAny, ArrayAny) => BinaryFunc::Gt => Bool, 1073;
                params!(RecordAny, RecordAny) => BinaryFunc::Gt => Bool, 2991;
            },
            ">=" => Scalar {
                params!(Numeric, Numeric) => BinaryFunc::Gte, 1757;
                params!(Bool, Bool) => BinaryFunc::Gte, 1695;
                params!(Int16, Int16) => BinaryFunc::Gte, 524;
                params!(Int32, Int32) => BinaryFunc::Gte, 525;
                params!(Int64, Int64) => BinaryFunc::Gte, 415;
                params!(Float32, Float32) => BinaryFunc::Gte, 625;
                params!(Float64, Float64) => BinaryFunc::Gte, 675;
                params!(Oid, Oid) => BinaryFunc::Gte, 612;
                params!(Date, Date) => BinaryFunc::Gte, 1098;
                params!(Time, Time) => BinaryFunc::Gte, 1113;
                params!(Timestamp, Timestamp) => BinaryFunc::Gte, 2065;
                params!(TimestampTz, TimestampTz) => BinaryFunc::Gte, 1325;
                params!(Uuid, Uuid) => BinaryFunc::Gte, 2977;
                params!(Interval, Interval) => BinaryFunc::Gte, 1335;
                params!(Bytes, Bytes) => BinaryFunc::Gte, 1960;
                params!(String, String) => BinaryFunc::Gte, 667;
                params!(Char, Char) => BinaryFunc::Gte, 1061;
                params!(Jsonb, Jsonb) => BinaryFunc::Gte, 3245;
                params!(ArrayAny, ArrayAny) => BinaryFunc::Gte => Bool, 1075;
                params!(RecordAny, RecordAny) => BinaryFunc::Gte => Bool, 2993;
            },
            // Warning! If you are writing functions here that do not simply use
            // `BinaryFunc::Eq`, you will break row equality (used e.g. DISTINCT
            // operations).
            "=" => Scalar {
                params!(Numeric, Numeric) => BinaryFunc::Eq, 1752;
                params!(Bool, Bool) => BinaryFunc::Eq, 91;
                params!(Int16, Int16) => BinaryFunc::Eq, 94;
                params!(Int32, Int32) => BinaryFunc::Eq, 96;
                params!(Int64, Int64) => BinaryFunc::Eq, 410;
                params!(Float32, Float32) => BinaryFunc::Eq, 620;
                params!(Float64, Float64) => BinaryFunc::Eq, 670;
                params!(Oid, Oid) => BinaryFunc::Eq, 607;
                params!(Date, Date) => BinaryFunc::Eq, 1093;
                params!(Time, Time) => BinaryFunc::Eq, 1108;
                params!(Timestamp, Timestamp) => BinaryFunc::Eq, 2060;
                params!(TimestampTz, TimestampTz) => BinaryFunc::Eq, 1320;
                params!(Uuid, Uuid) => BinaryFunc::Eq, 2972;
                params!(Interval, Interval) => BinaryFunc::Eq, 1330;
                params!(Bytes, Bytes) => BinaryFunc::Eq, 1955;
                params!(String, String) => BinaryFunc::Eq, 98;
                params!(Char, Char) => BinaryFunc::Eq, 1054;
                params!(Jsonb, Jsonb) => BinaryFunc::Eq, 3240;
                params!(ListAny, ListAny) => BinaryFunc::Eq => Bool, oid::FUNC_LIST_EQ_OID;
                params!(ArrayAny, ArrayAny) => BinaryFunc::Eq => Bool, 1070;
                params!(RecordAny, RecordAny) => BinaryFunc::Eq => Bool, 2988;
            },
            "<>" => Scalar {
                params!(Numeric, Numeric) => BinaryFunc::NotEq, 1753;
                params!(Bool, Bool) => BinaryFunc::NotEq, 85;
                params!(Int16, Int16) => BinaryFunc::NotEq, 519;
                params!(Int32, Int32) => BinaryFunc::NotEq, 518;
                params!(Int64, Int64) => BinaryFunc::NotEq, 411;
                params!(Float32, Float32) => BinaryFunc::NotEq, 621;
                params!(Float64, Float64) => BinaryFunc::NotEq, 671;
                params!(Oid, Oid) => BinaryFunc::NotEq, 608;
                params!(Date, Date) => BinaryFunc::NotEq, 1094;
                params!(Time, Time) => BinaryFunc::NotEq, 1109;
                params!(Timestamp, Timestamp) => BinaryFunc::NotEq, 2061;
                params!(TimestampTz, TimestampTz) => BinaryFunc::NotEq, 1321;
                params!(Uuid, Uuid) => BinaryFunc::NotEq, 2973;
                params!(Interval, Interval) => BinaryFunc::NotEq, 1331;
                params!(Bytes, Bytes) => BinaryFunc::NotEq, 1956;
                params!(String, String) => BinaryFunc::NotEq, 531;
                params!(Char, Char) => BinaryFunc::NotEq, 1057;
                params!(Jsonb, Jsonb) => BinaryFunc::NotEq, 3241;
                params!(ArrayAny, ArrayAny) => BinaryFunc::NotEq => Bool, 1071;
                params!(RecordAny, RecordAny) => BinaryFunc::NotEq => Bool, 2989;
            }
        }
    };
}

/// Resolves the operator to a set of function implementations.
pub fn resolve_op(op: &str) -> Result<&'static [FuncImpl<HirScalarExpr>], PlanError> {
    match OP_IMPLS.get(op) {
        Some(Func::Scalar(impls)) => Ok(&impls),
        Some(_) => unreachable!("all operators must be scalar functions"),
        // TODO: these require sql arrays
        // JsonContainsAnyFields
        // JsonContainsAllFields
        // TODO: these require json paths
        // JsonGetPath
        // JsonGetPathAsText
        // JsonDeletePath
        // JsonContainsPath
        // JsonApplyPathPredicate
        None => bail_unsupported!(format!("[{}]", op)),
    }
}
