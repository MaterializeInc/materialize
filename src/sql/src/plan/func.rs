// Copyright Materialize, Inc. All rights reserved.
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
use std::convert::TryFrom;
use std::fmt;
use std::rc::Rc;

use anyhow::bail;
use itertools::Itertools;
use lazy_static::lazy_static;

use crate::catalog::CatalogItemType;
use ore::collections::CollectionExt;
use repr::{ColumnName, Datum, RelationType, ScalarType};
use sql_parser::ast::{BinaryOperator, Expr, Ident, ObjectName, UnaryOperator};

use super::expr::{
    AggregateFunc, BinaryFunc, CoercibleScalarExpr, NullaryFunc, ScalarExpr, TableFunc, UnaryFunc,
    VariadicFunc,
};
use super::query::{self, ExprContext, QueryContext, QueryLifetime};
use super::scope::Scope;
use super::typeconv::{self, rescale_decimal, CastTo, CoerceTo};
use super::StatementContext;
use crate::names::PartialName;

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
    Numeric,
    Pseudo,
    String,
    Timespan,
    UserDefined,
}

impl TypeCategory {
    /// Extracted from PostgreSQL 9.6.
    /// ```ignore
    /// SELECT array_agg(typname), typcategory
    /// FROM pg_catalog.pg_type
    /// WHERE typname IN (
    ///  'bool', 'bytea', 'date', 'float4', 'float8', 'int4', 'int8', 'interval', 'jsonb',
    ///  'numeric', 'text', 'time', 'timestamp', 'timestamptz'
    /// )
    /// GROUP BY typcategory
    /// ORDER BY typcategory;
    /// ```
    fn from_type(typ: &ScalarType) -> Self {
        match typ {
            ScalarType::Array(_) => Self::Array,
            ScalarType::Bool => Self::Bool,
            ScalarType::Bytes | ScalarType::Jsonb | ScalarType::Uuid | ScalarType::List(_) => {
                Self::UserDefined
            }
            ScalarType::Date
            | ScalarType::Time
            | ScalarType::Timestamp
            | ScalarType::TimestampTz => Self::DateTime,
            ScalarType::Decimal(..)
            | ScalarType::Float32
            | ScalarType::Float64
            | ScalarType::Int32
            | ScalarType::Int64
            | ScalarType::Oid => Self::Numeric,
            ScalarType::Interval => Self::Timespan,
            ScalarType::String => Self::String,
            ScalarType::Record { .. } => Self::Pseudo,
        }
    }

    fn from_param(param: &ParamType) -> Self {
        match param {
            ParamType::Plain(t) => Self::from_type(t),
            ParamType::Any | ParamType::ArrayAny | ParamType::StringAny | ParamType::JsonbAny => {
                Self::Pseudo
            }
        }
    }

    /// Extracted from PostgreSQL 9.6.
    /// ```ignore
    /// SELECT typcategory, typname, typispreferred
    /// FROM pg_catalog.pg_type
    /// WHERE typispreferred = true
    /// ORDER BY typcategory;
    /// ```
    fn preferred_type(&self) -> Option<ScalarType> {
        match self {
            Self::Bool => Some(ScalarType::Bool),
            Self::DateTime => Some(ScalarType::TimestampTz),
            Self::Numeric => Some(ScalarType::Float64),
            Self::String => Some(ScalarType::String),
            Self::Timespan => Some(ScalarType::Interval),
            Self::Array | Self::Pseudo | Self::UserDefined => None,
        }
    }
}

struct Operation<R>(
    Box<dyn Fn(&ExprContext, Vec<ScalarExpr>) -> Result<R, anyhow::Error> + Send + Sync>,
);

/// Describes a single function's implementation.
pub struct FuncImpl<R> {
    params: ParamList,
    op: Operation<R>,
}

impl<R> fmt::Debug for FuncImpl<R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FuncImpl")
            .field("params", &self.params)
            .field("op", &"<omitted>")
            .finish()
    }
}

fn nullary_op<F, R>(f: F) -> Operation<R>
where
    F: Fn(&ExprContext) -> Result<R, anyhow::Error> + Send + Sync + 'static,
{
    Operation(Box::new(move |ecx, exprs| {
        assert!(exprs.is_empty());
        f(ecx)
    }))
}

fn identity_op() -> Operation<ScalarExpr> {
    unary_op(|_ecx, e| Ok(e))
}

fn unary_op<F, R>(f: F) -> Operation<R>
where
    F: Fn(&ExprContext, ScalarExpr) -> Result<R, anyhow::Error> + Send + Sync + 'static,
{
    Operation(Box::new(move |ecx, exprs| f(ecx, exprs.into_element())))
}

fn binary_op<F, R>(f: F) -> Operation<R>
where
    F: Fn(&ExprContext, ScalarExpr, ScalarExpr) -> Result<R, anyhow::Error> + Send + Sync + 'static,
{
    Operation(Box::new(move |ecx, exprs| {
        assert_eq!(exprs.len(), 2);
        let mut exprs = exprs.into_iter();
        let left = exprs.next().unwrap();
        let right = exprs.next().unwrap();
        f(ecx, left, right)
    }))
}

fn variadic_op<F, R>(f: F) -> Operation<R>
where
    F: Fn(&ExprContext, Vec<ScalarExpr>) -> Result<R, anyhow::Error> + Send + Sync + 'static,
{
    Operation(Box::new(f))
}

// Constructs a definition for a built-in out of a static SQL expression.
//
// The SQL expression should use the standard parameter syntax (`$1`, `$2`, ...)
// to refer to the inputs to the function. For example, a built-in function
// that takes two arguments and concatenates them with an arrow in between
// could be defined like so:
//
//     sql_op!("$1 || '<->' || $2")
//
// The number of parameters in the SQL expression must exactly match the number
// of parameters in the built-in's declaration. There is no support for
// variadic functions.
macro_rules! sql_op {
    ($l:literal) => {{
        lazy_static! {
            static ref EXPR: Expr = sql_parser::parser::parse_expr($l.into())
                .expect("static function definition failed to parse");
        }
        Operation(Box::new(move |ecx, args| {
            // Reconstruct an expression context where the parameter types are
            // bound to the types of the expressions in `args`.
            let mut scx = ecx.qcx.scx.clone();
            scx.param_types = Rc::new(RefCell::new(
                args.iter()
                    .enumerate()
                    .map(|(i, e)| (i + 1, ecx.scalar_type(e)))
                    .collect(),
            ));
            let qcx = QueryContext::root(&scx, ecx.qcx.lifetime);
            let ecx = ExprContext {
                qcx: &qcx,
                name: "static function definition",
                scope: &Scope::empty(None),
                relation_type: &RelationType::empty(),
                allow_aggregates: false,
                allow_subqueries: true,
            };

            // Plan the expression.
            let mut expr = query::plan_expr(&ecx, &*EXPR)?.type_as_any(&ecx)?;

            // Replace the parameters with the actual arguments.
            expr.visit_mut(&mut |e| match e {
                ScalarExpr::Parameter(i) => *e = args[*i - 1].clone(),
                _ => (),
            });

            Ok(expr)
        }))
    }};
}

impl From<UnaryFunc> for Operation<ScalarExpr> {
    fn from(u: UnaryFunc) -> Operation<ScalarExpr> {
        unary_op(move |_ecx, e| Ok(e.call_unary(u.clone())))
    }
}

impl From<BinaryFunc> for Operation<ScalarExpr> {
    fn from(b: BinaryFunc) -> Operation<ScalarExpr> {
        binary_op(move |_ecx, left, right| Ok(left.call_binary(right, b.clone())))
    }
}

impl From<VariadicFunc> for Operation<ScalarExpr> {
    fn from(v: VariadicFunc) -> Operation<ScalarExpr> {
        variadic_op(move |_ecx, exprs| {
            Ok(ScalarExpr::CallVariadic {
                func: v.clone(),
                exprs,
            })
        })
    }
}

impl From<AggregateFunc> for Operation<(ScalarExpr, AggregateFunc)> {
    fn from(a: AggregateFunc) -> Operation<(ScalarExpr, AggregateFunc)> {
        unary_op(move |_ecx, e| Ok((e, a.clone())))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
/// Describes possible types of function parameters.
///
/// Note that this is not exhaustive and will likely require additions.
pub enum ParamList {
    Exact(Vec<ParamType>),
    Repeat(Vec<ParamType>),
}

impl ParamList {
    /// Validates that the number of input elements are viable for this set of
    /// parameters.
    fn validate_arg_len(&self, input_len: usize) -> bool {
        match self {
            Self::Exact(p) => p.len() == input_len,
            Self::Repeat(p) => input_len % p.len() == 0 && input_len > 0,
        }
    }

    /// Matches a `&[ScalarType]` derived from the user's function argument
    /// against this `ParamList`'s permitted arguments.
    fn match_scalartypes(&self, types: &[&ScalarType]) -> bool {
        types
            .iter()
            .enumerate()
            .all(|(i, t)| self[i].accepts_type_directly(t))
    }
}

impl std::ops::Index<usize> for ParamList {
    type Output = ParamType;

    fn index(&self, i: usize) -> &Self::Output {
        match self {
            Self::Exact(p) => &p[i],
            Self::Repeat(p) => &p[i % p.len()],
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
    Plain(ScalarType),
    /// A psuedotype permitting any type.
    Any,
    /// A pseudotype permitting any type, but requires it to be cast to a `ScalarType::String`.
    StringAny,
    /// A pseudotype permitting any type, but requires it to be cast to a
    /// [`ScalarType::Jsonb`], or an element within a `Jsonb`.
    JsonbAny,
    /// A psuedotype permitting any array type.
    ArrayAny,
}

impl ParamType {
    /// Does `self` accept arguments of type `t` without casting?
    fn accepts_type_directly(&self, t: &ScalarType) -> bool {
        match (self, t) {
            (ParamType::Plain(s), o) => *s == o.desaturate(),
            (ParamType::Any, _) | (ParamType::StringAny, _) | (ParamType::JsonbAny, _) => true,
            (ParamType::ArrayAny, ScalarType::Array(_)) => true,
            (ParamType::ArrayAny, _) => false,
        }
    }

    /// Does `self` accept arguments of type `t` with an implicitly allowed cast?
    fn accepts_type_implicitly(&self, from_type: &ScalarType) -> bool {
        let cast_to = match self {
            ParamType::Plain(s) => CastTo::Implicit(s.clone()),
            ParamType::Any | ParamType::JsonbAny | ParamType::StringAny => return true,
            ParamType::ArrayAny => return matches!(from_type, ScalarType::Array(_)),
        };

        typeconv::get_cast(from_type, &cast_to).is_some()
    }

    /// Does `self` accept arguments of category `c`?
    fn accepts_cat(&self, c: &TypeCategory) -> bool {
        match (self, c) {
            (ParamType::Plain(_), c) => TypeCategory::from_param(&self) == *c,
            (ParamType::Any, _) | (ParamType::StringAny, _) | (ParamType::JsonbAny, _) => true,
            (ParamType::ArrayAny, TypeCategory::Array) => true,
            (ParamType::ArrayAny, _) => false,
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
}

impl PartialEq<ScalarType> for ParamType {
    fn eq(&self, other: &ScalarType) -> bool {
        match (self, other) {
            (ParamType::Plain(s), o) => *s == o.desaturate(),
            // Pseudotypes do not equal concrete types.
            (ParamType::Any, _)
            | (ParamType::ArrayAny, _)
            | (ParamType::StringAny, _)
            | (ParamType::JsonbAny, _) => false,
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

#[derive(Debug, Clone)]
/// Tracks candidate implementations.
pub struct Candidate<'a, R> {
    /// The implementation under consideration.
    fimpl: &'a FuncImpl<R>,
    exact_matches: usize,
    preferred_types: usize,
}

#[derive(Clone, Debug)]
/// Determines best implementation to use given some user-provided arguments.
/// For more detail, see `ArgImplementationMatcher::select_implementation`.
pub struct ArgImplementationMatcher<'a> {
    ident: &'a str,
    ecx: &'a ExprContext<'a>,
}

impl<'a> ArgImplementationMatcher<'a> {
    /// Selects the best implementation given the provided `args` using a
    /// process similar to [PostgreSQL's parser][pgparser], and returns the
    /// `ScalarExpr` to invoke that function.
    ///
    /// # Errors
    /// - When the provided arguments are not valid for any implementation, e.g.
    ///   cannot be converted to the appropriate types.
    /// - When all implementations are equally valid.
    ///
    /// [pgparser]: https://www.postgresql.org/docs/current/typeconv-oper.html
    pub fn select_implementation<R>(
        ident: &'a str,
        err_string_gen: fn(&str, &[Option<ScalarType>], String) -> String,
        ecx: &'a ExprContext<'a>,
        impls: &[FuncImpl<R>],
        cexprs: Vec<CoercibleScalarExpr>,
    ) -> Result<R, anyhow::Error> {
        // Immediately remove all `impls` we know are invalid.
        let l = cexprs.len();
        let impls = impls
            .iter()
            .filter(|i| i.params.validate_arg_len(l))
            .collect();
        let m = Self { ident, ecx };

        let types: Vec<_> = cexprs
            .iter()
            .map(|e| ecx.column_type(e).map(|t| t.scalar_type))
            .collect();

        // try-catch in Rust.
        match || -> Result<R, anyhow::Error> {
            let f = m.find_match(&types, impls)?;

            let mut exprs = Vec::new();
            for (i, cexpr) in cexprs.into_iter().enumerate() {
                exprs.push(m.coerce_arg_to_type(cexpr, &f.params[i])?);
            }

            (f.op.0)(ecx, exprs)
        }() {
            Ok(s) => Ok(s),
            Err(e) => bail!(err_string_gen(ident, &types, e.to_string())),
        }
    }

    /// Finds an exact match based on the arguments, or, if no exact match,
    /// finds the best match available. Patterned after [PostgreSQL's type
    /// conversion matching algorithm][pgparser].
    ///
    /// Inline prefixed with number are taken from the "Function Type
    /// Resolution" section of the aforelinked page.
    ///
    /// [pgparser]: https://www.postgresql.org/docs/current/typeconv-func.html
    fn find_match<'b, R>(
        &self,
        types: &[Option<ScalarType>],
        impls: Vec<&'b FuncImpl<R>>,
    ) -> Result<&'b FuncImpl<R>, anyhow::Error> {
        let all_types_known = types.iter().all(|t| t.is_some());

        // Check for exact match.
        if all_types_known {
            let known_types: Vec<_> = types.iter().filter_map(|t| t.as_ref()).collect();
            let matching_impls: Vec<&FuncImpl<_>> = impls
                .iter()
                .filter(|i| i.params.match_scalartypes(&known_types))
                .cloned()
                .collect();

            if matching_impls.len() == 1 {
                return Ok(&matching_impls[0]);
            }
        }

        // No exact match. Apply PostgreSQL's best match algorithm.
        // Generate candidates by assessing their compatibility with each
        // implementation's parameters.
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
            let mut valid_candidate = true;
            let mut exact_matches = 0;
            let mut preferred_types = 0;

            for (i, arg_type) in types.iter().enumerate() {
                let param_type = &fimpl.params[i];

                match arg_type {
                    Some(arg_type) if param_type == arg_type => {
                        exact_matches += 1;
                    }
                    Some(arg_type) => {
                        if !param_type.accepts_type_implicitly(arg_type) {
                            valid_candidate = false;
                            break;
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

            // 4.a. Discard candidate functions for which the input types do not match
            // and cannot be converted (using an implicit conversion) to match.
            // unknown literals are assumed to be convertible to anything for this
            // purpose.
            if valid_candidate {
                max_exact_matches = std::cmp::max(max_exact_matches, exact_matches);
                candidates.push(Candidate {
                    fimpl,
                    exact_matches,
                    preferred_types,
                });
            }
        }

        if candidates.is_empty() {
            bail!(
                "arguments cannot be implicitly cast to any implementation's parameters; \
                 try providing explicit casts"
            )
        }

        maybe_get_last_candidate!();

        // 4.c. Run through all candidates and keep those with the most exact matches on
        // input types. Keep all candidates if none have exact matches.
        candidates.retain(|c| c.exact_matches >= max_exact_matches);

        maybe_get_last_candidate!();

        // 4.d. Run through all candidates and keep those that accept preferred types
        // (of the input data type's type category) at the most positions where
        // type conversion will be required.
        let mut max_preferred_types = 0;
        for c in &candidates {
            max_preferred_types = std::cmp::max(max_preferred_types, c.preferred_types);
        }
        candidates.retain(|c| c.preferred_types >= max_preferred_types);

        maybe_get_last_candidate!();

        if all_types_known {
            bail!(
                "unable to determine which implementation to use; try providing \
                 explicit casts to match parameter types"
            )
        }

        let mut found_known = false;
        let mut types_match = true;
        let mut common_type: Option<ScalarType> = None;

        for (i, arg_type) in types.iter().enumerate() {
            let mut selected_category: Option<TypeCategory> = None;
            let mut found_string_candidate = false;
            let mut categories_match = true;

            match arg_type {
                // 4.e. If any input arguments are unknown, check the type categories accepted
                // at those argument positions by the remaining candidates.
                None => {
                    for c in candidates.iter() {
                        // 4.e. cont: At each  position, select the string category if
                        // any candidate accepts that category. (This bias
                        // towards string is appropriate since an
                        // unknown-type literal looks like a string.)
                        if c.fimpl.params[i].accepts_type_directly(&ScalarType::String) {
                            found_string_candidate = true;
                            selected_category = Some(TypeCategory::String);
                            break;
                        }
                        // 4.e. cont: Otherwise, if all the remaining candidates accept
                        // the same type category, select that category.
                        let this_category = TypeCategory::from_param(&c.fimpl.params[i]);
                        match (&selected_category, &this_category) {
                            (Some(selected_category), this_category) => {
                                categories_match =
                                    *selected_category == *this_category && categories_match
                            }
                            (None, this_category) => {
                                selected_category = Some(this_category.clone())
                            }
                        }
                    }

                    // 4.e. cont: Otherwise fail because the correct choice cannot be
                    // deduced without more clues.
                    if !found_string_candidate && !categories_match {
                        bail!(
                            "unable to determine which implementation to use; try providing \
                            explicit casts to match parameter types"
                        )
                    }

                    // 4.e. cont: Now discard candidates that do not accept the selected
                    // type category. Furthermore, if any candidate accepts a
                    // preferred type in that category, discard candidates that
                    // accept non-preferred types for that argument.
                    let selected_category = selected_category.unwrap();

                    let preferred_type = selected_category.preferred_type();
                    let mut found_preferred_type_candidate = false;
                    candidates.retain(|c| {
                        if let Some(typ) = &preferred_type {
                            found_preferred_type_candidate = c.fimpl.params[i]
                                .accepts_type_directly(typ)
                                || found_preferred_type_candidate;
                        }
                        c.fimpl.params[i].accepts_cat(&selected_category)
                    });

                    if found_preferred_type_candidate {
                        let preferred_type = preferred_type.unwrap();
                        candidates
                            .retain(|c| c.fimpl.params[i].accepts_type_directly(&preferred_type));
                    }
                }
                Some(typ) => {
                    found_known = true;
                    // Track if all known types are of the same type; use this info in 4.f.
                    if let Some(common_type) = &common_type {
                        types_match = types_match && *common_type == *typ
                    } else {
                        common_type = Some(typ.clone());
                    }
                }
            }
        }

        maybe_get_last_candidate!();

        // 4.f. If there are both unknown and known-type arguments, and all the
        // known-type arguments have the same type, assume that the unknown
        // arguments are also of that type, and check which candidates can
        // accept that type at the unknown-argument positions.
        // (ed: We know unknown argument exists if we're in this part of the code.)
        if found_known && types_match {
            let common_type = common_type.unwrap();
            for (i, raw_arg_type) in types.iter().enumerate() {
                if raw_arg_type.is_none() {
                    candidates.retain(|c| c.fimpl.params[i].accepts_type_directly(&common_type));
                }
            }

            maybe_get_last_candidate!();
        }

        bail!(
            "unable to determine which implementation to use; try providing \
             explicit casts to match parameter types"
        )
    }

    /// Generates `ScalarExpr` necessary to coerce `Expr` into the `ScalarType`
    /// corresponding to `ParameterType`; errors if not possible. This can only
    /// work within the `func` module because it relies on `ParameterType`.
    fn coerce_arg_to_type(
        &self,
        arg: CoercibleScalarExpr,
        typ: &ParamType,
    ) -> Result<ScalarExpr, anyhow::Error> {
        // TODO(sean): this function needs to take a global view of the
        // arguments to properly handle polymorphic types. For example, a
        // function that takes (ArrayAny, ArrayAny) needs to coerce both
        // arguments to the *same* array type. For now, we only have functions
        // that take a single ArrayAny as input, so we simply coerce to
        // Array(String) for ArrayAny parameters.
        use ScalarType::*;
        let coerce_to = match typ {
            ParamType::Plain(s) => CoerceTo::Plain(s.clone()),
            ParamType::Any => CoerceTo::Plain(String),
            ParamType::ArrayAny => CoerceTo::Plain(Array(Box::new(String))),
            ParamType::JsonbAny => CoerceTo::JsonbAny,
            ParamType::StringAny => CoerceTo::Plain(String),
        };
        let arg = typeconv::plan_coerce(self.ecx, arg, coerce_to)?;
        let arg_type = self.ecx.scalar_type(&arg);
        let cast_to = match typ {
            ParamType::Plain(Decimal(..)) if matches!(arg_type, Decimal(..)) => return Ok(arg),
            ParamType::Plain(List(..)) if matches!(arg_type, List(..)) => return Ok(arg),
            ParamType::Plain(s) => CastTo::Implicit(s.clone()),
            ParamType::Any => return Ok(arg),
            ParamType::ArrayAny => CastTo::Explicit(Array(Box::new(String))),
            ParamType::JsonbAny => CastTo::JsonbAny,
            ParamType::StringAny => CastTo::Explicit(String),
        };
        typeconv::plan_cast(self.ident, self.ecx, arg, cast_to)
    }
}

/// Provides shorthand for converting `Vec<ScalarType>` into `Vec<ParamType>`.
macro_rules! params {
    (($($p:expr),*)...) => { ParamList::Repeat(vec![$($p.into(),)*]) };
    ($($p:expr),*)      => { ParamList::Exact(vec![$($p.into(),)*]) };
}

/// Constructs builtin function map.
macro_rules! builtins {
    {
        $(
            $name:expr => $ty:ident {
                $($params:expr => $op:expr),+
            }
        ),+
    } => {{
        let mut builtins = HashMap::new();
        $(
            let impls = vec![
                $(FuncImpl {
                    params: $params.into(),
                    op: $op.into(),
                },)+
            ];
            let old = builtins.insert($name, Func::$ty(impls));
            assert!(old.is_none(), "duplicate entry in builtins list");
        )+
        builtins
    }};
}

pub struct TableFuncPlan {
    pub func: TableFunc,
    pub exprs: Vec<ScalarExpr>,
    pub column_names: Vec<Option<ColumnName>>,
}

pub enum Func {
    Scalar(Vec<FuncImpl<ScalarExpr>>),
    Aggregate(Vec<FuncImpl<(ScalarExpr, AggregateFunc)>>),
    Table(Vec<FuncImpl<TableFuncPlan>>),
}

lazy_static! {
    /// Correlates a built-in function name to its implementations.
    static ref PG_CATALOG_BUILTINS: HashMap<&'static str, Func> = {
        use ParamType::*;
        use ScalarType::*;
        builtins! {
            // Scalars.
            "abs" => Scalar {
                params!(Int32) => UnaryFunc::AbsInt32,
                params!(Int64) => UnaryFunc::AbsInt64,
                params!(Decimal(0, 0)) => UnaryFunc::AbsDecimal,
                params!(Float32) => UnaryFunc::AbsFloat32,
                params!(Float64) => UnaryFunc::AbsFloat64
            },
            "array_to_string" => Scalar {
                params!(ArrayAny, String) => variadic_op(array_to_string),
                params!(ArrayAny, String, String) => variadic_op(array_to_string)
            },
            "ascii" => Scalar {
                params!(String) => UnaryFunc::Ascii
            },
            "btrim" => Scalar {
                params!(String) => UnaryFunc::TrimWhitespace,
                params!(String, String) => BinaryFunc::Trim
            },
            "bit_length" => Scalar {
                params!(Bytes) => UnaryFunc::BitLengthBytes,
                params!(String) => UnaryFunc::BitLengthString
            },
            "ceil" => Scalar {
                params!(Float32) => UnaryFunc::CeilFloat32,
                params!(Float64) => UnaryFunc::CeilFloat64,
                params!(Decimal(0, 0)) => unary_op(|ecx, e| {
                    let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                    Ok(e.call_unary(UnaryFunc::CeilDecimal(s)))
                })
            },
            "char_length" => Scalar {
                params!(String) => UnaryFunc::CharLength
            },
            "concat" => Scalar {
                 params!((StringAny)...) => variadic_op(|_ecx, mut exprs| {
                    // Unlike all other `StringAny` casts, `concat` uses an
                    // implicit behavior for converting bools to strings.
                    for e in &mut exprs {
                        if let ScalarExpr::CallUnary {
                            func: func @ UnaryFunc::CastBoolToStringExplicit,
                            ..
                        } = e {
                            *func = UnaryFunc::CastBoolToStringImplicit;
                        }
                    }
                    Ok(ScalarExpr::CallVariadic { func: VariadicFunc::Concat, exprs })
                })
            },
            "convert_from" => Scalar {
                params!(Bytes, String) => BinaryFunc::ConvertFrom
            },
            "current_schemas" => Scalar {
                params!(Bool) => unary_op(|ecx, e| {
                    let with_sys = ScalarExpr::literal_1d_array(
                        ecx.qcx.scx.catalog.search_path(true).iter().map(|s| Datum::String(s)).collect(),
                        ScalarType::String)?;
                    let without_sys = ScalarExpr::literal_1d_array(
                        ecx.qcx.scx.catalog.search_path(false).iter().map(|s| Datum::String(s)).collect(),
                        ScalarType::String)?;
                    Ok(ScalarExpr::If {
                        cond: Box::new(e),
                        then: Box::new(with_sys),
                        els: Box::new(without_sys),
                    })
                })
            },
            "current_timestamp" => Scalar {
                params!() => nullary_op(|ecx| plan_current_timestamp(ecx, "current_timestamp"))
            },
            "date_part" => Scalar {
                params!(String, Interval) => BinaryFunc::DatePartInterval,
                params!(String, Timestamp) => BinaryFunc::DatePartTimestamp,
                params!(String, TimestampTz) => BinaryFunc::DatePartTimestampTz
            },
            "date_trunc" => Scalar {
                params!(String, Timestamp) => BinaryFunc::DateTruncTimestamp,
                params!(String, TimestampTz) => BinaryFunc::DateTruncTimestampTz
            },
            "floor" => Scalar {
                params!(Float32) => UnaryFunc::FloorFloat32,
                params!(Float64) => UnaryFunc::FloorFloat64,
                params!(Decimal(0, 0)) => unary_op(|ecx, e| {
                    let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                    Ok(e.call_unary(UnaryFunc::FloorDecimal(s)))
                })
            },
            "jsonb_array_length" => Scalar {
                params!(Jsonb) => UnaryFunc::JsonbArrayLength
            },
            "jsonb_build_array" => Scalar {
                params!() => VariadicFunc::JsonbBuildArray,
                params!((JsonbAny)...) => VariadicFunc::JsonbBuildArray
            },
            "jsonb_build_object" => Scalar {
                params!() => VariadicFunc::JsonbBuildObject,
                params!((StringAny, JsonbAny)...) =>
                    VariadicFunc::JsonbBuildObject
            },
            "jsonb_pretty" => Scalar {
                params!(Jsonb) => UnaryFunc::JsonbPretty
            },
            "jsonb_strip_nulls" => Scalar {
                params!(Jsonb) => UnaryFunc::JsonbStripNulls
            },
            "jsonb_typeof" => Scalar {
                params!(Jsonb) => UnaryFunc::JsonbTypeof
            },
            "length" => Scalar {
                params!(Bytes) => UnaryFunc::ByteLengthBytes,
                params!(String) => UnaryFunc::CharLength,
                params!(Bytes, String) => BinaryFunc::EncodedBytesCharLength
            },
            "octet_length" => Scalar {
                params!(Bytes) => UnaryFunc::ByteLengthBytes,
                params!(String) => UnaryFunc::ByteLengthString
            },
            "lpad" => Scalar {
                params!(String, Int64) => VariadicFunc::PadLeading,
                params!(String, Int64, String) => VariadicFunc::PadLeading
            },
            "ltrim" => Scalar {
                params!(String) => UnaryFunc::TrimLeadingWhitespace,
                params!(String, String) => BinaryFunc::TrimLeading
            },
            "make_timestamp" => Scalar {
                params!(Int64, Int64, Int64, Int64, Int64, Float64) => VariadicFunc::MakeTimestamp
            },
            "now" => Scalar {
                params!() => nullary_op(|ecx| plan_current_timestamp(ecx, "now"))
            },
            "obj_description" => Scalar {
                params!(Oid, String) => binary_op(|_ecx, _oid, _catalog| {
                    // This function is meant to return the comment on a
                    // database object, but we don't presently support comments,
                    // so stubbed out out to always return NULL.
                    Ok(ScalarExpr::literal_null(ScalarType::String))
                })
            },
            "pg_get_userbyid" => Scalar {
                params!(Oid) => sql_op!("'unknown (OID=' || $1 || ')'")
            },
            "replace" => Scalar {
                params!(String, String, String) => VariadicFunc::Replace
            },
            "round" => Scalar {
                params!(Float32) => UnaryFunc::RoundFloat32,
                params!(Float64) => UnaryFunc::RoundFloat64,
                params!(Decimal(0,0)) => unary_op(|ecx, e| {
                    let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                    Ok(e.call_unary(UnaryFunc::RoundDecimal(s)))
                }),
                params!(Decimal(0,0), Int64) => binary_op(|ecx, lhs, rhs| {
                    let (_, s) = ecx.scalar_type(&lhs).unwrap_decimal_parts();
                    Ok(lhs.call_binary(rhs, BinaryFunc::RoundDecimal(s)))
                })
            },
            "rtrim" => Scalar {
                params!(String) => UnaryFunc::TrimTrailingWhitespace,
                params!(String, String) => BinaryFunc::TrimTrailing
            },
            "split_part" => Scalar {
                params!(String, String, Int64) => VariadicFunc::SplitPart
            },
            "substr" => Scalar {
                params!(String, Int64) => VariadicFunc::Substr,
                params!(String, Int64, Int64) => VariadicFunc::Substr
            },
            "substring" => Scalar {
                params!(String, Int64) => VariadicFunc::Substr,
                params!(String, Int64, Int64) => VariadicFunc::Substr
            },
            "sqrt" => Scalar {
                params!(Float32) => UnaryFunc::SqrtFloat32,
                params!(Float64) => UnaryFunc::SqrtFloat64,
                params!(Decimal(0,0)) => unary_op(|ecx, e| {
                    let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                    Ok(e.call_unary(UnaryFunc::SqrtDec(s)))
                })
            },
            "to_char" => Scalar {
                params!(Timestamp, String) => BinaryFunc::ToCharTimestamp,
                params!(TimestampTz, String) => BinaryFunc::ToCharTimestampTz
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
                params!(JsonbAny) => identity_op()
            },
            "to_timestamp" => Scalar {
                params!(Float64) => UnaryFunc::ToTimestamp
            },

            // Aggregates.
            "array_agg" => Aggregate {
                params!(Any) => unary_op(|_ecx, _e| unsupported!("array_agg"))
            },
            "bool_and" => Aggregate {
                params!(Any) => unary_op(|_ecx, _e| unsupported!("bool_and"))
            },
            "bool_or" => Aggregate {
                params!(Any) => unary_op(|_ecx, _e| unsupported!("bool_or"))
            },
            "concat_agg" => Aggregate {
                params!(Any) => unary_op(|_ecx, _e| unsupported!("concat_agg"))
            },
            "count" => Aggregate {
                params!() => nullary_op(|_ecx| {
                    // COUNT(*) is equivalent to COUNT(true).
                    Ok((ScalarExpr::literal_true(), AggregateFunc::Count))
                }),
                params!(Any) => AggregateFunc::Count
            },
            "max" => Aggregate {
                params!(Int32) => AggregateFunc::MaxInt32,
                params!(Int64) => AggregateFunc::MaxInt64,
                params!(Float32) => AggregateFunc::MaxFloat32,
                params!(Float64) => AggregateFunc::MaxFloat64,
                params!(Decimal(0, 0)) => AggregateFunc::MaxDecimal,
                params!(Bool) => AggregateFunc::MaxBool,
                params!(String) => AggregateFunc::MaxString,
                params!(Date) => AggregateFunc::MaxDate,
                params!(Timestamp) => AggregateFunc::MaxTimestamp,
                params!(TimestampTz) => AggregateFunc::MaxTimestampTz
            },
            "min" => Aggregate {
                params!(Int32) => AggregateFunc::MinInt32,
                params!(Int64) => AggregateFunc::MinInt64,
                params!(Float32) => AggregateFunc::MinFloat32,
                params!(Float64) => AggregateFunc::MinFloat64,
                params!(Decimal(0, 0)) => AggregateFunc::MinDecimal,
                params!(Bool) => AggregateFunc::MinBool,
                params!(String) => AggregateFunc::MinString,
                params!(Date) => AggregateFunc::MinDate,
                params!(Timestamp) => AggregateFunc::MinTimestamp,
                params!(TimestampTz) => AggregateFunc::MinTimestampTz
            },
            "json_agg" => Aggregate {
                params!(Any) => unary_op(|_ecx, _e| unsupported!("json_agg"))
            },
            "jsonb_agg" => Aggregate {
                params!(JsonbAny) => unary_op(|_ecx, e| {
                    // `AggregateFunc::JsonbAgg` filters out `Datum::Null` (it
                    // needs to have *some* identity input), but the semantics
                    // of the SQL function require that `Datum::Null` is treated
                    // as `Datum::JsonbNull`. This call to `coalesce` converts
                    // between the two semantics.
                    let json_null = ScalarExpr::literal(Datum::JsonNull, ScalarType::Jsonb);
                    let e = ScalarExpr::CallVariadic {
                        func: VariadicFunc::Coalesce,
                        exprs: vec![e, json_null],
                    };
                    Ok((e, AggregateFunc::JsonbAgg))
                })
            },
            "string_agg" => Aggregate {
                params!(Any, String) => binary_op(|_ecx, _lhs, _rhs| unsupported!("string_agg"))
            },
            "sum" => Aggregate {
                params!(Int32) => AggregateFunc::SumInt32,
                params!(Int64) => AggregateFunc::SumInt64,
                params!(Float32) => AggregateFunc::SumFloat32,
                params!(Float64) => AggregateFunc::SumFloat64,
                params!(Decimal(0, 0)) => AggregateFunc::SumDecimal,
                params!(Interval) => unary_op(|_ecx, _e| {
                    // Explicitly providing this unsupported overload
                    // prevents `sum(NULL)` from choosing the `Float64`
                    // implementation, so that we match PostgreSQL's behavior.
                    // Plus we will one day want to support this overload.
                    unsupported!("sum(interval)");
                })
            },

            // Table functions.
            "generate_series" => Table {
                params!(Int32, Int32) => binary_op(move |_ecx, start, stop| {
                    Ok(TableFuncPlan {
                        func: TableFunc::GenerateSeriesInt32,
                        exprs: vec![start, stop],
                        column_names: vec![Some("generate_series".into())],
                    })
                }),
                params!(Int64, Int64) => binary_op(move |_ecx, start, stop| {
                    Ok(TableFuncPlan {
                        func: TableFunc::GenerateSeriesInt64,
                        exprs: vec![start, stop],
                        column_names: vec![Some("generate_series".into())],
                    })
                })
            },
            "jsonb_array_elements" => Table {
                params!(Jsonb) => unary_op(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        func: TableFunc::JsonbArrayElements { stringify: false },
                        exprs: vec![jsonb],
                        column_names: vec![Some("value".into())],
                    })
                })
            },
            "jsonb_array_elements_text" => Table {
                params!(Jsonb) => unary_op(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        func: TableFunc::JsonbArrayElements { stringify: true },
                        exprs: vec![jsonb],
                        column_names: vec![Some("value".into())],
                    })
                })
            },
            "jsonb_each" => Table {
                params!(Jsonb) => unary_op(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        func: TableFunc::JsonbEach { stringify: false },
                        exprs: vec![jsonb],
                        column_names: vec![Some("key".into()), Some("value".into())],
                    })
                })
            },
            "jsonb_each_text" => Table {
                params!(Jsonb) => unary_op(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        func: TableFunc::JsonbEach { stringify: true },
                        exprs: vec![jsonb],
                        column_names: vec![Some("key".into()), Some("value".into())],
                    })
                })
            },
            "jsonb_object_keys" => Table {
                params!(Jsonb) => unary_op(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        func: TableFunc::JsonbObjectKeys,
                        exprs: vec![jsonb],
                        column_names: vec![Some("jsonb_object_keys".into())],
                    })
                })
            },
            "internal_read_persisted_data" => Table {
                params!(String) => unary_op(move |ecx, source| {
                    let source = match source.into_literal_string(){
                        Some(id) => id,
                        None => bail!("source passed to internal_read_persisted_data must be literal string"),
                    };
                    let item = ecx.qcx.scx.resolve_item(ObjectName(vec![Ident::new(source.clone())]))?;
                    let entry = ecx.qcx.scx.catalog.get_item(&item);
                    match entry.item_type() {
                        CatalogItemType::Source => {},
                        _ =>  bail!("{} is a {}, but internal_read_persisted_data requires a source", source, entry.item_type()),
                    }
                    Ok(TableFuncPlan {
                        func: TableFunc::ReadPersistedData {
                            source: entry.id(),
                        },
                        exprs: vec![],
                        column_names: vec!["filename", "offset", "key", "value"].iter().map(|c| Some(ColumnName::from(*c))).collect(),
                    })
                })
            }
        }
    };

    static ref MZ_CATALOG_BUILTINS: HashMap<&'static str, Func> = {
        use ScalarType::*;
        builtins! {
            "csv_extract" => Table {
                params!(Int64, String) => binary_op(move |_ecx, ncols, input| {
                    let ncols = match ncols.into_literal_int64() {
                        None | Some(i64::MIN..=0) => {
                            bail!("csv_extract number of columns must be a positive integer literal");
                        },
                        Some(ncols) => ncols,
                    };
                    let ncols = usize::try_from(ncols).expect("known to be greater than zero");
                    Ok(TableFuncPlan {
                        func: TableFunc::CsvExtract(ncols),
                        exprs: vec![input],
                        column_names: (1..=ncols).map(|i| Some(format!("column{}", i).into())).collect(),
                    })
                })
            },
            "list_ndims" => Scalar {
                params!(List(Box::new(String))) => unary_op(|ecx, e| {
                    ecx.require_experimental_mode("list_ndims")?;
                    let d = ecx.scalar_type(&e).unwrap_list_n_dims();
                    Ok(ScalarExpr::literal(Datum::Int32(d as i32), ScalarType::Int32))
                })
            },
            "list_length" => Scalar {
                params!(List(Box::new(String))) => UnaryFunc::ListLength
            },
            "list_length_max" => Scalar {
                params!(List(Box::new(String)), Int64) => binary_op(|ecx, lhs, rhs| {
                    ecx.require_experimental_mode("list_length_max")?;
                    let max_dim = ecx.scalar_type(&lhs).unwrap_list_n_dims();
                    Ok(lhs.call_binary(rhs, BinaryFunc::ListLengthMax{ max_dim }))
                })
            },
            "mz_logical_timestamp" => Scalar {
                params!() => nullary_op(|ecx| {
                    match ecx.qcx.lifetime {
                        QueryLifetime::OneShot => {
                            Ok(ScalarExpr::CallNullary(NullaryFunc::MzLogicalTimestamp))
                        }
                        QueryLifetime::Static => bail!("mz_logical_timestamp cannot be used in static queries"),
                    }
                })
            },
            "mz_cluster_id" => Scalar {
                params!() => nullary_op(mz_cluster_id)
            },
            "regexp_extract" => Table {
                params!(String, String) => binary_op(move |_ecx, regex, haystack| {
                    let regex = match regex.into_literal_string() {
                        None => bail!("regex_extract requires a string literal as its first argument"),
                        Some(regex) => expr::AnalyzedRegex::new(&regex)?,
                    };
                    let column_names = regex
                        .capture_groups_iter()
                        .map(|cg| {
                            let name = cg.name.clone().unwrap_or_else(|| format!("column{}", cg.index));
                            Some(name.into())
                        })
                        .collect();
                    Ok(TableFuncPlan {
                        func: TableFunc::RegexpExtract(regex),
                        exprs: vec![haystack],
                        column_names,
                    })
                })
            },
            "repeat" => Table {
                params!(Int64) => unary_op(move |ecx, n| {
                    ecx.require_experimental_mode("repeat")?;
                    Ok(TableFuncPlan {
                        func: TableFunc::Repeat,
                        exprs: vec![n],
                        column_names: vec![]
                    })
                })
            }
        }
    };


    static ref MZ_INTERNAL_BUILTINS: HashMap<&'static str, Func> = {
        use ParamType::*;
        use ScalarType::*;
        builtins! {
            "mz_all" => Aggregate {
                params!(Any) => AggregateFunc::All
            },
            "mz_any" => Aggregate {
                params!(Any) => AggregateFunc::Any
            },
            "mz_avg_promotion" => Scalar {
                // Promotes a numeric type to the smallest fractional type that
                // can represent it. This is primarily useful for the avg
                // aggregate function, so that the avg of an integer column does
                // not get truncated to an integer, which would be surprising to
                // users (#549).
                params!(Float32) => identity_op(),
                params!(Float64) => identity_op(),
                params!(Decimal(0, 0)) => identity_op(),
                params!(Int32) => unary_op(|ecx, e| {
                      super::typeconv::plan_cast(
                          "internal.avg_promotion", ecx, e,
                          CastTo::Explicit(ScalarType::Decimal(10, 0)),
                      )
                })
            }
        }
    };
}

fn plan_current_timestamp(ecx: &ExprContext, name: &str) -> Result<ScalarExpr, anyhow::Error> {
    match ecx.qcx.lifetime {
        QueryLifetime::OneShot => Ok(ScalarExpr::literal(
            Datum::from(ecx.qcx.scx.pcx.wall_time),
            ScalarType::TimestampTz,
        )),
        QueryLifetime::Static => bail!("{} cannot be used in static queries", name),
    }
}

fn mz_cluster_id(ecx: &ExprContext) -> Result<ScalarExpr, anyhow::Error> {
    Ok(ScalarExpr::literal(
        Datum::from(ecx.qcx.scx.catalog.cluster_id()),
        ScalarType::Uuid,
    ))
}

fn array_to_string(ecx: &ExprContext, exprs: Vec<ScalarExpr>) -> Result<ScalarExpr, anyhow::Error> {
    let elem_type = match ecx.scalar_type(&exprs[0]) {
        ScalarType::Array(elem_type) => *elem_type,
        _ => unreachable!("array_to_string is guaranteed to receive array as first argument"),
    };
    Ok(ScalarExpr::CallVariadic {
        func: VariadicFunc::ArrayToString { elem_type },
        exprs,
    })
}

fn stringify_opt_scalartype(t: &Option<ScalarType>) -> String {
    match t {
        Some(t) => t.to_string(),
        None => "unknown".to_string(),
    }
}

fn func_err_string(ident: &str, types: &[Option<ScalarType>], hint: String) -> String {
    format!(
        "Cannot call function {}({}): {}",
        ident,
        types.iter().map(|o| stringify_opt_scalartype(o)).join(", "),
        hint,
    )
}

/// Resolves the name to a set of function implementations.
///
/// If the name does not specify a known built-in function, returns an error.
pub fn resolve(scx: &StatementContext, name: &PartialName) -> Result<&'static Func, anyhow::Error> {
    // NOTE(benesch): In theory, the catalog should be in charge of resolving
    // function names. In practice, it is much easier to do our own hardcoded
    // resolution here while all functions are builtins. This decision will
    // need to be revisited when either:
    //   * we support configuring the search path from its default, or
    //   * we support user-defined functions.

    if let Some(database) = &name.database {
        // If a database name is provided, we need only verify that the
        // database exists, as presently functions can only exist in ambient
        // schemas.
        let _ = scx.catalog.resolve_database(database)?;
    }
    let search_path = match name.schema.as_deref() {
        Some("pg_catalog") => vec![&*PG_CATALOG_BUILTINS],
        Some("mz_catalog") => vec![&*MZ_CATALOG_BUILTINS],
        Some("mz_internal") => vec![&*MZ_INTERNAL_BUILTINS],
        Some(_) => vec![],
        None => vec![&*MZ_CATALOG_BUILTINS, &*PG_CATALOG_BUILTINS],
    };
    for builtins in search_path {
        if let Some(func) = builtins.get(&*name.item) {
            return Ok(func);
        }
    }
    bail!("function \"{}\" does not exist", name)
}

/// Selects the correct function implementation from a list of implementations
/// given the provided arguments.
pub fn select_impl<R>(
    ecx: &ExprContext,
    name: &PartialName,
    impls: &[FuncImpl<R>],
    args: &[Expr],
) -> Result<R, anyhow::Error> {
    let mut cexprs = Vec::new();
    for arg in args {
        let cexpr = query::plan_expr(ecx, arg)?;
        cexprs.push(cexpr);
    }

    let ident = &name.to_string();
    ArgImplementationMatcher::select_implementation(ident, func_err_string, ecx, impls, cexprs)
}

lazy_static! {
    /// Correlates a `BinaryOperator` with all of its implementations.
    static ref BINARY_OP_IMPLS: HashMap<BinaryOperator, Func> = {
        use ScalarType::*;
        use BinaryOperator::*;
        use BinaryFunc::*;
        use ParamType::*;
        builtins! {
            // ARITHMETIC
            Plus => Scalar {
                params!(Int32, Int32) => AddInt32,
                params!(Int64, Int64) => AddInt64,
                params!(Float32, Float32) => AddFloat32,
                params!(Float64, Float64) => AddFloat64,
                params!(Decimal(0, 0), Decimal(0, 0)) => {
                    binary_op(|ecx, lhs, rhs| {
                        let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                        Ok(lexpr.call_binary(rexpr, AddDecimal))
                    })
                },
                params!(Interval, Interval) => AddInterval,
                params!(Timestamp, Interval) => AddTimestampInterval,
                params!(Interval, Timestamp) => {
                    binary_op(|_ecx, lhs, rhs| Ok(rhs.call_binary(lhs, AddTimestampInterval)))
                },
                params!(TimestampTz, Interval) => AddTimestampTzInterval,
                params!(Interval, TimestampTz) => {
                    binary_op(|_ecx, lhs, rhs| Ok(rhs.call_binary(lhs, AddTimestampTzInterval)))
                },
                params!(Date, Interval) => AddDateInterval,
                params!(Interval, Date) => {
                    binary_op(|_ecx, lhs, rhs| Ok(rhs.call_binary(lhs, AddDateInterval)))
                },
                params!(Date, Time) => AddDateTime,
                params!(Time, Date) => {
                    binary_op(|_ecx, lhs, rhs| Ok(rhs.call_binary(lhs, AddDateTime)))
                },
                params!(Time, Interval) => AddTimeInterval,
                params!(Interval, Time) => {
                    binary_op(|_ecx, lhs, rhs| Ok(rhs.call_binary(lhs, AddTimeInterval)))
                }
            },
            Minus => Scalar {
                params!(Int32, Int32) => SubInt32,
                params!(Int64, Int64) => SubInt64,
                params!(Float32, Float32) => SubFloat32,
                params!(Float64, Float64) => SubFloat64,
                params!(Decimal(0, 0), Decimal(0, 0)) => binary_op(|ecx, lhs, rhs| {
                    let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                    Ok(lexpr.call_binary(rexpr, SubDecimal))
                }),
                params!(Interval, Interval) => SubInterval,
                params!(Timestamp, Timestamp) => SubTimestamp,
                params!(TimestampTz, TimestampTz) => SubTimestampTz,
                params!(Timestamp, Interval) => SubTimestampInterval,
                params!(TimestampTz, Interval) => SubTimestampTzInterval,
                params!(Date, Date) => SubDate,
                params!(Date, Interval) => SubDateInterval,
                params!(Time, Time) => SubTime,
                params!(Time, Interval) => SubTimeInterval,
                params!(Jsonb, Int64) => JsonbDeleteInt64,
                params!(Jsonb, String) => JsonbDeleteString
                // TODO(jamii) there should be corresponding overloads for
                // Array(Int64) and Array(String)
            },
            Multiply => Scalar {
                params!(Int32, Int32) => MulInt32,
                params!(Int64, Int64) => MulInt64,
                params!(Float32, Float32) => MulFloat32,
                params!(Float64, Float64) => MulFloat64,
                params!(Decimal(0, 0), Decimal(0, 0)) => binary_op(|ecx, lhs, rhs| {
                    use std::cmp::*;
                    let (_, s1) = ecx.scalar_type(&lhs).unwrap_decimal_parts();
                    let (_, s2) = ecx.scalar_type(&rhs).unwrap_decimal_parts();
                    let so = max(max(min(s1 + s2, 12), s1), s2);
                    let si = s1 + s2;
                    let expr = lhs.call_binary(rhs, MulDecimal);
                    Ok(rescale_decimal(expr, si, so))
                })
            },
            Divide => Scalar {
                params!(Int32, Int32) => DivInt32,
                params!(Int64, Int64) => DivInt64,
                params!(Float32, Float32) => DivFloat32,
                params!(Float64, Float64) => DivFloat64,
                params!(Decimal(0, 0), Decimal(0, 0)) => binary_op(|ecx, lhs, rhs| {
                    use std::cmp::*;
                    let (_, s1) = ecx.scalar_type(&lhs).unwrap_decimal_parts();
                    let (_, s2) = ecx.scalar_type(&rhs).unwrap_decimal_parts();
                    // Pretend all 0-scale numerators were of the same scale as
                    // their denominators for improved accuracy.
                    let s1_mod = if s1 == 0 { s2 } else { s1 };
                    let s = max(min(12, s1_mod + 6), s1_mod);
                    let si = max(s + 1, s2);
                    let lhs = rescale_decimal(lhs, s1, si);
                    let expr = lhs.call_binary(rhs, DivDecimal);
                    Ok(rescale_decimal(expr, si - s2, s))
                })
            },
            Modulus => Scalar {
                params!(Int32, Int32) => ModInt32,
                params!(Int64, Int64) => ModInt64,
                params!(Float32, Float32) => ModFloat32,
                params!(Float64, Float64) => ModFloat64,
                params!(Decimal(0, 0), Decimal(0, 0)) => binary_op(|ecx, lhs, rhs| {
                    let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                    Ok(lexpr.call_binary(rexpr, ModDecimal))
                })
            },

            // BOOLEAN OPS
            BinaryOperator::And => Scalar {
                params!(Bool, Bool) => BinaryFunc::And
            },
            BinaryOperator::Or => Scalar {
                params!(Bool, Bool) => BinaryFunc::Or
            },

            // LIKE
            Like => Scalar {
                params!(String, String) => MatchLikePattern
            },
            NotLike => Scalar {
                params!(String, String) => binary_op(|_ecx, lhs, rhs| {
                    Ok(lhs
                        .call_binary(rhs, MatchLikePattern)
                        .call_unary(UnaryFunc::Not))
                })
            },

            // REGEX
            RegexMatch => Scalar {
                params!(String, String) => MatchRegex { case_insensitive: false }
            },
            RegexIMatch => Scalar {
                params!(String, String) => binary_op(|_ecx, lhs, rhs| {
                    Ok(lhs.call_binary(rhs, MatchRegex { case_insensitive: true }))
                })
            },
            RegexNotMatch => Scalar {
                params!(String, String) => binary_op(|_ecx, lhs, rhs| {
                    Ok(lhs
                        .call_binary(rhs, MatchRegex { case_insensitive: false })
                        .call_unary(UnaryFunc::Not))
                })
            },
            RegexNotIMatch => Scalar {
                params!(String, String) => binary_op(|_ecx, lhs, rhs| {
                    Ok(lhs
                        .call_binary(rhs, MatchRegex { case_insensitive: true })
                        .call_unary(UnaryFunc::Not))
                })
            },

            // CONCAT
            Concat => Scalar {
                vec![Plain(String), StringAny] => TextConcat,
                vec![StringAny, Plain(String)] => TextConcat,
                params!(String, String) => TextConcat,
                params!(Jsonb, Jsonb) => JsonbConcat
            },

            //JSON
            JsonGet => Scalar {
                params!(Jsonb, Int64) => JsonbGetInt64 { stringify: false },
                params!(Jsonb, String) => JsonbGetString { stringify: false }
            },
            JsonGetAsText => Scalar {
                params!(Jsonb, Int64) => JsonbGetInt64 { stringify: true },
                params!(Jsonb, String) => JsonbGetString { stringify: true }
            },
            JsonContainsJson => Scalar {
                params!(Jsonb, Jsonb) => JsonbContainsJsonb,
                params!(Jsonb, String) => binary_op(|_ecx, lhs, rhs| {
                    Ok(lhs.call_binary(
                        rhs.call_unary(UnaryFunc::CastStringToJsonb),
                        JsonbContainsJsonb,
                    ))
                }),
                params!(String, Jsonb) => binary_op(|_ecx, lhs, rhs| {
                    Ok(lhs.call_unary(UnaryFunc::CastStringToJsonb)
                          .call_binary(rhs, JsonbContainsJsonb))
                })
            },
            JsonContainedInJson => Scalar {
                params!(Jsonb, Jsonb) =>  binary_op(|_ecx, lhs, rhs| {
                    Ok(rhs.call_binary(
                        lhs,
                        JsonbContainsJsonb
                    ))
                }),
                params!(Jsonb, String) => binary_op(|_ecx, lhs, rhs| {
                    Ok(rhs.call_unary(UnaryFunc::CastStringToJsonb)
                          .call_binary(lhs, BinaryFunc::JsonbContainsJsonb))
                }),
                params!(String, Jsonb) => binary_op(|_ecx, lhs, rhs| {
                    Ok(rhs.call_binary(
                        lhs.call_unary(UnaryFunc::CastStringToJsonb),
                        BinaryFunc::JsonbContainsJsonb,
                    ))
                })
            },
            JsonContainsField => Scalar {
                params!(Jsonb, String) => JsonbContainsString
            },
            // COMPARISON OPS
            // n.b. Decimal impls are separated from other types because they
            // require a function pointer, which you cannot dynamically generate.
            BinaryOperator::Lt => Scalar {
                params!(Decimal(0, 0), Decimal(0, 0)) => {
                    binary_op(|ecx, lhs, rhs| {
                        let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                        Ok(lexpr.call_binary(rexpr, BinaryFunc::Lt))
                    })
                },
                params!(Bool, Bool) => BinaryFunc::Lt,
                params!(Int32, Int32) => BinaryFunc::Lt,
                params!(Int64, Int64) => BinaryFunc::Lt,
                params!(Float32, Float32) => BinaryFunc::Lt,
                params!(Float64, Float64) => BinaryFunc::Lt,
                params!(Oid, Oid) => BinaryFunc::Lt,
                params!(Date, Date) => BinaryFunc::Lt,
                params!(Time, Time) => BinaryFunc::Lt,
                params!(Timestamp, Timestamp) => BinaryFunc::Lt,
                params!(TimestampTz, TimestampTz) => BinaryFunc::Lt,
                params!(Interval, Interval) => BinaryFunc::Lt,
                params!(Bytes, Bytes) => BinaryFunc::Lt,
                params!(String, String) => BinaryFunc::Lt,
                params!(Jsonb, Jsonb) => BinaryFunc::Lt
            },
            BinaryOperator::LtEq => Scalar {
                params!(Decimal(0, 0), Decimal(0, 0)) => {
                    binary_op(|ecx, lhs, rhs| {
                        let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                        Ok(lexpr.call_binary(rexpr, BinaryFunc::Lte))
                    })
                },
                params!(Bool, Bool) => BinaryFunc::Lte,
                params!(Int32, Int32) => BinaryFunc::Lte,
                params!(Int64, Int64) => BinaryFunc::Lte,
                params!(Float32, Float32) => BinaryFunc::Lte,
                params!(Float64, Float64) => BinaryFunc::Lte,
                params!(Oid, Oid) => BinaryFunc::Lte,
                params!(Date, Date) => BinaryFunc::Lte,
                params!(Time, Time) => BinaryFunc::Lte,
                params!(Timestamp, Timestamp) => BinaryFunc::Lte,
                params!(TimestampTz, TimestampTz) => BinaryFunc::Lte,
                params!(Interval, Interval) => BinaryFunc::Lte,
                params!(Bytes, Bytes) => BinaryFunc::Lte,
                params!(String, String) => BinaryFunc::Lte,
                params!(Jsonb, Jsonb) => BinaryFunc::Lte
            },
            BinaryOperator::Gt => Scalar {
                params!(Decimal(0, 0), Decimal(0, 0)) => {
                    binary_op(|ecx, lhs, rhs| {
                        let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                        Ok(lexpr.call_binary(rexpr, BinaryFunc::Gt))
                    })
                },
                params!(Bool, Bool) => BinaryFunc::Gt,
                params!(Int32, Int32) => BinaryFunc::Gt,
                params!(Int64, Int64) => BinaryFunc::Gt,
                params!(Float32, Float32) => BinaryFunc::Gt,
                params!(Float64, Float64) => BinaryFunc::Gt,
                params!(Oid, Oid) => BinaryFunc::Gt,
                params!(Date, Date) => BinaryFunc::Gt,
                params!(Time, Time) => BinaryFunc::Gt,
                params!(Timestamp, Timestamp) => BinaryFunc::Gt,
                params!(TimestampTz, TimestampTz) => BinaryFunc::Gt,
                params!(Interval, Interval) => BinaryFunc::Gt,
                params!(Bytes, Bytes) => BinaryFunc::Gt,
                params!(String, String) => BinaryFunc::Gt,
                params!(Jsonb, Jsonb) => BinaryFunc::Gt
            },
            BinaryOperator::GtEq => Scalar {
                params!(Decimal(0, 0), Decimal(0, 0)) => {
                    binary_op(|ecx, lhs, rhs| {
                        let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                        Ok(lexpr.call_binary(rexpr, BinaryFunc::Gte))
                    })
                },
                params!(Bool, Bool) => BinaryFunc::Gte,
                params!(Int32, Int32) => BinaryFunc::Gte,
                params!(Int64, Int64) => BinaryFunc::Gte,
                params!(Float32, Float32) => BinaryFunc::Gte,
                params!(Float64, Float64) => BinaryFunc::Gte,
                params!(Oid, Oid) => BinaryFunc::Gte,
                params!(Date, Date) => BinaryFunc::Gte,
                params!(Time, Time) => BinaryFunc::Gte,
                params!(Timestamp, Timestamp) => BinaryFunc::Gte,
                params!(TimestampTz, TimestampTz) => BinaryFunc::Gte,
                params!(Interval, Interval) => BinaryFunc::Gte,
                params!(Bytes, Bytes) => BinaryFunc::Gte,
                params!(String, String) => BinaryFunc::Gte,
                params!(Jsonb, Jsonb) => BinaryFunc::Gte
            },
            BinaryOperator::Eq => Scalar {
                params!(Decimal(0, 0), Decimal(0, 0)) => {
                    binary_op(|ecx, lhs, rhs| {
                        let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                        Ok(lexpr.call_binary(rexpr, BinaryFunc::Eq))
                    })
                },
                params!(Bool, Bool) => BinaryFunc::Eq,
                params!(Int32, Int32) => BinaryFunc::Eq,
                params!(Int64, Int64) => BinaryFunc::Eq,
                params!(Float32, Float32) => BinaryFunc::Eq,
                params!(Float64, Float64) => BinaryFunc::Eq,
                params!(Oid, Oid) => BinaryFunc::Eq,
                params!(Date, Date) => BinaryFunc::Eq,
                params!(Time, Time) => BinaryFunc::Eq,
                params!(Timestamp, Timestamp) => BinaryFunc::Eq,
                params!(TimestampTz, TimestampTz) => BinaryFunc::Eq,
                params!(Interval, Interval) => BinaryFunc::Eq,
                params!(Bytes, Bytes) => BinaryFunc::Eq,
                params!(String, String) => BinaryFunc::Eq,
                params!(Jsonb, Jsonb) => BinaryFunc::Eq
            },
            BinaryOperator::NotEq => Scalar {
                params!(Decimal(0, 0), Decimal(0, 0)) => {
                    binary_op(|ecx, lhs, rhs| {
                        let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                        Ok(lexpr.call_binary(rexpr, BinaryFunc::NotEq))
                    })
                },
                params!(Bool, Bool) => BinaryFunc::NotEq,
                params!(Int32, Int32) => BinaryFunc::NotEq,
                params!(Int64, Int64) => BinaryFunc::NotEq,
                params!(Float32, Float32) => BinaryFunc::NotEq,
                params!(Float64, Float64) => BinaryFunc::NotEq,
                params!(Oid, Oid) => BinaryFunc::NotEq,
                params!(Date, Date) => BinaryFunc::NotEq,
                params!(Time, Time) => BinaryFunc::NotEq,
                params!(Timestamp, Timestamp) => BinaryFunc::NotEq,
                params!(TimestampTz, TimestampTz) => BinaryFunc::NotEq,
                params!(Interval, Interval) => BinaryFunc::NotEq,
                params!(Bytes, Bytes) => BinaryFunc::NotEq,
                params!(String, String) => BinaryFunc::NotEq,
                params!(Jsonb, Jsonb) => BinaryFunc::NotEq
            }
        }
    };
}

/// Rescales two decimals to have the same scale.
fn rescale_decimals_to_same(
    ecx: &ExprContext,
    lhs: ScalarExpr,
    rhs: ScalarExpr,
) -> (ScalarExpr, ScalarExpr) {
    let (_, s1) = ecx.scalar_type(&lhs).unwrap_decimal_parts();
    let (_, s2) = ecx.scalar_type(&rhs).unwrap_decimal_parts();
    let so = std::cmp::max(s1, s2);
    let lexpr = rescale_decimal(lhs, s1, so);
    let rexpr = rescale_decimal(rhs, s2, so);
    (lexpr, rexpr)
}

fn binary_op_err_string(ident: &str, types: &[Option<ScalarType>], hint: String) -> String {
    format!(
        "no overload for {} {} {}: {}",
        stringify_opt_scalartype(&types[0]),
        ident,
        stringify_opt_scalartype(&types[1]),
        hint,
    )
}

/// Plans a function compatible with the `BinaryOperator`.
pub fn plan_binary_op<'a>(
    ecx: &ExprContext,
    op: &'a BinaryOperator,
    left: &'a Expr,
    right: &'a Expr,
) -> Result<ScalarExpr, anyhow::Error> {
    let func = match BINARY_OP_IMPLS.get(&op) {
        Some(i) => i,
        // TODO: these require sql arrays
        // JsonContainsAnyFields
        // JsonContainsAllFields
        // TODO: these require json paths
        // JsonGetPath
        // JsonGetPathAsText
        // JsonDeletePath
        // JsonContainsPath
        // JsonApplyPathPredicate
        None => unsupported!(op),
    };

    let impls = match func {
        Func::Scalar(impls) => impls,
        _ => unreachable!("all binary operators must be scalar functions"),
    };

    let cexprs = vec![query::plan_expr(ecx, left)?, query::plan_expr(ecx, right)?];

    ArgImplementationMatcher::select_implementation(
        &op.to_string(),
        binary_op_err_string,
        ecx,
        impls,
        cexprs,
    )
}

lazy_static! {
    /// Correlates a `UnaryOperator` with all of its implementations.
    static ref UNARY_OP_IMPLS: HashMap<UnaryOperator, Func> = {
        use ParamType::*;
        use ScalarType::*;
        use UnaryOperator::*;
        builtins! {
            Not => Scalar {
                params!(Bool) => UnaryFunc::Not
            },

            Plus => Scalar {
                params!(Any) => identity_op()
            },

            Minus => Scalar {
                params!(Int32) => UnaryFunc::NegInt32,
                params!(Int64) => UnaryFunc::NegInt64,
                params!(Float32) => UnaryFunc::NegFloat32,
                params!(Float64) => UnaryFunc::NegFloat64,
                params!(ScalarType::Decimal(0, 0)) => UnaryFunc::NegDecimal,
                params!(Interval) => UnaryFunc::NegInterval
            }
        }
    };
}

fn unary_op_err_string(ident: &str, types: &[Option<ScalarType>], hint: String) -> String {
    format!(
        "no overload for {} {}: {}",
        ident,
        stringify_opt_scalartype(&types[0]),
        hint,
    )
}

/// Plans a function compatible with the `UnaryOperator`.
pub fn plan_unary_op<'a>(
    ecx: &ExprContext,
    op: &'a UnaryOperator,
    expr: &'a Expr,
) -> Result<ScalarExpr, anyhow::Error> {
    let func = match UNARY_OP_IMPLS.get(&op) {
        Some(i) => i,
        None => unsupported!(op),
    };

    let impls = match func {
        Func::Scalar(impls) => impls,
        _ => unreachable!("all unary operators must be scalar functions"),
    };

    let cexpr = vec![query::plan_expr(ecx, expr)?];

    ArgImplementationMatcher::select_implementation(
        &op.to_string(),
        unary_op_err_string,
        ecx,
        impls,
        cexpr,
    )
}
