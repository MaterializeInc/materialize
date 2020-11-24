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

use anyhow::{bail, Context};
use itertools::Itertools;
use lazy_static::lazy_static;

use ore::collections::CollectionExt;
use repr::{ColumnName, Datum, RelationType, ScalarType};
use sql_parser::ast::{Expr, Ident, ObjectName};

use super::expr::{
    AggregateFunc, BinaryFunc, CoercibleScalarExpr, NullaryFunc, ScalarExpr, TableFunc, UnaryFunc,
    VariadicFunc,
};
use super::query::{self, ExprContext, QueryContext, QueryLifetime};
use super::scope::Scope;
use super::typeconv::{self, rescale_decimal, CastContext};
use super::StatementContext;
use crate::catalog::CatalogItemType;
use crate::names::PartialName;

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
            ScalarType::Array(..) => Self::Array,
            ScalarType::Bool => Self::Bool,
            ScalarType::Bytes | ScalarType::Jsonb | ScalarType::Uuid => Self::UserDefined,
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
            ScalarType::List(..) => Self::List,
            ScalarType::String => Self::String,
            ScalarType::Record { .. } => Self::Pseudo,
            ScalarType::Map { .. } => Self::Pseudo,
        }
    }

    fn from_param(param: &ParamType) -> Self {
        match param {
            ParamType::Any
            | ParamType::ArrayAny
            | ParamType::ListAny
            | ParamType::ListElementAny
            | ParamType::NonVecAny
            | ParamType::MapAny => Self::Pseudo,
            ParamType::DecimalAny => Self::Numeric,
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
    fn preferred_type(&self) -> Option<ScalarType> {
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
                FuncSpec,
                Vec<CoercibleScalarExpr>,
                &ParamList,
            ) -> Result<R, anyhow::Error>
            + Send
            + Sync,
    >,
);

impl Operation<ScalarExpr> {
    /// Builds a unary operation that simply returns its input.
    fn identity() -> Operation<ScalarExpr> {
        Operation::unary(|_ecx, e| Ok(e))
    }
}

impl<R> Operation<R> {
    fn new<F>(f: F) -> Operation<R>
    where
        F: Fn(
                &ExprContext,
                FuncSpec,
                Vec<CoercibleScalarExpr>,
                &ParamList,
            ) -> Result<R, anyhow::Error>
            + Send
            + Sync
            + 'static,
    {
        Operation(Box::new(f))
    }

    /// Builds an operation that takes no arguments.
    fn nullary<F>(f: F) -> Operation<R>
    where
        F: Fn(&ExprContext) -> Result<R, anyhow::Error> + Send + Sync + 'static,
    {
        Self::variadic(move |ecx, exprs| {
            assert!(exprs.is_empty());
            f(ecx)
        })
    }

    /// Builds an operation that takes one argument.
    fn unary<F>(f: F) -> Operation<R>
    where
        F: Fn(&ExprContext, ScalarExpr) -> Result<R, anyhow::Error> + Send + Sync + 'static,
    {
        Self::variadic(move |ecx, exprs| f(ecx, exprs.into_element()))
    }

    /// Builds an operation that takes two arguments.
    fn binary<F>(f: F) -> Operation<R>
    where
        F: Fn(&ExprContext, ScalarExpr, ScalarExpr) -> Result<R, anyhow::Error>
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

    /// Builds an operation that takes any number of arguments.
    fn variadic<F>(f: F) -> Operation<R>
    where
        F: Fn(&ExprContext, Vec<ScalarExpr>) -> Result<R, anyhow::Error> + Send + Sync + 'static,
    {
        Self::new(move |ecx, spec, cexprs, params| {
            let exprs = coerce_args_to_types(ecx, spec, cexprs, params)?;
            f(ecx, exprs)
        })
    }
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
        Operation::variadic(move |ecx, args| {
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
            expr.splice_parameters(&args, 0);

            Ok(expr)
        })
    }};
}

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

impl From<NullaryFunc> for Operation<ScalarExpr> {
    fn from(n: NullaryFunc) -> Operation<ScalarExpr> {
        Operation::nullary(move |_ecx| Ok(ScalarExpr::CallNullary(n.clone())))
    }
}

impl From<UnaryFunc> for Operation<ScalarExpr> {
    fn from(u: UnaryFunc) -> Operation<ScalarExpr> {
        Operation::unary(move |_ecx, e| Ok(e.call_unary(u.clone())))
    }
}

impl From<BinaryFunc> for Operation<ScalarExpr> {
    fn from(b: BinaryFunc) -> Operation<ScalarExpr> {
        Operation::binary(move |_ecx, left, right| Ok(left.call_binary(right, b.clone())))
    }
}

impl From<VariadicFunc> for Operation<ScalarExpr> {
    fn from(v: VariadicFunc) -> Operation<ScalarExpr> {
        Operation::variadic(move |_ecx, exprs| {
            Ok(ScalarExpr::CallVariadic {
                func: v.clone(),
                exprs,
            })
        })
    }
}

impl From<AggregateFunc> for Operation<(ScalarExpr, AggregateFunc)> {
    fn from(a: AggregateFunc) -> Operation<(ScalarExpr, AggregateFunc)> {
        Operation::unary(move |_ecx, e| Ok((e, a.clone())))
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
    /// Determines whether `typs` are compatible with `self`.
    fn matches_argtypes(&self, typs: &[Option<ScalarType>]) -> bool {
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
                if !param.accepts_type(typ) {
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
            Self::Repeat(p) => input_len % p.len() == 0 && input_len > 0,
        }
    }

    /// Reports whether the parameter list contains any polymorphic parameters.
    fn has_polymorphic(&self) -> bool {
        let p = match self {
            ParamList::Exact(p) | ParamList::Repeat(p) => p,
        };
        p.iter().any(|p| p.is_polymorphic())
    }

    /// Enforces polymorphic type consistency by finding the concrete type
    /// that satisfies the constraints expressed by the polymorphic types in
    /// the parameter list.
    ///
    /// Polymorphic type consistency constraints include:
    /// - All arguments passed to `ArrayAny` must be `ScalarType::Array`s with
    ///   the same types of elements.
    /// - All arguments passed to `ListAny` must be `ScalarType::List`s with the
    ///   same types of elements. All arguments passed to `ListElementAny` must
    ///   also be of these elements' type.
    /// - All arguments passed to `MapAny` must be `ScalarType::Map`s with the
    ///   same type of value in each key, value pair.
    ///
    /// Returns `Some` if the constraints was successfully resolved, or `None`
    /// otherwise.
    fn resolve_polymorphic_types(&self, typs: &[Option<ScalarType>]) -> Option<ScalarType> {
        let mut constrained_type: Option<ScalarType> = None;
        let mut set_or_check_constrained_type = |typ: &ScalarType| {
            match constrained_type {
                None => constrained_type = Some(typ.clone()),
                Some(ref t) => {
                    if typ != t {
                        return Err(());
                    }
                }
            }
            Ok(())
        };

        // Determine the element on which to constrain the parameters.
        for (i, typ) in typs.iter().enumerate() {
            let param = &self[i];
            match (param, typ) {
                (ParamType::ListAny, Some(ScalarType::List(typ)))
                | (ParamType::ArrayAny, Some(ScalarType::Array(typ)))
                | (ParamType::MapAny, Some(ScalarType::Map { value_type: typ })) => {
                    set_or_check_constrained_type(typ).ok()?
                }
                (ParamType::ListElementAny, Some(typ)) | (ParamType::NonVecAny, Some(typ)) => {
                    set_or_check_constrained_type(typ).ok()?
                }
                // These checks don't need to be more exhaustive (e.g. failing
                // if arguments passed to `ListAny` are not `ScalartType::List`)
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
    /// A pseudotype permitting any type.
    Any,
    /// A special, Materialize-specific parameter type permitting a decimal of
    /// any precision and scale. Note that while `DecimalAny` matches the
    /// conceptual definition of the word "pseudotype", it does not match the
    /// PostgreSQL definition, as parameters of type `DecimalAny` are considered
    /// to exactly match arguments of decimal type.
    DecimalAny,
    /// A polymorphic pseudotype permitting any array type.  For more details,
    /// see [`resolve_polymorphic_types`].
    ArrayAny,
    /// A polymorphic pseudotype permitting a `ScalarType::List` of any element
    /// type.  For more details, see [`resolve_polymorphic_types`].
    ListAny,
    /// A polymorphic pseudotype permitting all types, with more constraints
    /// than `Any`, i.e. it is subject to polymorphic constraints. For more
    /// details, see [`resolve_polymorphic_types`].
    ListElementAny,
    /// A polymorphic pseudotype with the same behavior as `ListElementAny`,
    /// except it does not permit either `ScalarType::Array` or
    /// `ScalarType::List`.
    NonVecAny,
    /// A polymorphic pseudotype permitting a `ScalarType::Map` of any non-nested
    /// value type. For more details, see [`resolve_polymorphic_types`].
    MapAny,
    /// A standard parameter that accepts arguments that match its embedded
    /// `ScalarType`.
    Plain(ScalarType),
}

impl ParamType {
    /// Does `self` accept arguments of type `t`?
    fn accepts_type(&self, t: &ScalarType) -> bool {
        use ParamType::*;
        use ScalarType::*;

        match self {
            // To support list (and, soon, array) concatenation, we must tell
            // ourselves this white lie until
            // https://github.com/MaterializeInc/materialize/issues/4627
            ArrayAny => matches!(t, Array(..) | String),
            ListAny => matches!(t, List(..) | String),
            Any | ListElementAny => true,
            NonVecAny => !t.is_vec(),
            MapAny => matches!(t, Map { .. }),
            DecimalAny => {
                typeconv::get_direct_cast(CastContext::Implicit, t, &ScalarType::Decimal(0, 0))
                    .is_some()
            }
            Plain(to) => typeconv::get_direct_cast(CastContext::Implicit, t, to).is_some(),
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
            ArrayAny | ListAny | MapAny | ListElementAny | NonVecAny => true,
            Any | DecimalAny | Plain(_) => false,
        }
    }
}

impl PartialEq<ScalarType> for ParamType {
    fn eq(&self, other: &ScalarType) -> bool {
        match self {
            ParamType::Plain(s) => s == other,
            ParamType::DecimalAny => matches!(other, ScalarType::Decimal(_, _)),
            // All other types are pseudotypes, which do not equal concrete
            // types.
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
) -> Result<R, anyhow::Error>
where
    R: fmt::Debug,
{
    let types: Vec<_> = args.iter().map(|e| ecx.scalar_type(e)).collect();
    select_impl_inner(ecx, spec, impls, args, &types).with_context(|| {
        let types: Vec<_> = types
            .into_iter()
            .map(|ty| match ty {
                Some(ty) => ty.to_string(),
                None => "unknown".to_string(),
            })
            .collect();
        match (spec, types.as_slice()) {
            (FuncSpec::Func(name), _) => {
                format!("Cannot call function {}({})", name, types.join(", "))
            }
            (FuncSpec::Op(name), [typ]) => format!("no overload for {} {}", name, typ),
            (FuncSpec::Op(name), [ltyp, rtyp]) => {
                format!("no overload for {} {} {}", ltyp, name, rtyp)
            }
            (FuncSpec::Op(_), [..]) => unreachable!("non-unary non-binary operator"),
        }
    })
}

fn select_impl_inner<R>(
    ecx: &ExprContext,
    spec: FuncSpec,
    impls: &[FuncImpl<R>],
    cexprs: Vec<CoercibleScalarExpr>,
    types: &[Option<ScalarType>],
) -> Result<R, anyhow::Error>
where
    R: fmt::Debug,
{
    // 4.a. Discard candidate functions for which the input types do not
    // match and cannot be converted (using an implicit conversion) to
    // match. unknown literals are assumed to be convertible to anything for
    // this purpose.
    let impls: Vec<_> = impls
        .iter()
        .filter(|i| i.params.matches_argtypes(types))
        .collect();

    let f = find_match(types, impls)?;

    (f.op.0)(ecx, spec, cexprs, &f.params)
}

/// Finds an exact match based on the arguments, or, if no exact match, finds
/// the best match available. Patterned after [PostgreSQL's type conversion
/// matching algorithm][pgparser].
///
/// [pgparser]: https://www.postgresql.org/docs/current/typeconv-func.html
fn find_match<'a, R: std::fmt::Debug>(
    types: &[Option<ScalarType>],
    impls: Vec<&'a FuncImpl<R>>,
) -> Result<&'a FuncImpl<R>, anyhow::Error> {
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
        bail!(
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
            // 4.e. If any input arguments are unknown, check the type
            // categories accepted at those argument positions by the remaining
            // candidates.
            None => {
                for c in candidates.iter() {
                    let this_category = TypeCategory::from_param(&c.fimpl.params[i]);
                    match selected_category {
                        Some(ref mut selected_category) => {
                            // 4.e. cont: ...if all the remaining candidates
                            // accept the same type category, select that category.
                            categories_match =
                                selected_category == &this_category && categories_match;
                            // 4.e. cont: [except for...] select the string
                            // category if any candidate accepts that category.
                            // (This bias towards string is appropriate since an
                            // unknown-type literal looks like a string.)
                            if this_category == TypeCategory::String {
                                *selected_category = TypeCategory::String;
                                found_string_candidate = true;
                            }
                        }
                        None => selected_category = Some(this_category.clone()),
                    }
                }

                // 4.e. cont: Otherwise fail because the correct choice cannot
                // be deduced without more clues.
                // (ed: this doesn't mean fail entirely, simply moving onto 4.f)
                if !found_string_candidate && !categories_match {
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
                        found_preferred_type_candidate =
                            c.fimpl.params[i].accepts_type(typ) || found_preferred_type_candidate;
                    }
                    selected_category == TypeCategory::from_param(&c.fimpl.params[i])
                });

                if found_preferred_type_candidate {
                    let preferred_type = preferred_type.unwrap();
                    candidates.retain(|c| c.fimpl.params[i].accepts_type(&preferred_type));
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

        candidates.retain(|c| c.fimpl.params.matches_argtypes(&common_typed));

        maybe_get_last_candidate!();
    }

    bail!(
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
    spec: FuncSpec,
    args: Vec<CoercibleScalarExpr>,
    params: &ParamList,
) -> Result<Vec<ScalarExpr>, anyhow::Error> {
    let types: Vec<_> = args.iter().map(|e| ecx.scalar_type(e)).collect();
    let get_constrained_ty = || {
        params
            .resolve_polymorphic_types(&types)
            .expect("function selection verifies that polymorphic types successfully resolved")
    };

    let do_convert = |arg, ty: &ScalarType| {
        let arg = typeconv::plan_coerce(ecx, arg, ty)?;
        typeconv::plan_cast(spec, ecx, CastContext::Implicit, arg, ty)
    };

    let mut exprs = Vec::new();
    for (i, arg) in args.into_iter().enumerate() {
        let expr = match &params[i] {
            // Concrete type. Direct conversion.
            ParamType::Plain(ty) => do_convert(arg, ty)?,

            // Polymorphic pseudotypes. Convert based on constrained type.
            ParamType::ArrayAny => {
                let ty = ScalarType::Array(Box::new(get_constrained_ty()));
                do_convert(arg, &ty)?
            }
            ParamType::ListAny => {
                let ty = ScalarType::List(Box::new(get_constrained_ty()));
                do_convert(arg, &ty)?
            }
            ParamType::MapAny => {
                let ty = ScalarType::Map {
                    value_type: Box::new(get_constrained_ty()),
                };
                do_convert(arg, &ty)?
            }
            ParamType::ListElementAny => {
                let ty = get_constrained_ty();
                do_convert(arg, &ty)?
            }
            ParamType::NonVecAny => {
                let ty = get_constrained_ty();
                if ty.is_vec() {
                    bail!(
                        "could not constrain polymorphic type because {} used in \
                        position that does not accept arrays or lists",
                        ty
                    )
                }
                do_convert(arg, &ty)?
            }

            // Arbitrary decimal parameter. Converts to decimal but suppresses
            // decimal -> decimal casts, to avoid casting to the default scale
            // of 0.
            ParamType::DecimalAny => match (arg, &types[i]) {
                (CoercibleScalarExpr::Coerced(arg), Some(ScalarType::Decimal(_, _))) => arg,
                (arg, _) => {
                    let ty = ScalarType::Decimal(0, 0);
                    do_convert(arg, &ty)?
                }
            },

            // Special "any" psuedotype. Per PostgreSQL, uncoerced literals
            // are accepted, but uncoerced parameters are rejected.
            ParamType::Any => match arg {
                CoercibleScalarExpr::Parameter(n) => {
                    bail!("could not determine data type of parameter ${}", n)
                }
                _ => arg.type_as_any(ecx)?,
            },
        };
        exprs.push(expr);
    }
    Ok(exprs)
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

#[derive(Debug)]
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
                params!(DecimalAny) => UnaryFunc::AbsDecimal,
                params!(Float32) => UnaryFunc::AbsFloat32,
                params!(Float64) => UnaryFunc::AbsFloat64
            },
            "array_lower" => Scalar {
                params!(ArrayAny, Int64) => BinaryFunc::ArrayLower
            },
            "array_to_string" => Scalar {
                params!(ArrayAny, String) => Operation::variadic(array_to_string),
                params!(ArrayAny, String, String) => Operation::variadic(array_to_string)
            },
            "array_upper" => Scalar {
                params!(ArrayAny, Int64) => BinaryFunc::ArrayUpper
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
                params!(DecimalAny) => Operation::unary(|ecx, e| {
                    let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                    Ok(e.call_unary(UnaryFunc::CeilDecimal(s)))
                })
            },
            "char_length" => Scalar {
                params!(String) => UnaryFunc::CharLength
            },
            "concat" => Scalar {
                 params!((Any)...) => Operation::variadic(|ecx, cexprs| {
                    let mut exprs = vec![];
                    for expr in cexprs {
                        if ecx.scalar_type(&expr) == ScalarType::Bool {
                            // concat uses nonstandard bool -> string casts
                            // to match historical baggage in PostgreSQL.
                            exprs.push(expr.call_unary(UnaryFunc::CastBoolToStringNonstandard));
                        } else {
                            exprs.push(typeconv::to_string(ecx, expr));
                        }
                    }
                    Ok(ScalarExpr::CallVariadic { func: VariadicFunc::Concat, exprs })
                })
            },
            "convert_from" => Scalar {
                params!(Bytes, String) => BinaryFunc::ConvertFrom
            },
            "current_schemas" => Scalar {
                params!(Bool) => Operation::unary(|ecx, e| {
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
                params!() => Operation::nullary(|ecx| plan_current_timestamp(ecx, "current_timestamp"))
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
                params!(DecimalAny) => Operation::unary(|ecx, e| {
                    let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                    Ok(e.call_unary(UnaryFunc::FloorDecimal(s)))
                })
            },
            "jsonb_array_length" => Scalar {
                params!(Jsonb) => UnaryFunc::JsonbArrayLength
            },
            "jsonb_build_array" => Scalar {
                params!() => VariadicFunc::JsonbBuildArray,
                params!((Any)...) => Operation::variadic(|ecx, exprs| Ok(ScalarExpr::CallVariadic {
                    func: VariadicFunc::JsonbBuildArray,
                    exprs: exprs.into_iter().map(|e| typeconv::to_jsonb(ecx, e)).collect(),
                }))
            },
            "jsonb_build_object" => Scalar {
                params!() => VariadicFunc::JsonbBuildObject,
                params!((Any, Any)...) => Operation::variadic(|ecx, exprs| Ok(ScalarExpr::CallVariadic {
                    func: VariadicFunc::JsonbBuildObject,
                    exprs: exprs.into_iter().tuples().map(|(key, val)| {
                        let key = typeconv::to_string(ecx, key);
                        let val = typeconv::to_jsonb(ecx, val);
                        vec![key, val]
                    }).flatten().collect(),
                }))
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
                params!() => Operation::nullary(|ecx| plan_current_timestamp(ecx, "now"))
            },
            "obj_description" => Scalar {
                params!(Oid, String) => Operation::binary(|_ecx, _oid, _catalog| {
                    // This function is meant to return the comment on a
                    // database object, but we don't presently support comments,
                    // so stubbed out out to always return NULL.
                    Ok(ScalarExpr::literal_null(ScalarType::String))
                })
            },
            "pg_encoding_to_char" => Scalar {
                // Materialize only supports UT8-encoded databases. Return 'UTF8' if Postgres'
                // encoding id for UTF8 (6) is provided, otherwise return 'NULL'.
                params!(Int64) => sql_op!("CASE WHEN $1 = 6 THEN 'UTF8' ELSE NULL END")
            },
            "pg_get_userbyid" => Scalar {
                params!(Oid) => sql_op!("'unknown (OID=' || $1 || ')'")
            },
            "pg_table_is_visible" => Scalar {
                params!(Oid) => sql_op!(
                    "(SELECT s.name = ANY(current_schemas(true))
                     FROM mz_catalog.mz_objects o JOIN mz_catalog.mz_schemas s ON o.schema_id = s.id
                     WHERE o.oid = $1)"
                )
            },
            "pg_typeof" => Scalar {
                params!(Any) => Operation::new(|ecx, spec, exprs, params| {
                    // pg_typeof reports the type *before* coercion.
                    let name = match ecx.scalar_type(&exprs[0]) {
                        None => "unknown",
                        Some(ty) => pgrepr::Type::from(&ty).name(),
                    };

                    // For consistency with other functions, verify that
                    // coercion is possible, though we don't actually care about
                    // the coerced results.
                    coerce_args_to_types(ecx, spec, exprs, params)?;

                    // TODO(benesch): make this function have return type
                    // regtype, when we support that type. Document the function
                    // at that point. For now, it's useful enough to have this
                    // halfway version that returns a string.
                    Ok(ScalarExpr::literal(Datum::String(name), ScalarType::String))
                })
            },
            "regexp_match" => Scalar {
                params!(String, String) => VariadicFunc::RegexpMatch,
                params!(String, String, String) => VariadicFunc::RegexpMatch
            },
            "replace" => Scalar {
                params!(String, String, String) => VariadicFunc::Replace
            },
            "round" => Scalar {
                params!(Float32) => UnaryFunc::RoundFloat32,
                params!(Float64) => UnaryFunc::RoundFloat64,
                params!(DecimalAny) => Operation::unary(|ecx, e| {
                    let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                    Ok(e.call_unary(UnaryFunc::RoundDecimal(s)))
                }),
                params!(DecimalAny, Int64) => Operation::binary(|ecx, lhs, rhs| {
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
                params!(DecimalAny) => Operation::unary(|ecx, e| {
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
                params!(Any) => Operation::unary(|ecx, e| Ok(typeconv::to_jsonb(ecx, e)))
            },
            "to_timestamp" => Scalar {
                params!(Float64) => UnaryFunc::ToTimestamp
            },

            // Aggregates.
            "array_agg" => Aggregate {
                params!(Any) => Operation::unary(|_ecx, _e| unsupported!("array_agg"))
            },
            "bool_and" => Aggregate {
                params!(Any) => Operation::unary(|_ecx, _e| unsupported!("bool_and"))
            },
            "bool_or" => Aggregate {
                params!(Any) => Operation::unary(|_ecx, _e| unsupported!("bool_or"))
            },
            "concat_agg" => Aggregate {
                params!(Any) => Operation::unary(|_ecx, _e| unsupported!("concat_agg"))
            },
            "count" => Aggregate {
                params!() => Operation::nullary(|_ecx| {
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
                params!(DecimalAny) => AggregateFunc::MaxDecimal,
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
                params!(DecimalAny) => AggregateFunc::MinDecimal,
                params!(Bool) => AggregateFunc::MinBool,
                params!(String) => AggregateFunc::MinString,
                params!(Date) => AggregateFunc::MinDate,
                params!(Timestamp) => AggregateFunc::MinTimestamp,
                params!(TimestampTz) => AggregateFunc::MinTimestampTz
            },
            "json_agg" => Aggregate {
                params!(Any) => Operation::unary(|_ecx, _e| unsupported!("json_agg"))
            },
            "jsonb_agg" => Aggregate {
                params!(Any) => Operation::unary(|ecx, e| {
                    // `AggregateFunc::JsonbAgg` filters out `Datum::Null` (it
                    // needs to have *some* identity input), but the semantics
                    // of the SQL function require that `Datum::Null` is treated
                    // as `Datum::JsonbNull`. This call to `coalesce` converts
                    // between the two semantics.
                    let json_null = ScalarExpr::literal(Datum::JsonNull, ScalarType::Jsonb);
                    let e = ScalarExpr::CallVariadic {
                        func: VariadicFunc::Coalesce,
                        exprs: vec![typeconv::to_jsonb(ecx, e), json_null],
                    };
                    Ok((e, AggregateFunc::JsonbAgg))
                })
            },
            "string_agg" => Aggregate {
                params!(Any, String) => Operation::binary(|_ecx, _lhs, _rhs| unsupported!("string_agg"))
            },
            "sum" => Aggregate {
                params!(Int32) => AggregateFunc::SumInt32,
                params!(Int64) => AggregateFunc::SumInt64,
                params!(Float32) => AggregateFunc::SumFloat32,
                params!(Float64) => AggregateFunc::SumFloat64,
                params!(DecimalAny) => AggregateFunc::SumDecimal,
                params!(Interval) => Operation::unary(|_ecx, _e| {
                    // Explicitly providing this unsupported overload
                    // prevents `sum(NULL)` from choosing the `Float64`
                    // implementation, so that we match PostgreSQL's behavior.
                    // Plus we will one day want to support this overload.
                    unsupported!("sum(interval)");
                })
            },

            // Table functions.
            "generate_series" => Table {
                params!(Int32, Int32) => Operation::binary(move |_ecx, start, stop| {
                    Ok(TableFuncPlan {
                        func: TableFunc::GenerateSeriesInt32,
                        exprs: vec![start, stop],
                        column_names: vec![Some("generate_series".into())],
                    })
                }),
                params!(Int64, Int64) => Operation::binary(move |_ecx, start, stop| {
                    Ok(TableFuncPlan {
                        func: TableFunc::GenerateSeriesInt64,
                        exprs: vec![start, stop],
                        column_names: vec![Some("generate_series".into())],
                    })
                })
            },
            "jsonb_array_elements" => Table {
                params!(Jsonb) => Operation::unary(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        func: TableFunc::JsonbArrayElements { stringify: false },
                        exprs: vec![jsonb],
                        column_names: vec![Some("value".into())],
                    })
                })
            },
            "jsonb_array_elements_text" => Table {
                params!(Jsonb) => Operation::unary(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        func: TableFunc::JsonbArrayElements { stringify: true },
                        exprs: vec![jsonb],
                        column_names: vec![Some("value".into())],
                    })
                })
            },
            "jsonb_each" => Table {
                params!(Jsonb) => Operation::unary(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        func: TableFunc::JsonbEach { stringify: false },
                        exprs: vec![jsonb],
                        column_names: vec![Some("key".into()), Some("value".into())],
                    })
                })
            },
            "jsonb_each_text" => Table {
                params!(Jsonb) => Operation::unary(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        func: TableFunc::JsonbEach { stringify: true },
                        exprs: vec![jsonb],
                        column_names: vec![Some("key".into()), Some("value".into())],
                    })
                })
            },
            "jsonb_object_keys" => Table {
                params!(Jsonb) => Operation::unary(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        func: TableFunc::JsonbObjectKeys,
                        exprs: vec![jsonb],
                        column_names: vec![Some("jsonb_object_keys".into())],
                    })
                })
            },
            "internal_read_cached_data" => Table {
                params!(String) => Operation::unary(move |ecx, source| {
                    let source = match source.into_literal_string(){
                        Some(id) => id,
                        None => bail!("source passed to internal_read_cached_data must be literal string"),
                    };
                    let item = ecx.qcx.scx.resolve_item(ObjectName(vec![Ident::new(source.clone())]))?;
                    let entry = ecx.qcx.scx.catalog.get_item(&item);
                    match entry.item_type() {
                        CatalogItemType::Source => {},
                        _ =>  bail!("{} is a {}, but internal_read_cached_data requires a source", source, entry.item_type()),
                    }
                    let cache_directory = ecx.qcx.scx.catalog.cache_directory();
                    if cache_directory.is_none() {
                        bail!("source caching is currently disabled. Try rerunning Materialize with '--experimental'.");
                    }
                    Ok(TableFuncPlan {
                        func: TableFunc::ReadCachedData {
                            source: entry.id(),
                            cache_directory: cache_directory.expect("known to exist").to_path_buf(),
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
        use ParamType::*;
        builtins! {
            "csv_extract" => Table {
                params!(Int64, String) => Operation::binary(move |_ecx, ncols, input| {
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
            "list_append" => Scalar {
                vec![ListAny, ListElementAny] => BinaryFunc::ListElementConcat
            },
            "list_cat" => Scalar {
                vec![ListAny, ListAny] =>  BinaryFunc::ListListConcat
            },
            "list_ndims" => Scalar {
                vec![ListAny] => Operation::unary(|ecx, e| {
                    ecx.require_experimental_mode("list_ndims")?;
                    let d = ecx.scalar_type(&e).unwrap_list_n_dims();
                    Ok(ScalarExpr::literal(Datum::Int32(d as i32), ScalarType::Int32))
                })
            },
            "list_length" => Scalar {
                vec![ListAny] => UnaryFunc::ListLength
            },
            "list_length_max" => Scalar {
                vec![ListAny, Plain(Int64)] => Operation::binary(|ecx, lhs, rhs| {
                    ecx.require_experimental_mode("list_length_max")?;
                    let max_dim = ecx.scalar_type(&lhs).unwrap_list_n_dims();
                    Ok(lhs.call_binary(rhs, BinaryFunc::ListLengthMax{ max_dim }))
                })
            },
            "list_prepend" => Scalar {
                vec![ListElementAny, ListAny] => BinaryFunc::ElementListConcat
            },
            "mz_logical_timestamp" => Scalar {
                params!() => NullaryFunc::MzLogicalTimestamp
            },
            "mz_cluster_id" => Scalar {
                params!() => Operation::nullary(mz_cluster_id)
            },
            "regexp_extract" => Table {
                params!(String, String) => Operation::binary(move |_ecx, regex, haystack| {
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
                params!(Int64) => Operation::unary(move |ecx, n| {
                    ecx.require_experimental_mode("repeat")?;
                    Ok(TableFuncPlan {
                        func: TableFunc::Repeat,
                        exprs: vec![n],
                        column_names: vec![]
                    })
                })
            },
            "unnest" => Table {
                vec![ListAny] => Operation::unary(move |ecx, e| {
                    let el_typ =  ecx.scalar_type(&e).unwrap_list_element_type().clone();
                    Ok(TableFuncPlan {
                        func: TableFunc::UnnestList{ el_typ },
                        exprs: vec![e],
                        column_names: vec![Some("unnest".into())],
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
                params!(Float32) => Operation::identity(),
                params!(Float64) => Operation::identity(),
                params!(DecimalAny) => Operation::identity(),
                params!(Int32) => Operation::unary(|ecx, e| {
                      super::typeconv::plan_cast(
                          "internal.avg_promotion", ecx, CastContext::Explicit,
                          e, &ScalarType::Decimal(10, 0),
                      )
                })
            },
            "mz_classify_object_id" => Scalar {
                params!(String) => sql_op!(
                    "CASE
                        WHEN $1 LIKE 'u%' THEN 'user'
                        WHEN $1 LIKE 's%' THEN 'system'
                        WHEN $1 like 't%' THEN 'temp'
                    END"
                )
            },
            "mz_is_materialized" => Scalar {
                params!(String) => sql_op!("EXISTS (SELECT 1 FROM mz_indexes WHERE on_id = $1)")
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

/// Resolves the name to a set of function implementations.
///
/// If the name does not specify a known built-in function, returns an error.
pub fn resolve_func(
    scx: &StatementContext,
    name: &PartialName,
) -> Result<&'static Func, anyhow::Error> {
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

lazy_static! {
    /// Correlates an operator with all of its implementations.
    static ref OP_IMPLS: HashMap<&'static str, Func> = {
        use ScalarType::*;
        use BinaryFunc::*;
        use ParamType::*;
        builtins! {
            // ARITHMETIC
            "+" => Scalar {
                params!(Any) => Operation::new(|ecx, _spec, exprs, _params| {
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
                }),
                params!(Int32, Int32) => AddInt32,
                params!(Int64, Int64) => AddInt64,
                params!(Float32, Float32) => AddFloat32,
                params!(Float64, Float64) => AddFloat64,
                params!(DecimalAny, DecimalAny) => {
                    Operation::binary(|ecx, lhs, rhs| {
                        let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                        Ok(lexpr.call_binary(rexpr, AddDecimal))
                    })
                },
                params!(Interval, Interval) => AddInterval,
                params!(Timestamp, Interval) => AddTimestampInterval,
                params!(Interval, Timestamp) => {
                    Operation::binary(|_ecx, lhs, rhs| Ok(rhs.call_binary(lhs, AddTimestampInterval)))
                },
                params!(TimestampTz, Interval) => AddTimestampTzInterval,
                params!(Interval, TimestampTz) => {
                    Operation::binary(|_ecx, lhs, rhs| Ok(rhs.call_binary(lhs, AddTimestampTzInterval)))
                },
                params!(Date, Interval) => AddDateInterval,
                params!(Interval, Date) => {
                    Operation::binary(|_ecx, lhs, rhs| Ok(rhs.call_binary(lhs, AddDateInterval)))
                },
                params!(Date, Time) => AddDateTime,
                params!(Time, Date) => {
                    Operation::binary(|_ecx, lhs, rhs| Ok(rhs.call_binary(lhs, AddDateTime)))
                },
                params!(Time, Interval) => AddTimeInterval,
                params!(Interval, Time) => {
                    Operation::binary(|_ecx, lhs, rhs| Ok(rhs.call_binary(lhs, AddTimeInterval)))
                }
            },
            "-" => Scalar {
                params!(Int32) => UnaryFunc::NegInt32,
                params!(Int64) => UnaryFunc::NegInt64,
                params!(Float32) => UnaryFunc::NegFloat32,
                params!(Float64) => UnaryFunc::NegFloat64,
                params!(DecimalAny) => UnaryFunc::NegDecimal,
                params!(Interval) => UnaryFunc::NegInterval,
                params!(Int32, Int32) => SubInt32,
                params!(Int64, Int64) => SubInt64,
                params!(Float32, Float32) => SubFloat32,
                params!(Float64, Float64) => SubFloat64,
                params!(DecimalAny, DecimalAny) => Operation::binary(|ecx, lhs, rhs| {
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
            "*" => Scalar {
                params!(Int32, Int32) => MulInt32,
                params!(Int64, Int64) => MulInt64,
                params!(Float32, Float32) => MulFloat32,
                params!(Float64, Float64) => MulFloat64,
                params!(DecimalAny, DecimalAny) => Operation::binary(|ecx, lhs, rhs| {
                    use std::cmp::*;
                    let (_, s1) = ecx.scalar_type(&lhs).unwrap_decimal_parts();
                    let (_, s2) = ecx.scalar_type(&rhs).unwrap_decimal_parts();
                    let so = max(max(min(s1 + s2, 12), s1), s2);
                    let si = s1 + s2;
                    let expr = lhs.call_binary(rhs, MulDecimal);
                    Ok(rescale_decimal(expr, si, so))
                })
            },
            "/" => Scalar {
                params!(Int32, Int32) => DivInt32,
                params!(Int64, Int64) => DivInt64,
                params!(Float32, Float32) => DivFloat32,
                params!(Float64, Float64) => DivFloat64,
                params!(DecimalAny, DecimalAny) => Operation::binary(|ecx, lhs, rhs| {
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
            "%" => Scalar {
                params!(Int32, Int32) => ModInt32,
                params!(Int64, Int64) => ModInt64,
                params!(Float32, Float32) => ModFloat32,
                params!(Float64, Float64) => ModFloat64,
                params!(DecimalAny, DecimalAny) => Operation::binary(|ecx, lhs, rhs| {
                    let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                    Ok(lexpr.call_binary(rexpr, ModDecimal))
                })
            },

            // LIKE
            "~~" => Scalar {
                params!(String, String) => IsLikePatternMatch
            },
            "!~~" => Scalar {
                params!(String, String) => Operation::binary(|_ecx, lhs, rhs| {
                    Ok(lhs
                        .call_binary(rhs, IsLikePatternMatch)
                        .call_unary(UnaryFunc::Not))
                })
            },

            // REGEX
            "~" => Scalar {
                params!(String, String) => IsRegexpMatch { case_insensitive: false }
            },
            "~*" => Scalar {
                params!(String, String) => Operation::binary(|_ecx, lhs, rhs| {
                    Ok(lhs.call_binary(rhs, IsRegexpMatch { case_insensitive: true }))
                })
            },
            "!~" => Scalar {
                params!(String, String) => Operation::binary(|_ecx, lhs, rhs| {
                    Ok(lhs
                        .call_binary(rhs, IsRegexpMatch { case_insensitive: false })
                        .call_unary(UnaryFunc::Not))
                })
            },
            "!~*" => Scalar {
                params!(String, String) => Operation::binary(|_ecx, lhs, rhs| {
                    Ok(lhs
                        .call_binary(rhs, IsRegexpMatch { case_insensitive: true })
                        .call_unary(UnaryFunc::Not))
                })
            },

            // CONCAT
            "||" => Scalar {
                vec![Plain(String), NonVecAny] => Operation::binary(|ecx, lhs, rhs| {
                    let rhs = typeconv::plan_cast(
                        "text_concat",
                        ecx,
                        CastContext::Explicit,
                        rhs,
                        &ScalarType::String,
                    )?;
                    Ok(lhs.call_binary(rhs, TextConcat))
                }),
                vec![NonVecAny, Plain(String)] =>  Operation::binary(|ecx, lhs, rhs| {
                    let lhs = typeconv::plan_cast(
                        "text_concat",
                        ecx,
                        CastContext::Explicit,
                        lhs,
                        &ScalarType::String,
                    )?;
                    Ok(lhs.call_binary(rhs, TextConcat))
                }),
                params!(String, String) => TextConcat,
                params!(Jsonb, Jsonb) => JsonbConcat,
                params!(ListAny, ListAny) => ListListConcat,
                params!(ListAny, ListElementAny) => ListElementConcat,
                params!(ListElementAny, ListAny) => ElementListConcat
            },

            //JSON and MAP
            "->" => Scalar {
                params!(Jsonb, Int64) => JsonbGetInt64 { stringify: false },
                params!(Jsonb, String) => JsonbGetString { stringify: false },
                params!(MapAny, String) => MapGetValue,
                params!(MapAny, Plain(Array(Box::new(String)))) => MapGetValues
            },
            "->>" => Scalar {
                params!(Jsonb, Int64) => JsonbGetInt64 { stringify: true },
                params!(Jsonb, String) => JsonbGetString { stringify: true }
            },
            "@>" => Scalar {
                params!(Jsonb, Jsonb) => JsonbContainsJsonb,
                params!(Jsonb, String) => Operation::binary(|_ecx, lhs, rhs| {
                    Ok(lhs.call_binary(
                        rhs.call_unary(UnaryFunc::CastStringToJsonb),
                        JsonbContainsJsonb,
                    ))
                }),
                params!(String, Jsonb) => Operation::binary(|_ecx, lhs, rhs| {
                    Ok(lhs.call_unary(UnaryFunc::CastStringToJsonb)
                          .call_binary(rhs, JsonbContainsJsonb))
                }),
                params!(MapAny, MapAny) => MapContainsMap
            },
            "<@" => Scalar {
                params!(Jsonb, Jsonb) =>  Operation::binary(|_ecx, lhs, rhs| {
                    Ok(rhs.call_binary(
                        lhs,
                        JsonbContainsJsonb
                    ))
                }),
                params!(Jsonb, String) => Operation::binary(|_ecx, lhs, rhs| {
                    Ok(rhs.call_unary(UnaryFunc::CastStringToJsonb)
                          .call_binary(lhs, BinaryFunc::JsonbContainsJsonb))
                }),
                params!(String, Jsonb) => Operation::binary(|_ecx, lhs, rhs| {
                    Ok(rhs.call_binary(
                        lhs.call_unary(UnaryFunc::CastStringToJsonb),
                        BinaryFunc::JsonbContainsJsonb,
                    ))
                }),
                params!(MapAny, MapAny) => Operation::binary(|_ecx, lhs, rhs| {
                    Ok(rhs.call_binary(lhs, MapContainsMap))
                })
            },
            "?" => Scalar {
                params!(Jsonb, String) => JsonbContainsString,
                params!(MapAny, String) => MapContainsKey
            },
            "?&" => Scalar {
                params!(MapAny, Plain(Array(Box::new(String)))) => MapContainsAllKeys
            },
            "?|" => Scalar {
                params!(MapAny, Plain(Array(Box::new(String)))) => MapContainsAnyKeys
            },
            // COMPARISON OPS
            // n.b. Decimal impls are separated from other types because they
            // require a function pointer, which you cannot dynamically generate.
            "<" => Scalar {
                params!(DecimalAny, DecimalAny) => {
                    Operation::binary(|ecx, lhs, rhs| {
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
            "<=" => Scalar {
                params!(DecimalAny, DecimalAny) => {
                    Operation::binary(|ecx, lhs, rhs| {
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
            ">" => Scalar {
                params!(DecimalAny, DecimalAny) => {
                    Operation::binary(|ecx, lhs, rhs| {
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
            ">=" => Scalar {
                params!(DecimalAny, DecimalAny) => {
                    Operation::binary(|ecx, lhs, rhs| {
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
            "=" => Scalar {
                params!(DecimalAny, DecimalAny) => {
                    Operation::binary(|ecx, lhs, rhs| {
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
            "<>" => Scalar {
                params!(DecimalAny, DecimalAny) => {
                    Operation::binary(|ecx, lhs, rhs| {
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

/// Resolves the operator to a set of function implementations.
pub fn resolve_op(op: &str) -> Result<&'static [FuncImpl<ScalarExpr>], anyhow::Error> {
    match OP_IMPLS.get(op) {
        Some(Func::Scalar(impls)) => Ok(impls),
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
        None => unsupported!(op),
    }
}
