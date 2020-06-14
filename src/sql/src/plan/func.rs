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

use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;

use failure::bail;
use lazy_static::lazy_static;

use ore::collections::CollectionExt;
use repr::{ColumnType, Datum, ScalarType};
use sql_parser::ast::{BinaryOperator, Expr, UnaryOperator};

use super::cast::{self, rescale_decimal, CastTo};
use super::expr::{
    AggregateFunc, BinaryFunc, CoercibleScalarExpr, NullaryFunc, ScalarExpr, TableFunc, UnaryFunc,
    VariadicFunc,
};
use super::query::{self, CoerceTo, ExprContext, QueryLifetime};
use crate::unsupported;

#[derive(Clone, Debug, Eq, PartialEq)]
/// Mirrored from [PostgreSQL's `typcategory`][typcategory].
///
/// Note that Materialize also uses a number of pseudotypes when planning, but
/// we have yet to need to integrate them with `TypeCategory`.
///
/// [typcategory]:
/// https://www.postgresql.org/docs/9.6/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE
pub enum TypeCategory {
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
            ScalarType::Bool => Self::Bool,
            ScalarType::Bytes | ScalarType::Jsonb | ScalarType::List(_) => Self::UserDefined,
            ScalarType::Date
            | ScalarType::Time
            | ScalarType::Timestamp
            | ScalarType::TimestampTz => Self::DateTime,
            ScalarType::Decimal(..)
            | ScalarType::Float32
            | ScalarType::Float64
            | ScalarType::Int32
            | ScalarType::Int64 => Self::Numeric,
            ScalarType::Interval => Self::Timespan,
            ScalarType::String => Self::String,
        }
    }

    fn from_param(param: &ParamType) -> Self {
        match param {
            ParamType::Plain(t) => Self::from_type(t),
            ParamType::Any | ParamType::StringAny | ParamType::JsonbAny => Self::Pseudo,
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
            Self::Pseudo | Self::UserDefined => None,
        }
    }
}

struct Operation<R>(
    Box<dyn Fn(&ExprContext, Vec<ScalarExpr>) -> Result<R, failure::Error> + Send + Sync>,
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
    F: Fn(&ExprContext) -> Result<R, failure::Error> + Send + Sync + 'static,
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
    F: Fn(&ExprContext, ScalarExpr) -> Result<R, failure::Error> + Send + Sync + 'static,
{
    Operation(Box::new(move |ecx, exprs| f(ecx, exprs.into_element())))
}

fn binary_op<F, R>(f: F) -> Operation<R>
where
    F: Fn(&ExprContext, ScalarExpr, ScalarExpr) -> Result<R, failure::Error>
        + Send
        + Sync
        + 'static,
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
    F: Fn(&ExprContext, Vec<ScalarExpr>) -> Result<R, failure::Error> + Send + Sync + 'static,
{
    Operation(Box::new(f))
}

impl Into<Operation<ScalarExpr>> for UnaryFunc {
    fn into(self) -> Operation<ScalarExpr> {
        unary_op(move |_ecx, e| Ok(e.call_unary(self.clone())))
    }
}

impl Into<Operation<ScalarExpr>> for BinaryFunc {
    fn into(self) -> Operation<ScalarExpr> {
        binary_op(move |_ecx, left, right| Ok(left.call_binary(right, self.clone())))
    }
}

impl Into<Operation<ScalarExpr>> for VariadicFunc {
    fn into(self) -> Operation<ScalarExpr> {
        variadic_op(move |_ecx, exprs| {
            Ok(ScalarExpr::CallVariadic {
                func: self.clone(),
                exprs,
            })
        })
    }
}

impl Into<Operation<(ScalarExpr, AggregateFunc)>> for AggregateFunc {
    fn into(self) -> Operation<(ScalarExpr, AggregateFunc)> {
        unary_op(move |_ecx, e| Ok((e, self.clone())))
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
        use ParamList::*;
        match self {
            Exact(p) => p.iter().zip(types.iter()).all(|(p, t)| p.accepts_type(t)),
            Repeat(p) => types
                .iter()
                .enumerate()
                .all(|(i, t)| p[i % p.len()].accepts_type(t)),
        }
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
}

impl ParamType {
    // Does `self` accept arguments of type `t`?
    fn accepts_type(&self, t: &ScalarType) -> bool {
        match (self, t) {
            (ParamType::Plain(s), o) => *s == o.desaturate(),
            (ParamType::Any, _) | (ParamType::StringAny, _) | (ParamType::JsonbAny, _) => true,
        }
    }

    // Does `self` accept arguments of category `c`?
    fn accepts_cat(&self, c: &TypeCategory) -> bool {
        match (self, c) {
            (ParamType::Plain(_), c) => TypeCategory::from_param(&self) == *c,
            (ParamType::Any, _) | (ParamType::StringAny, _) | (ParamType::JsonbAny, _) => true,
        }
    }

    // Does `t`'s [`TypeCategory`] prefer `self`? This question can make
    // more sense with the understanding that pseudotypes are never preferred.
    fn is_preferred_by(&self, t: &ScalarType) -> bool {
        if let Some(pt) = TypeCategory::from_type(t).preferred_type() {
            *self == pt
        } else {
            false
        }
    }

    // Is `self` the preferred parameter type for its `TypeCategory`?
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
            (ParamType::Any, _) | (ParamType::StringAny, _) | (ParamType::JsonbAny, _) => false,
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
        ecx: &'a ExprContext<'a>,
        impls: &[FuncImpl<R>],
        args: &[Expr],
    ) -> Result<R, failure::Error> {
        // Immediately remove all `impls` we know are invalid.
        let l = args.len();
        let impls = impls
            .iter()
            .filter(|i| i.params.validate_arg_len(l))
            .collect();
        let mut m = Self { ident, ecx };

        let mut exprs = Vec::new();
        for arg in args {
            let expr = query::plan_coercible_expr(ecx, arg)?.0;
            exprs.push(expr);
        }

        let types: Vec<_> = exprs
            .iter()
            .map(|e| ecx.column_type(e).map(|t| t.scalar_type))
            .collect();

        let f = m.find_match(&types, impls)?;
        let exprs = m.generate_param_exprs(exprs, &f.params)?;
        (f.op.0)(ecx, exprs)
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
        &mut self,
        types: &[Option<ScalarType>],
        impls: Vec<&'b FuncImpl<R>>,
    ) -> Result<&'b FuncImpl<R>, failure::Error> {
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
                        if !self.is_coercion_possible(arg_type, &param_type) {
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
                        if c.fimpl.params[i].accepts_type(&ScalarType::String) {
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
                            found_preferred_type_candidate = c.fimpl.params[i].accepts_type(typ)
                                || found_preferred_type_candidate;
                        }
                        c.fimpl.params[i].accepts_cat(&selected_category)
                    });

                    if found_preferred_type_candidate {
                        let preferred_type = preferred_type.unwrap();
                        candidates.retain(|c| c.fimpl.params[i].accepts_type(&preferred_type));
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
                    candidates.retain(|c| c.fimpl.params[i].accepts_type(&common_type));
                }
            }

            maybe_get_last_candidate!();
        }

        bail!(
            "unable to determine which implementation to use; try providing \
             explicit casts to match parameter types"
        )
    }

    /// Plans `args` as `ScalarExprs` of that match the `ParamList`'s specified types.
    fn generate_param_exprs(
        &self,
        args: Vec<CoercibleScalarExpr>,
        params: &ParamList,
    ) -> Result<Vec<ScalarExpr>, failure::Error> {
        match params {
            ParamList::Exact(p) => {
                let mut exprs = Vec::new();
                for (arg, param) in args.into_iter().zip(p.iter()) {
                    exprs.push(self.coerce_arg_to_type(arg, param)?);
                }
                Ok(exprs)
            }
            ParamList::Repeat(p) => {
                let mut exprs = Vec::new();
                for (i, arg) in args.into_iter().enumerate() {
                    exprs.push(self.coerce_arg_to_type(arg, &p[i % p.len()])?);
                }
                Ok(exprs)
            }
        }
    }

    /// Checks that `arg_type` is coercible to the parameter type without
    /// actually planning the coercion.
    fn is_coercion_possible(&self, arg_type: &ScalarType, to_typ: &ParamType) -> bool {
        use CastTo::*;

        let cast_to = match to_typ {
            ParamType::Plain(s) => Implicit(s.clone()),
            ParamType::Any => return true,
            ParamType::JsonbAny => JsonbAny,
            ParamType::StringAny => Explicit(ScalarType::String),
        };

        cast::get_cast(arg_type, &cast_to).is_some()
    }

    /// Generates `ScalarExpr` necessary to coerce `Expr` into the `ScalarType`
    /// corresponding to `ParameterType`; errors if not possible. This can only
    /// work within the `func` module because it relies on `ParameterType`.
    fn coerce_arg_to_type(
        &self,
        arg: CoercibleScalarExpr,
        typ: &ParamType,
    ) -> Result<ScalarExpr, failure::Error> {
        use ScalarType::*;
        let coerce_to = match typ {
            ParamType::Plain(s) => CoerceTo::Plain(s.clone()),
            ParamType::Any => {
                // `CoerceTo::Nothing` might seem more appropriate here, but
                // that would reject literal NULLs. Since coercions are not
                // binding, coercing to a string has no effect on values that
                // have a different natural type (e.g., list literals), so this
                // is correct.
                CoerceTo::Plain(String)
            }
            ParamType::JsonbAny => CoerceTo::JsonbAny,
            ParamType::StringAny => CoerceTo::Plain(String),
        };
        let arg = query::plan_coerce(self.ecx, arg, coerce_to)?;
        let arg_type = self.ecx.scalar_type(&arg);
        let cast_to = match typ {
            ParamType::Plain(Decimal(..)) if matches!(arg_type, Decimal(..)) => return Ok(arg),
            ParamType::Plain(s) => CastTo::Implicit(s.clone()),
            ParamType::Any => return Ok(arg),
            ParamType::JsonbAny => CastTo::JsonbAny,
            ParamType::StringAny => CastTo::Explicit(String),
        };
        query::plan_cast_internal(self.ident, self.ecx, arg, cast_to)
    }
}

/// Provides shorthand for converting `Vec<ScalarType>` into `Vec<ParamType>`.
macro_rules! params {
    (($($p:expr),*)...) => { ParamList::Repeat(vec![$($p.into(),)*]) };
    ($($p:expr),*)      => { ParamList::Exact(vec![$($p.into(),)*]) };
}

/// Provides shorthand for inserting [`FuncImpl`]s into arbitrary `HashMap`s.
macro_rules! insert_impl {
    ($hashmap:ident, $key:expr, $($params:expr => $op:expr),+) => {
        let impls = vec![
            $(FuncImpl {
                params: $params.into(),
                op: $op.into(),
            },)+
        ];

        $hashmap.entry($key).or_default().extend(impls);
    };
}

/// Provides a macro to write HashMap "literals" for matching some key to
/// `Vec<FuncImpl>`.
macro_rules! impls {
    {
        $(
            $name:expr => {
                $($params:expr => $op:expr),+
            }
        ),+
    } => {{
        let mut m: HashMap<_, Vec<FuncImpl<_>>> = HashMap::new();
        $(
            insert_impl!(m, $name, $($params => $op),+);
        )+
        m
    }};
}

lazy_static! {
    /// Correlates a built-in function name to its implementations.
    static ref BUILTIN_IMPLS: HashMap<&'static str, Vec<FuncImpl<ScalarExpr>>> = {
        use ParamType::*;
        use ScalarType::*;
        impls! {
            "abs" => {
                params!(Int32) => UnaryFunc::AbsInt32,
                params!(Int64) => UnaryFunc::AbsInt64,
                params!(Decimal(0, 0)) => UnaryFunc::AbsDecimal,
                params!(Float32) => UnaryFunc::AbsFloat32,
                params!(Float64) => UnaryFunc::AbsFloat64
            },
            "ascii" => {
                params!(String) => UnaryFunc::Ascii
            },
            "btrim" => {
                params!(String) => UnaryFunc::TrimWhitespace,
                params!(String, String) => BinaryFunc::Trim
            },
            "bit_length" => {
                params!(Bytes) => UnaryFunc::BitLengthBytes,
                params!(String) => UnaryFunc::BitLengthString
            },
            "ceil" => {
                params!(Float32) => UnaryFunc::CeilFloat32,
                params!(Float64) => UnaryFunc::CeilFloat64,
                params!(Decimal(0, 0)) => unary_op(|ecx, e| {
                    let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                    Ok(e.call_unary(UnaryFunc::CeilDecimal(s)))
                })
            },
            "char_length" => {
                params!(String) => UnaryFunc::CharLength
            },
            "concat" => {
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
            "convert_from" => {
                params!(Bytes, String) => BinaryFunc::ConvertFrom
            },
            "current_timestamp" => {
                params!() => nullary_op(|ecx| plan_current_timestamp(ecx, "current_timestamp"))
            },
            "date_trunc" => {
                params!(String, Timestamp) => BinaryFunc::DateTruncTimestamp,
                params!(String, TimestampTz) => BinaryFunc::DateTruncTimestampTz
            },
            "floor" => {
                params!(Float32) => UnaryFunc::FloorFloat32,
                params!(Float64) => UnaryFunc::FloorFloat64,
                params!(Decimal(0, 0)) => unary_op(|ecx, e| {
                    let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                    Ok(e.call_unary(UnaryFunc::FloorDecimal(s)))
                })
            },
            "internal_avg_promotion" => {
                // Promotes a numeric type to the smallest fractional type that
                // can represent it. This is primarily useful for the avg
                // aggregate function, so that the avg of an integer column does
                // not get truncated to an integer, which would be surprising to
                // users (#549).
                params!(Float32) => identity_op(),
                params!(Float64) => identity_op(),
                params!(Decimal(0, 0)) => identity_op(),
                params!(Int32) => unary_op(|ecx, e| {
                      super::query::plan_cast_internal(
                          "internal.avg_promotion", ecx, e,
                          CastTo::Explicit(ScalarType::Decimal(10, 0)),
                      )
                })
            },
            "jsonb_array_length" => {
                params!(Jsonb) => UnaryFunc::JsonbArrayLength
            },
            "jsonb_build_array" => {
                params!() => VariadicFunc::JsonbBuildArray,
                params!((JsonbAny)...) => VariadicFunc::JsonbBuildArray
            },
            "jsonb_build_object" => {
                params!() => VariadicFunc::JsonbBuildObject,
                params!((StringAny, JsonbAny)...) =>
                    VariadicFunc::JsonbBuildObject
            },
            "jsonb_pretty" => {
                params!(Jsonb) => UnaryFunc::JsonbPretty
            },
            "jsonb_strip_nulls" => {
                params!(Jsonb) => UnaryFunc::JsonbStripNulls
            },
            "jsonb_typeof" => {
                params!(Jsonb) => UnaryFunc::JsonbTypeof
            },
            "length" => {
                params!(Bytes) => UnaryFunc::ByteLengthBytes,
                params!(String) => UnaryFunc::CharLength,
                params!(Bytes, String) => BinaryFunc::EncodedBytesCharLength
            },
            "octet_length" => {
                params!(Bytes) => UnaryFunc::ByteLengthBytes,
                params!(String) => UnaryFunc::ByteLengthString
            },
            "ltrim" => {
                params!(String) => UnaryFunc::TrimLeadingWhitespace,
                params!(String, String) => BinaryFunc::TrimLeading
            },
            "mz_logical_timestamp" => {
                params!() => nullary_op(|ecx| {
                    match ecx.qcx.lifetime {
                        QueryLifetime::OneShot => {
                            Ok(ScalarExpr::CallNullary(NullaryFunc::MzLogicalTimestamp))
                        }
                        QueryLifetime::Static => bail!("mz_logical_timestamp cannot be used in static queries"),
                    }
                })
            },
            "now" => {
                params!() => nullary_op(|ecx| plan_current_timestamp(ecx, "now"))
            },
            "replace" => {
                params!(String, String, String) => VariadicFunc::Replace
            },
            "round" => {
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
            "rtrim" => {
                params!(String) => UnaryFunc::TrimTrailingWhitespace,
                params!(String, String) => BinaryFunc::TrimTrailing
            },
            "substr" => {
                params!(String, Int64) => VariadicFunc::Substr,
                params!(String, Int64, Int64) => VariadicFunc::Substr
            },
            "substring" => {
                params!(String, Int64) => VariadicFunc::Substr,
                params!(String, Int64, Int64) => VariadicFunc::Substr
            },
            "sqrt" => {
                params!(Float32) => UnaryFunc::SqrtFloat32,
                params!(Float64) => UnaryFunc::SqrtFloat64,
                params!(Decimal(0,0)) => unary_op(|ecx, e| {
                    let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                    Ok(e.call_unary(UnaryFunc::SqrtDec(s)))
                })
            },
            "to_char" => {
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
            "to_jsonb" => {
                params!(JsonbAny) => identity_op()
            },
            "to_timestamp" => {
                params!(Float64) => UnaryFunc::ToTimestamp
            }
        }
    };
}

fn plan_current_timestamp(ecx: &ExprContext, name: &str) -> Result<ScalarExpr, failure::Error> {
    match ecx.qcx.lifetime {
        QueryLifetime::OneShot => Ok(ScalarExpr::literal(
            Datum::from(ecx.qcx.scx.pcx.wall_time),
            ColumnType::new(ScalarType::TimestampTz),
        )),
        QueryLifetime::Static => bail!("{} cannot be used in static queries", name),
    }
}

/// Gets a built-in scalar function and the `ScalarExpr`s required to invoke it.
pub fn select_scalar_func(
    ecx: &ExprContext,
    ident: &str,
    args: &[Expr],
) -> Result<ScalarExpr, failure::Error> {
    let impls = match BUILTIN_IMPLS.get(ident) {
        Some(i) => i,
        None => unsupported!(ident),
    };

    match ArgImplementationMatcher::select_implementation(ident, ecx, impls, args) {
        Ok(expr) => Ok(expr),
        Err(e) => bail!("Cannot call function '{}': {}", ident, e),
    }
}

lazy_static! {
    /// Correlates a `BinaryOperator` with all of its implementations.
    static ref BINARY_OP_IMPLS: HashMap<BinaryOperator, Vec<FuncImpl<ScalarExpr>>> = {
        use ScalarType::*;
        use BinaryOperator::*;
        use BinaryFunc::*;
        use ParamType::*;
        let mut m = impls! {
            // ARITHMETIC
            Plus => {
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
            Minus => {
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
            Multiply => {
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
            Divide => {
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
            Modulus => {
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
            BinaryOperator::And => {
                params!(Bool, Bool) => BinaryFunc::And
            },
            BinaryOperator::Or => {
                params!(Bool, Bool) => BinaryFunc::Or
            },

            // LIKE
            Like => {
                params!(String, String) => MatchLikePattern
            },
            NotLike => {
                params!(String, String) => binary_op(|_ecx, lhs, rhs| {
                    Ok(lhs
                        .call_binary(rhs, MatchLikePattern)
                        .call_unary(UnaryFunc::Not))
                })
            },

            // CONCAT
            Concat => {
                vec![Plain(String), StringAny] => TextConcat,
                vec![StringAny, Plain(String)] => TextConcat,
                params!(String, String) => TextConcat,
                params!(Jsonb, Jsonb) => JsonbConcat
            },

            //JSON
            JsonGet => {
                params!(Jsonb, Int64) => JsonbGetInt64 { stringify: false },
                params!(Jsonb, String) => JsonbGetString { stringify: false }
            },
            JsonGetAsText => {
                params!(Jsonb, Int64) => JsonbGetInt64 { stringify: true },
                params!(Jsonb, String) => JsonbGetString { stringify: true }
            },
            JsonContainsJson => {
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
            JsonContainedInJson => {
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
            JsonContainsField => {
                params!(Jsonb, String) => JsonbContainsString
            },
            // COMPARISON OPS
            // n.b. Decimal impls are separated from other types because they
            // require a function pointer, which you cannot dynamically generate.
            BinaryOperator::Lt => {
                params!(Decimal(0, 0), Decimal(0, 0)) => {
                    binary_op(|ecx, lhs, rhs| {
                        let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                        Ok(lexpr.call_binary(rexpr, BinaryFunc::Lt))
                    })
                }
            },
            BinaryOperator::LtEq => {
                params!(Decimal(0, 0), Decimal(0, 0)) => {
                    binary_op(|ecx, lhs, rhs| {
                        let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                        Ok(lexpr.call_binary(rexpr, BinaryFunc::Lte))
                    })
                }
            },
            BinaryOperator::Gt => {
                params!(Decimal(0, 0), Decimal(0, 0)) => {
                    binary_op(|ecx, lhs, rhs| {
                        let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                        Ok(lexpr.call_binary(rexpr, BinaryFunc::Gt))
                    })
                }
            },
            BinaryOperator::GtEq => {
                params!(Decimal(0, 0), Decimal(0, 0)) => {
                    binary_op(|ecx, lhs, rhs| {
                        let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                        Ok(lexpr.call_binary(rexpr, BinaryFunc::Gte))
                    })
                }
            },
            BinaryOperator::Eq => {
                params!(Decimal(0, 0), Decimal(0, 0)) => {
                    binary_op(|ecx, lhs, rhs| {
                        let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                        Ok(lexpr.call_binary(rexpr, BinaryFunc::Eq))
                    })
                }
            },
            BinaryOperator::NotEq => {
                params!(Decimal(0, 0), Decimal(0, 0)) => {
                    binary_op(|ecx, lhs, rhs| {
                        let (lexpr, rexpr) = rescale_decimals_to_same(ecx, lhs, rhs);
                        Ok(lexpr.call_binary(rexpr, BinaryFunc::NotEq))
                    })
                }
            }
        };

        for (op, func) in vec![
            (BinaryOperator::Lt, BinaryFunc::Lt),
            (BinaryOperator::LtEq, BinaryFunc::Lte),
            (BinaryOperator::Gt, BinaryFunc::Gt),
            (BinaryOperator::GtEq, BinaryFunc::Gte),
            (BinaryOperator::Eq, BinaryFunc::Eq),
            (BinaryOperator::NotEq, BinaryFunc::NotEq)
        ] {
            insert_impl!(m, op,
                params!(Bool, Bool) => func.clone(),
                params!(Int32, Int32) => func.clone(),
                params!(Int64, Int64) => func.clone(),
                params!(Float32, Float32) => func.clone(),
                params!(Float64, Float64) => func.clone(),
                params!(Date, Date) => func.clone(),
                params!(Time, Time) => func.clone(),
                params!(Timestamp, Timestamp) => func.clone(),
                params!(TimestampTz, TimestampTz) => func.clone(),
                params!(Interval, Interval) => func.clone(),
                params!(Bytes, Bytes) => func.clone(),
                params!(String, String) => func.clone(),
                params!(Jsonb, Jsonb) => func.clone()
            );
        }

        m
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

/// Plans a function compatible with the `BinaryOperator`.
pub fn plan_binary_op<'a>(
    ecx: &ExprContext,
    op: &'a BinaryOperator,
    left: &'a Expr,
    right: &'a Expr,
) -> Result<ScalarExpr, failure::Error> {
    let impls = match BINARY_OP_IMPLS.get(&op) {
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

    let args = vec![left.clone(), right.clone()];

    match ArgImplementationMatcher::select_implementation(&op.to_string(), ecx, impls, &args) {
        Ok(expr) => Ok(expr),
        Err(e) => {
            let lexpr = query::plan_expr(ecx, left, None)?;
            let rexpr = query::plan_expr(ecx, right, None)?;
            bail!(
                "no overload for {} {} {}: {}",
                ecx.scalar_type(&lexpr),
                op,
                ecx.scalar_type(&rexpr),
                e
            )
        }
    }
}

lazy_static! {
    /// Correlates a `UnaryOperator` with all of its implementations.
    static ref UNARY_OP_IMPLS: HashMap<UnaryOperator, Vec<FuncImpl<ScalarExpr>>> = {
        use ParamType::*;
        use ScalarType::*;
        use UnaryOperator::*;
        impls! {
            Not => {
                params!(Bool) => UnaryFunc::Not
            },

            Plus => {
                params!(Any) => identity_op()
            },

            Minus => {
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

/// Plans a function compatible with the `UnaryOperator`.
pub fn plan_unary_op<'a>(
    ecx: &ExprContext,
    op: &'a UnaryOperator,
    expr: &'a Expr,
) -> Result<ScalarExpr, failure::Error> {
    let impls = match UNARY_OP_IMPLS.get(&op) {
        Some(i) => i,
        None => unsupported!(op),
    };

    match ArgImplementationMatcher::select_implementation(
        &op.to_string(),
        ecx,
        impls,
        &[expr.clone()],
    ) {
        Ok(expr) => Ok(expr),
        Err(e) => {
            let lexpr = query::plan_expr(ecx, expr, None)?;
            bail!("no overload for {} {}: {}", op, ecx.scalar_type(&lexpr), e)
        }
    }
}

pub struct TableFuncPlan {
    pub func: TableFunc,
    pub exprs: Vec<ScalarExpr>,
    pub column_names: Vec<String>,
}

lazy_static! {
    /// Correlates a built-in function name to its implementations.
    static ref BUILTIN_TABLE_IMPLS: HashMap<&'static str, Vec<FuncImpl<TableFuncPlan>>> = {
        use ScalarType::*;
        impls! {
            "csv_extract" => {
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
                        column_names: (1..=ncols).map(|i| format!("column{}", i)).collect(),
                    })
                })
            },
            "generate_series" => {
                params!(Int32, Int32) => plan_generate_series(Int32),
                params!(Int64, Int64) => plan_generate_series(Int64)
            },
            "jsonb_array_elements" => {
                params!(Jsonb) => unary_op(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        func: TableFunc::JsonbArrayElements { stringify: false },
                        exprs: vec![jsonb],
                        column_names: vec!["value".into()],
                    })
                })
            },
            "jsonb_array_elements_text" => {
                params!(Jsonb) => unary_op(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        func: TableFunc::JsonbArrayElements { stringify: true },
                        exprs: vec![jsonb],
                        column_names: vec!["value".into()],
                    })
                })
            },
            "jsonb_each" => {
                params!(Jsonb) => unary_op(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        func: TableFunc::JsonbEach { stringify: false },
                        exprs: vec![jsonb],
                        column_names: vec!["key".into(), "value".into()],
                    })
                })
            },
            "jsonb_each_text" => {
                params!(Jsonb) => unary_op(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        func: TableFunc::JsonbEach { stringify: true },
                        exprs: vec![jsonb],
                        column_names: vec!["key".into(), "value".into()],
                    })
                })
            },
            "jsonb_object_keys" => {
                params!(Jsonb) => unary_op(move |_ecx, jsonb| {
                    Ok(TableFuncPlan {
                        func: TableFunc::JsonbObjectKeys,
                        exprs: vec![jsonb],
                        column_names: vec!["jsonb_object_keys".into()],
                    })
                })
            },
            "regexp_extract" => {
                params!(String, String) => binary_op(move |_ecx, regex, haystack| {
                    let regex = match regex.into_literal_string() {
                        None => bail!("regex_extract requires a string literal as its first argument"),
                        Some(regex) => expr::AnalyzedRegex::new(&regex)?,
                    };
                    let column_names = regex
                        .capture_groups_iter()
                        .map(|cg| cg.name.clone().unwrap_or_else(|| format!("column{}", cg.index)))
                        .collect();
                    Ok(TableFuncPlan {
                        func: TableFunc::RegexpExtract(regex),
                        exprs: vec![haystack],
                        column_names,
                    })
                })
            }
        }
    };
}

fn plan_generate_series(ty: ScalarType) -> Operation<TableFuncPlan> {
    variadic_op(move |_ecx, exprs| {
        Ok(TableFuncPlan {
            func: TableFunc::GenerateSeries(ty.clone()),
            exprs,
            column_names: vec!["generate_series".into()],
        })
    })
}

pub fn is_table_func(ident: &str) -> bool {
    BUILTIN_TABLE_IMPLS.get(ident).is_some()
}

/// Plans a built-in table function.
pub fn select_table_func(
    ecx: &ExprContext,
    ident: &str,
    args: &[Expr],
) -> Result<TableFuncPlan, failure::Error> {
    let impls = match BUILTIN_TABLE_IMPLS.get(ident) {
        Some(i) => i,
        None => unsupported!(ident),
    };

    match ArgImplementationMatcher::select_implementation(ident, ecx, impls, args) {
        Ok(expr) => Ok(expr),
        Err(e) => bail!("Cannot call function '{}': {}", ident, e),
    }
}

lazy_static! {
    /// Correlates a built-in function name to its implementations.
    static ref BUILTIN_AGGREGATE_IMPLS: HashMap<&'static str, Vec<FuncImpl<(ScalarExpr, AggregateFunc)>>> = {
        use ParamType::*;
        use ScalarType::*;
        impls! {
            "count" => {
                params!() => nullary_op(|_ecx| {
                    // We have to return *some* expr, even though `CountAll`'s
                    // implementation won't look at it, so we just provide
                    // a literal `true`.
                    //
                    // TODO(benesch): it would be nice if `CountAll` didn't
                    // exist, and `count(true)` was physically optimized to be
                    // as efficient as the current `CountALl` implementation.
                    Ok((ScalarExpr::literal_true(), AggregateFunc::CountAll))
                }),
                params!(Any) => AggregateFunc::Count
            },
            "max" => {
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
            "min" => {
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
            "jsonb_agg" => {
                params!(JsonbAny) => AggregateFunc::JsonbAgg
            },
            "sum" => {
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
            }
        }
    };
}

pub fn is_aggregate_func(ident: &str) -> bool {
    BUILTIN_AGGREGATE_IMPLS.get(ident).is_some()
}

/// Plans a built-in aggregate function.
pub fn select_aggregate_func(
    ecx: &ExprContext,
    ident: &str,
    args: &[Expr],
) -> Result<(ScalarExpr, AggregateFunc), failure::Error> {
    let impls = match BUILTIN_AGGREGATE_IMPLS.get(ident) {
        Some(i) => i,
        None => unsupported!(ident),
    };

    match ArgImplementationMatcher::select_implementation(ident, ecx, impls, args) {
        Ok(val) => Ok(val),
        Err(e) => bail!("Cannot call function '{}': {}", ident, e),
    }
}
