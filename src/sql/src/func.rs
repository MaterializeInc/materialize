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

use failure::bail;
use lazy_static::lazy_static;
use repr::ScalarType;
use sql_parser::ast::Expr;

use super::expr::{BinaryFunc, ScalarExpr, UnaryFunc, VariadicFunc};
use super::query::ExprContext;
use super::unsupported;

#[derive(Clone, Debug, Eq, PartialEq)]
/// Mirrored from [PostgreSQL's `typcategory`][typcategory].
///
/// [typcategory]: https://www.postgresql.org/docs/9.6/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE
enum TypeCategory {
    Bool,
    DateTime,
    Numeric,
    String,
    Timespan,
    UserDefined,
    Unknown,
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
    fn from_type(typ: &ScalarType) -> TypeCategory {
        match typ {
            ScalarType::Bool => TypeCategory::Bool,
            ScalarType::Bytes | ScalarType::Jsonb | ScalarType::List(_) => {
                TypeCategory::UserDefined
            }
            ScalarType::Date
            | ScalarType::Time
            | ScalarType::Timestamp
            | ScalarType::TimestampTz => TypeCategory::DateTime,
            ScalarType::Decimal(_, _)
            | ScalarType::Float32
            | ScalarType::Float64
            | ScalarType::Int32
            | ScalarType::Int64 => TypeCategory::Numeric,
            ScalarType::Interval => TypeCategory::Timespan,
            ScalarType::String => TypeCategory::String,
            ScalarType::Unknown => TypeCategory::Unknown,
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
            TypeCategory::Bool => Some(ScalarType::Bool),
            TypeCategory::DateTime => Some(ScalarType::TimestampTz),
            TypeCategory::Numeric => Some(ScalarType::Float64),
            TypeCategory::String => Some(ScalarType::String),
            TypeCategory::Timespan => Some(ScalarType::Interval),
            _ => None,
        }
    }
}

fn is_param_preferred_type_for_arg(param_type: &ScalarType, arg_type: &ScalarType) -> bool {
    match TypeCategory::from_type(&arg_type).preferred_type() {
        Some(preferred_type) => &preferred_type == param_type,
        _ => false,
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
/// Describes a single function's implementation.
pub struct FuncImpl {
    params: ParamList,
    op: OperationType,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
/// Describes possible types of function parameters.
///
/// Note that this is not exhaustive and will likely require additions.
pub enum ParamList {
    Exact(Vec<ParamType>),
    Repeat(Vec<ParamType>),
    AnyHomogeneous(ScalarType),
}

impl ParamList {
    /// Validates that the number of input elements are viable for this set of
    /// parameters.
    fn validate_arg_len(&self, input_len: usize) -> bool {
        match self {
            Self::Exact(p) => p.len() == input_len,
            Self::Repeat(p) => input_len % p.len() == 0 && input_len > 0,
            Self::AnyHomogeneous(_) => input_len > 0,
        }
    }

    /// Matches a `&[ScalarType]` derived from the user's function argument
    /// against this `ParamList`'s permitted arguments.
    fn match_scalartypes(&self, types: &[ScalarType]) -> bool {
        use ParamList::*;
        match self {
            Exact(p) => types.iter().zip(p.iter()).all(|(t, p)| t == p),
            Repeat(p) => types.iter().enumerate().all(|(i, t)| t == &p[i % p.len()]),
            AnyHomogeneous(_) => types.iter().all(|t| *t == types[0]),
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
    /// Permits any type, but requires it to be cast to a `ScalarType::String`.
    StringAny,
    /// Like `StringAny`, but pretends the argument was explicitly cast to
    /// a `ScalarType::String`, rather than implicitly cast.
    ExplicitStringAny,
    /// Permits types that can be cast to `JSONB` elements.
    JsonbAny,
}

impl PartialEq<ScalarType> for ParamType {
    fn eq(&self, other: &ScalarType) -> bool {
        use ScalarType::*;
        match (self, other) {
            (ParamType::StringAny, _) | (ParamType::ExplicitStringAny, _) => true,
            (ParamType::JsonbAny, o) => match o {
                Bool | Float64 | Float32 | Jsonb | String => true,
                _ => false,
            },
            // Param `Decimal` values are still unsaturated when we compare them
            // with user args.
            (ParamType::Plain(Decimal(_, _)), Decimal(_, _)) => true,
            (ParamType::Plain(s), o) => s == o,
        }
    }
}

impl PartialEq<ParamType> for ScalarType {
    fn eq(&self, other: &ParamType) -> bool {
        other == self
    }
}

impl From<&ParamType> for ScalarType {
    fn from(p: &ParamType) -> ScalarType {
        match p {
            ParamType::Plain(s) => s.clone(),
            ParamType::StringAny | ParamType::ExplicitStringAny => ScalarType::String,
            ParamType::JsonbAny => ScalarType::Jsonb,
        }
    }
}

pub trait Params {
    fn into_params(self) -> Vec<ParamType>;
}

/// Provides a shorthand for converting a `Vec<ScalarType>` to `Vec<ParamType>`.
/// Note that this moves the values out of `self` and into the resultant `Vec<ParamType>`.
impl Params for Vec<ScalarType> {
    fn into_params(self) -> Vec<ParamType> {
        self.into_iter().map(ParamType::Plain).collect()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
/// Represents generalizable operation types you can return from
/// `ArgImplementationMatcher`.
pub enum OperationType {
    /// Returns the `ScalarExpr` that is output from
    /// `ArgImplementationMatcher::generate_param_exprs`.
    ExprOnly,
    Unary(UnaryFunc),
    Binary(BinaryFunc),
    Variadic(VariadicFunc),
}

#[derive(Debug, Eq, PartialEq, Clone)]
/// Tracks candidate implementations during `ArgImplementationMatcher::best_match`.
struct Candidate {
    /// Represents the candidate's argument types; used to match some
    /// implementation's parameter types.
    arg_types: Vec<ScalarType>,
    exact_matches: usize,
    preferred_types: usize,
}

#[derive(Clone, Debug)]
/// Determines best implementation to use given some user-provided arguments.
/// For more detail, see `ArgImplementationMatcher::select_implementation`.
pub struct ArgImplementationMatcher<'a> {
    ident: &'a str,
    ecx: &'a ExprContext<'a>,
    impls: Vec<FuncImpl>,
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
    pub fn select_implementation(
        ident: &'a str,
        ecx: &'a ExprContext<'a>,
        impls: &[FuncImpl],
        args: &[sql_parser::ast::Expr],
    ) -> Result<ScalarExpr, failure::Error> {
        // Immediately remove all `impls` we know are invalid.
        let l = args.len();
        let impls = impls
            .iter()
            .filter(|i| i.params.validate_arg_len(l))
            .cloned()
            .collect();
        let mut m = Self { ident, ecx, impls };

        let mut raw_arg_types = Vec::new();
        for arg in args {
            let expr = super::query::plan_expr(ecx, &arg, Some(ScalarType::Unknown))?;
            raw_arg_types.push(ecx.scalar_type(&expr));
            Self::clear_expr_param_data(ecx, &arg);
        }

        // Exact match
        let f = if let Some(func) = m.get_implementation(&raw_arg_types) {
            func
        } else {
            // Best match
            m.best_match(args, &raw_arg_types)?
        };

        let mut exprs = m.generate_param_exprs(args, f.params)?;

        Ok(match f.op {
            OperationType::ExprOnly => exprs.remove(0),
            OperationType::Unary(func) => ScalarExpr::CallUnary {
                func,
                expr: Box::new(exprs.remove(0)),
            },
            OperationType::Binary(func) => ScalarExpr::CallBinary {
                func,
                expr1: Box::new(exprs.remove(0)),
                expr2: Box::new(exprs.remove(0)),
            },
            OperationType::Variadic(func) => ScalarExpr::CallVariadic { func, exprs },
        })
    }

    /// Returns a `FuncImpl` if `types` matches an implementation's parameters.
    ///
    /// Note that `types` must be the caller's argument's types to preserve
    /// Decimal scale and precision, which are used in `Self::saturate_decimals`.
    fn get_implementation(&self, types: &[ScalarType]) -> Option<FuncImpl> {
        let matching_impls: Vec<&FuncImpl> = self
            .impls
            .iter()
            .filter(|i| i.params.match_scalartypes(types))
            .collect();

        if matching_impls.len() == 1 {
            let f = Self::saturate_decimals(matching_impls[0], types);
            Some(f)
        } else {
            None
        }
    }

    /// If no exact match, finds the best match available. Patterned after
    /// [PostgreSQL's type conversion matching algorithm][pgparser].
    ///
    /// Inline prefixed with number are taken from the "Function Type
    /// Resolution" section of the aforelinked page.
    ///
    /// [pgparser]: https://www.postgresql.org/docs/current/typeconv-func.html
    fn best_match(
        &mut self,
        args: &[sql_parser::ast::Expr],
        raw_arg_types: &[ScalarType],
    ) -> Result<FuncImpl, failure::Error> {
        let mut candidates = Vec::new();
        let mut max_exact_matches = 0;
        let mut max_preferred_types = 0;

        // Generate candidates by assessing their compatibility with each
        // implementation's parameters.
        for fimpl in self.impls.iter() {
            let mut valid_candidate = true;
            let mut arg_types = Vec::new();
            let mut exact_matches = 0;
            let mut preferred_types = 0;
            let best_target_type = if let ParamList::AnyHomogeneous(h) = &fimpl.params {
                match super::query::best_target_type(raw_arg_types.iter().cloned()) {
                    Some(t) => Some(ParamType::Plain(t.clone())),
                    None => Some(ParamType::Plain(h.clone())),
                }
            } else {
                None
            };

            for (i, (arg, raw_arg_type)) in args.iter().zip(raw_arg_types.iter()).enumerate() {
                let param_type = match &fimpl.params {
                    ParamList::Exact(p) => &p[i],
                    ParamList::Repeat(p) => &p[i % p.len()],
                    ParamList::AnyHomogeneous(_) => best_target_type.as_ref().unwrap(),
                };

                let arg_type = if param_type == raw_arg_type {
                    exact_matches += 1;
                    raw_arg_type.clone()
                } else {
                    if self.coerce_arg_to_type(arg, &param_type).is_err() {
                        valid_candidate = false;
                        break;
                    }

                    // Increment `preferred_type` if:
                    // - This param is the preferred type for this argument's
                    //   type category
                    // - This argument is a string literal and this param is the
                    //   preferred type in its type category.
                    if is_param_preferred_type_for_arg(&param_type.into(), raw_arg_type)
                        || (arg.is_string_literal()
                            && is_param_preferred_type_for_arg(
                                &param_type.into(),
                                &param_type.into(),
                            ))
                    {
                        preferred_types += 1;
                    }

                    param_type.into()
                };

                arg_types.push(arg_type);
                Self::clear_expr_param_data(self.ecx, &arg);
            }

            // 4.a. Discard candidate functions for which the input types do not match
            // and cannot be converted (using an implicit conversion) to match.
            // unknown literals are assumed to be convertible to anything for this
            // purpose.
            if valid_candidate {
                max_exact_matches = std::cmp::max(max_exact_matches, exact_matches);
                max_preferred_types = std::cmp::max(max_preferred_types, preferred_types);
                candidates.push(Candidate {
                    arg_types,
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

        if let Some(func) = self.maybe_get_last_candidate(&candidates)? {
            return Ok(func);
        }

        // 4.c. Run through all candidates and keep those with the most exact matches on
        // input types. Keep all candidates if none have exact matches.
        candidates.retain(|c| c.exact_matches >= max_exact_matches);

        if let Some(func) = self.maybe_get_last_candidate(&candidates)? {
            return Ok(func);
        }

        // 4.d. Run through all candidates and keep those that accept preferred types
        // (of the input data type's type category) at the most positions where
        // type conversion will be required.
        candidates.retain(|c| c.preferred_types >= max_preferred_types);

        if let Some(func) = self.maybe_get_last_candidate(&candidates)? {
            return Ok(func);
        }

        // The rest of this function could be elided if we know we don't have
        // any Unknown types.
        let mut found_unknown = false;
        let mut found_known = false;
        let mut types_match = true;
        let mut common_type: Option<ScalarType> = None;

        for (i, raw_arg_type) in raw_arg_types.iter().enumerate() {
            let mut selected_category: Option<TypeCategory> = None;
            let mut found_string_candidate = false;
            let mut categories_match = true;

            match raw_arg_type {
                // 4.e. If any input arguments are unknown, check the type categories accepted
                // at those argument positions by the remaining candidates.
                ScalarType::Unknown => {
                    found_unknown = true;

                    for c in candidates.iter() {
                        let this_category = TypeCategory::from_type(&c.arg_types[i]);
                        match (&selected_category, &this_category) {
                            // 4.e. cont: At each  position, select the string category if
                            // any candidate accepts that category. (This bias
                            // towards string is appropriate since an
                            // unknown-type literal looks like a string.)
                            (Some(TypeCategory::String), _) => {}
                            (_, TypeCategory::String) => {
                                found_string_candidate = true;
                                selected_category = Some(TypeCategory::String);
                            }
                            // 4.e. cont: Otherwise, if all the remaining candidates accept
                            // the same type category, select that category.
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
                            found_preferred_type_candidate =
                                c.arg_types[i] == *typ || found_preferred_type_candidate;
                        }
                        selected_category == TypeCategory::from_type(&c.arg_types[i])
                    });

                    if found_preferred_type_candidate {
                        let preferred_type = preferred_type.unwrap();
                        candidates.retain(|c| c.arg_types[i] == preferred_type);
                    }
                }
                typ => {
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

        if let Some(func) = self.maybe_get_last_candidate(&candidates)? {
            return Ok(func);
        }

        // 4.f. If there are both unknown and known-type arguments, and all the
        // known-type arguments have the same type, assume that the unknown
        // arguments are also of that type, and check which candidates can
        // accept that type at the unknown-argument positions.
        if found_known && found_unknown && types_match {
            let common_type = common_type.unwrap();
            for (i, raw_arg_type) in raw_arg_types.iter().enumerate() {
                if *raw_arg_type == ScalarType::Unknown {
                    candidates.retain(|c| common_type == c.arg_types[i]);
                }
            }

            if let Some(func) = self.maybe_get_last_candidate(&candidates)? {
                return Ok(func);
            }
        }

        bail!(
            "unable to determine which implementation to use; try providing \
            explicit casts to match parameter types"
        )
    }

    fn maybe_get_last_candidate(
        &self,
        candidates: &[Candidate],
    ) -> Result<Option<FuncImpl>, failure::Error> {
        if candidates.len() == 1 {
            match self.get_implementation(&candidates[0].arg_types) {
                Some(func) => Ok(Some(func)),
                None => unreachable!(
                    "unable to get final function implementation with provided \
                    arguments"
                ),
            }
        } else {
            Ok(None)
        }
    }

    /// Because we use the same `ExprContext` throughout `Self::best_match`, we
    /// have to clean up any param types we provisionally inserted. This data
    /// gets re-inserted during `Self::generate_param_exprs`.
    fn clear_expr_param_data(ecx: &ExprContext, expr: &sql_parser::ast::Expr) {
        if let sql_parser::ast::Expr::Parameter(n) = expr {
            ecx.remove_param(*n);
        }
    }

    /// Rewrite any `Decimal` values in `FuncImpl` to use the users' arguments'
    /// scale, rather than the default value we use for matching implementations.
    fn saturate_decimals(f: &FuncImpl, types: &[ScalarType]) -> FuncImpl {
        use OperationType::*;
        use ParamType::*;
        use ScalarType::*;

        let mut f = f.clone();
        // TODO(sploiselle): Add support for saturating decimals in other
        // contexts.
        if let ParamList::Exact(ref mut param_list) = f.params {
            for (i, param) in param_list.iter_mut().enumerate() {
                if let Plain(Decimal(_, _)) = param {
                    *param = Plain(types[i].clone());
                }
            }
        }

        f.op = match f.op {
            Unary(UnaryFunc::CeilDecimal(_)) => match types[0] {
                ScalarType::Decimal(_, s) => Unary(UnaryFunc::CeilDecimal(s)),
                _ => unreachable!(),
            },
            Unary(UnaryFunc::FloorDecimal(_)) => match types[0] {
                ScalarType::Decimal(_, s) => Unary(UnaryFunc::FloorDecimal(s)),
                _ => unreachable!(),
            },
            Unary(UnaryFunc::RoundDecimal(_)) => match types[0] {
                ScalarType::Decimal(_, s) => Unary(UnaryFunc::RoundDecimal(s)),
                _ => unreachable!(),
            },
            Binary(BinaryFunc::RoundDecimal(_)) => match types[0] {
                ScalarType::Decimal(_, s) => Binary(BinaryFunc::RoundDecimal(s)),
                _ => unreachable!(),
            },
            other => other,
        };

        f
    }

    /// Plans `args` as `ScalarExprs` of that match the `ParamList`'s specified types.
    fn generate_param_exprs(
        &self,
        args: &[sql_parser::ast::Expr],
        params: ParamList,
    ) -> Result<Vec<ScalarExpr>, failure::Error> {
        match params {
            ParamList::AnyHomogeneous(h) => {
                super::query::plan_homogeneous_exprs(self.ident, self.ecx, args, Some(h))
            }
            ParamList::Exact(p) => {
                let mut exprs = Vec::new();
                for (arg, param) in args.iter().zip(p.iter()) {
                    exprs.push(self.coerce_arg_to_type(arg, param)?);
                }
                Ok(exprs)
            }
            ParamList::Repeat(p) => {
                let mut exprs = Vec::new();
                for (i, arg) in args.iter().enumerate() {
                    exprs.push(self.coerce_arg_to_type(arg, &p[i % p.len()])?);
                }
                Ok(exprs)
            }
        }
    }

    /// Generates `ScalarExpr` necessary to coerce `Expr` into the `ScalarType`
    /// corresponding to `ParameterType`; errors if not possible. This can only
    /// work within the `func` module because it relies on `ParameterType`.
    fn coerce_arg_to_type(
        &self,
        arg: &Expr,
        typ: &ParamType,
    ) -> Result<ScalarExpr, failure::Error> {
        use super::query::CastContext::*;
        let s: ScalarType = typ.into();
        let hinted_expr = super::query::plan_expr(self.ecx, &arg, Some(s.clone()))?;

        if self.ecx.scalar_type(&hinted_expr) == s {
            Ok(hinted_expr)
        } else if ParamType::JsonbAny == *typ {
            super::query::plan_to_jsonb(self.ecx, self.ident, hinted_expr)
        } else if ParamType::ExplicitStringAny == *typ {
            super::query::plan_cast_internal(self.ecx, Explicit, hinted_expr, s)
        } else if ParamType::StringAny == *typ || arg.is_string_literal() {
            super::query::plan_cast_internal(self.ecx, Implicit(self.ident), hinted_expr, s)
        } else {
            super::query::plan_cast_implicit(self.ecx, Implicit(self.ident), hinted_expr, s)
        }
    }
}

/// Provides shorthand for converting `Vec<ScalarType>` into `Vec<ParamType>`.
macro_rules! params(
    ( $($p:expr),* ) => {
        vec![$($p,)+].into_params()
    };
);

/// Provides a macro to write HashMap "literals" for matching function names to
/// `Vec<FuncImpl>`.
macro_rules! func_impls(
    {
        $(
            $name:expr => {
                $($params:expr => $op:expr),+
            }
        ),+
    } => {{
        let mut m: HashMap<&str, Vec<FuncImpl>> = HashMap::new();
        $(
            let impls = vec![
                $(FuncImpl {
                    params: $params.into(),
                    op: $op,
                },)+
            ];
            m.insert($name, impls);
        )+
        m
    }};
);

lazy_static! {
    /// Correlates a built-in function name to its implementations.
    static ref BIMPLS: HashMap<&'static str, Vec<FuncImpl>> = {
        use OperationType::*;
        use ParamList::*;
        use ParamType::*;
        use ScalarType::*;
        func_impls! {
            "abs" => {
                params!(Int32) => Unary(UnaryFunc::AbsInt32),
                params!(Int64) => Unary(UnaryFunc::AbsInt64),
                params!(Decimal(0, 0)) => Unary(UnaryFunc::AbsDecimal),
                params!(Float32) => Unary(UnaryFunc::AbsFloat32),
                params!(Float64) => Unary(UnaryFunc::AbsFloat64)
            },
            "ascii" => {
                params!(String) => Unary(UnaryFunc::Ascii)
            },
            "ceil" => {
                params!(Float32) => Unary(UnaryFunc::CeilFloat32),
                params!(Float64) => Unary(UnaryFunc::CeilFloat64),
                params!(Decimal(0, 0)) => Unary(UnaryFunc::CeilDecimal(0))
            },
            "coalesce" => {
                AnyHomogeneous(String) => Variadic(VariadicFunc::Coalesce)
            },
            "concat" => {
                Repeat(vec![StringAny]) => Variadic(VariadicFunc::Concat)
            },
            "convert_from" => {
                params!(Bytes, String) => Binary(BinaryFunc::ConvertFrom)
            },
            "date_trunc" => {
                params!(String, Timestamp) => Binary(BinaryFunc::DateTruncTimestamp),
                params!(String, TimestampTz) => Binary(BinaryFunc::DateTruncTimestampTz)
            },
            "floor" => {
                params!(Float32) => Unary(UnaryFunc::FloorFloat32),
                params!(Float64) => Unary(UnaryFunc::FloorFloat64),
                params!(Decimal(0, 0)) => Unary(UnaryFunc::FloorDecimal(0))
            },
            "jsonb_array_length" => {
                params!(Jsonb) => Unary(UnaryFunc::JsonbArrayLength)
            },
            "jsonb_build_array" => {
                Exact(vec![]) => Variadic(VariadicFunc::JsonbBuildArray),
                Repeat(vec![JsonbAny]) => Variadic(VariadicFunc::JsonbBuildArray)
            },
            "jsonb_build_object" => {
                Exact(vec![]) => Variadic(VariadicFunc::JsonbBuildObject),
                Repeat(vec![ExplicitStringAny, JsonbAny]) =>
                    Variadic(VariadicFunc::JsonbBuildObject)
            },
            "jsonb_pretty" => {
                params!(Jsonb) => Unary(UnaryFunc::JsonbPretty)
            },
            "jsonb_strip_nulls" => {
                params!(Jsonb) => Unary(UnaryFunc::JsonbStripNulls),
                // jsonb_strip_nulls does not want to cast NULLs to JSONB.
                params!(Unknown) => Unary(UnaryFunc::JsonbStripNulls)
            },
            "jsonb_typeof" => {
                params!(Jsonb) => Unary(UnaryFunc::JsonbTypeof)
            },
            "length" => {
                params!(Bytes) => Unary(UnaryFunc::LengthBytes),
                params!(String) => Variadic(VariadicFunc::LengthString),
                params!(String, String) => Variadic(VariadicFunc::LengthString)
            },
            "replace" => {
                params!(String, String, String) => Variadic(VariadicFunc::Replace)
            },
            "round" => {
                params!(Float32) => Unary(UnaryFunc::RoundFloat32),
                params!(Float64) => Unary(UnaryFunc::RoundFloat64),
                params!(Decimal(0,0)) => Unary(UnaryFunc::RoundDecimal(0)),
                params!(Decimal(0,0), Int64) => Binary(BinaryFunc::RoundDecimal(0))
            },
            "substr" => {
                params!(String, Int64) => Variadic(VariadicFunc::Substr),
                params!(String, Int64, Int64) => Variadic(VariadicFunc::Substr)
            },
            "substring" => {
                params!(String, Int64) => Variadic(VariadicFunc::Substr),
                params!(String, Int64, Int64) => Variadic(VariadicFunc::Substr)
            },
            "to_char" => {
                params!(Timestamp, String) => Binary(BinaryFunc::ToCharTimestamp),
                params!(TimestampTz, String) => Binary(BinaryFunc::ToCharTimestampTz)
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
                vec![JsonbAny] => ExprOnly
            },
            "to_timestamp" => {
                params!(Float64) => Unary(UnaryFunc::ToTimestamp)
            }
        }
    };
}

/// Gets a built-in scalar function and the `ScalarExpr`s required to invoke it.
pub fn select_scalar_func(
    ecx: &ExprContext,
    ident: &str,
    args: &[sql_parser::ast::Expr],
) -> Result<ScalarExpr, failure::Error> {
    let impls = match BIMPLS.get(ident) {
        Some(i) => i,
        None => unsupported!(ident),
    };

    match ArgImplementationMatcher::select_implementation(ident, ecx, impls, args) {
        Ok(expr) => Ok(expr),
        Err(e) => bail!("Cannot call function '{}': {}", ident, e),
    }
}
