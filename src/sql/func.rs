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

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use failure::bail;
use repr::ScalarType;
use sql_parser::ast::{Expr, Value};

use super::expr::{BinaryFunc, ScalarExpr, UnaryFunc, VariadicFunc};
use super::query::ExprContext;
use super::unsupported;

#[derive(Clone, Debug, Eq, PartialEq)]
// Mirrored from [PostgreSQL's
// `typcategory`](https://www.postgresql.org/docs/9.6/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE).
enum TypeCategory {
    Bool,
    DateTime,
    Numeric,
    String,
    Timespan,
    UserDefined,
    Unknown,
}

// Extracted from PostgreSQL 9.6.
// ```
// SELECT array_agg(typname), typcategory
// FROM pg_catalog.pg_type
// WHERE typname IN (
//  'bool', 'bytea', 'date', 'float4', 'float8', 'int4', 'int8', 'interval','jsonb', 'numeric',
//  'text', 'time', 'timestamp', 'timestamptz'
// )
// GROUP BY typcategory
// ORDER BY typcategory;
// ```
fn get_type_category(typ: &ScalarType) -> TypeCategory {
    match typ {
        ScalarType::Bool => TypeCategory::Bool,
        ScalarType::Bytes | ScalarType::Jsonb | ScalarType::List(_) => TypeCategory::UserDefined,
        ScalarType::Date | ScalarType::Time | ScalarType::Timestamp | ScalarType::TimestampTz => {
            TypeCategory::DateTime
        }
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

// Extracted from PostgreSQL 9.6.
// ```
// SELECT typcategory, typname, typispreferred
// FROM pg_catalog.pg_type
// WHERE typispreferred = true
// ORDER BY typcategory;
// ```
fn get_preferred_type(category: &TypeCategory) -> Option<ScalarType> {
    match category {
        TypeCategory::Bool => Some(ScalarType::Bool),
        TypeCategory::DateTime => Some(ScalarType::TimestampTz),
        TypeCategory::Numeric => Some(ScalarType::Float64),
        TypeCategory::String => Some(ScalarType::String),
        TypeCategory::Timespan => Some(ScalarType::Interval),
        _ => None,
    }
}

fn is_param_preferred_type_for_arg(param_type: &ScalarType, arg_type: &ScalarType) -> bool {
    let category = get_type_category(&arg_type);
    let preferred_type = match get_preferred_type(&category) {
        Some(typ) => typ,
        None => return false,
    };
    std::mem::discriminant(&preferred_type) == std::mem::discriminant(param_type)
}

/// Describes the parameter types of implementations. Provides a facade over
/// `ScalarType` for expressing more complex types of parameters.
#[derive(Debug, Clone, Eq)]
pub enum ParameterType {
    Unknown,
    Bool,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal(u8, u8),
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Interval,
    Bytes,
    String,
    /// Allows (and requires) the arguments to be cast to string; does so by
    /// setting `CastContext::Implict(f, ImplicitCastMod::PermitAnyToString)`.
    StringAny,
    Jsonb,
    /// Uses `super::query::plan_to_jsonb` during type coercion.
    JsonbAny,
}

impl ParameterType {
    fn get_scalar_type(&self) -> ScalarType {
        use ParameterType::*;
        match self {
            Unknown => ScalarType::Unknown,
            Bool => ScalarType::Bool,
            Int32 => ScalarType::Int32,
            Int64 => ScalarType::Int64,
            Float32 => ScalarType::Float32,
            Float64 => ScalarType::Float64,
            Decimal(s, p) => ScalarType::Decimal(*s, *p),
            Date => ScalarType::Date,
            Time => ScalarType::Time,
            Timestamp => ScalarType::Timestamp,
            TimestampTz => ScalarType::TimestampTz,
            Interval => ScalarType::Interval,
            Bytes => ScalarType::Bytes,
            String | StringAny => ScalarType::String,
            Jsonb | JsonbAny => ScalarType::Jsonb,
        }
    }
}

impl Hash for ParameterType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        use ParameterType::*;
        match self {
            Unknown => state.write_u8(0),
            Bool => state.write_u8(1),
            Int32 => state.write_u8(2),
            Int64 => state.write_u8(3),
            Float32 => state.write_u8(4),
            Float64 => state.write_u8(5),
            Decimal(_, _) => {
                state.write_u8(6);
            }
            Date => state.write_u8(7),
            Time => state.write_u8(8),
            Timestamp => state.write_u8(9),
            TimestampTz => state.write_u8(10),
            Interval => state.write_u8(11),
            Bytes => state.write_u8(12),
            // `String` and `StringAny` share the same hash for `implementation`
            // lookups.
            String | StringAny => state.write_u8(13),
            Jsonb => state.write_u8(14),
            JsonbAny => state.write_u8(15),
        }
    }
}

impl PartialEq for ParameterType {
    fn eq(&self, other: &Self) -> bool {
        use ParameterType::*;
        match (self, other) {
            (Unknown, Unknown)
            | (Bool, Bool)
            | (Int32, Int32)
            | (Int64, Int64)
            | (Float32, Float32)
            | (Float64, Float64)
            | (Decimal(_, _), Decimal(_, _))
            | (Date, Date)
            | (Time, Time)
            | (Timestamp, Timestamp)
            | (TimestampTz, TimestampTz)
            | (Interval, Interval)
            | (Bytes, Bytes)
            // Though `String` and `StringAny` share a hash, they should not be
            // equal because we need to differentiate between them when coercing
            // args to param types.
            | (String, String)
            | (StringAny, StringAny)
            | (Jsonb, Jsonb)
            | (JsonbAny, JsonbAny) => true,
            (_, _) => false,
        }
    }
}

impl PartialEq<ScalarType> for ParameterType {
    fn eq(&self, other: &ScalarType) -> bool {
        std::mem::discriminant(&self.get_scalar_type()) == std::mem::discriminant(other)
    }
}

impl PartialEq<ParameterType> for ScalarType {
    fn eq(&self, other: &ParameterType) -> bool {
        other.eq(self)
    }
}

impl From<&ParameterType> for ScalarType {
    fn from(param: &ParameterType) -> Self {
        param.get_scalar_type()
    }
}

impl From<&ScalarType> for ParameterType {
    fn from(typ: &ScalarType) -> Self {
        use ParameterType::*;
        match typ {
            // TODO(sploiselle): Integrate some List function to sort out
            // how its parameters should behave
            ScalarType::Unknown | ScalarType::List(_) => Unknown,
            ScalarType::Bool => Bool,
            ScalarType::Int32 => Int32,
            ScalarType::Int64 => Int64,
            ScalarType::Float32 => Float32,
            ScalarType::Float64 => Float64,
            ScalarType::Decimal(s, p) => Decimal(*s, *p),
            ScalarType::Date => Date,
            ScalarType::Time => Time,
            ScalarType::Timestamp => Timestamp,
            ScalarType::TimestampTz => TimestampTz,
            ScalarType::Interval => Interval,
            ScalarType::Bytes => Bytes,
            ScalarType::String => String,
            ScalarType::Jsonb => Jsonb,
        }
    }
}

#[derive(Debug, Clone, Eq)]
/// Represents generalizable operation types you can return from
/// `ArgImplementationMatcher`.
pub enum OperationType {
    /// Returns the `ScalarExpr` that is output from
    /// `ArgImplementationMatcher::generate_exprs_from_types`.
    ExprOnly,
    Unary(UnaryFunc),
    Binary(BinaryFunc),
    Variadic(VariadicFunc, VariadicParam),
}

// This impl is meant only to support `ArgImplementationMatcher`'s
// `operation_type` field, which is in turn only used for
// `Self::get_possible_implementation_key`.
impl Hash for OperationType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::ExprOnly => {
                state.write_u8(0);
            }
            Self::Unary(_) => {
                state.write_u8(1);
            }
            Self::Binary(_) => {
                state.write_u8(2);
            }
            Self::Variadic(_, arg_mod) => {
                state.write_u8(3);
                arg_mod.hash(state);
            }
        }
    }
}

// This impl is meant only to support `ArgImplementationMatcher`'s
// `operation_type` field, which is in turn only used for
// `Self::get_possible_implementation_key`.
impl PartialEq for OperationType {
    fn eq(&self, other: &Self) -> bool {
        use OperationType::*;
        match (self, other) {
            (ExprOnly, ExprOnly) | (Unary(_), Unary(_)) | (Binary(_), Binary(_)) => true,
            (Variadic(_, a), Variadic(_, b)) => a.eq(b),
            (ExprOnly, _) | (Unary(_), _) | (Binary(_), _) | (Variadic(_, _), _) => false,
        }
    }
}

impl OperationType {
    // Validates the number of params or arguments based on the type of
    // operation; however, error strings are only used when validating
    // implementation parameters.
    fn validate_input_len(&self, input_len: usize, is_arg: bool) -> Result<(), failure::Error> {
        let (valid, e) = match self {
            Self::ExprOnly | Self::Unary(_) => {
                (input_len == 1, format!("1 param, received {}", input_len))
            }
            Self::Binary(_) => (input_len == 2, format!("2 params, received {}", input_len)),
            Self::Variadic(_, arg_mod) => arg_mod.validate_input_len(input_len, is_arg),
        };

        if !valid {
            bail!("Invalid argument length: expected {}", e)
        } else {
            Ok(())
        }
    }

    // Converts `types` into a form that can potentially match the parameters of
    // a implementation of the given `OperationType`. In consequence, this is
    // necessary because variadic operations support multiple parameter
    // structures.
    fn get_possible_implementation_key(&self, types: &[ParameterType]) -> Vec<ParameterType> {
        match self {
            Self::ExprOnly | Self::Unary(_) | Self::Binary(_) => types.to_vec(),
            Self::Variadic(_, arg_mod) => arg_mod.get_possible_implementation_key(types),
        }
    }
}

#[derive(Debug, Clone, Eq)]
/// Describes different classes of variadic paprameters.
pub enum VariadicParam {
    /// Accepts any type, as long as all arguments are of the same type.
    ///
    /// When using `AnyHomogeneous`, provide an empty `Vec::<ScalarType>` as the
    /// implementation's parameter.
    AnyHomogeneous(ParameterType),
    /// Accepts `usize` arguments.
    MustEq(usize),
    /// Accepts repeated patterns of `usize` length.
    Repeat(usize),
}

// View comments on same impl for `OperationType`.
impl Hash for VariadicParam {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::AnyHomogeneous(_) => {
                state.write_u8(0);
            }
            Self::MustEq(_) => {
                state.write_u8(1);
            }
            Self::Repeat(s) => {
                state.write_u8(2);
                state.write_usize(*s);
            }
        }
    }
}

// View comments on same impl for `OperationType`.
impl PartialEq for VariadicParam {
    fn eq(&self, other: &Self) -> bool {
        use VariadicParam::*;
        match (self, other) {
            (AnyHomogeneous(_), AnyHomogeneous(_))
            | (MustEq(_), MustEq(_))
            | (Repeat(_), Repeat(_)) => true,
            (AnyHomogeneous(_), _) | (MustEq(_), _) | (Repeat(_), _) => false,
        }
    }
}

impl VariadicParam {
    // View comments on same function in `OperationType`.
    fn validate_input_len(&self, input_len: usize, is_arg: bool) -> (bool, String) {
        match self {
            Self::MustEq(expected) => (
                input_len == *expected,
                format!("{} params, received {}", *expected, input_len),
            ),
            Self::Repeat(arg_modulo) => (
                input_len % arg_modulo == 0 && input_len > 0,
                format!(
                    "X % {} == 0 && X > 0 params, but received {} params",
                    arg_modulo, input_len,
                ),
            ),
            Self::AnyHomogeneous(_) => (
                if is_arg { true } else { input_len == 0 },
                format!("0 params, but received {}", input_len),
            ),
        }
    }

    // View comments on same function in `OperationType`.
    fn get_possible_implementation_key(&self, types: &[ParameterType]) -> Vec<ParameterType> {
        match self {
            Self::MustEq(_) => types.to_vec(),
            Self::Repeat(n) => {
                if types.len() >= *n {
                    let mut base_types = Vec::new();
                    for i in 0..*n {
                        base_types.push(types[i].clone());
                    }
                    if types
                        .iter()
                        .enumerate()
                        .all(|(i, x)| *x == base_types[i % n])
                    {
                        return base_types;
                    }
                }

                types.to_vec()
            }
            Self::AnyHomogeneous(_) => Vec::new(),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
/// Tracks candidate implementations during `ArgImplementationMatcher::best_match`.
struct Candidate {
    // Represents the candidate's argument types; used as key to match some
    // implementation's parameter types.
    arg_types: Vec<ParameterType>,
    exact_matches: usize,
    preferred_types: usize,
}

#[derive(Clone, Debug)]
/// Determines best implementation to use given some user-provided arguments.
/// For more detail, see `ArgImplementationMatcher::select_implementation`.
pub struct ArgImplementationMatcher<'a> {
    ident: &'a str,
    ecx: &'a ExprContext<'a>,
    // Maps an implementation's parameters to its `OperationType`.
    implementations: HashMap<Vec<ParameterType>, OperationType>,
    // Tracks distinct `OperationType`s among implementations; this is used to
    // transform the caller's arguments' types into forms that can match the
    // implementation's parameters, i.e. the keys of `implementations`.
    operation_types: HashSet<OperationType>,
}

#[macro_export]
/// Provides a macro to write HashMap "literals" to be used with
/// `ArgImplementationMatcher.implementations`.
macro_rules! implementations(
    { $($key:expr => $value:expr),+ } => {
        {
            let mut m = ::std::collections::HashMap::new();
            $(
                m.insert($key, $value);
            )+
            m
        }
     };
);

impl<'a> ArgImplementationMatcher<'a> {
    /// Selects the best implementation given the provided `args` using a
    /// process similar to [PostgreSQL's
    /// parser](https://www.postgresql.org/docs/current/typeconv-oper.html), and
    /// returns the `ScalarExpr` to invoke that .
    ///
    /// Generate `implementations` using the `implementations!` macro in this
    /// package.
    ///
    /// # Errors
    /// - When the provided arguments are not valid for any implementation, e.g.
    ///   cannot be converted to the appropriate types.
    /// - When all implementations are equally valid.
    pub fn select_implementation(
        ident: &'a str,
        ecx: &'a ExprContext<'a>,
        implementations: HashMap<Vec<ParameterType>, OperationType>,
        args: &[sql_parser::ast::Expr],
    ) -> Result<ScalarExpr, failure::Error> {
        let mut m = Self::new(ident, ecx, implementations)?;
        let (op, types) = m.match_args_to_implementation(args)?;
        let op = Self::rewrite_enum_field_ops(op, &types);
        let mut exprs = m.generate_exprs_from_types(&op, args, &types)?;

        Ok(match op {
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
            OperationType::Variadic(func, _) => ScalarExpr::CallVariadic { func, exprs },
        })
    }

    fn new(
        ident: &'a str,
        ecx: &'a ExprContext<'a>,
        implementations: HashMap<Vec<ParameterType>, OperationType>,
    ) -> Result<Self, failure::Error> {
        let mut operation_types = HashSet::new();

        let mut seen_anyhomogeneous = false;

        // Checks implementations' invariants; meant to error for developers
        // working in package and should never error during runtime.
        for (params, op) in implementations.iter() {
            op.validate_input_len(params.len(), false)?;
            if let OperationType::Variadic(_, VariadicParam::AnyHomogeneous(_)) = op {
                if seen_anyhomogeneous {
                    bail!(
                        "You may only have one implementation that accepts \
                    any homogenously typed set of arguments"
                    )
                }
                seen_anyhomogeneous = true;
            }
            operation_types.insert(op.clone());
        }

        Ok(Self {
            ident,
            ecx,
            implementations,
            operation_types,
        })
    }

    fn match_args_to_implementation(
        &mut self,
        args: &[sql_parser::ast::Expr],
    ) -> Result<(OperationType, Vec<ParameterType>), failure::Error> {
        let mut raw_arg_types = Vec::new();
        for arg in args {
            let expr = super::query::plan_expr(self.ecx, &arg, Some(ScalarType::Unknown))?;
            raw_arg_types.push((&self.ecx.scalar_type(&expr)).into());
            self.clear_expr_param_data(&arg);
        }

        // Look for exact match.
        if let Some(func) = self.get_implementation(&raw_arg_types) {
            return Ok((func, raw_arg_types));
        }

        Ok(self.best_match(args, &raw_arg_types)?)
    }

    // Returns an implementation's `OperationType` if `types` matches an
    // implementation's parameters and some other bookkeeping is satisfied.
    //
    // Note that `types` must be the caller's argument's types to preserve
    // Decimal scale and precision.
    fn get_implementation(&self, types: &[ParameterType]) -> Option<OperationType> {
        // Rewrites all Decimal values to a consistent scale and precision
        // to support matching implementation parameters.
        // TODO(sploiselle): Add `ScalarType::List` support.
        let matchable_types: Vec<ParameterType> = types
            .iter()
            .map(|t| match t {
                ParameterType::Decimal(_, _) => ParameterType::Decimal(0, 0),
                other => other.clone(),
            })
            .collect();

        let possible_implementation_keys: Vec<Vec<ParameterType>> = self
            .operation_types
            .iter()
            .map(|op| op.get_possible_implementation_key(&matchable_types))
            .collect();

        for key in possible_implementation_keys {
            if let Some(op) = self.implementations.get(&key) {
                // Sanity check argument length for exact matches.
                if op.validate_input_len(types.len(), true).is_ok() {
                    return Some(op.clone());
                }
            }
        }

        None
    }

    // If no exact match, finds the best match available. Patterned after
    // [PostgreSQL's type converstion matching
    // algorithm](https://www.postgresql.org/docs/current/typeconv-func.html).
    // Comments prefixed with number are taken from the "Function Type
    // Resolution" section of the aforelinked page.
    fn best_match(
        &mut self,
        args: &[sql_parser::ast::Expr],
        raw_arg_types: &[ParameterType],
    ) -> Result<(OperationType, Vec<ParameterType>), failure::Error> {
        let mut candidates = Vec::new();
        let mut max_exact_matches = 0;
        let mut max_preferred_types = 0;

        // Generate candidates by assessing their compatibility with each
        // implementation's parameters.
        for (params, op) in self.implementations.iter() {
            // Do not generate candidates if the number of arguments is invalid
            // for this implementation.
            if op.validate_input_len(args.len(), true).is_err() {
                continue;
            }

            let mut valid_candidate = true;
            let mut arg_types = Vec::new();
            let mut exact_matches = 0;
            let mut preferred_types = 0;

            for (i, (arg, raw_arg_type)) in args.iter().zip(raw_arg_types.iter()).enumerate() {
                let param_type = if let OperationType::Variadic(_, VariadicParam::Repeat(n)) = op {
                    &params[i % *n]
                } else {
                    &params[i]
                };

                let arg_type =
                    if std::mem::discriminant(raw_arg_type) == std::mem::discriminant(param_type) {
                        exact_matches += 1;
                        // This block checks discriminants and returns the argument type
                        // to retain Decimal precision and scale.
                        raw_arg_types[i].clone()
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
                        if is_param_preferred_type_for_arg(&param_type.into(), &raw_arg_type.into())
                            || (is_arg_string_literal(arg)
                                && is_param_preferred_type_for_arg(
                                    &param_type.into(),
                                    &param_type.into(),
                                ))
                        {
                            preferred_types += 1;
                        }

                        param_type.clone()
                    };

                arg_types.push(arg_type);
                self.clear_expr_param_data(&arg);
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

        if let Some((func, types)) = self.maybe_get_last_candidate(&candidates)? {
            return Ok((func, types));
        }

        // 4.c. Run through all candidates and keep those with the most exact matches on
        // input types. Keep all candidates if none have exact matches.
        candidates.retain(|c| c.exact_matches >= max_exact_matches);

        if let Some((func, types)) = self.maybe_get_last_candidate(&candidates)? {
            return Ok((func, types));
        }

        // 4.d. Run through all candidates and keep those that accept preferred types
        // (of the input data type's type category) at the most positions where
        // type conversion will be required.
        candidates.retain(|c| c.preferred_types >= max_preferred_types);

        if let Some((func, types)) = self.maybe_get_last_candidate(&candidates)? {
            return Ok((func, types));
        }

        // This call could be entirely elided if there are no unknown-type arguments.
        match self.best_match_unknown_checks(raw_arg_types, &mut candidates)? {
            Some((func, types)) => Ok((func, types)),
            None => bail!(
                "unable to determine which implementation to use; try providing \
                explicit casts to match parameter types"
            ),
        }
    }

    fn best_match_unknown_checks(
        &mut self,
        raw_arg_types: &[ParameterType],
        candidates: &mut Vec<Candidate>,
    ) -> Result<Option<(OperationType, Vec<ParameterType>)>, failure::Error> {
        let mut found_unknown = false;
        let mut found_known = false;
        let mut types_match = true;
        let mut common_type: Option<ParameterType> = None;

        for (i, raw_arg_type) in raw_arg_types.iter().enumerate() {
            let mut selected_category: Option<TypeCategory> = None;
            let mut found_string_candidate = false;
            let mut categories_match = true;

            match raw_arg_type {
                // 4.e. If any input arguments are unknown, check the type categories accepted
                // at those argument positions by the remaining candidates.
                ParameterType::Unknown => {
                    found_unknown = true;

                    for c in candidates.iter() {
                        let this_category = get_type_category(&(&c.arg_types[i]).into());
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
                        return Ok(None);
                    }

                    // 4.e. cont: Now discard candidates that do not accept the selected
                    // type category. Furthermore, if any candidate accepts a
                    // preferred type in that category, discard candidates that
                    // accept non-preferred types for that argument.
                    let selected_category = selected_category.unwrap();

                    let preferred_type = get_preferred_type(&selected_category);
                    let mut found_preferred_type_candidate = false;
                    candidates.retain(|c| {
                        if let Some(typ) = &preferred_type {
                            found_preferred_type_candidate =
                                c.arg_types[i] == *typ || found_preferred_type_candidate;
                        }
                        selected_category == get_type_category(&(&c.arg_types[i]).into())
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

        if let Some((func, exprs)) = self.maybe_get_last_candidate(&candidates)? {
            return Ok(Some((func, exprs)));
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

            if let Some((func, types)) = self.maybe_get_last_candidate(&candidates)? {
                return Ok(Some((func, types)));
            }
        }

        Ok(None)
    }

    fn maybe_get_last_candidate(
        &self,
        candidates: &[Candidate],
    ) -> Result<Option<(OperationType, Vec<ParameterType>)>, failure::Error> {
        if candidates.len() == 1 {
            match self.get_implementation(&candidates[0].arg_types) {
                Some(func) => Ok(Some((func, candidates[0].arg_types.to_vec()))),
                None => bail!("logical error: please report me"),
            }
        } else {
            Ok(None)
        }
    }

    // Because we use the same `ExprContext` throughout `Self::best_match`, we
    // have to clean up any param types we provisionally inserted. This data
    // gets re-inserted during `Self::generate_exprs_from_types`.
    fn clear_expr_param_data(&self, expr: &sql_parser::ast::Expr) {
        if let sql_parser::ast::Expr::Parameter(n) = expr {
            self.ecx.remove_param(*n);
        }
    }

    // Rewrite operations that take enum field values to use the caller's
    // arguments' values, rather than the defaults used to determine matching
    // implementations.
    fn rewrite_enum_field_ops(op: OperationType, types: &[ParameterType]) -> OperationType {
        use OperationType::*;
        match op {
            Unary(UnaryFunc::CeilDecimal(_)) => match types[0] {
                ParameterType::Decimal(_, s) => Unary(UnaryFunc::CeilDecimal(s)),
                _ => unreachable!(),
            },
            Unary(UnaryFunc::FloorDecimal(_)) => match types[0] {
                ParameterType::Decimal(_, s) => Unary(UnaryFunc::FloorDecimal(s)),
                _ => unreachable!(),
            },
            Unary(UnaryFunc::RoundDecimal(_)) => match types[0] {
                ParameterType::Decimal(_, s) => Unary(UnaryFunc::RoundDecimal(s)),
                _ => unreachable!(),
            },
            Binary(BinaryFunc::RoundDecimal(_)) => match types[0] {
                ParameterType::Decimal(_, s) => Binary(BinaryFunc::RoundDecimal(s)),
                _ => unreachable!(),
            },
            other => other,
        }
    }

    // Plans `args` into `ScalarExprs` of the specified parameter `types`.
    fn generate_exprs_from_types(
        &self,
        op: &OperationType,
        args: &[sql_parser::ast::Expr],
        types: &[ParameterType],
    ) -> Result<Vec<ScalarExpr>, failure::Error> {
        match op {
            OperationType::Variadic(_, VariadicParam::AnyHomogeneous(hint)) => {
                super::query::plan_homogeneous_exprs(self.ident, self.ecx, args, Some(hint.into()))
            }
            _ => {
                // Arguments must be replanned into exprs to ensure parameters (e.g.
                // $1) receive the correct type.
                let mut exprs = Vec::new();
                for (arg, typ) in args.iter().zip(types.iter()) {
                    exprs.push(self.coerce_arg_to_type(arg, typ)?);
                }
                Ok(exprs)
            }
        }
    }

    // Generate `ScalarExpr` necessary to coerce `Expr` into the `ScalarType`
    // corresponding to `ParameterType`; errors if not possible. This can only
    // work within the `func` module because it relies on `ParameterType`.
    fn coerce_arg_to_type(
        &self,
        arg: &Expr,
        typ: &ParameterType,
    ) -> Result<ScalarExpr, failure::Error> {
        use super::query::CastContext::*;
        use super::query::ImplicitCastMod::*;
        let hinted_expr = super::query::plan_expr(self.ecx, &arg, Some(typ.into()))?;

        if self.ecx.scalar_type(&hinted_expr) == *typ {
            Ok(hinted_expr)
        } else if ParameterType::JsonbAny == *typ {
            super::query::plan_to_jsonb(self.ecx, self.ident, hinted_expr)
        } else {
            // JSONB compatibility requires side effects of explicit casts:
            // - Explicit boolean to string behavior
            // - Casting strings to any type
            let ccx = if self.ident.contains("jsonb") {
                Explicit
            } else if ParameterType::StringAny == *typ {
                Implicit(self.ident, PermitAnyToString)
            } else if is_arg_string_literal(arg) {
                Implicit(self.ident, PermitStringToAny)
            } else {
                Implicit(self.ident, None)
            };

            super::query::plan_cast_internal(self.ecx, ccx, hinted_expr, typ.into())
        }
    }
}

pub fn is_arg_string_literal(arg: &Expr) -> bool {
    if let Expr::Value(Value::SingleQuotedString(_)) = arg {
        true
    } else {
        false
    }
}

pub fn select_scalar_func(
    ecx: &ExprContext,
    ident: &str,
    args: &[sql_parser::ast::Expr],
) -> Result<ScalarExpr, failure::Error> {
    use OperationType::*;
    use ParameterType::*;
    let implementations = match ident {
        "abs" => {
            implementations! {
                vec![Int32] => Unary(UnaryFunc::AbsInt32),
                vec![Int64] => Unary(UnaryFunc::AbsInt64),
                vec![Float32] => Unary(UnaryFunc::AbsFloat32),
                vec![Float64] => Unary(UnaryFunc::AbsFloat64)
            }
        }
        "ascii" => {
            implementations! {
                vec![String] => Unary(UnaryFunc::Ascii)
            }
        }
        "ceil" => {
            implementations! {
                vec![Float32] => Unary(UnaryFunc::CeilFloat32),
                vec![Float64] => Unary(UnaryFunc::CeilFloat64),
                vec![Decimal(0, 0)] => Unary(UnaryFunc::CeilDecimal(0))
            }
        }
        "coalesce" => {
            implementations! {
                vec![] => Variadic(VariadicFunc::Coalesce,VariadicParam::AnyHomogeneous(String))
            }
        }
        "concat" => {
            implementations! {
                vec![StringAny] => Variadic(VariadicFunc::Concat, VariadicParam::Repeat(1))
            }
        }
        "convert_from" => {
            implementations! {
                vec![Bytes, String] => Binary(BinaryFunc::ConvertFrom)
            }
        }
        "date_trunc" => {
            implementations! {
                vec![String, Timestamp] => Binary(BinaryFunc::DateTruncTimestamp),
                vec![String, TimestampTz] => Binary(BinaryFunc::DateTruncTimestampTz)
            }
        }
        "floor" => {
            implementations! {
                vec![Float32] => Unary(UnaryFunc::FloorFloat32),
                vec![Float64] => Unary(UnaryFunc::FloorFloat64),
                vec![Decimal(0, 0)] => Unary(UnaryFunc::FloorDecimal(0))
            }
        }
        "jsonb_array_length" => {
            implementations! {
                vec![Jsonb] => Unary(UnaryFunc::JsonbArrayLength)
            }
        }
        "jsonb_build_array" => {
            implementations! {
                vec![] => Variadic(VariadicFunc::JsonbBuildArray, VariadicParam::MustEq(0)),
                vec![JsonbAny] => Variadic(VariadicFunc::JsonbBuildArray, VariadicParam::Repeat(1))
            }
        }
        "jsonb_build_object" => {
            implementations! {
                vec![] => Variadic(VariadicFunc::JsonbBuildObject,VariadicParam::MustEq(0)),
                vec![StringAny, JsonbAny] => Variadic(
                    VariadicFunc::JsonbBuildObject,
                    VariadicParam::Repeat(2)
                )
            }
        }
        "jsonb_pretty" => {
            implementations! {
                vec![Jsonb] => Unary(UnaryFunc::JsonbPretty)
            }
        }
        "jsonb_strip_nulls" => {
            implementations! {
                vec![Jsonb] => Unary(UnaryFunc::JsonbStripNulls),
                // jsonb_strip_nulls does not want to cast NULLs to JSONB.
                vec![Unknown] => Unary(UnaryFunc::JsonbStripNulls)
            }
        }
        "jsonb_typeof" => {
            implementations! {
                vec![Jsonb] => Unary(UnaryFunc::JsonbTypeof)
            }
        }
        "length" => {
            implementations! {
                vec![Bytes] => Unary(UnaryFunc::LengthBytes),
                vec![String] => Variadic(VariadicFunc::LengthString, VariadicParam::MustEq(1)),
                vec![String, String] => Variadic(
                    VariadicFunc::LengthString, VariadicParam::MustEq(2)
                )
            }
        }
        "make_timestamp" => {
            implementations! {
                vec![Int64,
                Int64,
                Int64,
                Int64,
                Int64,
                Float64] => Variadic(VariadicFunc::MakeTimestamp, VariadicParam::MustEq(6))
            }
        }
        "replace" => {
            implementations! {
                vec![String, String, String] => Variadic(
                    VariadicFunc::Replace, VariadicParam::MustEq(3)
                )
            }
        }
        "round" => {
            implementations! {
                vec![Float32] => Unary(UnaryFunc::RoundFloat32),
                vec![Float64] => Unary(UnaryFunc::RoundFloat64),
                vec![Decimal(0,0)] => Unary(UnaryFunc::RoundDecimal(0)),
                vec![Decimal(0,0), Int64] => Binary(BinaryFunc::RoundDecimal(0))
            }
        }
        "substring" | "substr" => {
            implementations! {
                vec![String, Int64] => Variadic(VariadicFunc::Substr, VariadicParam::MustEq(2)),
                vec![String, Int64, Int64] => Variadic(
                    VariadicFunc::Substr,
                    VariadicParam::MustEq(3)
                )
            }
        }
        "to_char" => {
            implementations! {
                vec![Timestamp, String] => Binary(BinaryFunc::ToCharTimestamp),
                vec![TimestampTz, String] => Binary(BinaryFunc::ToCharTimestampTz)
            }
        }
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
            implementations! {
                vec![JsonbAny] => ExprOnly
            }
        }
        "to_timestamp" => {
            implementations! {
                vec![Float64] => Unary(UnaryFunc::ToTimestamp)
            }
        }
        _ => unsupported!(ident),
    };

    match ArgImplementationMatcher::select_implementation(ident, ecx, implementations, args) {
        Ok(expr) => Ok(expr),
        Err(e) => bail!("Cannot call function '{}': {}", ident, e),
    }
}
