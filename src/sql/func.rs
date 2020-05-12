// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use failure::bail;
use repr::ScalarType;

use super::expr::{BinaryFunc, ScalarExpr, UnaryFunc, VariadicFunc};
use super::query::ExprContext;

#[derive(Debug, Eq, PartialEq, Clone)]
// Mirrored from [pg
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
//  'bool',
//  'bytea',
//  'date',
//  'float4',
//  'float8',
//  'int4',
//  'int8',
//  'interval',
//  'jsonb',
//  'numeric',
//  'text',
//  'time',
//  'timestamp',
//  'timestamptz'
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

#[derive(Debug, Eq, PartialEq, Clone)]
struct Candidate {
    arg_types: Vec<ScalarType>,
    exact_matches: usize,
    preferred_types_arg: usize,
    preferred_types_param: usize,
}

#[derive(Debug, Eq, PartialEq, Clone)]
// Unifies all functions (and operations in a future PR) into a single type.
enum OperationType {
    Unary(UnaryFunc),
    Binary(BinaryFunc),
    Variadic(VariadicFunc, VariadicArgMod),
}

impl OperationType {
    fn get_arg_len(&self) -> Option<usize> {
        use VariadicArgMod::*;
        match self {
            Self::Unary(_) => Some(1),
            Self::Binary(_) => Some(2),
            Self::Variadic(_, arg_mod) => match arg_mod {
                MustEq(arg_len) => Some(*arg_len),
                AnyHomogeneous(_) => None,
            },
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
enum VariadicArgMod {
    // Expresses that arguments may be of any type, as long as all arguments are
    // homogeneous. Use the embedded `ScalarType` as type hint if all arguments
    // are `ScalarType::Unknown`.
    //
    // When using `AnyHomogeneous`, provide an empty `Vec::<ScalarType>` as the
    // implementation's parameter.
    AnyHomogeneous(ScalarType),
    // Ensures that there are `usize` arguments.
    MustEq(usize),
}

#[derive(Clone, Debug)]
/// Determines best implementation to use given some user-provided arguments.
/// For more detail, see `ArgImplementationMatcher::select_implementation`.
struct ArgImplementationMatcher<'a> {
    ident: &'a str,
    ecx: &'a ExprContext<'a>,
    implementations: HashMap<Vec<ScalarType>, OperationType>,
}

#[macro_export]
// Provides a macro to write HashMap "literals."
macro_rules! implementations(
    { $($key:expr => $value:expr),+ } => {
        {
            let mut m = ::std::collections::HashMap::new();
            $(
                if let Variadic(_, VariadicArgMod::AnyHomogeneous(_)) = $value {
                    if !$key.is_empty() {
                        bail!("Variadic function implementations that accept any \
                        homogeneously typed set of arguments must not specify \
                        parameter types.")
                    }
                }
                m.insert($key, $value);
            )+
            m
        }
     };
);

impl<'a> ArgImplementationMatcher<'a> {
    /// Selects the best implementation given the provided `args` using a
    /// process similar to [PostgreSQL's
    /// parser](https://www.postgresql.org/docs/current/typeconv-oper.html).
    /// This includes both the function to call and the types that you should cast each
    /// argument to.
    ///
    /// Generate `implementations` using the `implementations!` macro in this
    /// package.
    ///
    /// # Errors
    /// - When a "best" implementation cannot be determined, e.g. because `args`
    ///   cannot be converted to the appropriate types or all implementations
    ///   are equally valid.
    pub fn select_implementation(
        ident: &'a str,
        ecx: &'a ExprContext<'a>,
        implementations: HashMap<Vec<ScalarType>, OperationType>,
        args: &[sql_parser::ast::Expr],
    ) -> Result<(OperationType, Vec<ScalarType>), failure::Error> {
        let mut m = Self::new(ident, ecx, implementations);

        let (op, types) = m.match_args_to_implementation(args)?;

        let op = Self::rewrite_enum_field_ops(op, &types);

        Ok((op, types))
    }

    fn new(
        ident: &'a str,
        ecx: &'a ExprContext<'a>,
        implementations: HashMap<Vec<ScalarType>, OperationType>,
    ) -> Self {
        Self {
            ident,
            ecx,
            implementations,
        }
    }

    fn match_args_to_implementation(
        &mut self,
        args: &[sql_parser::ast::Expr],
    ) -> Result<(OperationType, Vec<ScalarType>), failure::Error> {
        let mut raw_arg_types = Vec::new();
        for arg in args {
            let expr = super::query::plan_expr(self.ecx, &arg, Some(ScalarType::Unknown))?;
            raw_arg_types.push(self.ecx.scalar_type(&expr));
            // We must remove any params that have their types inserted into the
            // ExprContext because we use the same ExprContext throughout the
            // function selection process.
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
    // Decimal sclae and precision.
    fn get_implementation(&self, types: &[ScalarType]) -> Option<OperationType> {
        use OperationType::*;

        // Rewrites all Decimal values to a consistent scale and precision
        // to support matching implementation parameters.
        // TODO(sploiselle): Add `ScalarType::List` support.
        let matchable_types: Vec<ScalarType> = types
            .iter()
            .map(|t| match t {
                ScalarType::Decimal(_, _) => ScalarType::Decimal(38, 0),
                other => other.clone(),
            })
            .collect();

        // If all elements of `types` have the same type, return a
        // one-element vector of that type, which lets us match Variadic
        // functions that only have a single parameter type defined.
        let maybe_single_variadic_param = if matchable_types.len() > 0 {
            let common_type = &matchable_types[0];
            if types.iter().all(|x| x == common_type) {
                vec![common_type.clone()]
            } else {
                matchable_types.clone()
            }
        } else {
            matchable_types.clone()
        };

        // Get an operation if one of the following criteria are met:
        // 1. Any operation whose parameters exactly match `types`.
        // 2. Variadic functions with a single parameter, if `types` share the
        //   same discriminant.
        // 3. Variadic functions that accept any homogeneous type iff there are
        //   no other implementations.
        let op = if let Some(op) = self.implementations.get(&matchable_types) {
            // Case 1
            op.clone()
        } else if let Some(op) = self.implementations.get(&maybe_single_variadic_param) {
            // Case 2
            match op {
                Variadic(op, arg_mod) => Variadic(op.clone(), arg_mod.clone()),
                _ => return None,
            }
        } else if let Some(op) = self.implementations.get(&Vec::<ScalarType>::new()) {
            // Case 3
            match op {
                // Only allow eager matching if this is the only
                // implementation; if there are multiple implementations,
                // `best_match` will generate a candidate that matches exactly.
                Variadic(op, VariadicArgMod::AnyHomogeneous(hint))
                    if self.implementations.len() == 1 =>
                {
                    Variadic(op.clone(), VariadicArgMod::AnyHomogeneous(hint.clone()))
                }
                _ => return None,
            }
        } else {
            return None;
        };

        // Perform arg length validation for eager exact matches.
        if let Some(arg_len) = op.get_arg_len() {
            if types.len() != arg_len {
                return None;
            }
        }

        Some(op)
    }

    // Comments prefixed with number are taken from the "Function Type Resolution"
    // section of https://www.postgresql.org/docs/current/typeconv-func.html
    fn best_match(
        &mut self,
        args: &[sql_parser::ast::Expr],
        raw_arg_types: &[ScalarType],
    ) -> Result<(OperationType, Vec<ScalarType>), failure::Error> {
        let mut max_exact_matches = 0;
        let mut max_preferred_types_arg = 0;
        let mut max_preferred_types_param = 0;

        let mut candidates = Vec::new();

        for (params, op) in self.implementations.iter() {
            if let Some(arg_len) = op.get_arg_len() {
                if arg_len != args.len() {
                    bail!("expected {} arguments, received {}", arg_len, args.len())
                }
            }

            let single_variadic_param = if let OperationType::Variadic(_, _) = op {
                params.len() == 1
            } else {
                false
            };

            let mut valid_candidate = true;
            let mut arg_types = Vec::new();
            let mut exact_matches = 0;
            let mut preferred_types_arg = 0;
            let mut preferred_types_param = 0;

            for (i, (arg, raw_arg_type)) in args.iter().zip(raw_arg_types.iter()).enumerate() {
                let param_type =
                // Skip processing params if none exist, i.e. VariadicArgMod::AnyHomogeneous.
                if params.len() == 0 {
                    continue;
                } else if single_variadic_param {
                    &params[0]
                } else {
                    &params[i]
                };

                // Track desired argument type. N.B. this != `param_type`, e.g.
                // Decimal, which will contain `param_type` data instead of
                // `arg`.
                let arg_type =
                // Check if the expression already has the desired type,
                // i.e. exact match. Should only check by variant, not by
                // value.
                    if std::mem::discriminant(raw_arg_type) == std::mem::discriminant(&param_type)
                {
                    exact_matches += 1;
                    raw_arg_types[i].clone()
                } else {

                    // Ensure type if coercable to `param_type`.
                    match coerce_arg_to_type(self.ident, self.ecx, arg, &param_type) {
                        Ok(expr) => expr,
                        Err(_) => {
                            valid_candidate = false;
                            break;
                        }
                    };
                    if is_param_preferred_type_for_arg(param_type, &raw_arg_type) {
                        preferred_types_arg += 1;
                    } else if is_param_preferred_type_for_arg(param_type, param_type) {
                        preferred_types_param += 1;
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
                max_preferred_types_arg =
                    std::cmp::max(max_preferred_types_arg, preferred_types_arg);
                max_preferred_types_param =
                    std::cmp::max(max_preferred_types_param, preferred_types_param);
                candidates.push(Candidate {
                    arg_types,
                    exact_matches,
                    preferred_types_arg,
                    preferred_types_param,
                });
            }
        }

        if candidates.is_empty() {
            bail!(
                "arguments cannot be implicitly cast to any implementation's parameters;\
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
        candidates.retain(|c| c.preferred_types_arg >= max_preferred_types_arg);

        if let Some((func, types)) = self.maybe_get_last_candidate(&candidates)? {
            return Ok((func, types));
        }

        // Undocumented: Run through all candidates and keep those that accept
        // preferred types (of the function's parameters) at the most positions
        // where type conversion will be required.
        candidates.retain(|c| c.preferred_types_param >= max_preferred_types_param);

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
        raw_arg_types: &[ScalarType],
        candidates: &mut Vec<Candidate>,
    ) -> Result<Option<(OperationType, Vec<ScalarType>)>, failure::Error> {
        let mut found_unknown = false;
        let mut found_known = false;
        let mut types_match = false;
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
                        let this_category = get_type_category(&c.arg_types[i]);
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
                        selected_category == get_type_category(&c.arg_types[i])
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
    ) -> Result<Option<(OperationType, Vec<ScalarType>)>, failure::Error> {
        if candidates.len() == 1 {
            match self.get_implementation(&candidates[0].arg_types) {
                Some(func) => Ok(Some((func, candidates[0].arg_types.to_vec()))),
                None => {
                    bail!("Logical error: my expr candidate cannot be mapped to my func candidate")
                }
            }
        } else {
            Ok(None)
        }
    }

    // Remove any params that have their types inserted into the ExprContext
    // because we use the same ExprContext throughout the function selection
    // process. Clears any params that had their types inserted into `self.ecx`
    // while matching args to implementations. This is necessary because we're
    // using the actual query's `ExprContext` as a scratch space.
    fn clear_expr_param_data(&self, expr: &sql_parser::ast::Expr) {
        if let sql_parser::ast::Expr::Parameter(n) = expr {
            self.ecx.remove_param(*n);
        }
    }

    // Rewrite operations that take enum field values to use the caller's
    // arguments' values, rather than the defaults used to determine matching
    // implementations.
    fn rewrite_enum_field_ops(op: OperationType, types: &[ScalarType]) -> OperationType {
        use OperationType::*;
        let op = match op {
            Unary(UnaryFunc::CeilDecimal(_)) => match types[0] {
                ScalarType::Decimal(_, s) => Unary(UnaryFunc::CeilDecimal(s)),
                _ => unreachable!(),
            },
            other => other,
        };
        op
    }
}

// Generate `ScalarExpr` necessary to coerce `Expr` into `ScalarType`; errors if
// not possible.
fn coerce_arg_to_type(
    ident: &str,
    ecx: &ExprContext,
    arg: &sql_parser::ast::Expr,
    typ: &ScalarType,
) -> Result<ScalarExpr, failure::Error> {
    let hinted_expr = super::query::plan_expr(ecx, &arg, Some(typ.clone()))?;

    if std::mem::discriminant(&ecx.scalar_type(&hinted_expr)) == std::mem::discriminant(&typ) {
        return Ok(hinted_expr);
    }

    super::query::plan_cast_internal(
        &ecx,
        super::query::CastContext::Implicit(ident),
        hinted_expr,
        typ.clone(),
    )
}

pub fn select_function(
    ecx: &ExprContext,
    ident: &str,
    args: &[sql_parser::ast::Expr],
) -> Result<ScalarExpr, failure::Error> {
    use OperationType::*;

    let implementations = match ident {
        "abs" => {
            implementations! {
                vec![ScalarType::Int32] => Unary(UnaryFunc::AbsInt32),
                vec![ScalarType::Int64] => Unary(UnaryFunc::AbsInt64),
                vec![ScalarType::Float32] => Unary(UnaryFunc::AbsFloat32),
                vec![ScalarType::Float64] => Unary(UnaryFunc::AbsFloat64)
            }
        }
        "ascii" => {
            implementations! {
                vec![ScalarType::String] => Unary(UnaryFunc::Ascii)
            }
        }
        "ceil" => {
            implementations! {
                vec![ScalarType::Float32] => Unary(UnaryFunc::CeilFloat32),
                vec![ScalarType::Float64] => Unary(UnaryFunc::CeilFloat64),
                vec![ScalarType::Decimal(38, 0)] => Unary(UnaryFunc::CeilDecimal(0))
            }
        }
        "coalesce" => {
            implementations! {
                Vec::<ScalarType>::new() => Variadic(VariadicFunc::Coalesce, VariadicArgMod::AnyHomogeneous(ScalarType::String))
            }
        }
        "convert_from" => {
            implementations! {
                vec![ScalarType::Bytes, ScalarType::String] => Binary(BinaryFunc::ConvertFrom)
            }
        }
        "replace" => {
            implementations! {
                vec![ScalarType::String] => Variadic(VariadicFunc::Replace, VariadicArgMod::MustEq(3))
            }
        }
        _ => bail!("unsupported function: {}", ident),
    };

    let (func, types) =
        match ArgImplementationMatcher::select_implementation(ident, ecx, implementations, args) {
            Ok((func, types)) => (func, types),
            Err(e) => bail!("Cannot call function '{}': {}", ident, e),
        };

    let mut exprs = match &func {
        Variadic(_, VariadicArgMod::AnyHomogeneous(hint)) => {
            super::query::plan_homogeneous_exprs(ident, ecx, args, Some(hint.clone()))?
        }
        _ => {
            // Arguments must be replanned into exprs to ensure parameters (e.g.
            // $1) receive the correct type.
            let mut exprs = Vec::new();
            for (arg, typ) in args.iter().zip(types.iter()) {
                exprs.push(coerce_arg_to_type(ident, &ecx, arg, typ)?);
            }
            exprs
        }
    };

    match func {
        Unary(func) => Ok(ScalarExpr::CallUnary {
            func,
            expr: Box::new(exprs.remove(0)),
        }),
        Binary(func) => Ok(ScalarExpr::CallBinary {
            func,
            expr1: Box::new(exprs.remove(0)),
            expr2: Box::new(exprs.remove(0)),
        }),
        Variadic(func, _) => Ok(ScalarExpr::CallVariadic { func, exprs }),
    }
}
