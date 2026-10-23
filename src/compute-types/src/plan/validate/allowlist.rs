// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The functions admitted in user plans, each with a typing rule.
//!
//! The repository states no input signatures for most functions, so evaluation trusts the
//! planner to apply each function to the types it expects, and many function bodies panic
//! on an unexpected datum. A function is admitted only after its body, and the `eval`
//! dispatch that reaches it, was read and found not to panic on inputs that satisfy its
//! rule. Each entry notes what was checked.
//!
//! Two facts recur. Functions defined with `#[sqlfunc]` over typed parameters convert their
//! arguments with `InputDatumType::try_from_result`, and `LazyUnaryFunc::eval` and
//! `LazyBinaryFunc::eval` turn a datum of another type into `EvalError::Internal` rather
//! than a panic, and propagate nulls before calling the body. Their output types come from
//! the macro and read no input type, so computing them cannot panic.

use itertools::Itertools;
use mz_expr::func::variadic::{And, Or};
use mz_expr::{AggregateFunc, BinaryFunc, TableFunc, UnaryFunc, VariadicFunc};
use mz_repr::{ReprColumnType, ReprScalarType};

use crate::plan::scalar::LirScalarExpr;

/// The argument types a function admits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TypingRule {
    /// The arguments have exactly these scalar types, in order.
    Exact(Vec<ReprScalarType>),
    /// The arguments have one scalar type.
    SameAsEachOther,
    /// Every argument is a `bool`, for any number of arguments.
    AllBool,
    /// One argument of any type.
    Any,
    /// The arguments form rows of `types.len()` columns, so their number is a multiple of
    /// it, and argument `i` has the scalar type `types[i % types.len()]`. `types` is not
    /// empty.
    Rows(Vec<ReprScalarType>),
}

impl TypingRule {
    /// Checks argument types against the rule. `None` satisfies any rule, and stands for an
    /// argument no row reaches, on which the function is never evaluated. The validator
    /// rejects an argument of unknown type before calling this.
    ///
    /// Returns a description of the violation, if any.
    pub fn check(&self, args: &[Option<&ReprColumnType>]) -> Result<(), String> {
        let arg_types = || {
            let types: Vec<_> = args
                .iter()
                .map(|t| t.map_or("unknown".to_string(), |t| format!("{:?}", t.scalar_type)))
                .collect();
            format!("[{}]", types.join(", "))
        };
        match self {
            TypingRule::Exact(expected) => {
                let matches = args.len() == expected.len()
                    && args
                        .iter()
                        .zip_eq(expected)
                        .all(|(arg, e)| arg.is_none_or(|arg| arg.scalar_type == *e));
                if !matches {
                    return Err(format!("arguments {expected:?}, got {}", arg_types()));
                }
            }
            TypingRule::SameAsEachOther => {
                let mut known = args.iter().flatten();
                if let Some(first) = known.next() {
                    if known.any(|t| t.scalar_type != first.scalar_type) {
                        return Err(format!("arguments of the same type, got {}", arg_types()));
                    }
                }
            }
            TypingRule::AllBool => {
                if args
                    .iter()
                    .flatten()
                    .any(|t| t.scalar_type != ReprScalarType::Bool)
                {
                    return Err(format!("bool arguments, got {}", arg_types()));
                }
            }
            TypingRule::Any => {
                if args.len() != 1 {
                    return Err(format!("one argument, got {} arguments", args.len()));
                }
            }
            TypingRule::Rows(types) => {
                let width = types.len();
                if width == 0 || args.len() % width != 0 {
                    return Err(format!(
                        "a multiple of {width} arguments, got {} arguments",
                        args.len()
                    ));
                }
                let matches = args.iter().enumerate().all(|(i, arg)| {
                    arg.is_none_or(|arg| Some(&arg.scalar_type) == types.get(i % width))
                });
                if !matches {
                    return Err(format!("rows of arguments {types:?}, got {}", arg_types()));
                }
            }
        }
        Ok(())
    }
}

/// The typing rule of the function a call node applies, or `None` if the function is not
/// admitted in user plans or `expr` is not a call.
pub fn scalar_typing_rule(expr: &LirScalarExpr) -> Option<TypingRule> {
    use ReprScalarType::{Bool, Int32, Int64, MzTimestamp};
    match expr {
        LirScalarExpr::CallUnary { func, .. } => match func {
            // `#[sqlfunc]` over `bool`, `!a`.
            UnaryFunc::Not(_) => Some(TypingRule::Exact(vec![Bool])),
            // `#[sqlfunc]` over a bare `Datum`, which accepts every datum, `a.is_null()`.
            UnaryFunc::IsNull(_) => Some(TypingRule::Any),
            // `#[sqlfunc]` over `i32`, `i64::from(a)`.
            UnaryFunc::CastInt32ToInt64(_) => Some(TypingRule::Exact(vec![Int32])),
            // Temporal bounds. `#[sqlfunc]` over `i64` with `try_into` and over `Timestamp`
            // with `checked_add`, both returning an error when out of range.
            UnaryFunc::CastInt64ToMzTimestamp(_) => Some(TypingRule::Exact(vec![Int64])),
            UnaryFunc::StepMzTimestamp(_) => Some(TypingRule::Exact(vec![MzTimestamp])),
            _ => None,
        },
        LirScalarExpr::CallBinary { func, .. } => match func {
            // `#[sqlfunc]` over two `ExcludeNull<Datum>`, which accept every non-null datum,
            // comparing with the derived `PartialEq` and `Ord` of `Datum`. These order datums
            // of different variants without panicking, so only the meaning, not evaluation,
            // needs equal types. Equality of text is `Eq` on two `String`s.
            BinaryFunc::Eq(_)
            | BinaryFunc::NotEq(_)
            | BinaryFunc::Lt(_)
            | BinaryFunc::Lte(_)
            | BinaryFunc::Gt(_)
            | BinaryFunc::Gte(_) => Some(TypingRule::SameAsEachOther),
            // `#[sqlfunc]` over two `i64`, with `checked_add`, `checked_sub` and `checked_mul`
            // returning `NumericFieldOverflow`.
            BinaryFunc::AddInt64(_) | BinaryFunc::SubInt64(_) | BinaryFunc::MulInt64(_) => {
                Some(TypingRule::Exact(vec![Int64, Int64]))
            }
            _ => None,
        },
        LirScalarExpr::CallVariadic { func, .. } => match func {
            // Hand-written `eval` matching `True`, `False` and `Null` and `unreachable!` on any
            // other datum, so every argument must be a `bool`. Zero arguments evaluate to the
            // identity. `output_type` reads only nullability.
            VariadicFunc::And(And) | VariadicFunc::Or(Or) => Some(TypingRule::AllBool),
            _ => None,
        },
        LirScalarExpr::Column(..) | LirScalarExpr::Literal(..) | LirScalarExpr::If { .. } => None,
    }
}

/// The typing rule of a table function, or `None` if it is not admitted in user plans.
pub fn table_func_typing_rule(func: &TableFunc) -> Option<TypingRule> {
    match func {
        // `wrap` packs `datums.chunks(width)` into rows. `chunks(0)` panics, and an argument
        // count that is not a multiple of `width` leaves a short last row that the `FlatMap`
        // MFP indexes beyond. `output_type` clones `types`, which then has to have `width`
        // entries for the output to have the arity `width` states.
        TableFunc::Wrap { types, width } if *width >= 1 && types.len() == *width => {
            Some(TypingRule::Rows(
                types
                    .iter()
                    .map(|t| ReprScalarType::from(&t.scalar_type))
                    .collect(),
            ))
        }
        _ => None,
    }
}

/// The typing rule of an aggregate function over its single argument, or `None` if it is
/// not admitted in user plans.
///
/// Rendering evaluates aggregates in `mz_compute::render::reduce`: accumulable ones through
/// `datum_to_accumulator` and `finalize_accum`, hierarchical ones through their monoid,
/// and basic ones through `AggregateFunc::eval`.
pub fn aggregate_typing_rule(func: &AggregateFunc) -> Option<TypingRule> {
    use ReprScalarType::{Bool, Int64};
    match func {
        // `datum_to_accumulator` and `count` read only whether the datum is null.
        AggregateFunc::Count => Some(TypingRule::Any),
        // `datum_to_accumulator` accumulates `Int16`, `Int32`, `Int64`, the unsigned
        // integers, `MzTimestamp` and `Null`, and panics on other datums. `eval` reads
        // `Int16`, `Int32` and `Int64` and panics on others.
        AggregateFunc::SumInt64 => Some(TypingRule::Exact(vec![Int64])),
        // The monoid compares rows, and `eval` converts each non-null datum to `i64` with
        // `expect`.
        AggregateFunc::MaxInt64 | AggregateFunc::MinInt64 => Some(TypingRule::Exact(vec![Int64])),
        // `datum_to_accumulator` matches `True`, `False` and `Null` and panics on other
        // datums. `any` and `all` fold over the same three.
        AggregateFunc::Any | AggregateFunc::All => Some(TypingRule::Exact(vec![Bool])),
        _ => None,
    }
}
