// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Post-order rewrites for `CallBinary` nodes.

use std::mem;

use itertools::Itertools;
use mz_pgtz::timezone::{Timezone, TimezoneSpec};
use mz_repr::adt::datetime::DateTimeUnits;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::regex::Regex;
use mz_repr::{Datum, ReprColumnType, ReprScalarType, RowArena};

use crate::scalar::func::format::DateTimeFormat;
use crate::scalar::func::variadic::And;
use crate::scalar::func::{self, BinaryFunc, UnaryFunc, VariadicFunc, parse_timezone};
use crate::scalar::like_pattern;
use crate::{Eval, EvalError, MirScalarExpr};

pub(super) fn reduce_call_binary(
    e: &mut MirScalarExpr,
    column_types: &[ReprColumnType],
    temp_storage: &RowArena,
) {
    let MirScalarExpr::CallBinary { func, expr1, expr2 } = e else {
        unreachable!()
    };

    // Fold/propagate literal-shaped operands first; precompiles below assume
    // these have already fired.
    if expr1.is_literal() && expr2.is_literal() {
        *e = MirScalarExpr::literal(e.eval(&[], temp_storage), e.typ(column_types).scalar_type);
        return;
    }
    if (expr1.is_literal_null() || expr2.is_literal_null()) && func.propagates_nulls() {
        *e = MirScalarExpr::literal_null(e.typ(column_types).scalar_type);
        return;
    }
    if let Some(err) = expr1.as_literal_err() {
        *e = MirScalarExpr::literal(Err(err.clone()), e.typ(column_types).scalar_type);
        return;
    }
    if let Some(err) = expr2.as_literal_err() {
        *e = MirScalarExpr::literal(Err(err.clone()), e.typ(column_types).scalar_type);
        return;
    }

    // Calls where a literal operand makes the call the identity function on
    // the other operand reduce to that operand.
    if reduce_call_binary_identity(e) {
        return;
    }
    let MirScalarExpr::CallBinary { func, expr1, expr2 } = e else {
        unreachable!()
    };

    // Per-function dispatch. Each precompile fires only if its literal-shaped
    // argument is present; otherwise the call falls through unchanged.
    match func {
        BinaryFunc::IsLikeMatchCaseInsensitive(_) if expr2.is_literal() => {
            // We can at least precompile the regex.
            precompile_is_like(e, column_types, true);
        }
        BinaryFunc::IsLikeMatchCaseSensitive(_) if expr2.is_literal() => {
            // We can at least precompile the regex.
            precompile_is_like(e, column_types, false);
        }
        BinaryFunc::IsRegexpMatchCaseSensitive(_) | BinaryFunc::IsRegexpMatchCaseInsensitive(_) => {
            let case_insensitive = matches!(func, BinaryFunc::IsRegexpMatchCaseInsensitive(_));
            if let MirScalarExpr::Literal(Ok(row), _) = &**expr2 {
                *e = match Regex::new(row.unpack_first().unwrap_str(), case_insensitive) {
                    Ok(regex) => expr1
                        .take()
                        .call_unary(UnaryFunc::IsRegexpMatch(func::IsRegexpMatch(regex))),
                    Err(err) => {
                        MirScalarExpr::literal(Err(err.into()), e.typ(column_types).scalar_type)
                    }
                };
            }
        }
        BinaryFunc::ExtractInterval(_) if expr1.is_literal() => {
            precompile_date_units(e, column_types, |u| {
                UnaryFunc::ExtractInterval(func::ExtractInterval(u))
            })
        }
        BinaryFunc::ExtractTime(_) if expr1.is_literal() => {
            precompile_date_units(e, column_types, |u| {
                UnaryFunc::ExtractTime(func::ExtractTime(u))
            })
        }
        BinaryFunc::ExtractTimestamp(_) if expr1.is_literal() => {
            precompile_date_units(e, column_types, |u| {
                UnaryFunc::ExtractTimestamp(func::ExtractTimestamp(u))
            })
        }
        BinaryFunc::ExtractTimestampTz(_) if expr1.is_literal() => {
            precompile_date_units(e, column_types, |u| {
                UnaryFunc::ExtractTimestampTz(func::ExtractTimestampTz(u))
            })
        }
        BinaryFunc::ExtractDate(_) if expr1.is_literal() => {
            precompile_date_units(e, column_types, |u| {
                UnaryFunc::ExtractDate(func::ExtractDate(u))
            })
        }
        BinaryFunc::DatePartInterval(_) if expr1.is_literal() => {
            precompile_date_units(e, column_types, |u| {
                UnaryFunc::DatePartInterval(func::DatePartInterval(u))
            })
        }
        BinaryFunc::DatePartTime(_) if expr1.is_literal() => {
            precompile_date_units(e, column_types, |u| {
                UnaryFunc::DatePartTime(func::DatePartTime(u))
            })
        }
        BinaryFunc::DatePartTimestamp(_) if expr1.is_literal() => {
            precompile_date_units(e, column_types, |u| {
                UnaryFunc::DatePartTimestamp(func::DatePartTimestamp(u))
            })
        }
        BinaryFunc::DatePartTimestampTz(_) if expr1.is_literal() => {
            precompile_date_units(e, column_types, |u| {
                UnaryFunc::DatePartTimestampTz(func::DatePartTimestampTz(u))
            })
        }
        BinaryFunc::DateTruncTimestamp(_) if expr1.is_literal() => {
            precompile_date_units(e, column_types, |u| {
                UnaryFunc::DateTruncTimestamp(func::DateTruncTimestamp(u))
            })
        }
        BinaryFunc::DateTruncTimestampTz(_) if expr1.is_literal() => {
            precompile_date_units(e, column_types, |u| {
                UnaryFunc::DateTruncTimestampTz(func::DateTruncTimestampTz(u))
            })
        }
        BinaryFunc::TimezoneTimestampBinary(_) if expr1.is_literal() => {
            // If the timezone argument is a literal, and we're applying the function on many rows at the same
            // time we really don't want to parse it again and again, so we parse it once and embed it into the
            // UnaryFunc enum. The memory footprint of Timezone is small (8 bytes).
            precompile_timezone(e, column_types, |tz| {
                UnaryFunc::TimezoneTimestamp(func::TimezoneTimestamp(tz))
            });
        }
        BinaryFunc::TimezoneTimestampTzBinary(_) if expr1.is_literal() => {
            precompile_timezone(e, column_types, |tz| {
                UnaryFunc::TimezoneTimestampTz(func::TimezoneTimestampTz(tz))
            });
        }
        BinaryFunc::ToCharTimestamp(_) if expr2.is_literal() => {
            precompile_to_char(e, |format_string, format| {
                UnaryFunc::ToCharTimestamp(func::ToCharTimestamp {
                    format_string,
                    format,
                })
            });
        }
        BinaryFunc::ToCharTimestampTz(_) if expr2.is_literal() => {
            precompile_to_char(e, |format_string, format| {
                UnaryFunc::ToCharTimestampTz(func::ToCharTimestampTz {
                    format_string,
                    format,
                })
            });
        }
        BinaryFunc::Eq(_) | BinaryFunc::NotEq(_) if expr2 < expr1 => {
            // Canonically order elements so that deduplication works better.
            // Also, the `Literal([c1, c2]) = record_create(e1, e2)` matching
            // below relies on this canonical ordering.
            mem::swap(expr1, expr2);
        }
        _ => reduce_call_binary_eq_record(e),
    }
}

/// Rewrites a call whose literal operand makes it the identity function on
/// the other operand to that operand, returning whether it fired.
///
/// Only patterns that can change neither error nor null behavior are listed:
/// evaluation at the identity operand is infallible for every listed function
/// (division by one cannot error; adding an all-zero interval cannot leave
/// the timestamp domain; trimming the empty character set cannot grow the
/// string), every listed function propagates nulls, and the surviving operand
/// has the value and type of the original call. Functions that can error even
/// at their identity are deliberately absent: e.g. `text_concat` and
/// `repeat(s, 1)` re-validate the length of an oversized operand, so eliding
/// them would suppress that error. Floats are also absent: `-0.0 + 0.0` is
/// `+0.0`, so the additive identities are not exact, and we keep the float
/// story all-or-nothing for legibility. Numerics are absent pending an answer
/// on result scale.
fn reduce_call_binary_identity(e: &mut MirScalarExpr) -> bool {
    use BinaryFunc::*;
    let MirScalarExpr::CallBinary { func, expr1, expr2 } = e else {
        unreachable!()
    };
    // For each function: the literal datum under which the call is the
    // identity on the other operand, and whether the function commutes (so
    // the identity may appear on either side rather than only the right).
    let zero_interval = Datum::Interval(Interval::default());
    let (identity, commutes) = match func {
        AddInt16(_) => (Datum::Int(0), true),
        AddInt32(_) => (Datum::Int(0), true),
        AddInt64(_) => (Datum::Int(0), true),
        AddUint16(_) => (Datum::UInt(0), true),
        AddUint32(_) => (Datum::UInt(0), true),
        AddUint64(_) => (Datum::UInt(0), true),
        SubInt16(_) => (Datum::Int(0), false),
        SubInt32(_) => (Datum::Int(0), false),
        SubInt64(_) => (Datum::Int(0), false),
        SubUint16(_) => (Datum::UInt(0), false),
        SubUint32(_) => (Datum::UInt(0), false),
        SubUint64(_) => (Datum::UInt(0), false),
        MulInt16(_) => (Datum::Int(1), true),
        MulInt32(_) => (Datum::Int(1), true),
        MulInt64(_) => (Datum::Int(1), true),
        MulUint16(_) => (Datum::UInt(1), true),
        MulUint32(_) => (Datum::UInt(1), true),
        MulUint64(_) => (Datum::UInt(1), true),
        DivInt16(_) => (Datum::Int(1), false),
        DivInt32(_) => (Datum::Int(1), false),
        DivInt64(_) => (Datum::Int(1), false),
        DivUint16(_) => (Datum::UInt(1), false),
        DivUint32(_) => (Datum::UInt(1), false),
        DivUint64(_) => (Datum::UInt(1), false),
        // Temporal: an all-zero interval added to or subtracted from a
        // timestamp, time, or interval. (Dates are absent: their interval
        // arithmetic changes the type to timestamp.)
        AddInterval(_) => (zero_interval, true),
        SubInterval(_)
        | AddTimestampInterval(_)
        | AddTimestampTzInterval(_)
        | AddTimeInterval(_)
        | SubTimestampInterval(_)
        | SubTimestampTzInterval(_)
        | SubTimeInterval(_) => (zero_interval, false),
        // Bitwise: zero is the identity for or/xor, all-ones for and, and a
        // shift distance of zero (always an `int4`) leaves the value alone.
        BitOrInt16(_) | BitXorInt16(_) => (Datum::Int(0), true),
        BitOrInt32(_) | BitXorInt32(_) => (Datum::Int(0), true),
        BitOrInt64(_) | BitXorInt64(_) => (Datum::Int(0), true),
        BitOrUint16(_) | BitXorUint16(_) => (Datum::UInt(0), true),
        BitOrUint32(_) | BitXorUint32(_) => (Datum::UInt(0), true),
        BitOrUint64(_) | BitXorUint64(_) => (Datum::UInt(0), true),
        BitAndInt16(_) => (Datum::Int(-1), true),
        BitAndInt32(_) => (Datum::Int(-1), true),
        BitAndInt64(_) => (Datum::Int(-1), true),
        BitAndUint16(_) => (Datum::UInt(u64::from(u16::MAX)), true),
        BitAndUint32(_) => (Datum::UInt(u64::from(u32::MAX)), true),
        BitAndUint64(_) => (Datum::UInt(u64::MAX), true),
        BitShiftLeftInt16(_)
        | BitShiftRightInt16(_)
        | BitShiftLeftInt32(_)
        | BitShiftRightInt32(_)
        | BitShiftLeftInt64(_)
        | BitShiftRightInt64(_)
        | BitShiftLeftUint16(_)
        | BitShiftRightUint16(_)
        | BitShiftLeftUint32(_)
        | BitShiftRightUint32(_)
        | BitShiftLeftUint64(_)
        | BitShiftRightUint64(_) => (Datum::Int(0), false),
        // Trimming the empty set of characters.
        Trim(_) | TrimLeading(_) | TrimTrailing(_) => (Datum::String(""), false),
        _ => return false,
    };
    if matches!(expr2.as_literal(), Some(Ok(d)) if d == identity) {
        *e = expr1.take();
        true
    } else if commutes && matches!(expr1.as_literal(), Some(Ok(d)) if d == identity) {
        *e = expr2.take();
        true
    } else {
        false
    }
}

/// Decomposes equality with `RecordCreate` on the right-hand side.
///
/// Handles two cases:
/// - `Literal([c1, ...]) = record_create(e1, ...)` → `c1 = e1 AND ...`
/// - `record_create(a1, ...) = record_create(b1, ...)` → `a1 = b1 AND ...`
///
/// `MapFilterProject::literal_constraints` relies on the first transform,
/// because `(e1,e2) IN ((1,2))` is desugared using `record_create`.
fn reduce_call_binary_eq_record(e: &mut MirScalarExpr) {
    let MirScalarExpr::CallBinary { func, expr1, expr2 } = e else {
        unreachable!()
    };

    match (&*func, &**expr1, &**expr2) {
        (
            BinaryFunc::Eq(_),
            MirScalarExpr::Literal(
                Ok(lit_row),
                ReprColumnType {
                    scalar_type:
                        ReprScalarType::Record {
                            fields: field_types,
                            ..
                        },
                    ..
                },
            ),
            MirScalarExpr::CallVariadic {
                func: VariadicFunc::RecordCreate(..),
                exprs: rec_create_args,
            },
        ) => {
            // Literal([c1, c2]) = record_create(e1, e2)
            //  -->
            // c1 = e1 AND c2 = e2
            //
            // (Records are represented as lists.)
            //
            // `MapFilterProject::literal_constraints` relies on this transform,
            // because `(e1,e2) IN ((1,2))` is desugared using `record_create`.
            if let Datum::List(datum_list) = lit_row.unpack_first() {
                *e = MirScalarExpr::call_variadic(
                    And,
                    datum_list
                        .iter()
                        .zip_eq(field_types)
                        .zip_eq(rec_create_args)
                        .map(|((d, typ), a)| {
                            MirScalarExpr::literal_ok(d, typ.scalar_type.clone())
                                .call_binary(a.clone(), func::Eq)
                        })
                        .collect(),
                );
            }
        }
        (
            BinaryFunc::Eq(_),
            MirScalarExpr::CallVariadic {
                func: VariadicFunc::RecordCreate(..),
                exprs: rec_create_args1,
            },
            MirScalarExpr::CallVariadic {
                func: VariadicFunc::RecordCreate(..),
                exprs: rec_create_args2,
            },
        ) => {
            // record_create(a1, a2, ...) = record_create(b1, b2, ...)
            //  -->
            // a1 = b1 AND a2 = b2 AND ...
            //
            // This is similar to the previous reduction, but this one kicks in also
            // when only some (or none) of the record fields are literals. This
            // enables the discovery of literal constraints for those fields.
            //
            // Note that there is a similar decomposition in
            // `mz_sql::plan::transform_ast::Desugarer`, but that is earlier in the
            // pipeline than the compilation of IN lists to `record_create`.
            *e = MirScalarExpr::call_variadic(
                And,
                rec_create_args1
                    .into_iter()
                    .zip_eq(rec_create_args2)
                    .map(|(a, b)| a.clone().call_binary(b.clone(), func::Eq))
                    .collect(),
            );
        }
        _ => {}
    }
}

/// Specializes a binary date/time function call whose units argument is a
/// literal string. Produces either a unary call with the parsed units baked
/// in, or a literal `UnknownUnits` error.
fn precompile_date_units<F>(e: &mut MirScalarExpr, column_types: &[ReprColumnType], build_unary: F)
where
    F: FnOnce(DateTimeUnits) -> UnaryFunc,
{
    let MirScalarExpr::CallBinary { expr1, expr2, .. } = e else {
        unreachable!()
    };
    let units_str = expr1.as_literal_str().unwrap();
    *e = match units_str.parse::<DateTimeUnits>() {
        Ok(units) => MirScalarExpr::CallUnary {
            func: build_unary(units),
            expr: Box::new(expr2.take()),
        },
        Err(_) => MirScalarExpr::literal(
            Err(EvalError::UnknownUnits(units_str.into())),
            e.typ(column_types).scalar_type,
        ),
    };
}

/// Specializes a binary timezone-applying function call whose timezone
/// argument is a literal. Produces either a unary call with the parsed
/// timezone baked in, or a literal error.
fn precompile_timezone<F>(e: &mut MirScalarExpr, column_types: &[ReprColumnType], build_unary: F)
where
    F: FnOnce(Timezone) -> UnaryFunc,
{
    let MirScalarExpr::CallBinary { expr1, expr2, .. } = e else {
        unreachable!()
    };
    let tz_str = expr1.as_literal_str().unwrap();
    *e = match parse_timezone(tz_str, TimezoneSpec::Posix) {
        Ok(tz) => MirScalarExpr::CallUnary {
            func: build_unary(tz),
            expr: Box::new(expr2.take()),
        },
        Err(err) => MirScalarExpr::literal(Err(err), e.typ(column_types).scalar_type),
    };
}

/// Specializes a `to_char_*` binary call whose format-string argument is a
/// literal, by compiling the format into the unary form.
fn precompile_to_char<F>(e: &mut MirScalarExpr, build_unary: F)
where
    F: FnOnce(String, DateTimeFormat) -> UnaryFunc,
{
    let MirScalarExpr::CallBinary { expr1, expr2, .. } = e else {
        unreachable!()
    };
    let format_str = expr2.as_literal_str().unwrap().to_owned();
    let compiled = DateTimeFormat::compile(&format_str);
    *e = MirScalarExpr::CallUnary {
        func: build_unary(format_str, compiled),
        expr: Box::new(expr1.take()),
    };
}

/// Specializes an `IsLikeMatch{CaseSensitive,CaseInsensitive}` binary call
/// whose pattern argument is a literal, by precompiling the matcher.
fn precompile_is_like(
    e: &mut MirScalarExpr,
    column_types: &[ReprColumnType],
    case_insensitive: bool,
) {
    let MirScalarExpr::CallBinary { expr1, expr2, .. } = e else {
        unreachable!()
    };
    let pattern = expr2.as_literal_str().unwrap();
    *e = match like_pattern::compile(pattern, case_insensitive) {
        Ok(matcher) => expr1
            .take()
            .call_unary(UnaryFunc::IsLikeMatch(func::IsLikeMatch(matcher))),
        Err(err) => MirScalarExpr::literal(Err(err), e.typ(column_types).scalar_type),
    };
}

#[cfg(test)]
mod tests {
    use mz_repr::adt::interval::Interval;
    use mz_repr::{Datum, ReprScalarType};

    use crate::MirScalarExpr;
    use crate::scalar::func;

    #[mz_ore::test]
    fn identity_operand_folds() {
        let int32 = [ReprScalarType::Int.nullable(true)];
        let col = || MirScalarExpr::column(0);
        let lit = |v| MirScalarExpr::literal_ok(Datum::Int(v), ReprScalarType::Int);

        // Identity operands fold to the other operand, on either side for
        // commutative functions and on the right for the rest.
        for mut e in [
            col().call_binary(lit(0), func::AddInt32),
            lit(0).call_binary(col(), func::AddInt32),
            col().call_binary(lit(0), func::SubInt32),
            col().call_binary(lit(1), func::MulInt32),
            lit(1).call_binary(col(), func::MulInt32),
            col().call_binary(lit(1), func::DivInt32),
            col().call_binary(lit(0), func::BitOrInt32),
            col().call_binary(lit(0), func::BitXorInt32),
            col().call_binary(lit(-1), func::BitAndInt32),
            col().call_binary(lit(0), func::BitShiftLeftInt32),
            col().call_binary(lit(0), func::BitShiftRightInt32),
        ] {
            e.reduce(&int32);
            assert_eq!(e, col(), "expected fold to the column");
        }

        // Non-identity literals, and identities on the wrong side of a
        // non-commutative function, do not fold to the operand.
        for mut e in [
            col().call_binary(lit(1), func::AddInt32),
            lit(0).call_binary(col(), func::SubInt32),
            lit(1).call_binary(col(), func::DivInt32),
            col().call_binary(lit(0), func::BitAndInt32),
        ] {
            e.reduce(&int32);
            assert_ne!(e, col(), "expected no fold to the column");
        }
    }

    #[mz_ore::test]
    fn identity_operand_folds_trim_and_interval() {
        let col = || MirScalarExpr::column(0);

        let string = [ReprScalarType::String.nullable(true)];
        let empty = || MirScalarExpr::literal_ok(Datum::String(""), ReprScalarType::String);
        for f in [
            crate::BinaryFunc::Trim(func::Trim),
            crate::BinaryFunc::TrimLeading(func::TrimLeading),
            crate::BinaryFunc::TrimTrailing(func::TrimTrailing),
        ] {
            let mut e = col().call_binary(empty(), f);
            e.reduce(&string);
            assert_eq!(e, col());
        }

        let timestamp = [ReprScalarType::Timestamp {}.nullable(true)];
        let zero = MirScalarExpr::literal_ok(
            Datum::Interval(Interval::default()),
            ReprScalarType::Interval,
        );
        let mut e = col().call_binary(zero, func::AddTimestampInterval);
        e.reduce(&timestamp);
        assert_eq!(e, col());
    }
}
