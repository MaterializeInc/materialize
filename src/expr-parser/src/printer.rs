// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Printing of [`MirScalarExpr`] in the syntax accepted by the parser.
//!
//! Unlike the EXPLAIN printer, which renders function variants by their SQL
//! name (losing the exact variant, e.g. `+` for every add function), this
//! printer renders exact variants by their canonical name. Parsing the output
//! reconstructs the original expression. Test tooling uses this to generate
//! parser inputs.

use mz_expr::{BinaryFunc, MirScalarExpr, UnaryFunc, VariadicFunc, func};
use mz_repr::adt::jsonb::JsonbRef;
use mz_repr::{Datum, ReprColumnType, ReprScalarType, SqlScalarType};

/// Renders a [`MirScalarExpr`] such that [`crate::try_parse_scalar`] parses it
/// back to an equal expression.
///
/// Returns an error for datum types the literal syntax cannot express.
pub fn print_scalar(expr: &MirScalarExpr) -> Result<String, String> {
    use MirScalarExpr::*;
    match expr {
        Column(i, _name) => Ok(format!("#{i}")),
        Literal(Ok(row), typ) => print_literal(row.unpack_first(), typ),
        Literal(Err(err), _typ) => match err {
            mz_expr::EvalError::Internal(msg) => Ok(format!("error(\"internal error: {msg}\")")),
            err => Err(format!("cannot print error literal: {err}")),
        },
        CallUnmaterializable(func) => Ok(format!("{func}()")),
        CallUnary { func, expr } => {
            let expr = print_scalar(expr)?;
            match func {
                UnaryFunc::IsNull(_) => Ok(format!("(({expr}) IS NULL)")),
                UnaryFunc::RecordGet(func::RecordGet(index)) => {
                    Ok(format!("record_get[{index}]({expr})"))
                }
                UnaryFunc::CastInt32ToNumeric(func::CastInt32ToNumeric(Some(scale))) => Ok(
                    format!("cast_int32_to_numeric[{}]({expr})", scale.into_u8()),
                ),
                func => Ok(format!("{}({expr})", func.variant_name())),
            }
        }
        CallBinary { func, expr1, expr2 } => {
            let expr1 = print_scalar(expr1)?;
            let expr2 = print_scalar(expr2)?;
            // Infix operators with a unique variant can render in the more
            // readable EXPLAIN form without losing the exact variant.
            let infix = match func {
                BinaryFunc::Eq(_) => Some("="),
                BinaryFunc::NotEq(_) => Some("!="),
                BinaryFunc::Lt(_) => Some("<"),
                BinaryFunc::Lte(_) => Some("<="),
                BinaryFunc::Gt(_) => Some(">"),
                BinaryFunc::Gte(_) => Some(">="),
                _ => None,
            };
            match infix {
                Some(op) => Ok(format!("({expr1} {op} {expr2})")),
                None => Ok(format!("{}({expr1}, {expr2})", func.variant_name())),
            }
        }
        CallVariadic { func, exprs } => {
            let args = exprs
                .iter()
                .map(print_scalar)
                .collect::<Result<Vec<_>, _>>()?;
            // The parser flattens nested infix AND/OR chains into a single
            // variadic call, so the infix form is only faithful for calls
            // with at least two arguments and no directly nested call of the
            // same function.
            let infix_safe = || {
                exprs.len() >= 2
                    && !exprs
                        .iter()
                        .any(|e| matches!(e, CallVariadic { func: f, .. } if f == func))
            };
            match func {
                VariadicFunc::And(_) if infix_safe() => Ok(format!("({})", args.join(" AND "))),
                VariadicFunc::Or(_) if infix_safe() => Ok(format!("({})", args.join(" OR "))),
                VariadicFunc::RecordCreate(func::variadic::RecordCreate { field_names }) => {
                    let names = field_names
                        .iter()
                        .map(|n| format!("{:?}", n.as_str()))
                        .collect::<Vec<_>>()
                        .join(", ");
                    Ok(format!("record_create[{names}]({})", args.join(", ")))
                }
                VariadicFunc::ListCreate(func::variadic::ListCreate { elem_type }) => {
                    let elem_type = print_sql_scalar_type(elem_type)?;
                    Ok(format!("list_create[{elem_type}]({})", args.join(", ")))
                }
                func => Ok(format!("{}({})", func.variant_name(), args.join(", "))),
            }
        }
        If { cond, then, els } => Ok(format!(
            "case when {} then {} else {} end",
            print_scalar(cond)?,
            print_scalar(then)?,
            print_scalar(els)?
        )),
    }
}

fn print_literal(datum: Datum, typ: &ReprColumnType) -> Result<String, String> {
    let unsupported = |what: &dyn std::fmt::Debug| Err(format!("cannot print literal {what:?}"));
    match datum {
        Datum::Null => Ok(format!(
            "null::{}",
            print_repr_scalar_type(&typ.scalar_type)?
        )),
        Datum::True => Ok("true".to_string()),
        Datum::False => Ok("false".to_string()),
        Datum::Int16(i) => Ok(format!("{i}::smallint")),
        Datum::Int32(i) => Ok(format!("{i}::integer")),
        Datum::Int64(i) => Ok(format!("{i}")),
        Datum::Float64(f) => Ok(format!("{:?}", f.into_inner())),
        Datum::Numeric(n) => Ok(format!("{n}::numeric")),
        Datum::String(s) => Ok(format!("{s:?}")),
        datum if typ.scalar_type == ReprScalarType::Jsonb => Ok(format!(
            "{:?}::jsonb",
            JsonbRef::from_datum(datum).to_string()
        )),
        datum => unsupported(&datum),
    }
}

fn print_repr_scalar_type(typ: &ReprScalarType) -> Result<String, String> {
    let name = match typ {
        ReprScalarType::Bool => "boolean",
        ReprScalarType::Int16 => "smallint",
        ReprScalarType::Int32 => "integer",
        ReprScalarType::Int64 => "bigint",
        ReprScalarType::Float64 => "double precision",
        ReprScalarType::Numeric => "numeric",
        ReprScalarType::String => "text",
        ReprScalarType::Jsonb => "jsonb",
        typ => Err(format!("cannot print scalar type {typ:?}"))?,
    };
    Ok(name.to_string())
}

fn print_sql_scalar_type(typ: &SqlScalarType) -> Result<String, String> {
    print_repr_scalar_type(&ReprScalarType::from(typ))
}
