// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Maintains a catalog of valid casts between [`repr::ScalarType`]s, as well as
//! other cast-related functions.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;

use anyhow::bail;
use lazy_static::lazy_static;

use expr::VariadicFunc;
use repr::{ColumnName, ColumnType, Datum, RelationType, ScalarType};

use super::expr::{BinaryFunc, CoercibleScalarExpr, ColumnRef, ScalarExpr, UnaryFunc};
use super::query::{ExprContext, QueryContext};
use super::scope::Scope;

/// A function that, when invoked, casts the input expression to the
/// target type.
pub struct CastOp(
    Box<
        dyn Fn(&ExprContext, ScalarExpr, &ScalarType) -> Result<ScalarExpr, anyhow::Error>
            + Send
            + Sync,
    >,
);

impl CastOp {
    fn new<F>(f: F) -> CastOp
    where
        F: Fn(&ExprContext, ScalarExpr, &ScalarType) -> Result<ScalarExpr, anyhow::Error>
            + Send
            + Sync
            + 'static,
    {
        CastOp(Box::new(f))
    }
}

impl From<UnaryFunc> for CastOp {
    fn from(u: UnaryFunc) -> CastOp {
        CastOp::new(move |_ecx, expr, _ty| Ok(expr.call_unary(u.clone())))
    }
}

// Cast `e` (`Jsonb`) to `Float64` and then to `cast_to`.
fn from_jsonb_f64_cast(
    ecx: &ExprContext,
    e: ScalarExpr,
    cast_to: &ScalarType,
) -> Result<ScalarExpr, anyhow::Error> {
    let from_f64_to_cast =
        get_direct_cast(CastContext::Explicit, &ScalarType::Float64, &cast_to).unwrap();
    (from_f64_to_cast.0)(ecx, e.call_unary(UnaryFunc::CastJsonbToFloat64), cast_to)
}

/// Describes the context of a cast.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CastContext {
    /// Implicit casts are "no-brainer" casts that apply automatically in
    /// expressions. They are typically lossless, such as `ScalarType::Int32` to
    /// `ScalarType::Int64`.
    Implicit,
    /// Assignment casts are "reasonable" casts that make sense to apply
    /// automatically in `INSERT` statements, but are surprising enough that
    /// they don't apply implicitly in expressions.
    Assignment,
    /// Explicit casts are casts that are possible but may be surprising, like
    /// casting `ScalarType::Json` to `ScalarType::Int32`, and therefore they do
    /// not happen unless explicitly requested by the user with a cast operator.
    Explicit,
}

/// The implementation of a cast.
struct CastImpl {
    ///
    op: CastOp,
    context: CastContext,
}

macro_rules! casts(
    {
        $(
            $from_to:expr => $cast_context:ident: $cast_op:expr
        ),+
    } => {{
        let mut m: HashMap<(ScalarType, ScalarType), CastImpl> = HashMap::new();
        $(
            m.insert($from_to, CastImpl {
                op: CastOp::from($cast_op),
                context: CastContext::$cast_context,
            });
        )+
        m
    }};
);

lazy_static! {
    /// Used when the [`ScalarExpr`] is already of the desired [`ScalarType`].
    static ref NOOP_CAST: CastOp = CastOp::new(|_ecx: &ExprContext, e, _cast_to: &ScalarType| Ok(e));

    static ref VALID_CASTS: HashMap<(ScalarType, ScalarType), CastImpl> = {
        use ScalarType::*;
        use UnaryFunc::*;

        casts! {
            // BOOL
            (Bool, Int32) => Explicit: CastBoolToInt32,
            (Bool, String) => Assignment: CastBoolToString,

            //INT32
            (Int32, Bool) => Explicit: CastInt32ToBool,
            (Int32, Oid) => Implicit: CastInt32ToOid,
            (Int32, Int64) => Implicit: CastInt32ToInt64,
            (Int32, Float32) => Implicit: CastInt32ToFloat32,
            (Int32, Float64) => Implicit: CastInt32ToFloat64,
            (Int32, Decimal(0, 0)) => Implicit: CastOp::new(|_ecx, e, to_type| {
                let (_, s) = to_type.unwrap_decimal_parts();
                Ok(rescale_decimal(e.call_unary(CastInt32ToDecimal), 0, s))
            }),
            (Int32, String) => Assignment: CastInt32ToString,

            // INT64
            (Int64, Bool) => Explicit: CastInt64ToBool,
            (Int64, Int32) => Assignment: CastInt64ToInt32,
            (Int64, Decimal(0, 0)) => Implicit: CastOp::new(|_ecx, e, to_type| {
                let (_, s) = to_type.unwrap_decimal_parts();
                Ok(rescale_decimal(e.call_unary(CastInt64ToDecimal), 0, s))
            }),
            (Int64, Float32) => Implicit: CastInt64ToFloat32,
            (Int64, Float64) => Implicit: CastInt64ToFloat64,
            (Int64, String) => Assignment: CastInt64ToString,

            // OID
            (Oid, Int32) => Assignment: CastOidToInt32,
            (Oid, String) => Explicit: CastInt32ToString,

            // FLOAT32
            (Float32, Int64) => Assignment: CastFloat32ToInt64,
            (Float32, Float64) => Implicit: CastFloat32ToFloat64,
            (Float32, Decimal(0, 0)) => Assignment: CastOp::new(|_ecx, e, to_type| {
                let (_, s) = to_type.unwrap_decimal_parts();
                let s = ScalarExpr::literal(Datum::from(i32::from(s)), to_type.clone());
                Ok(e.call_binary(s, BinaryFunc::CastFloat32ToDecimal))
            }),
            (Float32, String) => Assignment: CastFloat32ToString,

            // FLOAT64
            (Float64, Int32) => Assignment: CastFloat64ToInt32,
            (Float64, Int64) => Assignment: CastFloat64ToInt64,
            (Float64, Decimal(0, 0)) => Assignment: CastOp::new(|_ecx, e, to_type| {
                let (_, s) = to_type.unwrap_decimal_parts();
                let s = ScalarExpr::literal(Datum::from(i32::from(s)), to_type.clone());
                Ok(e.call_binary(s, BinaryFunc::CastFloat64ToDecimal))
            }),
            (Float64, String) => Assignment: CastFloat64ToString,

            // DECIMAL
            (Decimal(0, 0), Int32) => Assignment: CastOp::new(|ecx, e, _to_type| {
                let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                Ok(e.call_unary(CastDecimalToInt32(s)))
            }),
            (Decimal(0, 0), Int64) => Assignment: CastOp::new(|ecx, e, _to_type| {
                let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                Ok(e.call_unary(CastDecimalToInt64(s)))
            }),
            (Decimal(0, 0), Float32) => Implicit: CastOp::new(|ecx, e, _to_type| {
                let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                let factor = 10_f32.powi(i32::from(s));
                let factor =
                    ScalarExpr::literal(Datum::from(factor), Float32);
                Ok(e.call_unary(CastSignificandToFloat32)
                    .call_binary(factor, BinaryFunc::DivFloat32))
            }),
            (Decimal(0, 0), Float64) => Implicit: CastOp::new(|ecx, e, _to_type| {
                let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                let factor = 10_f64.powi(i32::from(s));
                let factor = ScalarExpr::literal(Datum::from(factor), Float32);
                Ok(e.call_unary(CastSignificandToFloat64)
                    .call_binary(factor, BinaryFunc::DivFloat64))
            }),
            (Decimal(0, 0), Decimal(0, 0)) => Implicit: CastOp::new(|ecx, e, to_type| {
                let (_, f) = ecx.scalar_type(&e).unwrap_decimal_parts();
                let (_, t) = to_type.unwrap_decimal_parts();
                Ok(rescale_decimal(e, f, t))
            }),
            (Decimal(0, 0), String) => Assignment: CastOp::new(|ecx, e, _to_type| {
                let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                Ok(e.call_unary(CastDecimalToString(s)))
            }),

            // DATE
            (Date, Timestamp) => Implicit: CastDateToTimestamp,
            (Date, TimestampTz) => Implicit: CastDateToTimestampTz,
            (Date, String) => Assignment: CastDateToString,

            // TIME
            (Time, Interval) => Implicit: CastTimeToInterval,
            (Time, String) => Assignment: CastTimeToString,

            // TIMESTAMP
            (Timestamp, Date) => Assignment: CastTimestampToDate,
            (Timestamp, TimestampTz) => Implicit: CastTimestampToTimestampTz,
            (Timestamp, String) => Assignment: CastTimestampToString,

            // TIMESTAMPTZ
            (TimestampTz, Date) => Assignment: CastTimestampTzToDate,
            (TimestampTz, Timestamp) => Assignment: CastTimestampTzToTimestamp,
            (TimestampTz, String) => Assignment: CastTimestampTzToString,

            // INTERVAL
            (Interval, Time) => Assignment: CastIntervalToTime,
            (Interval, String) => Assignment: CastIntervalToString,

            // BYTES
            (Bytes, String) => Assignment: CastBytesToString,

            // STRING
            (String, Bool) => Explicit: CastStringToBool,
            (String, Int32) => Explicit: CastStringToInt32,
            (String, Int64) => Explicit: CastStringToInt64,
            (String, Oid) => Explicit: CastStringToInt32,
            (String, Float32) => Explicit: CastStringToFloat32,
            (String, Float64) => Explicit: CastStringToFloat64,
            (String, Decimal(0, 0)) => Explicit: CastOp::new(|_ecx, e, to_type| {
                let (_, s) = to_type.unwrap_decimal_parts();
                Ok(e.call_unary(CastStringToDecimal(s)))
            }),
            (String, Date) => Explicit: CastStringToDate,
            (String, Time) => Explicit: CastStringToTime,
            (String, Timestamp) => Explicit: CastStringToTimestamp,
            (String, TimestampTz) => Explicit: CastStringToTimestampTz,
            (String, Interval) => Explicit: CastStringToInterval,
            (String, Bytes) => Explicit: CastStringToBytes,
            (String, Jsonb) => Explicit: CastStringToJsonb,
            (String, Uuid) => Explicit: CastStringToUuid,

            // RECORD
            (Record { fields: vec![] }, String) => Assignment: CastOp::new(|ecx, e, _to_type| {
                let ty = ecx.scalar_type(&e);
                Ok(e.call_unary(CastRecordToString { ty }))
            }),

            // ARRAY
            (Array(Box::new(String)), String) => Assignment: CastOp::new(|ecx, e, _to_type| {
                let ty = ecx.scalar_type(&e);
                Ok(e.call_unary(CastArrayToString { ty }))
            }),

            // LIST
            (List(Box::new(String)), String) => Assignment: CastOp::new(|ecx, e, _to_type| {
                let ty = ecx.scalar_type(&e);
                Ok(e.call_unary(CastListToString { ty }))
            }),

            // JSONB
            (Jsonb, Bool) => Explicit: CastJsonbToBool,
            (Jsonb, Int32) => Explicit: CastOp::new(from_jsonb_f64_cast),
            (Jsonb, Int64) => Explicit: CastOp::new(from_jsonb_f64_cast),
            (Jsonb, Float32) => Explicit: CastOp::new(from_jsonb_f64_cast),
            (Jsonb, Float64) => Explicit: CastJsonbToFloat64,
            (Jsonb, Decimal(0, 0)) => Explicit: CastOp::new(from_jsonb_f64_cast),
            (Jsonb, String) => Assignment: CastJsonbToString,

            // UUID
            (Uuid, String) => Assignment: CastUuidToString
        }
    };
}

/// Get casts directly between two [`ScalarType`]s, with control over the
/// allowed [`CastContext`]. More complex casts, such as between
/// `ScalarType::List`s, are handled elsewhere.
pub fn get_direct_cast(
    ccx: CastContext,
    from: &ScalarType,
    to: &ScalarType,
) -> Option<&'static CastOp> {
    use CastContext::*;

    if from == to {
        return Some(&NOOP_CAST);
    }

    let cast = VALID_CASTS.get(&(from.desaturate(), to.desaturate()))?;

    match (ccx, cast.context) {
        (Explicit, Implicit) | (Explicit, Assignment) | (Explicit, Explicit) => Some(&cast.op),
        (Assignment, Implicit) | (Assignment, Assignment) => Some(&cast.op),
        (Implicit, Implicit) => Some(&cast.op),
        _ => None,
    }
}

pub fn rescale_decimal(expr: ScalarExpr, s1: u8, s2: u8) -> ScalarExpr {
    match s1.cmp(&s2) {
        Ordering::Less => {
            let factor = 10_i128.pow(u32::from(s2 - s1));
            let factor = ScalarExpr::literal(Datum::from(factor), ScalarType::Decimal(38, s2 - s1));
            expr.call_binary(factor, BinaryFunc::MulDecimal)
        }
        Ordering::Equal => expr,
        Ordering::Greater => {
            let factor = 10_i128.pow(u32::from(s1 - s2));
            let factor = ScalarExpr::literal(Datum::from(factor), ScalarType::Decimal(38, s1 - s2));
            expr.call_binary(factor, BinaryFunc::DivDecimal)
        }
    }
}

/// Converts an expression to `ScalarType::String`.
///
/// All types are convertible to string, so this never fails.
pub fn to_string(ecx: &ExprContext, expr: ScalarExpr) -> ScalarExpr {
    plan_cast(
        "to_string",
        ecx,
        CastContext::Explicit,
        expr,
        &ScalarType::String,
    )
    .expect("cast known to exist")
}

/// Converts an expression to `ScalarType::Jsonb`.
///
/// The rules are as follows:
///   * `ScalarType::Boolean`s become JSON booleans.
///   * All numeric types are converted to `Float64`s, then become JSON numbers.
///   * Records are converted to a JSON object where the record's field names
///     are the keys of the object, and the record's fields are recursively
///     converted to JSON by `to_jsonb`.
///   * Other types are converted to strings by their usual cast function an
//      become JSON strings.
///   * A `Datum::Null` of any type becomes a JSON null.
pub fn to_jsonb(ecx: &ExprContext, expr: ScalarExpr) -> ScalarExpr {
    use ScalarType::*;

    match ecx.scalar_type(&expr) {
        Bool => expr.call_unary(UnaryFunc::CastJsonbOrNullToJsonb),
        Int32 | Int64 | Float32 | Float64 | Decimal(..) => {
            plan_cast("to_jsonb", ecx, CastContext::Explicit, expr, &Float64)
                .expect("cast known to exist")
                .call_unary(UnaryFunc::CastJsonbOrNullToJsonb)
        }
        Record { fields } => {
            let mut exprs = vec![];
            for (i, (name, _ty)) in fields.iter().enumerate() {
                exprs.push(ScalarExpr::literal(
                    Datum::String(name.as_str()),
                    ScalarType::String,
                ));
                exprs.push(to_jsonb(
                    ecx,
                    expr.clone().call_unary(UnaryFunc::RecordGet(i)),
                ));
            }
            ScalarExpr::CallVariadic {
                func: VariadicFunc::JsonbBuildObject,
                exprs,
            }
        }
        Jsonb => expr,
        _ => to_string(ecx, expr).call_unary(UnaryFunc::CastJsonbOrNullToJsonb),
    }
}

// Tracks order of preferences for implicit casts for each [`TypeCategory`] that
// contains multiple types, but does so irrespective of [`TypeCategory`].
//
// We could make this deterministic, but it offers no real benefit because the
// information it provides is used in fallible functions anyway, so a bad guess
// just gets caught elsewhere.
fn guess_compatible_cast_type(types: &[ScalarType]) -> Option<&ScalarType> {
    types.iter().max_by_key(|scalar_type| match scalar_type {
        // [`TypeCategory::Numeric`]
        ScalarType::Int32 => 0,
        ScalarType::Int64 => 1,
        ScalarType::Decimal(_, _) => 2,
        ScalarType::Float32 => 3,
        ScalarType::Float64 => 4,
        // [`TypeCategory::DateTime`]
        ScalarType::Date => 5,
        ScalarType::Timestamp => 6,
        ScalarType::TimestampTz => 7,
        _ => 8,
    })
}

/// Guesses the most-common type among a set of [`ScalarType`]s that all members
/// can be cast to. Returns `None` if a common type cannot be deduced.
///
/// The returned type is not guaranteed to be accurate because we ignore type
/// categories, e.g. on input `[ScalarType::Date, ScalarType::Int32]`, will guess
/// that `Date` is the common type.
///
/// However, if there _is_ a common type among the input, it will correctly
/// determine it, i.e. returns false positives but never false negatives.
///
/// The `types` parameter is meant to represent the types inferred from a
/// `Vec<CoercibleScalarExpr>`. If no known types are present in the `types`
/// parameter, it will try to use a provided type hint, instead.
///
/// Note that this function implements the type-determination components of
/// Postgres' ["`UNION`, `CASE`, and Related Constructs"][union-type-conv] type
/// conversion.
///
/// [union-type-conv]:
/// https://www.postgresql.org/docs/12/typeconv-union-case.html
pub fn guess_best_common_type(
    types: &[Option<ScalarType>],
    type_hint: Option<&ScalarType>,
) -> Option<ScalarType> {
    // Remove unknown types.
    let known_types: Vec<_> = types.iter().filter_map(|t| t.as_ref()).cloned().collect();

    if known_types.is_empty() {
        if type_hint.is_some() {
            return type_hint.cloned();
        }
        return Some(ScalarType::String);
    }

    if known_types.iter().all(|t| *t == known_types[0]) {
        return Some(known_types[0].clone());
    }

    // Determine best cast type among known types.
    if let Some(btt) = guess_compatible_cast_type(&known_types) {
        if let ScalarType::Decimal(_, _) = btt {
            // Determine best decimal scale (i.e. largest).
            let mut max_s = 0;
            for t in known_types {
                if let ScalarType::Decimal(_, s) = t {
                    max_s = std::cmp::max(s, max_s);
                }
            }
            return Some(ScalarType::Decimal(38, max_s));
        } else {
            return Some(btt.clone());
        }
    }

    None
}

pub fn plan_coerce<'a>(
    ecx: &'a ExprContext,
    e: CoercibleScalarExpr,
    coerce_to: &ScalarType,
) -> Result<ScalarExpr, anyhow::Error> {
    use CoercibleScalarExpr::*;

    Ok(match e {
        Coerced(e) => e,

        LiteralNull => ScalarExpr::literal_null(coerce_to.clone()),

        LiteralString(s) => {
            let lit = ScalarExpr::literal(Datum::String(&s), ScalarType::String);
            plan_cast("string literal", ecx, CastContext::Explicit, lit, coerce_to)?
        }

        LiteralRecord(exprs) => {
            let arity = exprs.len();
            let coercions = match coerce_to {
                ScalarType::Record { fields, .. } if fields.len() == arity => {
                    fields.iter().map(|(_name, ty)| ty).cloned().collect()
                }
                _ => vec![ScalarType::String; exprs.len()],
            };
            let mut out = vec![];
            for (e, coerce_to) in exprs.into_iter().zip(coercions) {
                out.push(plan_coerce(ecx, e, &coerce_to)?);
            }
            ScalarExpr::CallVariadic {
                func: VariadicFunc::RecordCreate {
                    field_names: (0..arity)
                        .map(|i| ColumnName::from(format!("f{}", i + 1)))
                        .collect(),
                },
                exprs: out,
            }
        }

        Parameter(n) => {
            let prev = ecx.param_types().borrow_mut().insert(n, coerce_to.clone());
            assert!(prev.is_none());
            ScalarExpr::Parameter(n)
        }
    })
}

/// Plan a cast to a [`ScalarType::List`] or [`ScalarType::Map`], using an
/// iterative process to perform the cast to each element in `to`.
pub fn plan_iterative_cast(
    ecx: &ExprContext,
    ccx: CastContext,
    expr: ScalarExpr,
    from: &ScalarType,
    to: &ScalarType,
) -> Result<ScalarExpr, anyhow::Error> {
    use ScalarType::*;

    assert!(matches!(to, List(..) | Map { .. }));

    if from == to {
        return Ok(expr);
    }

    let (from_source, to_component_type) = match (from, to) {
        (List(from_el_typ), List(to_el_typ)) => (*from_el_typ.clone(), *to_el_typ.clone()),
        (String, List(el_typ)) => (String, *el_typ.clone()),
        (String, Map { value_type }) => {
            ecx.require_experimental_mode("maps")?;
            if let ScalarType::Map { .. } = **value_type {
                unsupported!("nested map types");
            }
            (String, *value_type.clone())
        }
        _ => bail!("invalid cast from {} to {}", from, to),
    };

    // Reconstruct an expression context where the expression is evaluated on
    // the "first column" of some imaginary row.
    let mut scx = ecx.qcx.scx.clone();
    scx.param_types = Rc::new(RefCell::new(BTreeMap::new()));
    let qcx = QueryContext::root(&scx, ecx.qcx.lifetime);
    let relation_type = RelationType {
        column_types: vec![ColumnType {
            nullable: true,
            scalar_type: from_source,
        }],
        keys: vec![vec![0]],
    };
    let ecx = ExprContext {
        qcx: &qcx,
        name: "plan_iterative_cast",
        scope: &Scope::empty(None),
        relation_type: &relation_type,
        allow_aggregates: false,
        allow_subqueries: true,
    };

    let col_expr = ScalarExpr::Column(ColumnRef {
        level: 0,
        column: 0,
    });

    // Determine the `ScalarExpr` required to cast our column to the target
    // component type. We'll need to call this on each of the source's values
    // to perform the cast.
    let cast_expr = plan_cast(
        "plan_iterative_cast",
        &ecx,
        ccx,
        col_expr,
        &to_component_type,
    )?;

    let return_ty = to.clone();
    let cast_expr = Box::new(cast_expr.lower_uncorrelated().expect(
        "lower_uncorrelated should not fail given that there is no correlation \
        in the input col_expr",
    ));

    Ok(expr.call_unary(match (from, to) {
        (List(..), List(..)) => UnaryFunc::CastList1ToList2 {
            return_ty,
            cast_expr,
        },
        (String, List(..)) => UnaryFunc::CastStringToList {
            return_ty,
            cast_expr,
        },
        (String, Map { .. }) => UnaryFunc::CastStringToMap {
            return_ty,
            cast_expr,
        },
        _ => unreachable!(
            "already prevented match on incompatible types in \
        plan_iterative_cast"
        ),
    }))
}

/// Plans a cast between [`ScalarType`]s, specifying which types of casts are
/// permitted using [`CastTo`].
///
/// # Errors
///
/// If a cast between the `ScalarExpr`'s base type and the specified type is:
/// - Not possible, e.g. `Bytes` to `Decimal`
/// - Not permitted, e.g. implicitly casting from `Float64` to `Float32`.
/// - Not implemented yet
pub fn plan_cast<'a, D>(
    caller_name: D,
    ecx: &ExprContext<'a>,
    ccx: CastContext,
    expr: ScalarExpr,
    cast_to: &ScalarType,
) -> Result<ScalarExpr, anyhow::Error>
where
    D: fmt::Display,
{
    let from_typ = ecx.scalar_type(&expr);
    let cast_bail = || {
        bail!(
            "{} does not support {}casting from {} to {}",
            caller_name,
            if ccx == CastContext::Implicit {
                "implicitly "
            } else {
                ""
            },
            from_typ,
            &cast_to,
        )
    };

    match cast_to {
        ScalarType::List(..) | ScalarType::Map { .. } => {
            match plan_iterative_cast(ecx, ccx, expr, &from_typ, &cast_to) {
                Ok(e) => Ok(e),
                Err(_) => cast_bail(),
            }
        }
        _ => {
            let cast_op = match get_direct_cast(ccx, &from_typ, cast_to) {
                Some(cast_op) => cast_op,
                None => return cast_bail(),
            };
            (cast_op.0)(ecx, expr, cast_to)
        }
    }
}
