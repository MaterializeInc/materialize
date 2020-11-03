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

/// Describes methods of planning a conversion between [`ScalarType`]s, which
/// can be invoked with [`CastOp::gen_expr`].
pub enum CastOp {
    U(UnaryFunc),
    F(fn(&ExprContext, ScalarExpr, CastTo) -> Result<ScalarExpr, anyhow::Error>),
}

impl fmt::Debug for CastOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CastOp::U(u) => write!(f, "CastOp::U({:?})", u),
            CastOp::F(_) => write!(f, "CastOp::F"),
        }
    }
}

// Provides a shorthand for writing `CastOp::U`.
impl From<UnaryFunc> for CastOp {
    fn from(u: UnaryFunc) -> CastOp {
        CastOp::U(u)
    }
}

impl CastOp {
    /// Generates the [`ScalarExpr`] to cast between [`ScalarType`]s.
    pub fn gen_expr(
        &self,
        ecx: &ExprContext,
        e: ScalarExpr,
        cast_to: CastTo,
    ) -> Result<ScalarExpr, anyhow::Error> {
        match self {
            CastOp::U(u) => Ok(e.call_unary(u.clone())),
            CastOp::F(f) => f(ecx, e, cast_to),
        }
    }
}

// Used when the [`ScalarExpr`] is already of the desired [`ScalarType`].
fn noop_cast(_: &ExprContext, e: ScalarExpr, _: CastTo) -> Result<ScalarExpr, anyhow::Error> {
    Ok(e)
}

// Cast `e` (`Jsonb`) to `Float64` and then to `cast_to`.
fn from_jsonb_f64_cast(
    ecx: &ExprContext,
    e: ScalarExpr,
    cast_to: CastTo,
) -> Result<ScalarExpr, anyhow::Error> {
    let from_f64_to_cast = get_cast(&ScalarType::Float64, &cast_to).unwrap();
    from_f64_to_cast.gen_expr(ecx, e.call_unary(UnaryFunc::CastJsonbToFloat64), cast_to)
}

/// Describes the context of a cast.
#[allow(dead_code)]
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

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
/// Describes the target type of a cast and the context for the cast.
///
/// NOTE(benesch): we may want to refactor this to have one
/// `CastTo::Plain(CastContext)` variant, rather than duplicating each of the
/// `CastContext` variants. That's a larger refactor than I'm willing to take on
/// right now, though.
pub enum CastTo {
    /// Only allow implicit casts.
    Implicit(ScalarType),
    /// Allow assignment or implicit casts.
    Assignment(ScalarType),
    /// Allow explicit, assignment, or implicit casts.
    Explicit(ScalarType),
}

impl fmt::Display for CastTo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CastTo::Implicit(t) | CastTo::Assignment(t) | CastTo::Explicit(t) => write!(f, "{}", t),
        }
    }
}

impl CastTo {
    pub fn scalar_type(&self) -> ScalarType {
        match self {
            CastTo::Implicit(t) | CastTo::Assignment(t) | CastTo::Explicit(t) => t.clone(),
        }
    }
}

macro_rules! casts(
    {
        $(
            $from_castto:expr => $castop:expr
        ),+
    } => {{
        let mut m: HashMap<(ScalarType, CastTo), CastOp> = HashMap::new();
        $(
            m.insert($from_castto, $castop.into());
        )+
        m
    }};
);

lazy_static! {
    static ref VALID_CASTS: HashMap<(ScalarType, CastTo), CastOp> = {
        use CastTo::*;
        use ScalarType::*;
        use UnaryFunc::*;

        casts! {
            // BOOL
            (Bool, Explicit(Int32)) => CastBoolToInt32,
            (Bool, Assignment(String)) => CastBoolToString,

            //INT32
            (Int32, Explicit(Bool)) => CastInt32ToBool,
            (Int32, Implicit(Oid)) => CastInt32ToOid,
            (Int32, Implicit(Int64)) => CastInt32ToInt64,
            (Int32, Implicit(Float32)) => CastInt32ToFloat32,
            (Int32, Implicit(Float64)) => CastInt32ToFloat64,
            (Int32, Implicit(Decimal(0, 0))) => CastOp::F(|_ecx, e, to_type| {
                let (_, s) = to_type.scalar_type().unwrap_decimal_parts();
                Ok(rescale_decimal(e.call_unary(CastInt32ToDecimal), 0, s))
            }),
            (Int32, Assignment(String)) => CastInt32ToString,

            // INT64
            (Int64, Explicit(Bool)) => CastInt64ToBool,
            (Int64, Assignment(Int32)) => CastInt64ToInt32,
            (Int64, Implicit(Decimal(0, 0))) => CastOp::F(|_ecx, e, to_type| {
                let (_, s) = to_type.scalar_type().unwrap_decimal_parts();
                Ok(rescale_decimal(e.call_unary(CastInt64ToDecimal), 0, s))
            }),
            (Int64, Implicit(Float32)) => CastInt64ToFloat32,
            (Int64, Implicit(Float64)) => CastInt64ToFloat64,
            (Int64, Assignment(String)) => CastInt64ToString,

            // OID
            (Oid, Assignment(Int32)) => CastOidToInt32,
            (Oid, Explicit(String)) => CastInt32ToString,

            // FLOAT32
            (Float32, Assignment(Int64)) => CastFloat32ToInt64,
            (Float32, Implicit(Float64)) => CastFloat32ToFloat64,
            (Float32, Assignment(Decimal(0, 0))) => CastOp::F(|_ecx, e, to_type| {
                let (_, s) = to_type.scalar_type().unwrap_decimal_parts();
                let s = ScalarExpr::literal(
                    Datum::from(i32::from(s)), to_type.scalar_type()
                );
                Ok(e.call_binary(s, BinaryFunc::CastFloat32ToDecimal))
            }),
            (Float32, Assignment(String)) => CastFloat32ToString,

            // FLOAT64
            (Float64, Assignment(Int32)) => CastFloat64ToInt32,
            (Float64, Assignment(Int64)) => CastFloat64ToInt64,
            (Float64, Assignment(Decimal(0, 0))) => CastOp::F(|_ecx, e, to_type| {
                let (_, s) = to_type.scalar_type().unwrap_decimal_parts();
                let s = ScalarExpr::literal(Datum::from(
                    i32::from(s)), to_type.scalar_type());
                Ok(e.call_binary(s, BinaryFunc::CastFloat64ToDecimal))
            }),
            (Float64, Assignment(String)) => CastFloat64ToString,

            // DECIMAL
            (Decimal(0, 0), Assignment(Int32)) => CastOp::F(|ecx, e, _to_type| {
                let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                Ok(e.call_unary(CastDecimalToInt32(s)))
            }),
            (Decimal(0, 0), Assignment(Int64)) => CastOp::F(|ecx, e, _to_type| {
                let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                Ok(e.call_unary(CastDecimalToInt64(s)))
            }),
            (Decimal(0, 0), Implicit(Float32)) => CastOp::F(|ecx, e, _to_type| {
                let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                let factor = 10_f32.powi(i32::from(s));
                let factor =
                    ScalarExpr::literal(Datum::from(factor), Float32);
                Ok(e.call_unary(CastSignificandToFloat32)
                    .call_binary(factor, BinaryFunc::DivFloat32))
            }),
            (Decimal(0, 0), Implicit(Float64)) => CastOp::F(|ecx, e, _to_type| {
                let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                let factor = 10_f64.powi(i32::from(s));
                let factor =
                    ScalarExpr::literal(Datum::from(factor), Float32);
                Ok(e.call_unary(CastSignificandToFloat64)
                    .call_binary(factor, BinaryFunc::DivFloat64))
            }),
            (Decimal(0, 0), Implicit(Decimal(0, 0))) => CastOp::F(|ecx, e, to_type| {
                let (_, f) = ecx.scalar_type(&e).unwrap_decimal_parts();
                let (_, t) = to_type.scalar_type().unwrap_decimal_parts();
                Ok(rescale_decimal(e, f, t))
            }),
            (Decimal(0, 0), Assignment(String)) => CastOp::F(|ecx, e, _to_type| {
                let (_, s) = ecx.scalar_type(&e).unwrap_decimal_parts();
                Ok(e.call_unary(CastDecimalToString(s)))
            }),

            // DATE
            (Date, Implicit(Timestamp)) => CastDateToTimestamp,
            (Date, Implicit(TimestampTz)) => CastDateToTimestampTz,
            (Date, Assignment(String)) => CastDateToString,

            // TIME
            (Time, Implicit(Interval)) => CastTimeToInterval,
            (Time, Assignment(String)) => CastTimeToString,

            // TIMESTAMP
            (Timestamp, Assignment(Date)) => CastTimestampToDate,
            (Timestamp, Implicit(TimestampTz)) => CastTimestampToTimestampTz,
            (Timestamp, Assignment(String)) => CastTimestampToString,

            // TIMESTAMPTZ
            (TimestampTz, Assignment(Date)) => CastTimestampTzToDate,
            (TimestampTz, Assignment(Timestamp)) => CastTimestampTzToTimestamp,
            (TimestampTz, Assignment(String)) => CastTimestampTzToString,

            // INTERVAL
            (Interval, Assignment(Time)) => CastIntervalToTime,
            (Interval, Assignment(String)) => CastIntervalToString,

            // BYTES
            (Bytes, Assignment(String)) => CastBytesToString,

            // STRING
            (String, Explicit(Bool)) => CastStringToBool,
            (String, Explicit(Int32)) => CastStringToInt32,
            (String, Explicit(Int64)) => CastStringToInt64,
            (String, Explicit(Oid)) => CastStringToInt32,
            (String, Explicit(Float32)) => CastStringToFloat32,
            (String, Explicit(Float64)) => CastStringToFloat64,
            (String, Explicit(Decimal(0,0))) => CastOp::F(|_ecx, e, to_type| {
                let (_, s) = to_type.scalar_type().unwrap_decimal_parts();
                Ok(e.call_unary(CastStringToDecimal(s)))
            }),
            (String, Explicit(Date)) => CastStringToDate,
            (String, Explicit(Time)) => CastStringToTime,
            (String, Explicit(Timestamp)) => CastStringToTimestamp,
            (String, Explicit(TimestampTz)) => CastStringToTimestampTz,
            (String, Explicit(Interval)) => CastStringToInterval,
            (String, Explicit(Bytes)) => CastStringToBytes,
            (String, Explicit(Jsonb)) => CastStringToJsonb,
            (String, Explicit(Uuid)) => CastStringToUuid,

            // RECORD
            (Record { fields: vec![] }, Assignment(String)) => CastOp::F(|ecx, e, _to_type| {
                let ty = ecx.scalar_type(&e);
                Ok(e.call_unary(CastRecordToString { ty }))
            }),

            // ARRAY
            (Array(Box::new(String)), Assignment(String)) => CastOp::F(|ecx, e, _to_type| {
                let ty = ecx.scalar_type(&e);
                Ok(e.call_unary(CastArrayToString { ty }))
            }),

            // LIST
            (List(Box::new(String)), Assignment(String)) => CastOp::F(|ecx, e, _to_type| {
                let ty = ecx.scalar_type(&e);
                Ok(e.call_unary(CastListToString { ty }))
            }),

            // JSONB
            (Jsonb, Explicit(Bool)) => CastJsonbToBool,
            (Jsonb, Explicit(Int32)) => CastOp::F(from_jsonb_f64_cast),
            (Jsonb, Explicit(Int64)) => CastOp::F(from_jsonb_f64_cast),
            (Jsonb, Explicit(Float32)) => CastOp::F(from_jsonb_f64_cast),
            (Jsonb, Explicit(Float64)) => CastJsonbToFloat64,
            (Jsonb, Explicit(Decimal(0, 0))) => CastOp::F(from_jsonb_f64_cast),
            (Jsonb, Assignment(String)) => CastJsonbToString,

            // UUID
            (Uuid, Assignment(String)) => CastUuidToString
        }
    };
}

/// Get a cast, if one exists, from a [`ScalarType`] to another, with control
/// over allowing implicit or explicit casts using [`CastTo`]. For casts between
/// `ScalarType::List`s, see [`plan_cast_between_lists`].
///
/// Use the returned [`CastOp`] with [`CastOp::gen_expr`].
pub fn get_cast<'a>(from: &ScalarType, cast_to: &CastTo) -> Option<&'a CastOp> {
    use CastTo::*;

    if *from == cast_to.scalar_type() {
        return Some(&CastOp::F(noop_cast));
    }

    let cast_to = match cast_to {
        Implicit(t) => Implicit(t.desaturate()),
        Assignment(t) => Assignment(t.desaturate()),
        Explicit(t) => Explicit(t.desaturate()),
    };

    let mut cast = VALID_CASTS.get(&(from.desaturate(), cast_to.clone()));

    // If no explicit implementation, look for an assignment one.
    if let (None, CastTo::Explicit(t)) = (cast, cast_to.clone()) {
        cast = VALID_CASTS.get(&(from.desaturate(), CastTo::Assignment(t)));
    }

    // If no assignment implementation, look for an implicit one.
    if let (None, CastTo::Explicit(t)) | (None, CastTo::Assignment(t)) = (cast, cast_to) {
        cast = VALID_CASTS.get(&(from.desaturate(), CastTo::Implicit(t)));
    }

    cast
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
    plan_cast("to_string", ecx, expr, CastTo::Explicit(ScalarType::String))
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
            plan_cast("to_jsonb", ecx, expr, CastTo::Explicit(Float64))
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
    type_hint: Option<ScalarType>,
) -> Option<ScalarType> {
    // Remove unknown types.
    let known_types: Vec<_> = types.iter().filter_map(|t| t.as_ref()).cloned().collect();

    if known_types.is_empty() {
        if type_hint.is_some() {
            return type_hint;
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
    coerce_to: ScalarType,
) -> Result<ScalarExpr, anyhow::Error> {
    use CoercibleScalarExpr::*;

    Ok(match e {
        Coerced(e) => e,

        LiteralNull => ScalarExpr::literal_null(coerce_to),

        LiteralString(s) => {
            let lit = ScalarExpr::literal(Datum::String(&s), ScalarType::String);
            plan_cast("string literal", ecx, lit, CastTo::Explicit(coerce_to))?
        }

        LiteralRecord(exprs) => {
            let arity = exprs.len();
            let coercions = match coerce_to {
                ScalarType::Record { fields, .. } if fields.len() == arity => {
                    fields.into_iter().map(|(_name, ty)| ty).collect()
                }
                _ => vec![ScalarType::String; exprs.len()],
            };
            let mut out = vec![];
            for (e, coerce_to) in exprs.into_iter().zip(coercions) {
                out.push(plan_coerce(ecx, e, coerce_to)?);
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
            let prev = ecx.param_types().borrow_mut().insert(n, coerce_to);
            assert!(prev.is_none());
            ScalarExpr::Parameter(n)
        }
    })
}

/// Plans a cast between from a [`ScalarType::List`]s to another.
pub fn plan_cast_between_lists<'a, D>(
    caller_name: D,
    ecx: &ExprContext<'a>,
    expr: ScalarExpr,
    from: &ScalarType,
    cast_to: CastTo,
) -> Result<ScalarExpr, anyhow::Error>
where
    D: fmt::Display,
{
    let from_element_typ = from.unwrap_list_element_type();

    let cast_to_element_typ = match cast_to {
        CastTo::Implicit(ScalarType::List(ref elem_typ)) => CastTo::Implicit((**elem_typ).clone()),
        CastTo::Explicit(ScalarType::List(ref elem_typ)) => CastTo::Explicit((**elem_typ).clone()),
        _ => panic!("get_cast_between_lists requires cast_to to be a list"),
    };

    if *from_element_typ == cast_to_element_typ.scalar_type() {
        return CastOp::F(noop_cast).gen_expr(ecx, expr, cast_to);
    }

    // Reconstruct an expression context where the expression is evaluated on
    // the "first column" of some imaginary row.
    let mut scx = ecx.qcx.scx.clone();
    scx.param_types = Rc::new(RefCell::new(BTreeMap::new()));
    let qcx = QueryContext::root(&scx, ecx.qcx.lifetime);
    let relation_type = RelationType {
        column_types: vec![ColumnType {
            nullable: true,
            scalar_type: from_element_typ.clone(),
        }],
        keys: vec![vec![0]],
    };
    let ecx = ExprContext {
        qcx: &qcx,
        name: "plan_cast_between_lists",
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
    // element type. We'll need to call this on each element of the original
    // list to perform the cast.
    let cast_expr = plan_cast(caller_name, &ecx, col_expr, cast_to_element_typ)?;

    Ok(expr.call_unary(UnaryFunc::CastList1ToList2 {
        return_ty: cast_to.scalar_type(),
        cast_expr: Box::new(cast_expr
            .lower_uncorrelated()
            .expect("lower_uncorrelated should not fail given that there is no correlation in the input col_expr")
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
    expr: ScalarExpr,
    cast_to: CastTo,
) -> Result<ScalarExpr, anyhow::Error>
where
    D: fmt::Display,
{
    match (ecx.scalar_type(&expr), cast_to.scalar_type()) {
        (ScalarType::List(t), ScalarType::List(_)) => {
            plan_cast_between_lists(caller_name, ecx, expr, &ScalarType::List(t), cast_to)
        }
        (from_typ, _) => {
            let cast_op = match get_cast(&from_typ, &cast_to) {
                Some(cast_op) => cast_op,
                None => bail!(
                    "{} does not support {}casting from {} to {}",
                    caller_name,
                    if let CastTo::Implicit(_) = cast_to {
                        "implicitly "
                    } else {
                        ""
                    },
                    from_typ,
                    cast_to
                ),
            };
            cast_op.gen_expr(ecx, expr, cast_to)
        }
    }
}
