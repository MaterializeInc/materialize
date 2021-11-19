// Copyright Materialize, Inc. and contributors. All rights reserved.
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
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;

use anyhow::bail;
use lazy_static::lazy_static;

use expr::func;
use expr::VariadicFunc;
use repr::{ColumnName, ColumnType, Datum, RelationType, ScalarBaseType, ScalarType};

use super::expr::{CoercibleScalarExpr, ColumnRef, HirScalarExpr, UnaryFunc};
use super::query::{ExprContext, QueryContext};
use super::scope::Scope;

/// Like func::sql_impl_func, but for casts.
fn sql_impl_cast(expr: &'static str) -> CastTemplate {
    let invoke = crate::func::sql_impl(expr);
    CastTemplate::new(move |ecx, _ccx, from_type, _to_type| {
        let mut out = invoke(&ecx.qcx, vec![from_type.clone()]).ok()?;
        Some(move |e| {
            out.splice_parameters(&[e], 0);
            out
        })
    })
}

/// A cast is a function that takes a `ScalarExpr` to another `ScalarExpr`.
type Cast = Box<dyn FnOnce(HirScalarExpr) -> HirScalarExpr>;

/// A cast template is a function that produces a `Cast` given a concrete input
/// and output type. A template can return `None` to indicate that it is
/// incapable of producing a cast for the specified types.
///
/// Cast templates are used to share code for similar casts, where the input or
/// output type is of one "category" of type. For example, a single cast
/// template handles converting from strings to any list type. Without cast
/// templates, we'd have to enumerate every possible list -> list conversion,
/// which is impractical.
struct CastTemplate(
    Box<dyn Fn(&ExprContext, CastContext, &ScalarType, &ScalarType) -> Option<Cast> + Send + Sync>,
);

impl CastTemplate {
    fn new<T, C>(t: T) -> CastTemplate
    where
        T: Fn(&ExprContext, CastContext, &ScalarType, &ScalarType) -> Option<C>
            + Send
            + Sync
            + 'static,
        C: FnOnce(HirScalarExpr) -> HirScalarExpr + 'static,
    {
        CastTemplate(Box::new(move |ecx, ccx, from_ty, to_ty| {
            t(ecx, ccx, from_ty, to_ty).map(|o| Box::new(o) as Cast)
        }))
    }
}

impl From<UnaryFunc> for CastTemplate {
    fn from(u: UnaryFunc) -> CastTemplate {
        CastTemplate::new(move |_ecx, _ccx, _from, _to| {
            let u = u.clone();
            Some(move |expr: HirScalarExpr| expr.call_unary(u))
        })
    }
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
    template: CastTemplate,
    context: CastContext,
}

macro_rules! casts(
    {
        $(
            $from_to:expr => $cast_context:ident: $cast_template:expr
        ),+
    } => {{
        let mut m = HashMap::new();
        $(
            m.insert($from_to, CastImpl {
                template: $cast_template.into(),
                context: CastContext::$cast_context,
            });
        )+
        m
    }};
);

lazy_static! {
    static ref VALID_CASTS: HashMap<(ScalarBaseType, ScalarBaseType), CastImpl> = {
        use ScalarBaseType::*;
        use UnaryFunc::*;

        casts! {
            // BOOL
            (Bool, Int32) => Explicit: CastBoolToInt32,
            (Bool, String) => Assignment: CastBoolToString,

            //INT16
            (Int16, Int32) => Implicit: CastInt16ToInt32,
            (Int16, Int64) => Implicit: CastInt16ToInt64,
            (Int16, Float32) => Implicit: CastInt16ToFloat32,
            (Int16, Float64) => Implicit: CastInt16ToFloat64,
            (Int16, Numeric) => Implicit: CastTemplate::new(|_ecx, _ccx, _from_type, to_type| {
                let s = to_type.unwrap_numeric_scale();
                Some(move |e: HirScalarExpr| e.call_unary(CastInt16ToNumeric(s)))
            }),
            (Int16, String) => Assignment: CastInt16ToString,

            //INT32
            (Int32, Bool) => Explicit: CastInt32ToBool,
            (Int32, Oid) => Implicit: CastInt32ToOid,
            (Int32, RegProc) => Implicit: CastInt32ToRegProc,
            (Int32, RegType) => Implicit: CastInt32ToRegType,
            (Int32, Int16) => Assignment: CastInt32ToInt16,
            (Int32, Int64) => Implicit: CastInt32ToInt64,
            (Int32, Float32) => Implicit: CastInt32ToFloat32,
            (Int32, Float64) => Implicit: CastInt32ToFloat64,
            (Int32, Numeric) => Implicit: CastTemplate::new(|_ecx, _ccx, _from_type, to_type| {
                let s = to_type.unwrap_numeric_scale();
                Some(move |e: HirScalarExpr| e.call_unary(CastInt32ToNumeric(s)))
            }),
            (Int32, String) => Assignment: CastInt32ToString,

            // INT64
            (Int64, Bool) => Explicit: CastInt64ToBool,
            (Int64, Int16) => Assignment: CastInt64ToInt16,
            (Int64, Int32) => Assignment: CastInt64ToInt32,
            (Int64, Numeric) => Implicit: CastTemplate::new(|_ecx, _ccx, _from_type, to_type| {
                let s = to_type.unwrap_numeric_scale();
                Some(move |e: HirScalarExpr| e.call_unary(CastInt64ToNumeric(s)))
            }),
            (Int64, Float32) => Implicit: CastInt64ToFloat32,
            (Int64, Float64) => Implicit: CastInt64ToFloat64,
            (Int64, String) => Assignment: CastInt64ToString,

            // OID
            (Oid, Int32) => Assignment: CastOidToInt32,
            (Oid, String) => Explicit: CastInt32ToString,
            (Oid, RegProc) => Assignment: CastOidToRegProc,
            (Oid, RegType) => Assignment: CastOidToRegType,

            // REGPROC
            (RegProc,Oid) => Implicit: CastRegProcToOid,

            // REGTYPE
            (RegType,Oid) => Implicit: CastRegTypeToOid,

            // FLOAT32
            (Float32, Int16) => Assignment: CastFloat32ToInt16(func::CastFloat32ToInt16),
            (Float32, Int32) => Assignment: CastFloat32ToInt32(func::CastFloat32ToInt32),
            (Float32, Int64) => Assignment: CastFloat32ToInt64(func::CastFloat32ToInt64),
            (Float32, Float64) => Implicit: CastFloat32ToFloat64(func::CastFloat32ToFloat64),
            (Float32, Numeric) => Assignment: CastTemplate::new(|_ecx, _ccx, _from_type, to_type| {
                let s = to_type.unwrap_numeric_scale();
                Some(move |e: HirScalarExpr| e.call_unary(CastFloat32ToNumeric(s)))
            }),
            (Float32, String) => Assignment: CastFloat32ToString(func::CastFloat32ToString),

            // FLOAT64
            (Float64, Int16) => Assignment: CastFloat64ToInt16(func::CastFloat64ToInt16),
            (Float64, Int32) => Assignment: CastFloat64ToInt32(func::CastFloat64ToInt32),
            (Float64, Int64) => Assignment: CastFloat64ToInt64(func::CastFloat64ToInt64),
            (Float64, Float32) => Assignment: CastFloat64ToFloat32(func::CastFloat64ToFloat32),
            (Float64, Numeric) => Assignment: CastTemplate::new(|_ecx, _ccx, _from_type, to_type| {
                let s = to_type.unwrap_numeric_scale();
                Some(move |e: HirScalarExpr| e.call_unary(CastFloat64ToNumeric(s)))
            }),
            (Float64, String) => Assignment: CastFloat64ToString(func::CastFloat64ToString),

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
            (String, Int16) => Explicit: CastStringToInt16,
            (String, Int32) => Explicit: CastStringToInt32,
            (String, Int64) => Explicit: CastStringToInt64,
            (String, Oid) => Explicit: CastStringToInt32,
            // A regproc represents a function by oid. Converting from string to regproc
            // does a lookup of the function name and expects exactly one function to
            // match it. You can also specify (in postgres) a string that's a valid
            // int4 and it'll happily cast it (without verifying that the int4 matches
            // a function oid). To support this, use a SQL expression that checks if
            // the input might be a valid int4 with a regex, otherwise try to lookup in
            // mz_functions. CASE will return NULL if the subquery returns zero results,
            // so use mz_error_if_null to coerce that into an error. This is hacky and
            // incomplete in a few ways, but gets us close enough to making drivers happy.
            // TODO: Support the correct error code for does not exist (42883).
            (String, RegProc) => Explicit: sql_impl_cast("(
                SELECT
                    CASE
                    WHEN $1 IS NULL THEN NULL
                    WHEN $1 ~ '^\\d+$' THEN $1::pg_catalog.oid::pg_catalog.regproc
                    ELSE (
                        mz_internal.mz_error_if_null(
                            (SELECT oid::pg_catalog.regproc FROM mz_catalog.mz_functions WHERE name = $1),
                            'function \"' || $1 || '\" does not exist'
                        )
                    )
                    END
            )"),
            // A regtype represents a type by oid. Converting from string to regtype
            // does a lookup of the function name and expects exactly one type to
            // match it. You can also specify (in postgres) a string that's a valid
            // int4 and it'll happily cast it (without verifying that the int4 matches
            // a type oid). To support this, use a SQL expression that checks if
            // the input might be a valid int4 with a regex, otherwise try to lookup in
            // mz_types. CASE will return NULL if the subquery returns zero results,
            // so use mz_error_if_null to coerce that into an error. This is hacky and
            // incomplete in a few ways, but gets us close enough to making drivers happy.
            // TODO: Support the correct error code for does not exist (42883).
            (String, RegType) => Explicit: sql_impl_cast("(
                SELECT
                    CASE
                    WHEN $1 IS NULL THEN NULL
                    WHEN $1 ~ '^\\d+$' THEN $1::pg_catalog.oid::pg_catalog.regtype
                    ELSE (
                        mz_internal.mz_error_if_null(
                            (SELECT oid::pg_catalog.regtype FROM mz_catalog.mz_types WHERE name = $1),
                            'type \"' || $1 || '\" does not exist'
                        )
                    )
                    END
            )"),

            (String, Float32) => Explicit: CastStringToFloat32,
            (String, Float64) => Explicit: CastStringToFloat64,
            (String, Numeric) => Explicit: CastTemplate::new(|_ecx, _ccx, _from_type, to_type| {
                let s = to_type.unwrap_numeric_scale();
                Some(move |e: HirScalarExpr| e.call_unary(CastStringToNumeric(s)))
            }),
            (String, Date) => Explicit: CastStringToDate,
            (String, Time) => Explicit: CastStringToTime,
            (String, Timestamp) => Explicit: CastStringToTimestamp,
            (String, TimestampTz) => Explicit: CastStringToTimestampTz,
            (String, Interval) => Explicit: CastStringToInterval,
            (String, Bytes) => Explicit: CastStringToBytes,
            (String, Jsonb) => Explicit: CastStringToJsonb,
            (String, Uuid) => Explicit: CastStringToUuid,
            (String, Array) => Explicit: CastTemplate::new(|ecx, ccx, from_type, to_type| {
                let return_ty = to_type.clone();
                let to_el_type = to_type.unwrap_array_element_type();
                let cast_expr = plan_hypothetical_cast(ecx, ccx, from_type, to_el_type)?;
                Some(|e: HirScalarExpr| e.call_unary(UnaryFunc::CastStringToArray {
                    return_ty,
                    cast_expr: Box::new(cast_expr),
                }))
            }),
            (String, List) => Explicit: CastTemplate::new(|ecx, ccx, from_type, to_type| {
                let return_ty = to_type.clone();
                let to_el_type = to_type.unwrap_list_element_type();
                let cast_expr = plan_hypothetical_cast(ecx, ccx, from_type, to_el_type)?;
                Some(|e: HirScalarExpr| e.call_unary(UnaryFunc::CastStringToList {
                    return_ty,
                    cast_expr: Box::new(cast_expr),
                }))
            }),
            (String, Map) => Explicit: CastTemplate::new(|ecx, ccx, from_type, to_type| {
                let return_ty = to_type.clone();
                let to_val_type = to_type.unwrap_map_value_type();
                let cast_expr = plan_hypothetical_cast(ecx, ccx, from_type, to_val_type)?;
                Some(|e: HirScalarExpr| e.call_unary(UnaryFunc::CastStringToMap {
                    return_ty,
                    cast_expr: Box::new(cast_expr),
                }))
            }),
            (String, Char) => Assignment: CastTemplate::new(|_ecx, ccx, _from_type, to_type| {
                let length = to_type.unwrap_char_varchar_length();
                Some(move |e: HirScalarExpr| e.call_unary(CastStringToChar {length, fail_on_len: ccx == CastContext::Assignment}))
            }),
            (String, VarChar) => Assignment: CastTemplate::new(|_ecx, ccx, _from_type, to_type| {
                let length = to_type.unwrap_char_varchar_length();
                Some(move |e: HirScalarExpr| e.call_unary(CastStringToVarChar {length, fail_on_len: ccx == CastContext::Assignment}))
            }),

            // CHAR
            (Char, String) => Implicit: CastCharToString,
            (Char, Char) => Implicit: CastTemplate::new(|_ecx, ccx, _from_type, to_type| {
                let length = to_type.unwrap_char_varchar_length();
                Some(move |e: HirScalarExpr| e.call_unary(CastStringToChar {length, fail_on_len: ccx == CastContext::Assignment}))
            }),
            (Char, VarChar) => Assignment: CastTemplate::new(|_ecx, ccx, _from_type, to_type| {
                let length = to_type.unwrap_char_varchar_length();
                Some(move |e: HirScalarExpr| e.call_unary(CastStringToVarChar {length, fail_on_len: ccx == CastContext::Assignment}))
            }),

            // VARCHAR
            (VarChar, String) => Implicit: CastVarCharToString,
            (VarChar, Char) => Implicit: CastTemplate::new(|_ecx, ccx, _from_type, to_type| {
                let length = to_type.unwrap_char_varchar_length();
                Some(move |e: HirScalarExpr| e.call_unary(CastStringToChar {length, fail_on_len: ccx == CastContext::Assignment}))
            }),
            (VarChar, VarChar) => Implicit: CastTemplate::new(|_ecx, ccx, _from_type, to_type| {
                let length = to_type.unwrap_char_varchar_length();
                Some(move |e: HirScalarExpr| e.call_unary(CastStringToVarChar {length, fail_on_len: ccx == CastContext::Assignment}))
            }),

            // RECORD
            (Record, String) => Assignment: CastTemplate::new(|_ecx, _ccx, from_type, _to_type| {
                let ty = from_type.clone();
                Some(|e: HirScalarExpr| e.call_unary(CastRecordToString { ty }))
            }),

            // ARRAY
            (Array, String) => Assignment: CastTemplate::new(|_ecx, _ccx, from_type, _to_type| {
                let ty = from_type.clone();
                Some(|e: HirScalarExpr| e.call_unary(CastArrayToString { ty }))
            }),

            // LIST
            (List, String) => Assignment: CastTemplate::new(|_ecx, _ccx, from_type, _to_type| {
                let ty = from_type.clone();
                Some(|e: HirScalarExpr| e.call_unary(CastListToString { ty }))
            }),
            (List, List) => Implicit: CastTemplate::new(|ecx, ccx, from_type, to_type| {
                let return_ty = to_type.clone();
                let from_el_type = from_type.unwrap_list_element_type();
                let to_el_type = to_type.unwrap_list_element_type();
                let cast_expr = plan_hypothetical_cast(ecx, ccx, from_el_type, to_el_type)?;
                Some(|e: HirScalarExpr| e.call_unary(UnaryFunc::CastList1ToList2 {
                    return_ty,
                    cast_expr: Box::new(cast_expr),
                }))
            }),

            // MAP
            (Map, String) => Assignment: CastTemplate::new(|_ecx, _ccx, from_type, _to_type| {
                let ty = from_type.clone();
                Some(|e: HirScalarExpr| e.call_unary(CastMapToString { ty }))
            }),

            // JSONB
            (Jsonb, Bool) => Explicit: CastJsonbToBool,
            (Jsonb, Int16) => Explicit: CastJsonbToInt16,
            (Jsonb, Int32) => Explicit: CastJsonbToInt32,
            (Jsonb, Int64) => Explicit: CastJsonbToInt64,
            (Jsonb, Float32) => Explicit: CastJsonbToFloat32,
            (Jsonb, Float64) => Explicit: CastJsonbToFloat64,
            (Jsonb, Numeric) => Explicit: CastTemplate::new(|_ecx, _ccx, _from_type, to_type| {
                let s = to_type.unwrap_numeric_scale();
                Some(move |e: HirScalarExpr| e.call_unary(CastJsonbToNumeric(s)))
            }),
            (Jsonb, String) => Assignment: CastJsonbToString,

            // UUID
            (Uuid, String) => Assignment: CastUuidToString,

            // Numeric
            (Numeric, Numeric) => Assignment: CastTemplate::new(|_ecx, _ccx, _from_type, to_type| {
                let scale = to_type.unwrap_numeric_scale();
                Some(move |e: HirScalarExpr| match scale {
                    None => e,
                    Some(scale) => e.call_unary(UnaryFunc::RescaleNumeric(scale)),
                })
            }),
            (Numeric, Float32) => Implicit: CastNumericToFloat32,
            (Numeric, Float64) => Implicit: CastNumericToFloat64,
            (Numeric, Int16) => Assignment: CastNumericToInt16,
            (Numeric, Int32) => Assignment: CastNumericToInt32,
            (Numeric, Int64) => Assignment: CastNumericToInt64,
            (Numeric, String) => Assignment: CastNumericToString
        }
    };
}

/// Get casts directly between two [`ScalarType`]s, with control over the
/// allowed [`CastContext`].
fn get_cast(
    ecx: &ExprContext,
    ccx: CastContext,
    from: &ScalarType,
    to: &ScalarType,
) -> Option<Cast> {
    use CastContext::*;

    // Determines if types are equal in a way that does not require casting.
    fn embedded_value_equality(ccx: &CastContext, l: &ScalarType, r: &ScalarType) -> bool {
        use ScalarType::*;
        match (l, r) {
            (Array(l), Array(r)) => embedded_value_equality(&ccx, &l, &r),
            (
                List {
                    element_type: l,
                    custom_oid: oid_l,
                },
                List {
                    element_type: r,
                    custom_oid: oid_r,
                },
            )
            | (
                Map {
                    value_type: l,
                    custom_oid: oid_l,
                },
                Map {
                    value_type: r,
                    custom_oid: oid_r,
                },
            ) => oid_l == oid_r && embedded_value_equality(&ccx, &l, &r),
            (l, r) if ccx == &Implicit => l.base_eq(r),
            (l, r) => l == r,
        }
    }

    if embedded_value_equality(&ccx, &from, &to) {
        return Some(Box::new(|expr| expr));
    }

    // Determines if types are equal irrespective of any custom types.
    fn structural_equality(l: &ScalarType, r: &ScalarType) -> bool {
        use ScalarType::*;
        match (l, r) {
            (Array(l), Array(r))
            | (
                List {
                    element_type: l, ..
                },
                List {
                    element_type: r, ..
                },
            )
            | (Map { value_type: l, .. }, Map { value_type: r, .. }) => structural_equality(&l, &r),
            (l, r) => l == r,
        }
    }

    if (from.is_custom_type() || to.is_custom_type()) && structural_equality(&from, to) {
        // CastInPlace allowed if going between custom and anonymous or if cast
        // explicitly requested.
        if from.is_custom_type() ^ to.is_custom_type() || ccx == CastContext::Explicit {
            let return_ty = to.clone();
            return Some(Box::new(move |expr| {
                expr.call_unary(UnaryFunc::CastInPlace { return_ty })
            }));
        }
    }

    let imp = VALID_CASTS.get(&(from.into(), to.into()))?;
    let template = match (ccx, imp.context) {
        (Explicit, Implicit) | (Explicit, Assignment) | (Explicit, Explicit) => Some(&imp.template),
        (Assignment, Implicit) | (Assignment, Assignment) => Some(&imp.template),
        (Implicit, Implicit) => Some(&imp.template),
        _ => None,
    };
    template.and_then(|template| (template.0)(ecx, ccx, &from, to))
}

/// Converts an expression to `ScalarType::String`.
///
/// All types are convertible to string, so this never fails.
pub fn to_string(ecx: &ExprContext, expr: HirScalarExpr) -> HirScalarExpr {
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
pub fn to_jsonb(ecx: &ExprContext, expr: HirScalarExpr) -> HirScalarExpr {
    use ScalarType::*;

    match ecx.scalar_type(&expr) {
        Bool | Jsonb | Int64 | Float64 => expr.call_unary(UnaryFunc::CastJsonbOrNullToJsonb),
        Int16 | Int32 => plan_cast("to_jsonb", ecx, CastContext::Explicit, expr, &Int64)
            .expect("cast known to exist")
            .call_unary(UnaryFunc::CastJsonbOrNullToJsonb),
        Float32 | Numeric { .. } => {
            plan_cast("to_jsonb", ecx, CastContext::Explicit, expr, &Float64)
                .expect("cast known to exist")
                .call_unary(UnaryFunc::CastJsonbOrNullToJsonb)
        }
        Record { fields, .. } => {
            let mut exprs = vec![];
            for (i, (name, _ty)) in fields.iter().enumerate() {
                exprs.push(HirScalarExpr::literal(
                    Datum::String(name.as_str()),
                    ScalarType::String,
                ));
                exprs.push(to_jsonb(
                    ecx,
                    expr.clone().call_unary(UnaryFunc::RecordGet(i)),
                ));
            }
            HirScalarExpr::CallVariadic {
                func: VariadicFunc::JsonbBuildObject,
                exprs,
            }
        }
        _ => to_string(ecx, expr).call_unary(UnaryFunc::CastJsonbOrNullToJsonb),
    }
}

/// Guesses the most-common type among a set of [`ScalarType`]s that all members
/// can be cast to. Returns `None` if a common type cannot be deduced.
///
/// The returned type is not guaranteed to be accurate because we ignore type
/// categories, e.g. on input `[ScalarType::Date, ScalarType::Int32]`, will
/// guess that `Date` is the common type.
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
/// ## Type hints
/// Some types contain embedded values, e.g. [`ScalarType::Numeric`] contains
/// the values' scales. When choosing a common type, and the `type_hint` is of
/// the type chosen, you should guess _it_ because the given type hint can
/// contain a distinct embedded value necessary to retain type invariants.
///
/// [union-type-conv]:
/// https://www.postgresql.org/docs/12/typeconv-union-case.html
pub fn guess_best_common_type(
    types: &[Option<ScalarType>],
    type_hint: Option<&ScalarType>,
) -> Option<ScalarType> {
    // Remove unknown types and chain type_hint
    let known_types: Vec<_> = types
        .into_iter()
        .filter_map(|v| match v {
            None => type_hint.cloned(),
            v => v.clone(),
        })
        .collect();

    if known_types.is_empty() {
        return Some(ScalarType::String);
    }

    // Tracks order of preferences for implicit casts for each [`TypeCategory`] that
    // contains multiple types, but does so irrespective of [`TypeCategory`].
    //
    // We could make this deterministic, but it offers no real benefit because the
    // information it provides is used in fallible functions anyway, so a bad guess
    // just gets caught elsewhere.
    let r = known_types
        .iter()
        .max_by_key(|scalar_type| match scalar_type {
            // TypeCategory::String
            ScalarType::VarChar { .. } => 0,
            ScalarType::Char { .. } => 1,
            ScalarType::String => 2,
            // TypeCategory::Numeric
            ScalarType::Int16 => 3,
            ScalarType::Int32 => 4,
            ScalarType::Int64 => 5,
            ScalarType::Numeric { .. } => 6,
            ScalarType::Float32 => 7,
            ScalarType::Float64 => 8,
            // TypeCategory::DateTime
            ScalarType::Date => 9,
            ScalarType::Timestamp => 10,
            ScalarType::TimestampTz => 11,
            _ => 12,
        })
        .unwrap();

    match type_hint {
        Some(th) if r.base_eq(th) => type_hint.cloned(),
        _ => Some(r.default_embedded_value()),
    }
}

pub fn plan_coerce<'a>(
    ecx: &'a ExprContext,
    e: CoercibleScalarExpr,
    coerce_to: &ScalarType,
) -> Result<HirScalarExpr, anyhow::Error> {
    use CoercibleScalarExpr::*;

    Ok(match e {
        Coerced(e) => e,

        LiteralNull => HirScalarExpr::literal_null(coerce_to.clone()),

        LiteralString(s) => {
            let lit = HirScalarExpr::literal(Datum::String(&s), ScalarType::String);
            let ccx = match coerce_to {
                // Postgres' literal string parsing functions for bpchar
                // (bpcharin) and varchar (varcharin) have the same semantics as
                // the behavior in Assignment casts (other types don't have this complexity)
                ScalarType::Char { .. } | ScalarType::VarChar { .. } => CastContext::Assignment,
                _ => CastContext::Explicit,
            };
            plan_cast("string literal", ecx, ccx, lit, coerce_to)?
        }

        LiteralRecord(exprs) => {
            let arity = exprs.len();
            let coercions = match coerce_to {
                ScalarType::Record { fields, .. } if fields.len() == arity => fields
                    .iter()
                    .map(|(_name, ty)| &ty.scalar_type)
                    .cloned()
                    .collect(),
                _ => vec![ScalarType::String; exprs.len()],
            };
            let mut out = vec![];
            for (e, coerce_to) in exprs.into_iter().zip(coercions) {
                out.push(plan_coerce(ecx, e, &coerce_to)?);
            }
            HirScalarExpr::CallVariadic {
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
            HirScalarExpr::Parameter(n)
        }
    })
}

/// Similar to `plan_cast`, but for situations where you only know the type of
/// the input expression (`from`) and not the expression itself. The returned
/// expression refers to the first column of some imaginary row, where the first
/// column is assumed to have type `from`.
///
/// If casting from `from` to `to` is not possible, returns `None`.
pub fn plan_hypothetical_cast(
    ecx: &ExprContext,
    ccx: CastContext,
    from: &ScalarType,
    to: &ScalarType,
) -> Option<::expr::MirScalarExpr> {
    // Reconstruct an expression context where the expression is evaluated on
    // the "first column" of some imaginary row.
    let mut scx = ecx.qcx.scx.clone();
    scx.param_types = RefCell::new(BTreeMap::new());
    let qcx = QueryContext::root(&scx, ecx.qcx.lifetime);
    let relation_type = RelationType {
        column_types: vec![ColumnType {
            nullable: true,
            scalar_type: from.clone(),
        }],
        keys: vec![vec![0]],
    };
    let ecx = ExprContext {
        qcx: &qcx,
        name: "plan_hypothetical_cast",
        scope: &Scope::empty(None),
        relation_type: &relation_type,
        allow_aggregates: false,
        allow_subqueries: true,
    };

    let col_expr = HirScalarExpr::Column(ColumnRef {
        level: 0,
        column: 0,
    });

    // Determine the `ScalarExpr` required to cast our column to the target
    // component type.
    Some(
        plan_cast("plan_hypothetical_cast", &ecx, ccx, col_expr, to)
            .ok()?
            .lower_uncorrelated()
            .expect(
                "lower_uncorrelated should not fail given that there is no correlation \
                in the input col_expr",
            ),
    )
}

/// Plans a cast between [`ScalarType`]s, specifying which types of casts are
/// permitted using [`CastContext`].
///
/// # Errors
///
/// If a cast between the `ScalarExpr`'s base type and the specified type is:
/// - Not possible, e.g. `Bytes` to `Interval`
/// - Not permitted, e.g. implicitly casting from `Float64` to `Float32`.
/// - Not implemented yet
pub fn plan_cast<D>(
    caller_name: D,
    ecx: &ExprContext,
    ccx: CastContext,
    expr: HirScalarExpr,
    cast_to: &ScalarType,
) -> Result<HirScalarExpr, anyhow::Error>
where
    D: fmt::Display,
{
    use ScalarType::*;

    let cast_from = ecx.scalar_type(&expr);

    // Close over ccx, cast_from, and cast_to to simplify error messages in the
    // face of intermediate expressions.
    let cast_inner = |from, to, expr| match get_cast(ecx, ccx, from, to) {
        Some(cast) => Ok(cast(expr)),
        None => bail!(
            "{} does not support {}casting from {} to {}",
            caller_name,
            if ccx == CastContext::Implicit {
                "implicitly "
            } else {
                ""
            },
            ecx.humanize_scalar_type(&cast_from),
            ecx.humanize_scalar_type(&cast_to),
        ),
    };

    // Get cast which might include parameter rewrites + generating intermediate
    // expressions.
    //
    // n.b PG solves this problem by making casts use the same process as their
    // typical function selection, which already applies these semantics. Until
    // we refactor this function to approximately use the function selection
    // infrastructure, this is probably the fewest LOC change.
    match (&cast_from, cast_to) {
        // Rewrite from char, varchar as from string
        (Char { .. } | VarChar { .. }, dest) if !dest.is_string_like() => {
            cast_inner(&String, dest, expr)
        }
        // If to is char or varchar, use intermediate string expression.
        (source, dest @ Char { .. } | dest @ VarChar { .. }) if !source.is_string_like() => {
            let source_to_str_expr = cast_inner(source, &String, expr)?;
            cast_inner(&String, dest, source_to_str_expr)
        }
        // Standard cast
        (source, dest) => cast_inner(source, dest, expr),
    }
}

/// Reports whether it is possible to perform a cast from the specified types.
pub fn can_cast(
    ecx: &ExprContext,
    ccx: CastContext,
    cast_from: ScalarType,
    cast_to: ScalarType,
) -> bool {
    // All char values are cast to strings during casts, so this transformation
    // is equivalent.
    let cast_from = match cast_from {
        ScalarType::Char { .. } | ScalarType::VarChar { .. } => ScalarType::String,
        from => from,
    };
    let cast_to = match cast_to {
        ScalarType::Char { .. } | ScalarType::VarChar { .. } => ScalarType::String,
        to => to,
    };
    get_cast(ecx, ccx, &cast_from, &cast_to).is_some()
}
