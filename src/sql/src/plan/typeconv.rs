// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Maintains a catalog of valid casts between [`mz_repr::ScalarType`]s, as well as
//! other cast-related functions.

use itertools::Itertools;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;

use once_cell::sync::Lazy;

use mz_expr::func;
use mz_expr::VariadicFunc;
use mz_repr::{ColumnName, ColumnType, Datum, RelationType, ScalarBaseType, ScalarType};

use super::error::PlanError;
use super::expr::{CoercibleScalarExpr, ColumnRef, HirScalarExpr, UnaryFunc};
use super::query::{ExprContext, QueryContext};
use super::scope::Scope;
use crate::catalog::TypeCategory;

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

impl<const N: usize> From<[UnaryFunc; N]> for CastTemplate {
    fn from(funcs: [UnaryFunc; N]) -> CastTemplate {
        CastTemplate::new(move |_ecx, _ccx, _from, _to| {
            let funcs = funcs.clone();
            Some(move |mut expr: HirScalarExpr| {
                for func in funcs {
                    expr = expr.call_unary(func.clone());
                }
                expr
            })
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

static VALID_CASTS: Lazy<HashMap<(ScalarBaseType, ScalarBaseType), CastImpl>> = Lazy::new(|| {
    use ScalarBaseType::*;
    use UnaryFunc::*;

    casts! {
        // BOOL
        (Bool, Int32) => Explicit: CastBoolToInt32(func::CastBoolToInt32),
        (Bool, String) => Assignment: CastBoolToString(func::CastBoolToString),

        //INT16
        (Int16, Int32) => Implicit: CastInt16ToInt32(func::CastInt16ToInt32),
        (Int16, Int64) => Implicit: CastInt16ToInt64(func::CastInt16ToInt64),
        (Int16, UInt16) => Assignment: CastInt16ToUint16(func::CastInt16ToUint16),
        (Int16, UInt32) => Assignment: CastInt16ToUint32(func::CastInt16ToUint32),
        (Int16, UInt64) => Assignment: CastInt16ToUint64(func::CastInt16ToUint64),
        (Int16, Float32) => Implicit: CastInt16ToFloat32(func::CastInt16ToFloat32),
        (Int16, Float64) => Implicit: CastInt16ToFloat64(func::CastInt16ToFloat64),
        (Int16, Numeric) => Implicit: CastTemplate::new(|_ecx, _ccx, _from_type, to_type| {
            let s = to_type.unwrap_numeric_max_scale();
            Some(move |e: HirScalarExpr| e.call_unary(CastInt16ToNumeric(func::CastInt16ToNumeric(s))))
        }),
        (Int16, Oid) => Implicit: [
            CastInt16ToInt32(func::CastInt16ToInt32),
            CastInt32ToOid(func::CastInt32ToOid),
        ],
        (Int16, RegClass) => Implicit: [
            CastInt16ToInt32(func::CastInt16ToInt32),
            CastInt32ToOid(func::CastInt32ToOid),
            CastOidToRegClass(func::CastOidToRegClass),
        ],
        (Int16, RegProc) => Implicit: [
            CastInt16ToInt32(func::CastInt16ToInt32),
            CastInt32ToOid(func::CastInt32ToOid),
            CastOidToRegProc(func::CastOidToRegProc),
        ],
        (Int16, RegType) => Implicit: [
            CastInt16ToInt32(func::CastInt16ToInt32),
            CastInt32ToOid(func::CastInt32ToOid),
            CastOidToRegType(func::CastOidToRegType),
        ],
        (Int16, String) => Assignment: CastInt16ToString(func::CastInt16ToString),

        //INT32
        (Int32, Bool) => Explicit: CastInt32ToBool(func::CastInt32ToBool),
        (Int32, Oid) => Implicit: CastInt32ToOid(func::CastInt32ToOid),
        (Int32, RegClass) => Implicit: [
            CastInt32ToOid(func::CastInt32ToOid),
            CastOidToRegClass(func::CastOidToRegClass),
        ],
        (Int32, RegProc) => Implicit: [
            CastInt32ToOid(func::CastInt32ToOid),
            CastOidToRegProc(func::CastOidToRegProc),
        ],
        (Int32, RegType) => Implicit: [
            CastInt32ToOid(func::CastInt32ToOid),
            CastOidToRegType(func::CastOidToRegType),
        ],
        (Int32, PgLegacyChar) => Explicit: CastInt32ToPgLegacyChar(func::CastInt32ToPgLegacyChar),
        (Int32, Int16) => Assignment: CastInt32ToInt16(func::CastInt32ToInt16),
        (Int32, Int64) => Implicit: CastInt32ToInt64(func::CastInt32ToInt64),
        (Int32, UInt16) => Assignment: CastInt32ToUint16(func::CastInt32ToUint16),
        (Int32, UInt32) => Assignment: CastInt32ToUint32(func::CastInt32ToUint32),
        (Int32, UInt64) => Assignment: CastInt32ToUint64(func::CastInt32ToUint64),
        (Int32, Float32) => Implicit: CastInt32ToFloat32(func::CastInt32ToFloat32),
        (Int32, Float64) => Implicit: CastInt32ToFloat64(func::CastInt32ToFloat64),
        (Int32, Numeric) => Implicit: CastTemplate::new(|_ecx, _ccx, _from_type, to_type| {
            let s = to_type.unwrap_numeric_max_scale();
            Some(move |e: HirScalarExpr| e.call_unary(CastInt32ToNumeric(func::CastInt32ToNumeric(s))))
        }),
        (Int32, String) => Assignment: CastInt32ToString(func::CastInt32ToString),

        // INT64
        (Int64, Bool) => Explicit: CastInt64ToBool(func::CastInt64ToBool),
        (Int64, Int16) => Assignment: CastInt64ToInt16(func::CastInt64ToInt16),
        (Int64, Int32) => Assignment: CastInt64ToInt32(func::CastInt64ToInt32),
        (Int64, UInt16) => Assignment: CastInt64ToUint16(func::CastInt64ToUint16),
        (Int64, UInt32) => Assignment: CastInt64ToUint32(func::CastInt64ToUint32),
        (Int64, UInt64) => Assignment: CastInt64ToUint64(func::CastInt64ToUint64),
        (Int64, Numeric) => Implicit: CastTemplate::new(|_ecx, _ccx, _from_type, to_type| {
            let s = to_type.unwrap_numeric_max_scale();
            Some(move |e: HirScalarExpr| e.call_unary(CastInt64ToNumeric(func::CastInt64ToNumeric(s))))
        }),
        (Int64, Float32) => Implicit: CastInt64ToFloat32(func::CastInt64ToFloat32),
        (Int64, Float64) => Implicit: CastInt64ToFloat64(func::CastInt64ToFloat64),
        (Int64, Oid) => Implicit: CastInt64ToOid(func::CastInt64ToOid),
        (Int64, RegClass) => Implicit: [
            CastInt64ToOid(func::CastInt64ToOid),
            CastOidToRegClass(func::CastOidToRegClass),
        ],
        (Int64, RegProc) => Implicit: [
            CastInt64ToOid(func::CastInt64ToOid),
            CastOidToRegProc(func::CastOidToRegProc),
        ],
        (Int64, RegType) => Implicit: [
            CastInt64ToOid(func::CastInt64ToOid),
            CastOidToRegType(func::CastOidToRegType),
        ],
        (Int64, String) => Assignment: CastInt64ToString(func::CastInt64ToString),

        // UINT16
        (UInt16, UInt32) => Implicit: CastUint16ToUint32(func::CastUint16ToUint32),
        (UInt16, UInt64) => Implicit: CastUint16ToUint64(func::CastUint16ToUint64),
        (UInt16, Int32) => Implicit: CastUint16ToInt32(func::CastUint16ToInt32),
        (UInt16, Int64) => Implicit: CastUint16ToInt64(func::CastUint16ToInt64),
        (UInt16, Numeric) => Implicit: CastTemplate::new(|_ecx, _ccx, _from_type, to_type| {
            let s = to_type.unwrap_numeric_max_scale();
            Some(move |e: HirScalarExpr| e.call_unary(CastUint16ToNumeric(func::CastUint16ToNumeric(s))))
        }),
        (UInt16, Float32) => Implicit: CastUint16ToFloat32(func::CastUint16ToFloat32),
        (UInt16, Float64) => Implicit: CastUint16ToFloat64(func::CastUint16ToFloat64),
        (UInt16, String) => Assignment: CastUint16ToString(func::CastUint16ToString),

        // UINT32
        (UInt32, UInt16) => Assignment: CastUint32ToUint16(func::CastUint32ToUint16),
        (UInt32, UInt64) => Implicit: CastUint32ToUint64(func::CastUint32ToUint64),
        (UInt32, Int32) => Assignment: CastUint32ToInt32(func::CastUint32ToInt32),
        (UInt32, Int64) => Implicit: CastUint32ToInt64(func::CastUint32ToInt64),
        (UInt32, Numeric) => Implicit: CastTemplate::new(|_ecx, _ccx, _from_type, to_type| {
            let s = to_type.unwrap_numeric_max_scale();
            Some(move |e: HirScalarExpr| e.call_unary(CastUint32ToNumeric(func::CastUint32ToNumeric(s))))
        }),
        (UInt32, Float32) => Implicit: CastUint32ToFloat32(func::CastUint32ToFloat32),
        (UInt32, Float64) => Implicit: CastUint32ToFloat64(func::CastUint32ToFloat64),
        (UInt32, String) => Assignment: CastUint32ToString(func::CastUint32ToString),

        // UINT64
        (UInt64, UInt16) => Assignment: CastUint64ToUint16(func::CastUint64ToUint16),
        (UInt64, UInt32) => Assignment: CastUint64ToUint32(func::CastUint64ToUint32),
        (UInt64, Int32) => Assignment: CastUint64ToInt32(func::CastUint64ToInt32),
        (UInt64, Int64) => Assignment: CastUint64ToInt64(func::CastUint64ToInt64),
        (UInt64, Numeric) => Implicit: CastTemplate::new(|_ecx, _ccx, _from_type, to_type| {
            let s = to_type.unwrap_numeric_max_scale();
            Some(move |e: HirScalarExpr| e.call_unary(CastUint64ToNumeric(func::CastUint64ToNumeric(s))))
        }),
        (UInt64, Float32) => Implicit: CastUint64ToFloat32(func::CastUint64ToFloat32),
        (UInt64, Float64) => Implicit: CastUint64ToFloat64(func::CastUint64ToFloat64),
        (UInt64, String) => Assignment: CastUint64ToString(func::CastUint64ToString),


        // OID
        (Oid, Int32) => Assignment: CastOidToInt32(func::CastOidToInt32),
        (Oid, Int64) => Assignment: CastOidToInt32(func::CastOidToInt32),
        (Oid, String) => Explicit: CastOidToString(func::CastOidToString),
        (Oid, RegClass) => Assignment: CastOidToRegClass(func::CastOidToRegClass),
        (Oid, RegProc) => Assignment: CastOidToRegProc(func::CastOidToRegProc),
        (Oid, RegType) => Assignment: CastOidToRegType(func::CastOidToRegType),

        // REGCLASS
        (RegClass, Oid) => Implicit: CastRegClassToOid(func::CastRegClassToOid),
        (RegClass, String) => Explicit: sql_impl_cast("(
                SELECT COALESCE(t.name, v.x::pg_catalog.text)
                FROM (
                    VALUES ($1::pg_catalog.oid)) AS v(x)
                    LEFT JOIN mz_catalog.mz_objects AS t
                    ON t.oid = v.x
            )"),

        // REGPROC
        (RegProc, Oid) => Implicit: CastRegProcToOid(func::CastRegProcToOid),
        (RegProc, String) => Explicit: sql_impl_cast("(
                SELECT COALESCE(t.name, v.x::pg_catalog.text)
                FROM (
                    VALUES ($1::pg_catalog.oid)) AS v(x)
                    LEFT JOIN mz_catalog.mz_functions AS t
                    ON t.oid = v.x
            )"),

        // REGTYPE
        (RegType, Oid) => Implicit: CastRegTypeToOid(func::CastRegTypeToOid),
        (RegType, String) => Explicit: sql_impl_cast("(
                SELECT COALESCE(t.name, v.x::pg_catalog.text)
                FROM (
                    VALUES ($1::pg_catalog.oid)) AS v(x)
                    LEFT JOIN mz_catalog.mz_types AS t
                    ON t.oid = v.x
            )"),

        // FLOAT32
        (Float32, Int16) => Assignment: CastFloat32ToInt16(func::CastFloat32ToInt16),
        (Float32, Int32) => Assignment: CastFloat32ToInt32(func::CastFloat32ToInt32),
        (Float32, Int64) => Assignment: CastFloat32ToInt64(func::CastFloat32ToInt64),
        (Float32, UInt16) => Assignment: CastFloat32ToUint16(func::CastFloat32ToUint16),
        (Float32, UInt32) => Assignment: CastFloat32ToUint32(func::CastFloat32ToUint32),
        (Float32, UInt64) => Assignment: CastFloat32ToUint64(func::CastFloat32ToUint64),
        (Float32, Float64) => Implicit: CastFloat32ToFloat64(func::CastFloat32ToFloat64),
        (Float32, Numeric) => Assignment: CastTemplate::new(|_ecx, _ccx, _from_type, to_type| {
            let s = to_type.unwrap_numeric_max_scale();
            Some(move |e: HirScalarExpr| e.call_unary(CastFloat32ToNumeric(func::CastFloat32ToNumeric(s))))
        }),
        (Float32, String) => Assignment: CastFloat32ToString(func::CastFloat32ToString),

        // FLOAT64
        (Float64, Int16) => Assignment: CastFloat64ToInt16(func::CastFloat64ToInt16),
        (Float64, Int32) => Assignment: CastFloat64ToInt32(func::CastFloat64ToInt32),
        (Float64, Int64) => Assignment: CastFloat64ToInt64(func::CastFloat64ToInt64),
        (Float64, UInt16) => Assignment: CastFloat64ToUint16(func::CastFloat64ToUint16),
        (Float64, UInt32) => Assignment: CastFloat64ToUint32(func::CastFloat64ToUint32),
        (Float64, UInt64) => Assignment: CastFloat64ToUint64(func::CastFloat64ToUint64),
        (Float64, Float32) => Assignment: CastFloat64ToFloat32(func::CastFloat64ToFloat32),
        (Float64, Numeric) => Assignment: CastTemplate::new(|_ecx, _ccx, _from_type, to_type| {
            let s = to_type.unwrap_numeric_max_scale();
            Some(move |e: HirScalarExpr| e.call_unary(CastFloat64ToNumeric(func::CastFloat64ToNumeric(s))))
        }),
        (Float64, String) => Assignment: CastFloat64ToString(func::CastFloat64ToString),

        // DATE
        (Date, Timestamp) => Implicit: CastDateToTimestamp(func::CastDateToTimestamp),
        (Date, TimestampTz) => Implicit: CastDateToTimestampTz(func::CastDateToTimestampTz),
        (Date, String) => Assignment: CastDateToString(func::CastDateToString),

        // TIME
        (Time, Interval) => Implicit: CastTimeToInterval(func::CastTimeToInterval),
        (Time, String) => Assignment: CastTimeToString(func::CastTimeToString),

        // TIMESTAMP
        (Timestamp, Date) => Assignment: CastTimestampToDate(func::CastTimestampToDate),
        (Timestamp, TimestampTz) => Implicit: CastTimestampToTimestampTz(func::CastTimestampToTimestampTz),
        (Timestamp, Time) => Assignment: CastTimestampToTime(func::CastTimestampToTime),
        (Timestamp, String) => Assignment: CastTimestampToString(func::CastTimestampToString),

        // TIMESTAMPTZ
        (TimestampTz, Date) => Assignment: CastTimestampTzToDate(func::CastTimestampTzToDate),
        (TimestampTz, Timestamp) => Assignment: CastTimestampTzToTimestamp(func::CastTimestampTzToTimestamp),
        (TimestampTz, Time) => Assignment: CastTimestampTzToTime(func::CastTimestampTzToTime),
        (TimestampTz, String) => Assignment: CastTimestampTzToString(func::CastTimestampTzToString),

        // INTERVAL
        (Interval, Time) => Assignment: CastIntervalToTime(func::CastIntervalToTime),
        (Interval, String) => Assignment: CastIntervalToString(func::CastIntervalToString),

        // BYTES
        (Bytes, String) => Assignment: CastBytesToString(func::CastBytesToString),

        // STRING
        (String, Bool) => Explicit: CastStringToBool(func::CastStringToBool),
        (String, Int16) => Explicit: CastStringToInt16(func::CastStringToInt16),
        (String, Int32) => Explicit: CastStringToInt32(func::CastStringToInt32),
        (String, Int64) => Explicit: CastStringToInt64(func::CastStringToInt64),
        (String, UInt16) => Explicit: CastStringToUint16(func::CastStringToUint16),
        (String, UInt32) => Explicit: CastStringToUint32(func::CastStringToUint32),
        (String, UInt64) => Explicit: CastStringToUint64(func::CastStringToUint64),
        (String, Oid) => Explicit: CastStringToOid(func::CastStringToOid),

        // STRING to REG*
        // A reg* type represents a specific type of object by oid.
        // Converting from string to reg* does a lookup of the object name
        // in the corresponding mz_catalog table and expects exactly one object to match it.
        // You can also specify (in postgres) a string that's a valid
        // int4 and it'll happily cast it (without verifying that the int4 matches
        // an object oid). To support this, use a SQL expression that checks if
        // the input might be a valid int4 with a regex, otherwise try to lookup in
        // the table. CASE will return NULL if the subquery returns zero results,
        // so use mz_error_if_null to coerce that into an error. This is hacky and
        // incomplete in a few ways, but gets us close enough to making drivers happy.
        // TODO: Support the correct error code for does not exist (42883).
        (String, RegClass) => Explicit: sql_impl_cast("(
                SELECT
                    CASE
                    WHEN $1 IS NULL THEN NULL
                    WHEN $1 ~ '^\\d+$' THEN $1::pg_catalog.oid::pg_catalog.regclass
                    ELSE (
                        mz_internal.mz_error_if_null(
                            (SELECT oid::pg_catalog.regclass FROM mz_catalog.mz_objects WHERE name = $1),
                            'object \"' || $1 || '\" does not exist'
                        )
                    )
                    END
            )"),
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

        (String, Float32) => Explicit: CastStringToFloat32(func::CastStringToFloat32),
        (String, Float64) => Explicit: CastStringToFloat64(func::CastStringToFloat64),
        (String, Numeric) => Explicit: CastTemplate::new(|_ecx, _ccx, _from_type, to_type| {
            let s = to_type.unwrap_numeric_max_scale();
            Some(move |e: HirScalarExpr| e.call_unary(CastStringToNumeric(func::CastStringToNumeric(s))))
        }),
        (String, Date) => Explicit: CastStringToDate(func::CastStringToDate),
        (String, Time) => Explicit: CastStringToTime(func::CastStringToTime),
        (String, Timestamp) => Explicit: CastStringToTimestamp(func::CastStringToTimestamp),
        (String, TimestampTz) => Explicit: CastStringToTimestampTz(func::CastStringToTimestampTz),
        (String, Interval) => Explicit: CastStringToInterval(func::CastStringToInterval),
        (String, Bytes) => Explicit: CastStringToBytes(func::CastStringToBytes),
        (String, Jsonb) => Explicit: CastStringToJsonb(func::CastStringToJsonb),
        (String, Uuid) => Explicit: CastStringToUuid(func::CastStringToUuid),
        (String, Array) => Explicit: CastTemplate::new(|ecx, ccx, from_type, to_type| {
            let return_ty = to_type.clone();
            let to_el_type = to_type.unwrap_array_element_type();
            let cast_expr = plan_hypothetical_cast(ecx, ccx, from_type, to_el_type)?;
            Some(|e: HirScalarExpr| e.call_unary(UnaryFunc::CastStringToArray(func::CastStringToArray {
                return_ty,
                cast_expr: Box::new(cast_expr),
            })))
        }),
        (String, List) => Explicit: CastTemplate::new(|ecx, ccx, from_type, to_type| {
            let return_ty = to_type.clone();
            let to_el_type = to_type.unwrap_list_element_type();
            let cast_expr = plan_hypothetical_cast(ecx, ccx, from_type, to_el_type)?;
            Some(|e: HirScalarExpr| e.call_unary(UnaryFunc::CastStringToList(func::CastStringToList {
                return_ty,
                cast_expr: Box::new(cast_expr),
            })))
        }),
        (String, Map) => Explicit: CastTemplate::new(|ecx, ccx, from_type, to_type| {
            let return_ty = to_type.clone();
            let to_val_type = to_type.unwrap_map_value_type();
            let cast_expr = plan_hypothetical_cast(ecx, ccx, from_type, to_val_type)?;
            Some(|e: HirScalarExpr| e.call_unary(UnaryFunc::CastStringToMap(func::CastStringToMap {
                return_ty,
                cast_expr: Box::new(cast_expr),
            })))
        }),
        (String, Int2Vector) => Explicit: CastStringToInt2Vector(func::CastStringToInt2Vector),
        (String, Char) => Implicit: CastTemplate::new(|_ecx, ccx, _from_type, to_type| {
            let length = to_type.unwrap_char_length();
            Some(move |e: HirScalarExpr| e.call_unary(CastStringToChar(func::CastStringToChar {length, fail_on_len: ccx != CastContext::Explicit})))
        }),
        (String, VarChar) => Implicit: CastTemplate::new(|_ecx, ccx, _from_type, to_type| {
            let length = to_type.unwrap_varchar_max_length();
            Some(move |e: HirScalarExpr| e.call_unary(CastStringToVarChar(func::CastStringToVarChar {length, fail_on_len: ccx != CastContext::Explicit})))
        }),
        (String, PgLegacyChar) => Assignment: CastStringToPgLegacyChar(func::CastStringToPgLegacyChar),
        // CHAR
        (Char, String) => Implicit: CastCharToString(func::CastCharToString),
        (Char, Char) => Implicit: CastTemplate::new(|_ecx, ccx, _from_type, to_type| {
            let length = to_type.unwrap_char_length();
            Some(move |e: HirScalarExpr| e.call_unary(CastStringToChar(func::CastStringToChar {length, fail_on_len: ccx != CastContext::Explicit})))
        }),
        (Char, VarChar) => Implicit: CastTemplate::new(|_ecx, ccx, _from_type, to_type| {
            let length = to_type.unwrap_varchar_max_length();
            Some(move |e: HirScalarExpr| e.call_unary(CastStringToVarChar(func::CastStringToVarChar {length, fail_on_len: ccx != CastContext::Explicit})))
        }),
        (Char, PgLegacyChar) => Assignment: CastStringToPgLegacyChar(func::CastStringToPgLegacyChar),

        // VARCHAR
        (VarChar, String) => Implicit: CastVarCharToString(func::CastVarCharToString),
        (VarChar, Char) => Implicit: CastTemplate::new(|_ecx, ccx, _from_type, to_type| {
            let length = to_type.unwrap_char_length();
            Some(move |e: HirScalarExpr| e.call_unary(CastStringToChar(func::CastStringToChar {length, fail_on_len: ccx != CastContext::Explicit})))
        }),
        (VarChar, VarChar) => Implicit: CastTemplate::new(|_ecx, ccx, _from_type, to_type| {
            let length = to_type.unwrap_varchar_max_length();
            Some(move |e: HirScalarExpr| e.call_unary(CastStringToVarChar(func::CastStringToVarChar {length, fail_on_len: ccx != CastContext::Explicit})))
        }),
        (VarChar, PgLegacyChar) => Assignment: CastStringToPgLegacyChar(func::CastStringToPgLegacyChar),

        //PG LEGACY CHAR
        (PgLegacyChar, String) => Implicit: CastPgLegacyCharToString(func::CastPgLegacyCharToString),
        (PgLegacyChar, Char) => Assignment: CastTemplate::new(|_ecx, ccx, _from_type, to_type| {
            let length = to_type.unwrap_char_length();
            Some(move |e: HirScalarExpr| e.call_unary(CastStringToChar(func::CastStringToChar {length, fail_on_len: ccx == CastContext::Assignment})))
        }),
        (PgLegacyChar, VarChar) => Assignment: CastTemplate::new(|_ecx, ccx, _from_type, to_type| {
            let length = to_type.unwrap_varchar_max_length();
            Some(move |e: HirScalarExpr| e.call_unary(CastStringToVarChar(func::CastStringToVarChar {length, fail_on_len: ccx == CastContext::Assignment})))
        }),
        (PgLegacyChar, Int32) => Explicit: CastPgLegacyCharToInt32(func::CastPgLegacyCharToInt32),

        // RECORD
        (Record, String) => Assignment: CastTemplate::new(|_ecx, _ccx, from_type, _to_type| {
            let ty = from_type.clone();
            Some(|e: HirScalarExpr| e.call_unary(CastRecordToString(func::CastRecordToString { ty })))
        }),
        (Record, Record) => Implicit: CastTemplate::new(|ecx, ccx, from_type, to_type| {
            if from_type.unwrap_record_element_type().len() != to_type.unwrap_record_element_type().len() {
                return None;
            }

            if let (l @ ScalarType::Record {custom_id: Some(..), ..}, r) = (from_type, to_type) {
                // Changing `from`'s custom_id requires at least Assignment context
                if ccx == CastContext::Implicit && l != r {
                    return None;
                }
            }

            let cast_exprs = from_type.unwrap_record_element_type()
                .iter()
                .zip_eq(to_type.unwrap_record_element_type())
                .map(|(f, t)| plan_hypothetical_cast(ecx, ccx, f, t))
                .collect::<Option<Vec<_>>>()?;
            let to = to_type.clone();
            Some(|e: HirScalarExpr| e.call_unary(CastRecord1ToRecord2(func::CastRecord1ToRecord2 { return_ty: to, cast_exprs })))
        }),

        // ARRAY
        (Array, String) => Assignment: CastTemplate::new(|_ecx, _ccx, from_type, _to_type| {
            let ty = from_type.clone();
            Some(|e: HirScalarExpr| e.call_unary(CastArrayToString(func::CastArrayToString { ty })))
        }),
        (Array, List) => Explicit: CastArrayToListOneDim(func::CastArrayToListOneDim),

        // INT2VECTOR
        (Int2Vector, Array) => Implicit: CastTemplate::new(|_ecx, _ccx, _from_type, _to_type| {
            Some(|e: HirScalarExpr| e.call_unary(UnaryFunc::CastInt2VectorToArray(func::CastInt2VectorToArray)))
        }),
        (Int2Vector, String) => Explicit: CastInt2VectorToString(func::CastInt2VectorToString),

        // LIST
        (List, String) => Assignment: CastTemplate::new(|_ecx, _ccx, from_type, _to_type| {
            let ty = from_type.clone();
            Some(|e: HirScalarExpr| e.call_unary(CastListToString(func::CastListToString { ty })))
        }),
        (List, List) => Implicit: CastTemplate::new(|ecx, ccx, from_type, to_type| {

            if let (l @ ScalarType::List {custom_id: Some(..), ..}, r) = (from_type, to_type) {
                // Changing `from`'s custom_id requires at least Assignment context
                if ccx == CastContext::Implicit && !l.base_eq(r) {
                    return None;
                }
            }

            let return_ty = to_type.clone();
            let from_el_type = from_type.unwrap_list_element_type();
            let to_el_type = to_type.unwrap_list_element_type();
            let cast_expr = plan_hypothetical_cast(ecx, ccx, from_el_type, to_el_type)?;
            Some(|e: HirScalarExpr| e.call_unary(UnaryFunc::CastList1ToList2(func::CastList1ToList2 {
                return_ty,
                cast_expr: Box::new(cast_expr),
            })))
        }),

        // MAP
        (Map, String) => Assignment: CastTemplate::new(|_ecx, _ccx, from_type, _to_type| {
            let ty = from_type.clone();
            Some(|e: HirScalarExpr| e.call_unary(CastMapToString(func::CastMapToString { ty })))
        }),

        // JSONB
        (Jsonb, Bool) => Explicit: CastJsonbToBool(func::CastJsonbToBool),
        (Jsonb, Int16) => Explicit: CastJsonbToInt16(func::CastJsonbToInt16),
        (Jsonb, Int32) => Explicit: CastJsonbToInt32(func::CastJsonbToInt32),
        (Jsonb, Int64) => Explicit: CastJsonbToInt64(func::CastJsonbToInt64),
        (Jsonb, Float32) => Explicit: CastJsonbToFloat32(func::CastJsonbToFloat32),
        (Jsonb, Float64) => Explicit: CastJsonbToFloat64(func::CastJsonbToFloat64),
        (Jsonb, Numeric) => Explicit: CastTemplate::new(|_ecx, _ccx, _from_type, to_type| {
            let s = to_type.unwrap_numeric_max_scale();
            Some(move |e: HirScalarExpr| e.call_unary(CastJsonbToNumeric(func::CastJsonbToNumeric(s))))
        }),
        (Jsonb, String) => Assignment: CastJsonbToString(func::CastJsonbToString),

        // UUID
        (Uuid, String) => Assignment: CastUuidToString(func::CastUuidToString),

        // Numeric
        (Numeric, Numeric) => Assignment: CastTemplate::new(|_ecx, _ccx, _from_type, to_type| {
            let scale = to_type.unwrap_numeric_max_scale();
            Some(move |e: HirScalarExpr| match scale {
                None => e,
                Some(scale) => e.call_unary(UnaryFunc::RescaleNumeric(func::RescaleNumeric(scale))),
            })
        }),
        (Numeric, Float32) => Implicit: CastNumericToFloat32(func::CastNumericToFloat32),
        (Numeric, Float64) => Implicit: CastNumericToFloat64(func::CastNumericToFloat64),
        (Numeric, Int16) => Assignment: CastNumericToInt16(func::CastNumericToInt16),
        (Numeric, Int32) => Assignment: CastNumericToInt32(func::CastNumericToInt32),
        (Numeric, Int64) => Assignment: CastNumericToInt64(func::CastNumericToInt64),
        (Numeric, UInt16) => Assignment: CastNumericToUint16(func::CastNumericToUint16),
        (Numeric, UInt32) => Assignment: CastNumericToUint32(func::CastNumericToUint32),
        (Numeric, UInt64) => Assignment: CastNumericToUint64(func::CastNumericToUint64),
        (Numeric, String) => Assignment: CastNumericToString(func::CastNumericToString)
    }
});

/// Get casts directly between two [`ScalarType`]s, with control over the
/// allowed [`CastContext`].
fn get_cast(
    ecx: &ExprContext,
    ccx: CastContext,
    from: &ScalarType,
    to: &ScalarType,
) -> Option<Cast> {
    use CastContext::*;

    if from == to || (ccx == Implicit && from.base_eq(to)) {
        return Some(Box::new(|expr| expr));
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
    plan_cast(ecx, CastContext::Explicit, expr, &ScalarType::String).expect("cast known to exist")
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
        Bool | Jsonb | Numeric { .. } => expr.call_unary(UnaryFunc::CastJsonbOrNullToJsonb(
            func::CastJsonbOrNullToJsonb,
        )),
        Int16 | Int32 | Float32 | Float64 => plan_cast(
            ecx,
            CastContext::Explicit,
            expr,
            &Numeric { max_scale: None },
        )
        .expect("cast known to exist")
        .call_unary(UnaryFunc::CastJsonbOrNullToJsonb(
            func::CastJsonbOrNullToJsonb,
        )),
        Record { fields, .. } => {
            let mut exprs = vec![];
            for (i, (name, _ty)) in fields.iter().enumerate() {
                exprs.push(HirScalarExpr::literal(
                    Datum::String(name.as_str()),
                    ScalarType::String,
                ));
                exprs.push(to_jsonb(
                    ecx,
                    expr.clone()
                        .call_unary(UnaryFunc::RecordGet(func::RecordGet(i))),
                ));
            }
            HirScalarExpr::CallVariadic {
                func: VariadicFunc::JsonbBuildObject,
                exprs,
            }
        }
        _ => to_string(ecx, expr).call_unary(UnaryFunc::CastJsonbOrNullToJsonb(
            func::CastJsonbOrNullToJsonb,
        )),
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
/// [union-type-conv]: https://www.postgresql.org/docs/12/typeconv-union-case.html
pub fn guess_best_common_type(
    ecx: &ExprContext,
    types: &[Option<ScalarType>],
) -> Result<ScalarType, PlanError> {
    // This function is a direct translation of `select_common_type` in
    // PostgreSQL.
    // https://github.com/postgres/postgres/blob/d1b307eef/src/backend/parser/parse_coerce.c#L1288-L1308

    // Remove unknown types.
    let types: Vec<_> = types.into_iter().filter_map(|v| v.as_ref()).collect();

    // If no known types, fall back to `String`.
    if types.is_empty() {
        return Ok(ScalarType::String);
    }

    let mut types = types.into_iter();

    // Start by guessing the first type, then look at each following type in
    // turn.
    let mut candidate = types.next().unwrap();
    for typ in types {
        if TypeCategory::from_type(candidate) != TypeCategory::from_type(typ) {
            // The next type is in a different category; give up.
            sql_bail!(
                "{} types {} and {} cannot be matched",
                ecx.name,
                ecx.humanize_scalar_type(&candidate),
                ecx.humanize_scalar_type(&typ),
            );
        } else if TypeCategory::from_type(candidate).preferred_type().as_ref() != Some(candidate)
            && can_cast(ecx, CastContext::Implicit, &candidate, &typ)
            && !can_cast(ecx, CastContext::Implicit, &typ, &candidate)
        {
            // The current candidate is not the preferred type for its category
            // and the next type is implicitly convertible to the current
            // candidate, but not vice-versa, so take the next type as the new
            // candidate.
            candidate = typ;
        }
    }
    Ok(candidate.without_modifiers())
}

pub fn plan_coerce<'a>(
    ecx: &'a ExprContext,
    e: CoercibleScalarExpr,
    coerce_to: &ScalarType,
) -> Result<HirScalarExpr, PlanError> {
    use CoercibleScalarExpr::*;

    Ok(match e {
        Coerced(e) => e,

        LiteralNull => HirScalarExpr::literal_null(coerce_to.clone()),

        LiteralString(s) => {
            let lit = HirScalarExpr::literal(Datum::String(&s), ScalarType::String);
            // Per PostgreSQL, string literal explicitly casts to the base type.
            // The caller is responsible for applying any desired modifiers
            // (with either implicit or explicit semantics) via a separate call
            // to `plan_cast`.
            let coerce_to_base = &coerce_to.without_modifiers();
            plan_cast(ecx, CastContext::Explicit, lit, &coerce_to_base)?
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
) -> Option<mz_expr::MirScalarExpr> {
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
        scope: &Scope::empty(),
        relation_type: &relation_type,
        allow_aggregates: false,
        allow_subqueries: true,
        allow_windows: false,
    };

    let col_expr = HirScalarExpr::Column(ColumnRef {
        level: 0,
        column: 0,
    });

    // Determine the `ScalarExpr` required to cast our column to the target
    // component type.
    Some(
        plan_cast(&ecx, ccx, col_expr, to)
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
pub fn plan_cast(
    ecx: &ExprContext,
    ccx: CastContext,
    expr: HirScalarExpr,
    to: &ScalarType,
) -> Result<HirScalarExpr, PlanError> {
    let from = ecx.scalar_type(&expr);

    // Close over `ccx`, `from`, and `to` to simplify error messages in the
    // face of intermediate expressions.
    let cast_inner = |from, to, expr| match get_cast(ecx, ccx, from, to) {
        Some(cast) => Ok(cast(expr)),
        None => sql_bail!(
            "{} does not support {}casting from {} to {}",
            ecx.name,
            if ccx == CastContext::Implicit {
                "implicitly "
            } else {
                ""
            },
            ecx.humanize_scalar_type(&from),
            ecx.humanize_scalar_type(&to),
        ),
    };

    // Get cast which might include parameter rewrites + generating intermediate
    // expressions.
    //
    // String-like types get special handling to match PostgreSQL.
    // See: https://github.com/postgres/postgres/blob/6b04abdfc/src/backend/parser/parse_coerce.c#L3205-L3223
    let from_category = TypeCategory::from_type(&from);
    let to_category = TypeCategory::from_type(&to);
    if from_category == TypeCategory::String && to_category != TypeCategory::String {
        // Converting from stringlike to something non-stringlike. Handle as if
        // `from` were a `ScalarType::String.
        cast_inner(&ScalarType::String, to, expr)
    } else if from_category != TypeCategory::String && to_category == TypeCategory::String {
        // Converting from non-stringlike to something stringlike. Convert to a
        // `ScalarType::String` and then to the desired type.
        let expr = cast_inner(&from, &ScalarType::String, expr)?;
        cast_inner(&ScalarType::String, to, expr)
    } else {
        // Standard cast.
        cast_inner(&from, to, expr)
    }
}

/// Reports whether it is possible to perform a cast from the specified types.
pub fn can_cast(
    ecx: &ExprContext,
    ccx: CastContext,
    mut cast_from: &ScalarType,
    mut cast_to: &ScalarType,
) -> bool {
    // All stringlike types are treated like `ScalarType::String` during casts.
    if TypeCategory::from_type(&cast_from) == TypeCategory::String {
        cast_from = &ScalarType::String;
    }
    if TypeCategory::from_type(&cast_to) == TypeCategory::String {
        cast_to = &ScalarType::String;
    }
    get_cast(ecx, ccx, &cast_from, &cast_to).is_some()
}
