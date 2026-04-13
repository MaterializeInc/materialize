// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Storage-specific scalar expression and cast function types, decoupled from
//! `MirScalarExpr` to avoid a dependency on the compute layer.

use mz_repr::adt::char::CharLength;
use mz_repr::adt::numeric::NumericMaxScale;
use mz_repr::adt::timestamp::TimestampPrecision;
use mz_repr::adt::varchar::VarCharMaxLength;
use mz_repr::{ReprColumnType, Row, SqlScalarType};
use serde::{Deserialize, Serialize};

/// A scalar expression used in storage contexts, covering only the subset of
/// operations needed for string-to-column casts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum StorageScalarExpr {
    /// A reference to a column by index.
    Column(usize),
    /// A literal value together with its column type.
    Literal(Row, ReprColumnType),
    /// A unary function application.
    CallUnary(CastFunc, Box<StorageScalarExpr>),
    /// Return an error if the inner expression evaluates to null.
    ErrorIfNull(Box<StorageScalarExpr>, String),
}

/// Cast functions from string to a typed value, mirroring the subset of
/// `mz_expr::UnaryFunc` variants used when casting source columns.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CastFunc {
    CastStringToBool,
    CastStringToPgLegacyChar,
    CastStringToPgLegacyName,
    CastStringToBytes,
    CastStringToInt16,
    CastStringToInt32,
    CastStringToInt64,
    CastStringToFloat32,
    CastStringToFloat64,
    CastStringToOid,
    CastStringToUint16,
    CastStringToUint32,
    CastStringToUint64,
    CastStringToDate,
    CastStringToTime,
    CastStringToInterval,
    CastStringToUuid,
    CastStringToJsonb,
    CastStringToMzTimestamp,
    CastStringToInt2Vector,
    CastStringToNumeric(Option<NumericMaxScale>),
    CastStringToTimestamp(Option<TimestampPrecision>),
    CastStringToTimestampTz(Option<TimestampPrecision>),
    CastStringToChar {
        length: Option<CharLength>,
        fail_on_len: bool,
    },
    CastStringToVarChar {
        length: Option<VarCharMaxLength>,
        fail_on_len: bool,
    },
    CastStringToArray {
        return_ty: SqlScalarType,
        cast_expr: Box<StorageScalarExpr>,
    },
    CastStringToList {
        return_ty: SqlScalarType,
        cast_expr: Box<StorageScalarExpr>,
    },
    CastStringToMap {
        return_ty: SqlScalarType,
        cast_expr: Box<StorageScalarExpr>,
    },
    CastStringToRange {
        return_ty: SqlScalarType,
        cast_expr: Box<StorageScalarExpr>,
    },
}
