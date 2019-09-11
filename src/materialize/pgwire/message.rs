// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use bytes::Bytes;
use chrono::{NaiveDate, NaiveDateTime};

use super::types::PgType;
use repr::decimal::Decimal;
use repr::{ColumnType, Datum, Interval, RelationType, ScalarType};

#[allow(dead_code)]
#[derive(Debug)]
pub enum Severity {
    Error,
    Fatal,
    Panic,
    Warning,
    Notice,
    Debug,
    Info,
    Log,
}

impl Severity {
    pub fn string(&self) -> &'static str {
        match self {
            Severity::Error => "ERROR",
            Severity::Fatal => "FATAL",
            Severity::Panic => "PANIC",
            Severity::Warning => "WARNING",
            Severity::Notice => "NOTICE",
            Severity::Debug => "DEBUG",
            Severity::Info => "INFO",
            Severity::Log => "LOG",
        }
    }
}

#[derive(Debug)]
pub enum FrontendMessage {
    Startup {
        version: u32,
    },
    Query {
        query: Bytes,
    },
    Parse {
        name: String,
        sql: String,
        parameter_data_type_count: u16,
        parameter_data_types: Vec<u32>,
    },
    Terminate,
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum BackendMessage {
    AuthenticationOk,
    CommandComplete {
        tag: String,
    },
    EmptyQueryResponse,
    ReadyForQuery,
    RowDescription(Vec<FieldDescription>),
    DataRow(Vec<Option<FieldValue>>),
    ParameterStatus(&'static str, String),
    ParseComplete,
    ErrorResponse {
        severity: Severity,
        code: &'static str,
        message: String,
        detail: Option<String>,
    },
    CopyOutResponse,
    CopyData(Vec<u8>),
}

#[derive(Debug)]
pub struct FieldDescription {
    pub name: String,
    pub table_id: u32,
    pub column_id: u16,
    pub type_oid: u32,
    pub type_len: i16,
    // https://github.com/cockroachdb/cockroach/blob/3e8553e249a842e206aa9f4f8be416b896201f10/pkg/sql/pgwire/conn.go#L1115-L1123
    pub type_mod: i32,
    pub format: FieldFormat,
}

#[allow(dead_code)]
#[derive(Copy, Clone, Debug)]
pub enum FieldFormat {
    Text = 0,
    Binary = 1,
}

/// PGWire-specific representations of Datums
#[derive(Debug)]
pub enum FieldValue {
    Bool(bool),
    Bytea(Vec<u8>),
    Int4(i32),
    Int8(i64),
    Float4(f32),
    Float8(f64),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    Interval(Interval),
    Text(String),
    Numeric(Decimal),
}

impl FieldValue {
    pub fn from_datum(datum: Datum, typ: &ColumnType) -> Option<FieldValue> {
        match datum {
            Datum::Null => None,
            Datum::True => Some(FieldValue::Bool(true)),
            Datum::False => Some(FieldValue::Bool(false)),
            Datum::Int32(i) => Some(FieldValue::Int4(i)),
            Datum::Int64(i) => Some(FieldValue::Int8(i)),
            Datum::Float32(f) => Some(FieldValue::Float4(*f)),
            Datum::Float64(f) => Some(FieldValue::Float8(*f)),
            Datum::Date(d) => Some(FieldValue::Date(d)),
            Datum::Timestamp(d) => Some(FieldValue::Timestamp(d)),
            Datum::Interval(i) => Some(FieldValue::Interval(i)),
            Datum::Decimal(d) => {
                let (_, scale) = typ.scalar_type.unwrap_decimal_parts();
                Some(FieldValue::Numeric(d.with_scale(scale)))
            }
            Datum::Bytes(b) => Some(FieldValue::Bytea(b)),
            Datum::String(s) => Some(FieldValue::Text(s)),
            Datum::Regex(_) => panic!("Datum::Regex cannot be converted into a FieldValue"),
        }
    }
}

pub fn field_values_from_row(row: Vec<Datum>, typ: &RelationType) -> Vec<Option<FieldValue>> {
    row.into_iter()
        .zip(typ.column_types.iter())
        .map(|(col, typ)| FieldValue::from_datum(col, typ))
        .collect()
}

pub fn row_description_from_type(typ: &RelationType) -> Vec<FieldDescription> {
    typ.column_types
        .iter()
        .map(|typ| {
            let pg_type: PgType = (&typ.scalar_type).into();
            FieldDescription {
                name: typ.name.as_ref().unwrap_or(&"?column?".into()).to_owned(),
                table_id: 0,
                column_id: 0,
                type_oid: pg_type.oid,
                type_len: pg_type.typlen,
                type_mod: match &typ.scalar_type {
                    // NUMERIC types pack their precision and size into the
                    // type_mod field. The high order bits store the precision
                    // while the low order bits store the scale + 4 (!).
                    //
                    // https://github.com/postgres/postgres/blob/e435c1e7d/src/backend/utils/adt/numeric.c#L6364-L6367
                    ScalarType::Decimal(precision, scale) => {
                        ((i32::from(*precision) << 16) | i32::from(*scale)) + 4
                    }
                    _ => -1,
                },
                format: FieldFormat::Text,
            }
        })
        .collect()
}
