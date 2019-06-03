// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use bytes::Bytes;

use super::types::PgType;
use crate::repr::{ColumnType, Datum, RelationType, ScalarType};

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
    Startup { version: u32 },
    Query { query: Bytes },
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
    DataRow(Vec<Datum>),
    ErrorResponse {
        severity: Severity,
        code: &'static str,
        message: String,
        detail: Option<String>,
    },
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
                type_mod: -1,
                format: FieldFormat::Text,
            }
        })
        .collect()
}
