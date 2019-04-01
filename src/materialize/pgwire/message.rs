// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

use bytes::Bytes;

use super::oid;
use crate::repr::{Datum, FType, Type};

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

#[derive(Debug)]
pub enum BackendMessage {
    AuthenticationOk,
    CommandComplete {
        tag: &'static str,
    },
    EmptyQueryResponse,
    ReadyForQuery,
    RowDescription(Vec<FieldDescription>),
    DataRow(Vec<FieldValue>),
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

#[derive(Copy, Clone, Debug)]
pub enum FieldFormat {
    Text = 0,
    Binary = 1,
}

#[derive(Debug)]
pub enum FieldValue {
    Null,
    Datum(Datum),
}

pub fn row_description_from_type(typ: &Type) -> Vec<FieldDescription> {
    match &typ.ftype {
        FType::Tuple(types) => types
            .iter()
            .map(|typ| FieldDescription {
                name: typ
                    .name
                    .as_ref()
                    .unwrap_or(&"?column?".into())
                    .to_owned(),
                table_id: 0,
                column_id: 0,
                type_oid: match typ.ftype {
                    FType::String => oid::STRING,
                    FType::Int64 => oid::INT,
                    _ => unimplemented!(),
                },
                type_len: match typ.ftype {
                    FType::String => -1,
                    FType::Int64 => 8,
                    _ => unimplemented!(),
                },
                type_mod: -1,
                format: FieldFormat::Text,
            })
            .collect(),
        _ => unimplemented!(),
    }
}
