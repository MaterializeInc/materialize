// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use enum_kinds::EnumKind;
use itertools::Itertools;
use mz_adapter::session::TransactionCode;
use mz_pgwire_common::ErrorResponse;
use mz_repr::{ColumnName, RelationDesc};

/// Internal representation of a backend [message]
///
/// [message]: https://www.postgresql.org/docs/11/protocol-message-formats.html
#[derive(Debug, EnumKind)]
#[enum_kind(BackendMessageKind)]
pub enum BackendMessage {
    AuthenticationOk,
    AuthenticationCleartextPassword,
    CommandComplete {
        tag: String,
    },
    EmptyQueryResponse,
    ReadyForQuery(TransactionCode),
    RowDescription(Vec<FieldDescription>),
    DataRow(Vec<Option<mz_pgrepr::Value>>),
    ParameterStatus(&'static str, String),
    BackendKeyData {
        conn_id: u32,
        secret_key: u32,
    },
    ParameterDescription(Vec<mz_pgrepr::Type>),
    PortalSuspended,
    NoData,
    ParseComplete,
    BindComplete,
    CloseComplete,
    ErrorResponse(ErrorResponse),
    CopyInResponse {
        overall_format: mz_pgwire_common::Format,
        column_formats: Vec<mz_pgwire_common::Format>,
    },
    CopyOutResponse {
        overall_format: mz_pgwire_common::Format,
        column_formats: Vec<mz_pgwire_common::Format>,
    },
    CopyData(Vec<u8>),
    CopyDone,
}

impl From<ErrorResponse> for BackendMessage {
    fn from(err: ErrorResponse) -> BackendMessage {
        BackendMessage::ErrorResponse(err)
    }
}

#[derive(Debug)]
pub struct FieldDescription {
    pub name: ColumnName,
    pub table_id: u32,
    pub column_id: u16,
    pub type_oid: u32,
    pub type_len: i16,
    pub type_mod: i32,
    pub format: mz_pgwire_common::Format,
}

pub fn encode_row_description(
    desc: &RelationDesc,
    formats: &[mz_pgwire_common::Format],
) -> Vec<FieldDescription> {
    desc.iter()
        .zip_eq(formats)
        .map(|((name, typ), format)| {
            let pg_type = mz_pgrepr::Type::from(&typ.scalar_type);
            FieldDescription {
                name: name.clone(),
                table_id: 0,
                column_id: 0,
                type_oid: pg_type.oid(),
                type_len: pg_type.typlen(),
                type_mod: pg_type.typmod(),
                format: *format,
            }
        })
        .collect()
}
