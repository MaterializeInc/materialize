// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helpers for handling events from a Webhook source.

use std::collections::BTreeMap;

use mz_adapter::{AdapterError, AppendWebhookResponse};
use mz_ore::str::StrExt;
use mz_repr::adt::jsonb::JsonbPacker;
use mz_repr::{ColumnType, Datum, Row, ScalarType};
use mz_storage_client::controller::StorageError;

use anyhow::Context;
use axum::extract::Path;
use axum::response::IntoResponse;
use axum::Extension;
use bytes::Bytes;
use http::StatusCode;
use thiserror::Error;

use crate::http::Delayed;

pub async fn handle_webhook(
    Extension(client): Extension<Delayed<mz_adapter::Client>>,
    Path((database, schema, name)): Path<(String, String, String)>,
    headers: http::HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Get our adapter client.
    let client = client.await.context("receiving client")?;
    let conn_id = client.new_conn_id().context("allocate connection id")?;

    // Get an appender for the provided object, if that object exists.
    let maybe_appender = client
        .append_webhook(database.clone(), schema.clone(), name.clone(), conn_id)
        .await?;
    let Some(AppendWebhookResponse { tx, body_ty, header_ty }) = maybe_appender else {
        let desc = format!("{database}.{schema}.{name}");
        return Err(WebhookError::NotFound(desc.quoted().to_string()));
    };

    // Pack our body and headers into a Row.
    let row = pack_row(body, headers, body_ty, header_ty)?;

    // Send the row to get appended.
    tx.append(vec![(row, 1)]).await?;

    Ok(())
}

/// Given the body and headers of a request, pack them into a [`Row`].
fn pack_row(
    body: Bytes,
    headers: http::HeaderMap,
    body_ty: ColumnType,
    header_ty: Option<ColumnType>,
) -> Result<Row, WebhookError> {
    // If we're including headers then we have two columns.
    let num_cols = header_ty.as_ref().map(|_| 2).unwrap_or(1);

    // Pack our row.
    let mut row = Row::with_capacity(num_cols);
    let mut packer = row.packer();

    // Pack our body into a row.
    match body_ty.scalar_type {
        ScalarType::Bytes => packer.push(Datum::Bytes(&body[..])),
        ty @ ScalarType::String => {
            let s = std::str::from_utf8(&body).map_err(|m| WebhookError::InvalidBody {
                ty,
                msg: m.to_string(),
            })?;
            packer.push(Datum::String(s));
        }
        ty @ ScalarType::Jsonb => {
            let jsonb_packer = JsonbPacker::new(&mut packer);
            jsonb_packer
                .pack_slice(&body[..])
                .map_err(|m| WebhookError::InvalidBody {
                    ty,
                    msg: m.to_string(),
                })?;
        }
        ty => {
            Err(anyhow::anyhow!(
                "Invalid body type for Webhook source: {ty:?}"
            ))?;
        }
    }

    if header_ty.is_some() {
        // Pack our headers into a row, if we're supposed to.
        let mut headers_s = BTreeMap::new();
        for (name, val) in headers.iter() {
            if let Ok(val_s) = val.to_str() {
                headers_s.insert(name.as_str(), val_s);
            }
        }

        packer.push_dict(
            headers_s
                .iter()
                .map(|(name, val)| (*name, Datum::String(*val))),
        );
    }

    Ok(row)
}

/// Errors we can encounter when appending data to a Webhook Source.
///
/// Webhook sources are a bit special since they are handled by `environmentd` (all other sources
/// are handled by `clusterd`) and data is "pushed" to them (all other source pull data). The
/// errors also generally need to map to HTTP status codes that we can use to respond to a webhook
/// request. As such, webhook errors don't cleanly map to any existing error type, hence the
/// existence of this error type.
#[derive(Error, Debug)]
pub enum WebhookError {
    #[error("no object was found at the path {}", .0.quoted())]
    NotFound(String),
    #[error("this feature is currently unsupported: {0}")]
    Unsupported(&'static str),
    #[error("failed to deserialize body as {ty:?}: {msg}")]
    InvalidBody { ty: ScalarType, msg: String },
    #[error("internal storage failure! {0:?}")]
    InternalStorageError(StorageError),
    #[error("internal adapter failure! {0:?}")]
    InternalAdapterError(AdapterError),
    #[error("internal failure! {0:?}")]
    Internal(#[from] anyhow::Error),
}

impl From<StorageError> for WebhookError {
    fn from(err: StorageError) -> Self {
        match err {
            // TODO(parkmycar): Maybe map this to a HTTP 410 Gone instead of 404?
            StorageError::IdentifierMissing(id) => WebhookError::NotFound(id.to_string()),
            e => WebhookError::InternalStorageError(e),
        }
    }
}

impl From<AdapterError> for WebhookError {
    fn from(err: AdapterError) -> Self {
        match err {
            AdapterError::Unsupported(feat) => WebhookError::Unsupported(feat),
            e => WebhookError::InternalAdapterError(e),
        }
    }
}

impl IntoResponse for WebhookError {
    fn into_response(self) -> axum::response::Response {
        match self {
            e @ WebhookError::NotFound(_) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
            e @ WebhookError::Unsupported(_) => {
                (StatusCode::BAD_REQUEST, e.to_string()).into_response()
            }
            e @ WebhookError::InvalidBody { .. } => {
                (StatusCode::BAD_REQUEST, e.to_string()).into_response()
            }
            e @ WebhookError::InternalStorageError(StorageError::ResourceExhausted(_)) => {
                (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response()
            }
            e @ WebhookError::InternalStorageError(_)
            | e @ WebhookError::InternalAdapterError(_)
            | e @ WebhookError::Internal(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::response::IntoResponse;
    use bytes::Bytes;
    use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
    use mz_adapter::AdapterError;
    use mz_repr::{ColumnType, GlobalId, ScalarType};
    use mz_storage_client::controller::StorageError;
    use proptest::prelude::*;

    use super::{pack_row, WebhookError};

    #[mz_ore::test]
    fn smoke_test_adapter_error_response_status() {
        // Unsupported errors get mapped to a certain response status.
        let resp = WebhookError::from(AdapterError::Unsupported("test")).into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // All other errors should map to 500.
        let resp = WebhookError::from(AdapterError::Internal("test".to_string())).into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[mz_ore::test]
    fn smoke_test_storage_error_response_status() {
        // Resource exhausted should get mapped to a specific status code.
        let resp = WebhookError::from(StorageError::ResourceExhausted("test")).into_response();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);

        // IdentifierMissing should also get mapped to a specific status code.
        let resp =
            WebhookError::from(StorageError::IdentifierMissing(GlobalId::User(42))).into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        // All other errors should map to 500.
        let resp = WebhookError::from(AdapterError::Internal("test".to_string())).into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[mz_ore::test]
    fn test_pack_invalid_column_type() {
        let body = Bytes::from(vec![42, 42, 42, 42]);
        let headers: HeaderMap<HeaderValue> = HeaderMap::default();

        // Int64 is an invalid column type for a webhook source.
        let body_ty = ColumnType {
            scalar_type: ScalarType::Int64,
            nullable: false,
        };
        assert!(pack_row(body, headers, body_ty, None).is_err());
    }

    fn headermap_strat() -> impl Strategy<Value = HeaderMap> {
        // TODO(parkmycar): We can probably be better about generating a random but valid
        // HeaderMap. This will do for now though.
        let name_strat = proptest::string::string_regex("[a-zA-Z0-9]+").unwrap();
        let val_strat = proptest::string::string_regex("[a-zA-Z0-9]*").unwrap();

        proptest::collection::vec((name_strat, val_strat), 0..32).prop_map(|pairs| {
            pairs
                .into_iter()
                .map(|(key, val)| {
                    (
                        HeaderName::from_bytes(key.as_bytes()).unwrap(),
                        HeaderValue::from_str(&val).unwrap(),
                    )
                })
                .collect()
        })
    }

    proptest! {
        #[mz_ore::test]
        fn proptest_pack_row_never_panics(
            body: Vec<u8>,
            headers in headermap_strat(),
            body_ty: ColumnType,
            header_ty: Option<ColumnType>
        ) {
            let body = Bytes::from(body);
            // Call this method to make sure it doesn't panic.
            let _ = pack_row(body, headers, body_ty, header_ty);
        }

        #[mz_ore::test]
        fn proptest_pack_row_succeeds_for_bytes(
            body: Vec<u8>,
            headers in headermap_strat(),
            include_headers: bool,
        ) {
            let body = Bytes::from(body);

            let body_ty = ColumnType { scalar_type: ScalarType::Bytes, nullable: false };
            let header_ty = include_headers.then(|| ColumnType {
                scalar_type: ScalarType::Map {
                    value_type: Box::new(ScalarType::String),
                    custom_id: None,
                },
                nullable: false,
            });

            prop_assert!(pack_row(body, headers, body_ty, header_ty).is_ok());
        }

        #[mz_ore::test]
        fn proptest_pack_row_succeeds_for_strings(
            body: String,
            headers in headermap_strat(),
            include_headers: bool,
        ) {
            let body = Bytes::from(body);

            let body_ty = ColumnType { scalar_type: ScalarType::String, nullable: false };
            let header_ty = include_headers.then(|| ColumnType {
                scalar_type: ScalarType::Map {
                    value_type: Box::new(ScalarType::String),
                    custom_id: None,
                },
                nullable: false,
            });

            prop_assert!(pack_row(body, headers, body_ty, header_ty).is_ok());
        }
    }
}
