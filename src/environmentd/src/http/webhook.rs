// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helpers for handling events from a Webhook source.

use std::sync::Arc;

use http::header::RETRY_AFTER;
use mz_adapter::{AdapterError, AppendWebhookError, AppendWebhookResponse};
use mz_ore::collections::HashSet;
use mz_ore::str::StrExt;
use mz_repr::adt::jsonb::JsonbPacker;
use mz_repr::{ColumnType, Datum, Row, ScalarType};
use mz_sql::plan::{WebhookHeaderFilters, WebhookHeaders};
use mz_storage_types::controller::StorageError;

use anyhow::Context;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, StatusCode};
use thiserror::Error;

pub async fn handle_webhook(
    State(client): State<mz_adapter::Client>,
    Path((database, schema, name)): Path<(String, String, String)>,
    headers: http::HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Record the time we receive the request, for use if validation checks the current timestamp.
    let received_at = client.now();
    let conn_id = client.new_conn_id().context("allocate connection id")?;

    // Check if a header was provided twice, if so, return an error.
    let mut seen_set = HashSet::with_capacity(headers.keys_len());
    for (name, _val) in headers.iter() {
        if !seen_set.insert(name) {
            let msg = format!("{} provided more than once", name.as_str());
            return Err(WebhookError::InvalidHeaders(msg));
        }
    }
    let headers = Arc::new(headers);

    // Get an appender for the provided object, if that object exists.
    let AppendWebhookResponse {
        tx,
        body_ty,
        header_tys,
        validator,
        expected_catalog_revision,
    } = client
        .get_webhook_appender(database, schema, name, conn_id)
        .await?;

    // If this source requires validation, then validate!
    if let Some(validator) = validator {
        let valid = validator
            .eval(Bytes::clone(&body), Arc::clone(&headers), received_at)
            .await?;
        if !valid {
            return Err(WebhookError::ValidationFailed);
        }
    }

    // Pack our body and headers into a Row.
    let row = pack_row(body, &headers, body_ty, header_tys)?;

    // Send the row to get appended.
    tx.append(vec![(row, 1)], expected_catalog_revision).await?;

    Ok::<_, WebhookError>(())
}

/// Given the body and headers of a request, pack them into a [`Row`].
fn pack_row(
    body: Bytes,
    headers: &http::HeaderMap,
    body_ty: ColumnType,
    header_tys: WebhookHeaders,
) -> Result<Row, WebhookError> {
    // Try to estimate how many bytes it will take to store headers, within some bounds.
    const BYTES_PER_HEADER: usize = 16;
    const MAX_HEADER_ESTIMATE: usize = 1 * 1024;

    // 1 column for the body plus however many are needed for the headers.
    let num_cols = 1 + header_tys.num_columns();
    let mut num_cols_written = 0;

    // Pack our row, trying to estimate how many bytes we'll need.
    let estimate_header_bytes = header_tys
        .header_column
        .as_ref()
        .map(|_| headers.len().saturating_mul(BYTES_PER_HEADER))
        .unwrap_or(0);
    let estimate_header_bytes = estimate_header_bytes.min(MAX_HEADER_ESTIMATE);

    let mut row = Row::with_capacity(body.len().saturating_add(estimate_header_bytes));
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
    num_cols_written += 1;

    // Pack the headers into our row, if required.
    if let Some(filters) = header_tys.header_column {
        packer.push_dict(
            filter_headers(headers, &filters).map(|(name, val)| (name, Datum::String(val))),
        );
        num_cols_written += 1;
    }

    // Pack the mapped headers.
    for idx in num_cols_written..num_cols {
        let (header_name, use_bytes) = header_tys
            .mapped_headers
            .get(&idx)
            .ok_or_else(|| anyhow::anyhow!("Invalid header column index {idx}"))?;
        let header = headers.get(header_name);
        let datum = match header {
            Some(h) if *use_bytes => Datum::Bytes(h.as_bytes()),
            Some(h) => match h.to_str() {
                Ok(s) => Datum::String(s),
                Err(err) => {
                    tracing::debug!(?err, "Mapped header is non UTF-8");
                    Datum::Null
                }
            },
            None => Datum::Null,
        };
        packer.push(datum);
    }

    Ok(row)
}

fn filter_headers<'a: 'b, 'b>(
    headers: &'a http::HeaderMap,
    filters: &'b WebhookHeaderFilters,
) -> impl Iterator<Item = (&'a str, &'a str)> + 'b {
    headers
        .iter()
        .filter(|(header_name, _val)| {
            // If our block list is empty, then don't filter anything.
            filters.block.is_empty() || !filters.block.contains(header_name.as_str())
        })
        .filter(|(header_name, _val)| {
            // If our allow list is empty, then don't filter anything.
            filters.allow.is_empty() || filters.allow.contains(header_name.as_str())
        })
        .filter_map(|(header_name, header_val)| {
            // Note: we skip values that are not valid UTF-8.
            let header_val = header_val.to_str().ok()?;
            Some((header_name.as_str(), header_val))
        })
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
    #[error("the required auth could not be found")]
    SecretMissing,
    #[error("this feature is currently unsupported: {0}")]
    Unsupported(&'static str),
    #[error("headers of request were invalid: {0}")]
    InvalidHeaders(String),
    #[error("failed to deserialize body as {ty:?}: {msg}")]
    InvalidBody { ty: ScalarType, msg: String },
    #[error("failed to validate the request")]
    ValidationFailed,
    #[error("error occurred while running validation")]
    ValidationError,
    #[error("service unavailable")]
    Unavailable,
    #[error("service temporarily unavailable, please retry")]
    TemporarilyUnavailable,
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
            StorageError::IdentifierMissing(id) | StorageError::IdentifierInvalid(id) => {
                WebhookError::NotFound(id.to_string())
            }
            StorageError::ShuttingDown(_) => WebhookError::Unavailable,
            StorageError::RaceDetected => WebhookError::TemporarilyUnavailable,
            e => WebhookError::InternalStorageError(e),
        }
    }
}

impl From<AdapterError> for WebhookError {
    fn from(err: AdapterError) -> Self {
        match err {
            AdapterError::Unsupported(feat) => WebhookError::Unsupported(feat),
            AdapterError::UnknownWebhookSource {
                database,
                schema,
                name,
            } => WebhookError::NotFound(format!("'{database}.{schema}.{name}'")),
            e => WebhookError::InternalAdapterError(e),
        }
    }
}

impl From<AppendWebhookError> for WebhookError {
    fn from(err: AppendWebhookError) -> Self {
        match err {
            AppendWebhookError::MissingSecret => WebhookError::SecretMissing,
            AppendWebhookError::ValidationError => WebhookError::ValidationError,
            AppendWebhookError::NonUtf8Body => WebhookError::InvalidBody {
                ty: ScalarType::String,
                msg: "invalid".to_string(),
            },
            AppendWebhookError::InternalError => {
                WebhookError::Internal(anyhow::anyhow!("failed to run validation"))
            }
        }
    }
}

impl IntoResponse for WebhookError {
    fn into_response(self) -> axum::response::Response {
        match self {
            e @ WebhookError::NotFound(_) | e @ WebhookError::SecretMissing => {
                (StatusCode::NOT_FOUND, e.to_string()).into_response()
            }
            e @ WebhookError::Unsupported(_)
            | e @ WebhookError::InvalidBody { .. }
            | e @ WebhookError::ValidationFailed
            | e @ WebhookError::ValidationError => {
                (StatusCode::BAD_REQUEST, e.to_string()).into_response()
            }
            e @ WebhookError::InvalidHeaders(_) => {
                (StatusCode::UNAUTHORIZED, e.to_string()).into_response()
            }
            e @ WebhookError::Unavailable => {
                (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response()
            }
            e @ WebhookError::TemporarilyUnavailable => {
                // Indicates the caller should retry after 3 seconds, which we arbitrarily picked.
                let mut headers = HeaderMap::new();
                headers.insert(RETRY_AFTER, HeaderValue::from_static("3"));

                (StatusCode::SERVICE_UNAVAILABLE, headers, e.to_string()).into_response()
            }
            e @ WebhookError::InternalStorageError(StorageError::ResourceExhausted(_)) => {
                (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response()
            }
            e @ WebhookError::InternalStorageError(_)
            | e @ WebhookError::InternalAdapterError(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
            WebhookError::Internal(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                e.root_cause().to_string(),
            )
                .into_response(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use axum::response::IntoResponse;
    use bytes::Bytes;
    use http::{HeaderMap, StatusCode};
    use mz_adapter::AdapterError;
    use mz_repr::{ColumnType, GlobalId, ScalarType};
    use mz_sql::plan::{WebhookHeaderFilters, WebhookHeaders};
    use mz_storage_types::controller::StorageError;
    use proptest::prelude::*;

    use super::{filter_headers, pack_row, WebhookError};

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
        let headers = HeaderMap::default();

        // Int64 is an invalid column type for a webhook source.
        let body_ty = ColumnType {
            scalar_type: ScalarType::Int64,
            nullable: false,
        };
        assert!(pack_row(body, &headers, body_ty, WebhookHeaders::default()).is_err());
    }

    #[mz_ore::test]
    fn smoke_test_filter_headers() {
        let block = BTreeSet::from(["foo".to_string()]);
        let allow = BTreeSet::from(["bar".to_string()]);

        let mut headers = HeaderMap::default();
        headers.insert("foo", "1".parse().unwrap());
        headers.insert("bar", "2".parse().unwrap());
        headers.insert("baz", "3".parse().unwrap());

        let mut filters = WebhookHeaderFilters::default();
        filters.block = block.clone();

        let mut h = filter_headers(&headers, &filters);
        assert_eq!(h.next().unwrap().0, "bar");
        assert_eq!(h.next().unwrap().0, "baz");
        assert!(h.next().is_none());

        let mut filters = WebhookHeaderFilters::default();
        filters.allow = allow.clone();

        let mut h = filter_headers(&headers, &filters);
        assert_eq!(h.next().unwrap().0, "bar");
        assert!(h.next().is_none());

        let mut filters = WebhookHeaderFilters::default();
        filters.allow = allow;
        filters.block = block;

        let mut h = filter_headers(&headers, &filters);
        assert_eq!(h.next().unwrap().0, "bar");
        assert!(h.next().is_none());
    }

    #[mz_ore::test]
    fn filter_headers_block_overrides_allow() {
        let block = BTreeSet::from(["foo".to_string()]);
        let allow = block.clone();

        let mut headers = HeaderMap::default();
        headers.insert("foo", "1".parse().unwrap());
        headers.insert("bar", "2".parse().unwrap());
        headers.insert("baz", "3".parse().unwrap());
        let filters = WebhookHeaderFilters { block, allow };

        // We should yield nothing since we block the only thing we allow.
        let mut h = filter_headers(&headers, &filters);
        assert!(h.next().is_none());
    }

    proptest! {
        #[mz_ore::test]
        fn proptest_pack_row_never_panics(
            body: Vec<u8>,
            body_ty: ColumnType,
            headers: BTreeMap<String, String>,
            non_existent_headers: Vec<String>,
            block: BTreeSet<String>,
            allow: BTreeSet<String>,
        ) {
            let body = Bytes::from(body);

            // Include the headers column with a random set of block and allow.
            let filters = WebhookHeaderFilters { block, allow };
            // Include half of the existing headers, append on some non-existing ones too.
            let mut use_bytes = false;
            let mapped_headers = headers
                .keys()
                .take(headers.len() / 2)
                .chain(non_existent_headers.iter())
                .cloned()
                .enumerate()
                .map(|(idx, name)| {
                    use_bytes = !use_bytes;
                    (idx + 2, (name, use_bytes))
                })
                .collect();
            let header_tys = WebhookHeaders {
                header_column: Some(filters),
                mapped_headers,
            };

            let headers: HeaderMap = headers
                .into_iter()
                .filter_map(|(name, val)| {
                    let name = http::HeaderName::try_from(name).ok()?;
                    let value = http::HeaderValue::try_from(val).ok()?;
                    Some((name, value))
                })
                .collect();

            // Call this method to make sure it doesn't panic.
            let _ = pack_row(body, &headers, body_ty, header_tys);
        }

        #[mz_ore::test]
        fn proptest_pack_row_succeeds_for_bytes(
            body: Vec<u8>,
            headers: BTreeMap<String, String>,
            include_headers: bool,
        ) {
            let body = Bytes::from(body);
            let headers: HeaderMap = headers
                .into_iter()
                .filter_map(|(name, val)| {
                    let name = http::HeaderName::try_from(name).ok()?;
                    let value = http::HeaderValue::try_from(val).ok()?;
                    Some((name, value))
                })
                .collect();

            let body_ty = ColumnType { scalar_type: ScalarType::Bytes, nullable: false };
            let mut header_tys = WebhookHeaders::default();
            header_tys.header_column = include_headers.then(Default::default);

            prop_assert!(pack_row(body, &headers, body_ty, header_tys).is_ok());
        }

        #[mz_ore::test]
        fn proptest_pack_row_succeeds_for_strings(
            body: String,
            headers: BTreeMap<String, String>,
            include_headers: bool,
        ) {
            let body = Bytes::from(body);
            let headers: HeaderMap = headers
                .into_iter()
                .filter_map(|(name, val)| {
                    let name = http::HeaderName::try_from(name).ok()?;
                    let value = http::HeaderValue::try_from(val).ok()?;
                    Some((name, value))
                })
                .collect();

            let body_ty = ColumnType { scalar_type: ScalarType::String, nullable: false };
            let mut header_tys = WebhookHeaders::default();
            header_tys.header_column = include_headers.then(Default::default);

            prop_assert!(pack_row(body, &headers, body_ty, header_tys).is_ok());
        }

        #[mz_ore::test]
        fn proptest_pack_row_succeeds_for_selective_headers(
            body: String,
            headers: BTreeMap<String, String>,
            include_headers: bool,
            non_existent_headers: Vec<String>,
            block: BTreeSet<String>,
            allow: BTreeSet<String>,
        ) {
            let body = Bytes::from(body);
            let body_ty = ColumnType { scalar_type: ScalarType::String, nullable: false };

            // Include the headers column with a random set of block and allow.
            let filters = WebhookHeaderFilters { block, allow };
            // Include half of the existing headers, append on some non-existing ones too.
            let mut use_bytes = false;
            let column_offset = if include_headers { 2 } else { 1 };
            let mapped_headers = headers
                .keys()
                .take(headers.len() / 2)
                .chain(non_existent_headers.iter())
                .cloned()
                .enumerate()
                .map(|(idx, name)| {
                    use_bytes = !use_bytes;
                    (idx + column_offset, (name, use_bytes))
                })
                .collect();
            let header_tys = WebhookHeaders {
                header_column: include_headers.then_some(filters),
                mapped_headers,
            };

            let headers: HeaderMap = headers
                .into_iter()
                .filter_map(|(name, val)| {
                    let name = http::HeaderName::try_from(name).ok()?;
                    let value = http::HeaderValue::try_from(val).ok()?;
                    Some((name, value))
                })
                .collect();

            prop_assert!(pack_row(body, &headers, body_ty, header_tys).is_ok());
        }
    }
}
