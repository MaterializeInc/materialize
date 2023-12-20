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
use std::sync::Arc;

use mz_adapter::{AdapterError, AppendWebhookError, AppendWebhookResponse, WebhookAppenderCache};
use mz_ore::retry::{Retry, RetryResult};
use mz_ore::str::StrExt;
use mz_repr::adt::jsonb::JsonbPacker;
use mz_repr::{ColumnType, Datum, Row, ScalarType};
use mz_sql::plan::{WebhookHeaderFilters, WebhookHeaders};
use mz_storage_types::controller::StorageError;

use axum::extract::{Path, State};
use axum::response::IntoResponse;
use bytes::Bytes;
use http::StatusCode;
use thiserror::Error;

use crate::http::WebhookState;

pub async fn handle_webhook(
    State(WebhookState {
        adapter_client,
        webhook_cache,
    }): State<WebhookState>,
    Path((database, schema, name)): Path<(String, String, String)>,
    headers: http::HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Collect headers into a map, while converting them into strings.
    let mut headers_s = BTreeMap::new();
    for (name, val) in headers.iter() {
        if let Ok(val_s) = val.to_str().map(|s| s.to_string()) {
            // If a header is included more than once, bail returning an error to the user.
            let existing = headers_s.insert(name.as_str().to_string(), val_s);
            if existing.is_some() {
                let msg = format!("{} provided more than once", name.as_str());
                return Err(WebhookError::InvalidHeaders(msg));
            }
        }
    }
    let headers = Arc::new(headers_s);

    // Append to the webhook source, retrying if we race with a concurrent `ALTER SOURCE` op.
    Retry::default()
        .max_tries(2)
        .retry_async(|_| async {
            let result = append_webhook(
                &adapter_client,
                &webhook_cache,
                &database,
                &schema,
                &name,
                &body,
                &headers,
            )
            .await;

            // Note: think carefully before adding more errors here, we need to make sure we don't
            // append data more than once.
            match result {
                Ok(()) => RetryResult::Ok(()),
                Err(e @ WebhookError::ChannelClosed) => RetryResult::RetryableErr(e),
                Err(e) => RetryResult::FatalErr(e),
            }
        })
        .await?;

    Ok::<_, WebhookError>(())
}

/// Append the provided `body` and `headers` to the webhook source identified via `database`,
/// `schema`, and `name`.
async fn append_webhook(
    adapter_client: &mz_adapter::Client,
    webhook_cache: &WebhookAppenderCache,
    database: &str,
    schema: &str,
    name: &str,
    body: &Bytes,
    headers: &Arc<BTreeMap<String, String>>,
) -> Result<(), WebhookError> {
    // Shenanigans to get the types working for the async retry.
    let (database, schema, name) = (database.to_string(), schema.to_string(), name.to_string());

    // Record the time we receive the request, for use if validation checks the current timestamp.
    let received_at = adapter_client.now();

    // Get an appender for the provided object, if that object exists.
    let AppendWebhookResponse {
        tx,
        body_ty,
        header_tys,
        validator,
    } = async {
        let mut guard = webhook_cache.entries.lock().await;

        // Remove the appender from our map, only re-insert it, if it's valid.
        match guard.remove(&(database.clone(), schema.clone(), name.clone())) {
            Some(appender) if !appender.tx.is_closed() => {
                guard.insert((database, schema, name), appender.clone());
                Ok::<_, AdapterError>(appender)
            }
            // We don't have a valid appender, so we need to get one.
            //
            // Note: we hold the lock while we acquire and appender to prevent a dogpile.
            _ => {
                tracing::info!(?database, ?schema, ?name, "fetching webhook appender");
                adapter_client.metrics().webhook_get_appender.inc();

                // Acquire and cache a new appender.
                let appender = adapter_client
                    .get_webhook_appender(database.clone(), schema.clone(), name.clone())
                    .await?;

                guard.insert((database, schema, name), appender.clone());

                Ok(appender)
            }
        }
    }
    .await?;

    // If this source requires validation, then validate!
    if let Some(validator) = validator {
        let valid = validator
            .eval(Bytes::clone(body), Arc::clone(headers), received_at)
            .await?;
        if !valid {
            return Err(WebhookError::ValidationFailed);
        }
    }

    // Pack our body and headers into a Row.
    let row = pack_row(Bytes::clone(body), headers, body_ty, header_tys)?;

    // Send the row to get appended.
    tx.append(vec![(row, 1)]).await?;

    Ok(())
}

/// Given the body and headers of a request, pack them into a [`Row`].
fn pack_row(
    body: Bytes,
    headers: &BTreeMap<String, String>,
    body_ty: ColumnType,
    header_tys: WebhookHeaders,
) -> Result<Row, WebhookError> {
    // 1 column for the body plus however many are needed for the headers.
    let num_cols = 1 + header_tys.num_columns();
    let mut num_cols_written = 0;

    // Pack our row.
    let mut row = Row::with_capacity(body.len());
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
            Some(h) => Datum::String(h),
            None => Datum::Null,
        };
        packer.push(datum);
    }

    Ok(row)
}

fn filter_headers<'a: 'b, 'b>(
    headers: &'a BTreeMap<String, String>,
    filters: &'b WebhookHeaderFilters,
) -> impl Iterator<Item = (&'a str, &'a str)> + 'b {
    headers
        .iter()
        .filter(|(header_name, _val)| {
            // If our block list is empty, then don't filter anything.
            filters.block.is_empty() || !filters.block.contains(*header_name)
        })
        .filter(|(header_name, _val)| {
            // If our allow list is empty, then don't filter anything.
            filters.allow.is_empty() || filters.allow.contains(*header_name)
        })
        .map(|(key, val)| (key.as_str(), val.as_str()))
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
    #[error("internal service temporarily closed")]
    ChannelClosed,
    #[error("internal storage failure! {0:?}")]
    InternalStorageError(StorageError),
    #[error("internal adapter failure! {0:?}")]
    InternalAdapterError(AdapterError),
    #[error("internal failure! {0:?}")]
    Internal(#[from] anyhow::Error),
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
            AppendWebhookError::ChannelClosed => WebhookError::ChannelClosed,
            AppendWebhookError::StorageError(storage_err) => {
                match storage_err {
                    // TODO(parkmycar): Maybe map this to a HTTP 410 Gone instead of 404?
                    StorageError::IdentifierMissing(id) | StorageError::IdentifierInvalid(id) => {
                        WebhookError::NotFound(id.to_string())
                    }
                    StorageError::ShuttingDown(_) => WebhookError::Unavailable,
                    e => WebhookError::InternalStorageError(e),
                }
            }
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
            e @ WebhookError::InternalStorageError(StorageError::ResourceExhausted(_)) => {
                (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response()
            }
            e @ WebhookError::ChannelClosed
            | e @ WebhookError::InternalStorageError(_)
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
    use http::StatusCode;
    use mz_adapter::{AdapterError, AppendWebhookError};
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
        let resp = WebhookError::from(AppendWebhookError::StorageError(
            StorageError::ResourceExhausted("test"),
        ))
        .into_response();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);

        // IdentifierMissing should also get mapped to a specific status code.
        let resp = WebhookError::from(AppendWebhookError::StorageError(
            StorageError::IdentifierMissing(GlobalId::User(42)),
        ))
        .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        // All other errors should map to 500.
        let resp = WebhookError::from(AdapterError::Internal("test".to_string())).into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[mz_ore::test]
    fn test_pack_invalid_column_type() {
        let body = Bytes::from(vec![42, 42, 42, 42]);
        let headers = BTreeMap::default();

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

        let headers = BTreeMap::from([
            ("foo".to_string(), "1".to_string()),
            ("bar".to_string(), "2".to_string()),
            ("baz".to_string(), "3".to_string()),
        ]);
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

        let headers = BTreeMap::from([
            ("foo".to_string(), "1".to_string()),
            ("bar".to_string(), "2".to_string()),
            ("baz".to_string(), "3".to_string()),
        ]);
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

            prop_assert!(pack_row(body, &headers, body_ty, header_tys).is_ok());
        }
    }
}
