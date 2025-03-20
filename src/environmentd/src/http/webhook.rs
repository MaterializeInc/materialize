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

use mz_adapter::{AppendWebhookError, AppendWebhookResponse, WebhookAppenderCache};
use mz_ore::cast::CastFrom;
use mz_ore::retry::{Retry, RetryResult};
use mz_ore::str::StrExt;
use mz_repr::adt::jsonb::Jsonb;
use mz_repr::{Datum, Row, RowPacker, ScalarType, Timestamp};
use mz_sql::plan::{WebhookBodyFormat, WebhookHeaderFilters, WebhookHeaders};
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
                Err(e @ AppendWebhookError::ChannelClosed) => RetryResult::RetryableErr(e),
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
) -> Result<(), AppendWebhookError> {
    // Shenanigans to get the types working for the async retry.
    let (database, schema, name) = (database.to_string(), schema.to_string(), name.to_string());

    // Record the time we receive the request, for use if validation checks the current timestamp.
    let received_at = adapter_client.now();

    // Get an appender for the provided object, if that object exists.
    let AppendWebhookResponse {
        tx,
        body_format,
        header_tys,
        validator,
    } = async {
        let mut guard = webhook_cache.entries.lock().await;

        // Remove the appender from our map, only re-insert it, if it's valid.
        match guard.remove(&(database.clone(), schema.clone(), name.clone())) {
            Some(appender) if !appender.tx.is_closed() => {
                guard.insert((database, schema, name), appender.clone());
                Ok::<_, AppendWebhookError>(appender)
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

    // These must happen before validation as we do not know if validation or
    // packing will succeed and appending will begin
    tx.increment_messages_received(1);
    tx.increment_bytes_received(u64::cast_from(body.len()));

    // If this source requires validation, then validate!
    if let Some(validator) = validator {
        let valid = validator
            .eval(Bytes::clone(body), Arc::clone(headers), received_at)
            .await?;
        if !valid {
            return Err(AppendWebhookError::ValidationFailed);
        }
    }

    // Pack our body and headers into a Row.
    let rows = pack_rows(body, &body_format, headers, &header_tys)?;

    // Send the row to get appended.
    tx.append(rows).await?;

    Ok(())
}

/// Packs the body and headers of a webhook request into as many rows as necessary.
///
/// TODO(parkmycar): Should we be consolidating the returned Rows here? Presumably something in
/// storage would already be doing it, so no need to do it twice?
fn pack_rows(
    body: &[u8],
    body_format: &WebhookBodyFormat,
    headers: &BTreeMap<String, String>,
    header_tys: &WebhookHeaders,
) -> Result<Vec<(Row, i64)>, AppendWebhookError> {
    // This method isn't that "deep" but it reflects the way we intend for the packing process to
    // work and makes testing easier.
    let rows = transform_body(body, body_format)?
        .into_iter()
        .map(|row| pack_header(row, headers, header_tys).map(|row| (row, 1)))
        .collect::<Result<_, _>>()?;
    Ok(rows)
}

/// Transforms the body of a webhook request into a `Vec<BodyRow>`.
fn transform_body(
    body: &[u8],
    format: &WebhookBodyFormat,
) -> Result<Vec<BodyRow>, AppendWebhookError> {
    let rows = match format {
        WebhookBodyFormat::Bytes => {
            vec![Row::pack_slice(&[Datum::Bytes(body)])]
        }
        WebhookBodyFormat::Text => {
            let s = std::str::from_utf8(body)
                .map_err(|m| AppendWebhookError::InvalidUtf8Body { msg: m.to_string() })?;
            vec![Row::pack_slice(&[Datum::String(s)])]
        }
        WebhookBodyFormat::Json { array } => {
            let objects = serde_json::Deserializer::from_slice(body)
                // Automatically expand multiple JSON objects delimited by whitespace, e.g.
                // newlines, into a single batch.
                .into_iter::<serde_json::Value>()
                // Optionally expand a JSON array into separate rows, if requested.
                .flat_map(|value| match value {
                    Ok(serde_json::Value::Array(inners)) if *array => {
                        itertools::Either::Left(inners.into_iter().map(Result::Ok))
                    }
                    value => itertools::Either::Right(std::iter::once(value)),
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|m| AppendWebhookError::InvalidJsonBody { msg: m.to_string() })?;

            // Note: `into_iter()` should be re-using the underlying allocation of the `objects`
            // vector, and it's more readable to split these into separate iterators.
            let rows = objects
                .into_iter()
                // Map a JSON object into a Row.
                .map(|o| {
                    let row = Jsonb::from_serde_json(o)
                        .map_err(|m| AppendWebhookError::InvalidJsonBody { msg: m.to_string() })?
                        .into_row();
                    Ok::<_, AppendWebhookError>(row)
                })
                .collect::<Result<_, _>>()?;

            rows
        }
    };

    // A `Row` cannot describe its schema without unpacking it. To add some safety we wrap the
    // returned `Row`s in a newtype to signify they already have the "body" column packed.
    let body_rows = rows.into_iter().map(BodyRow).collect();

    Ok(body_rows)
}

/// Pack the headers of a request into a [`Row`].
fn pack_header(
    mut body_row: BodyRow,
    headers: &BTreeMap<String, String>,
    header_tys: &WebhookHeaders,
) -> Result<Row, AppendWebhookError> {
    // 1 column for the body plus however many are needed for the headers.
    let num_cols = 1 + header_tys.num_columns();
    // The provided Row already has the Body written.
    let mut num_cols_written = 1;

    let mut packer = RowPacker::for_existing_row(body_row.inner_mut());

    // Pack the headers into our row, if required.
    if let Some(filters) = &header_tys.header_column {
        packer.push_dict(
            filter_headers(headers, filters).map(|(name, val)| (name, Datum::String(val))),
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

    Ok(body_row.into_inner())
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

/// A [`Row`] that has the body of a request already packed into it.
///
/// Note: if you're constructing a [`BodyRow`] you need to guarantee the only column packed into
/// the [`Row`] is a single "body" column.
#[repr(transparent)]
struct BodyRow(Row);

impl BodyRow {
    /// Obtain a mutable reference to the inner [`Row`].
    fn inner_mut(&mut self) -> &mut Row {
        &mut self.0
    }

    /// Return the inner [`Row`].
    fn into_inner(self) -> Row {
        self.0
    }
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
    #[error("internal storage failure! {0:?}")]
    InternalStorageError(StorageError<Timestamp>),
    #[error("internal failure! {0:?}")]
    Internal(#[from] anyhow::Error),
}

impl From<AppendWebhookError> for WebhookError {
    fn from(err: AppendWebhookError) -> Self {
        match err {
            AppendWebhookError::MissingSecret => WebhookError::SecretMissing,
            AppendWebhookError::ValidationError => WebhookError::ValidationError,
            AppendWebhookError::InvalidUtf8Body { msg } => WebhookError::InvalidBody {
                ty: ScalarType::String,
                msg,
            },
            AppendWebhookError::InvalidJsonBody { msg } => WebhookError::InvalidBody {
                ty: ScalarType::Jsonb,
                msg,
            },
            AppendWebhookError::UnknownWebhook {
                database,
                schema,
                name,
            } => WebhookError::NotFound(format!("'{database}.{schema}.{name}'")),
            AppendWebhookError::ValidationFailed => WebhookError::ValidationFailed,
            AppendWebhookError::ChannelClosed => {
                WebhookError::Internal(anyhow::anyhow!("channel closed"))
            }
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
            AppendWebhookError::InternalError(err) => WebhookError::Internal(err),
        }
    }
}

impl IntoResponse for WebhookError {
    fn into_response(self) -> axum::response::Response {
        match self {
            e @ WebhookError::NotFound(_) | e @ WebhookError::SecretMissing => {
                (StatusCode::NOT_FOUND, e.to_string()).into_response()
            }
            e @ WebhookError::InvalidBody { .. }
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
            e @ WebhookError::InternalStorageError(_) => {
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
    use mz_adapter::AppendWebhookError;
    use mz_ore::assert_none;
    use mz_repr::{GlobalId, Row};
    use mz_sql::plan::{WebhookBodyFormat, WebhookHeaderFilters, WebhookHeaders};
    use mz_storage_types::controller::StorageError;
    use proptest::prelude::*;
    use proptest::strategy::Union;

    use super::{filter_headers, pack_rows, WebhookError};

    // TODO(parkmycar): Move this strategy to `ore`?
    fn arbitrary_json() -> impl Strategy<Value = serde_json::Value> {
        let json_leaf = Union::new(vec![
            any::<()>().prop_map(|_| serde_json::Value::Null).boxed(),
            any::<bool>().prop_map(serde_json::Value::Bool).boxed(),
            any::<i64>()
                .prop_map(|x| serde_json::Value::Number(x.into()))
                .boxed(),
            any::<f64>()
                .prop_map(|x| {
                    let x: serde_json::value::Number =
                        x.to_string().parse().expect("failed to parse f64");
                    serde_json::Value::Number(x)
                })
                .boxed(),
            any::<String>().prop_map(serde_json::Value::String).boxed(),
        ]);

        json_leaf.prop_recursive(4, 32, 8, |element| {
            Union::new(vec![
                prop::collection::vec(element.clone(), 0..16)
                    .prop_map(serde_json::Value::Array)
                    .boxed(),
                prop::collection::hash_map(".*", element, 0..16)
                    .prop_map(|map| serde_json::Value::Object(map.into_iter().collect()))
                    .boxed(),
            ])
        })
    }

    #[track_caller]
    fn check_rows(rows: &Vec<(Row, i64)>, expected_rows: usize, expected_cols: usize) {
        assert_eq!(rows.len(), expected_rows);
        for (row, _diff) in rows {
            assert_eq!(row.unpack().len(), expected_cols);
        }
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
        filters.block.clone_from(&block);

        let mut h = filter_headers(&headers, &filters);
        assert_eq!(h.next().unwrap().0, "bar");
        assert_eq!(h.next().unwrap().0, "baz");
        assert_none!(h.next());

        let mut filters = WebhookHeaderFilters::default();
        filters.allow.clone_from(&allow);

        let mut h = filter_headers(&headers, &filters);
        assert_eq!(h.next().unwrap().0, "bar");
        assert_none!(h.next());

        let mut filters = WebhookHeaderFilters::default();
        filters.allow = allow;
        filters.block = block;

        let mut h = filter_headers(&headers, &filters);
        assert_eq!(h.next().unwrap().0, "bar");
        assert_none!(h.next());
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
        assert_none!(h.next());
    }

    #[mz_ore::test]
    fn test_json_array_single() {
        let single_raw = r#"
        {
            "event_type": "i am a single object",
            "another_field": 42
        }
        "#;

        // We should get a single Row regardless of whether or not we're requested to expand.
        let rows = pack_rows(
            single_raw.as_bytes(),
            &WebhookBodyFormat::Json { array: false },
            &BTreeMap::default(),
            &WebhookHeaders::default(),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);

        // We should get a single Row regardless of whether or not we're requested to expand.
        let rows = pack_rows(
            single_raw.as_bytes(),
            &WebhookBodyFormat::Json { array: true },
            &BTreeMap::default(),
            &WebhookHeaders::default(),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[mz_ore::test]
    fn test_json_deserializer_multi() {
        let multi_raw = r#"
            [
                { "event_type": "smol" },
                { "event_type": "dog" }
            ]
        "#;

        let rows = pack_rows(
            multi_raw.as_bytes(),
            &WebhookBodyFormat::Json { array: false },
            &BTreeMap::default(),
            &WebhookHeaders::default(),
        )
        .unwrap();
        // If we don't expand the body, we should have a single row.
        assert_eq!(rows.len(), 1);

        let rows = pack_rows(
            multi_raw.as_bytes(),
            &WebhookBodyFormat::Json { array: true },
            &BTreeMap::default(),
            &WebhookHeaders::default(),
        )
        .unwrap();
        // If we _do_ expand the body, we should have a two rows.
        assert_eq!(rows.len(), 2);
    }

    proptest! {
        #[mz_ore::test]
        fn proptest_pack_row_never_panics(
            body: Vec<u8>,
            body_ty: WebhookBodyFormat,
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
            let _ = pack_rows(&body[..], &body_ty, &headers, &header_tys);
        }

        #[mz_ore::test]
        fn proptest_pack_row_succeeds_for_bytes(
            body: Vec<u8>,
            headers: BTreeMap<String, String>,
            include_headers: bool,
        ) {
            let body = Bytes::from(body);

            let body_ty = WebhookBodyFormat::Bytes;
            let mut header_tys = WebhookHeaders::default();
            header_tys.header_column = include_headers.then(Default::default);

            let rows = pack_rows(&body[..], &body_ty, &headers, &header_tys).unwrap();
            check_rows(&rows, 1, header_tys.num_columns() + 1);
        }

        #[mz_ore::test]
        fn proptest_pack_row_succeeds_for_strings(
            body: String,
            headers: BTreeMap<String, String>,
            include_headers: bool,
        ) {
            let body = Bytes::from(body);

            let body_ty = WebhookBodyFormat::Text;
            let mut header_tys = WebhookHeaders::default();
            header_tys.header_column = include_headers.then(Default::default);

            let rows = pack_rows(&body[..], &body_ty, &headers, &header_tys).unwrap();
            check_rows(&rows, 1, header_tys.num_columns() + 1);
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
            let body_ty = WebhookBodyFormat::Text;

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

            let rows = pack_rows(&body[..], &body_ty, &headers, &header_tys).unwrap();
            check_rows(&rows, 1, header_tys.num_columns() + 1);
        }

        #[mz_ore::test]
        fn proptest_pack_json_with_array_expansion(
            body in arbitrary_json(),
            expand_array: bool,
            headers: BTreeMap<String, String>,
            include_headers: bool,
        ) {
            let json_raw = serde_json::to_vec(&body).unwrap();
            let mut header_tys = WebhookHeaders::default();
            header_tys.header_column = include_headers.then(Default::default);

            let rows = pack_rows(
                &json_raw[..],
                &WebhookBodyFormat::Json { array: expand_array },
                &headers,
                &header_tys,
            )
            .unwrap();

            let expected_num_rows = match body {
                serde_json::Value::Array(inner) if expand_array => inner.len(),
                _ => 1,
            };
            check_rows(&rows, expected_num_rows, header_tys.num_columns() + 1);
        }
    }
}
