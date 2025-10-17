// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Context;
use chrono::{DateTime, Utc};
use derivative::Derivative;
use mz_ore::cast::CastFrom;
use mz_repr::{Datum, Diff, Row, RowArena, Timestamp};
use mz_secrets::SecretsReader;
use mz_secrets::cache::CachingSecretsReader;
use mz_sql::plan::{WebhookBodyFormat, WebhookHeaders, WebhookValidation, WebhookValidationSecret};
use mz_storage_client::controller::MonotonicAppender;
use mz_storage_client::statistics::WebhookStatistics;
use mz_storage_types::controller::StorageError;
use tokio::sync::Semaphore;

use crate::optimize::dataflows::{ExprPrepStyle, prep_scalar_expr};

/// Errors returns when attempting to append to a webhook.
#[derive(thiserror::Error, Debug)]
pub enum AppendWebhookError {
    // A secret that we need for validation has gone missing.
    #[error("could not read a required secret")]
    MissingSecret,
    #[error("the provided request body is not UTF-8: {msg}")]
    InvalidUtf8Body { msg: String },
    #[error("the provided request body is not valid JSON: {msg}")]
    InvalidJsonBody { msg: String },
    #[error("webhook source '{database}.{schema}.{name}' does not exist")]
    UnknownWebhook {
        database: String,
        schema: String,
        name: String,
    },
    #[error("failed to validate the request")]
    ValidationFailed,
    // Note: we should _NEVER_ add more detail to this error, including the actual error we got
    // when running validation. This is because the error messages might contain info about the
    // arguments provided to the validation expression, we could contains user SECRETs. So by
    // including any more detail we might accidentally expose SECRETs.
    #[error("validation error")]
    ValidationError,
    #[error("internal channel closed")]
    ChannelClosed,
    #[error("internal error: {0:?}")]
    InternalError(#[from] anyhow::Error),
    #[error("internal storage failure! {0:?}")]
    StorageError(#[from] StorageError<mz_repr::Timestamp>),
}

/// Contains all of the components necessary for running webhook validation.
///
/// To actually validate a webhook request call [`AppendWebhookValidator::eval`].
#[derive(Clone)]
pub struct AppendWebhookValidator {
    validation: WebhookValidation,
    secrets_reader: CachingSecretsReader,
}

impl AppendWebhookValidator {
    pub fn new(validation: WebhookValidation, secrets_reader: CachingSecretsReader) -> Self {
        AppendWebhookValidator {
            validation,
            secrets_reader,
        }
    }

    pub async fn eval(
        self,
        body: bytes::Bytes,
        headers: Arc<BTreeMap<String, String>>,
        received_at: DateTime<Utc>,
    ) -> Result<bool, AppendWebhookError> {
        let AppendWebhookValidator {
            validation,
            secrets_reader,
        } = self;

        let WebhookValidation {
            mut expression,
            relation_desc: _,
            secrets,
            bodies: body_columns,
            headers: header_columns,
        } = validation;

        // Use the secrets reader to get any secrets.
        let mut secret_contents = BTreeMap::new();
        for WebhookValidationSecret {
            id,
            column_idx,
            use_bytes,
        } in secrets
        {
            let secret = secrets_reader
                .read(id)
                .await
                .map_err(|_| AppendWebhookError::MissingSecret)?;
            secret_contents.insert(column_idx, (secret, use_bytes));
        }

        // Transform any calls to `now()` into a constant representing of the current time.
        //
        // Note: we do this outside the closure, because otherwise there are some odd catch unwind
        // boundary errors, and this shouldn't be too computationally expensive.
        prep_scalar_expr(
            &mut expression,
            ExprPrepStyle::WebhookValidation { now: received_at },
        )
        .map_err(|err| {
            tracing::error!(?err, "failed to evaluate current time");
            AppendWebhookError::ValidationError
        })?;

        // Create a closure to run our validation, this allows lifetimes and unwind boundaries to
        // work.
        let validate = move || {
            // Gather our Datums for evaluation
            //
            // TODO(parkmycar): Re-use the RowArena when we implement rate limiting.
            let temp_storage = RowArena::default();
            let mut datums = Vec::with_capacity(
                body_columns.len() + header_columns.len() + secret_contents.len(),
            );

            // Append all of our body columns.
            for (column_idx, use_bytes) in body_columns {
                assert_eq!(column_idx, datums.len(), "body index and datums mismatch!");

                let datum = if use_bytes {
                    Datum::Bytes(&body[..])
                } else {
                    let s = std::str::from_utf8(&body[..])
                        .map_err(|m| AppendWebhookError::InvalidUtf8Body { msg: m.to_string() })?;
                    Datum::String(s)
                };
                datums.push(datum);
            }

            // Append all of our header columns, re-using Row packings.
            //
            let headers_byte = std::cell::OnceCell::new();
            let headers_text = std::cell::OnceCell::new();
            for (column_idx, use_bytes) in header_columns {
                assert_eq!(column_idx, datums.len(), "index and datums mismatch!");

                let row = if use_bytes {
                    headers_byte.get_or_init(|| {
                        let mut row = Row::with_capacity(1);
                        let mut packer = row.packer();
                        packer.push_dict(
                            headers
                                .iter()
                                .map(|(name, val)| (name.as_str(), Datum::Bytes(val.as_bytes()))),
                        );
                        row
                    })
                } else {
                    headers_text.get_or_init(|| {
                        let mut row = Row::with_capacity(1);
                        let mut packer = row.packer();
                        packer.push_dict(
                            headers
                                .iter()
                                .map(|(name, val)| (name.as_str(), Datum::String(val))),
                        );
                        row
                    })
                };
                datums.push(row.unpack_first());
            }

            // Append all of our secrets to our datums, in the correct column order.
            for column_idx in datums.len()..datums.len() + secret_contents.len() {
                // Get the secret that corresponds with what is the next "column";
                let (secret, use_bytes) = secret_contents
                    .get(&column_idx)
                    .expect("more secrets to provide, but none for the next column");

                if *use_bytes {
                    datums.push(Datum::Bytes(secret));
                } else {
                    let secret_str = std::str::from_utf8(&secret[..]).expect("valid UTF-8");
                    datums.push(Datum::String(secret_str));
                }
            }

            // Run our validation
            let valid = expression
                .eval_pop(&datums[..], &temp_storage, &mut Vec::new())
                .map_err(|_| AppendWebhookError::ValidationError)?;
            match valid {
                Datum::True => Ok::<_, AppendWebhookError>(true),
                Datum::False | Datum::Null => Ok(false),
                _ => unreachable!("Creating a webhook source asserts we return a boolean"),
            }
        };

        // Then run the validation itself.
        let valid = mz_ore::task::spawn_blocking(
            || "webhook-validator-expr",
            move || {
                // Since the validation expression is technically a user defined function, we want to
                // be extra careful and guard against issues taking down the entire process.
                mz_ore::panic::catch_unwind(validate).map_err(|_| {
                    tracing::error!("panic while validating webhook request!");
                    AppendWebhookError::ValidationError
                })
            },
        )
        .await
        .context("joining on validation")
        .map_err(|e| {
            tracing::error!("Failed to run validation for webhook, {e}");
            AppendWebhookError::ValidationError
        })??;

        valid
    }
}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub struct AppendWebhookResponse {
    /// Channel to monotonically append rows to a webhook source.
    pub tx: WebhookAppender,
    /// Column type for the `body` column.
    pub body_format: WebhookBodyFormat,
    /// Types of the columns for the headers of a request.
    pub header_tys: WebhookHeaders,
    /// Expression used to validate a webhook request.
    #[derivative(Debug = "ignore")]
    pub validator: Option<AppendWebhookValidator>,
}

/// A wrapper around [`MonotonicAppender`] that can get closed by the `Coordinator` if the webhook
/// gets modified.
#[derive(Clone, Debug)]
pub struct WebhookAppender {
    tx: MonotonicAppender<Timestamp>,
    guard: WebhookAppenderGuard,
    // Shared statistics related to this webhook.
    stats: Arc<WebhookStatistics>,
}

impl WebhookAppender {
    /// Checks if the [`WebhookAppender`] has closed.
    pub fn is_closed(&self) -> bool {
        self.guard.is_closed()
    }

    /// Appends updates to the linked webhook source.
    pub async fn append(&self, updates: Vec<(Row, Diff)>) -> Result<(), AppendWebhookError> {
        if self.is_closed() {
            return Err(AppendWebhookError::ChannelClosed);
        }

        let count = u64::cast_from(updates.len());
        self.stats
            .updates_staged
            .fetch_add(count, Ordering::Relaxed);
        let updates = updates.into_iter().map(|update| update.into()).collect();
        self.tx.append(updates).await?;
        self.stats
            .updates_committed
            .fetch_add(count, Ordering::Relaxed);
        Ok(())
    }

    /// Increment the `messages_received` user-facing statistics. This
    /// should be incremented even if the request is invalid.
    pub fn increment_messages_received(&self, msgs: u64) {
        self.stats
            .messages_received
            .fetch_add(msgs, Ordering::Relaxed);
    }

    /// Increment the `bytes_received` user-facing statistics. This
    /// should be incremented even if the request is invalid.
    pub fn increment_bytes_received(&self, bytes: u64) {
        self.stats
            .bytes_received
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn new(
        tx: MonotonicAppender<Timestamp>,
        guard: WebhookAppenderGuard,
        stats: Arc<WebhookStatistics>,
    ) -> Self {
        WebhookAppender { tx, guard, stats }
    }
}

/// When a webhook, or it's containing schema and database, get modified we need to invalidate any
/// outstanding [`WebhookAppender`]s. This is because `Adapter`s will cache [`WebhookAppender`]s to
/// increase performance, and the (database, schema, name) tuple they cached an appender for is now
/// incorrect.
#[derive(Clone, Debug)]
pub struct WebhookAppenderGuard {
    is_closed: Arc<AtomicBool>,
}

impl WebhookAppenderGuard {
    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::SeqCst)
    }
}

/// A handle to invalidate [`WebhookAppender`]s. See the comment on [`WebhookAppenderGuard`] for
/// more detail.
///
/// Note: to invalidate the associated [`WebhookAppender`]s, you must drop the corresponding
/// [`WebhookAppenderInvalidator`].
#[derive(Debug)]
pub struct WebhookAppenderInvalidator {
    is_closed: Arc<AtomicBool>,
}
// We want to enforce unique ownership over the ability to invalidate a `WebhookAppender`.
static_assertions::assert_not_impl_all!(WebhookAppenderInvalidator: Clone);

impl WebhookAppenderInvalidator {
    pub(crate) fn new() -> WebhookAppenderInvalidator {
        let is_closed = Arc::new(AtomicBool::new(false));
        WebhookAppenderInvalidator { is_closed }
    }

    pub fn guard(&self) -> WebhookAppenderGuard {
        WebhookAppenderGuard {
            is_closed: Arc::clone(&self.is_closed),
        }
    }
}

impl Drop for WebhookAppenderInvalidator {
    fn drop(&mut self) {
        self.is_closed.store(true, Ordering::SeqCst);
    }
}

pub type WebhookAppenderName = (String, String, String);

/// A cache of [`WebhookAppender`]s and other metadata required for appending to a wbhook source.
///
/// Entries in the cache get invalidated when a [`WebhookAppender`] closes, at which point the
/// entry should be dropped from the cache and a request made to the `Coordinator` for a new one.
#[derive(Debug, Clone)]
pub struct WebhookAppenderCache {
    pub entries: Arc<tokio::sync::Mutex<BTreeMap<WebhookAppenderName, AppendWebhookResponse>>>,
}

impl WebhookAppenderCache {
    pub fn new() -> Self {
        WebhookAppenderCache {
            entries: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
        }
    }
}

/// Manages how many concurrent webhook requests we allow at once.
#[derive(Debug, Clone)]
pub struct WebhookConcurrencyLimiter {
    semaphore: Arc<Semaphore>,
    prev_limit: usize,
}

impl WebhookConcurrencyLimiter {
    pub fn new(limit: usize) -> Self {
        let semaphore = Arc::new(Semaphore::new(limit));

        WebhookConcurrencyLimiter {
            semaphore,
            prev_limit: limit,
        }
    }

    /// Returns the underlying [`Semaphore`] used for limiting.
    pub fn semaphore(&self) -> Arc<Semaphore> {
        Arc::clone(&self.semaphore)
    }

    /// Updates the limit of how many concurrent requests can be run at once.
    pub fn set_limit(&mut self, new_limit: usize) {
        if new_limit > self.prev_limit {
            // Add permits.
            let diff = new_limit.saturating_sub(self.prev_limit);
            tracing::debug!("Adding {diff} permits");

            self.semaphore.add_permits(diff);
        } else if new_limit < self.prev_limit {
            // Remove permits.
            let diff = self.prev_limit.saturating_sub(new_limit);
            let diff = u32::try_from(diff).unwrap_or(u32::MAX);
            tracing::debug!("Removing {diff} permits");

            let semaphore = self.semaphore();

            // Kind of janky, but the recommended way to reduce the amount of permits is to spawn
            // a task the acquires and then forgets old permits.
            mz_ore::task::spawn(|| "webhook-concurrency-limiter-drop-permits", async move {
                if let Ok(permit) = Semaphore::acquire_many_owned(semaphore, diff).await {
                    permit.forget()
                }
            });
        }

        // Store our new limit.
        self.prev_limit = new_limit;
        tracing::debug!("New limit, {} permits", self.prev_limit);
    }
}

impl Default for WebhookConcurrencyLimiter {
    fn default() -> Self {
        WebhookConcurrencyLimiter::new(mz_sql::WEBHOOK_CONCURRENCY_LIMIT)
    }
}

#[cfg(test)]
mod test {
    use mz_ore::assert_err;

    use super::WebhookConcurrencyLimiter;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn smoke_test_concurrency_limiter() {
        let mut limiter = WebhookConcurrencyLimiter::new(10);

        let semaphore_a = limiter.semaphore();
        let _permit_a = semaphore_a.try_acquire_many(10).expect("acquire");

        let semaphore_b = limiter.semaphore();
        assert_err!(semaphore_b.try_acquire());

        // Increase our limit.
        limiter.set_limit(15);

        // This should now succeed!
        let _permit_b = semaphore_b.try_acquire().expect("acquire");

        // Decrease our limit.
        limiter.set_limit(5);

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // This should fail again.
        assert_err!(semaphore_b.try_acquire());
    }
}
