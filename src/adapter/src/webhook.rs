// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use anyhow::Context;
use mz_expr::visit::Visit;
use mz_expr::{MirScalarExpr, UnmaterializableFunc};
use mz_ore::now::{to_datetime, NowFn};
use mz_repr::{ColumnType, Datum, Row, RowArena};
use mz_secrets::cache::CachingSecretsReader;
use mz_secrets::SecretsReader;
use mz_sql::plan::{WebhookHeaders, WebhookValidation, WebhookValidationSecret};
use mz_storage_client::controller::MonotonicAppender;
use tokio::sync::Semaphore;

/// Errors returns when running validation of a webhook request.
#[derive(thiserror::Error, Debug)]
pub enum AppendWebhookError {
    // A secret that we need for validation has gone missing.
    #[error("could not read a required secret")]
    MissingSecret,
    #[error("the provided request body is not UTF-8")]
    NonUtf8Body,
    // Note: we should _NEVER_ add more detail to this error, including the actual error we got
    // when running validation. This is because the error messages might contain info about the
    // arguments provided to the validation expression, we could contains user SECRETs. So by
    // including any more detail we might accidentally expose SECRETs.
    #[error("validation failed")]
    ValidationError,
    // Note: we should _NEVER_ add more detail to this error, see above as to why.
    #[error("internal error when validating request")]
    InternalError,
}

/// Contains all of the components necessary for running webhook validation.
///
/// To actually validate a webhook request call [`AppendWebhookValidator::eval`].
pub struct AppendWebhookValidator {
    validation: WebhookValidation,
    secrets_reader: CachingSecretsReader,
    now_fn: NowFn,
}

impl AppendWebhookValidator {
    pub fn new(
        validation: WebhookValidation,
        secrets_reader: CachingSecretsReader,
        now_fn: NowFn,
    ) -> Self {
        AppendWebhookValidator {
            validation,
            secrets_reader,
            now_fn,
        }
    }

    pub async fn eval(
        self,
        body: bytes::Bytes,
        headers: Arc<BTreeMap<String, String>>,
    ) -> Result<bool, AppendWebhookError> {
        let AppendWebhookValidator {
            validation,
            secrets_reader,
            now_fn,
        } = self;

        let WebhookValidation {
            expression,
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
        let expression = eval_current_time(expression, now_fn()).map_err(|err| {
            tracing::error!(?err, "failed to evaluate current time");
            AppendWebhookError::InternalError
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
                        .map_err(|_| AppendWebhookError::NonUtf8Body)?;
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
                .eval(&datums[..], &temp_storage)
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
                    AppendWebhookError::InternalError
                })
            },
        )
        .await
        .context("joining on validation")
        .map_err(|e| {
            tracing::error!("Failed to run validation for webhook, {e}");
            AppendWebhookError::InternalError
        })??;

        valid
    }
}

pub struct AppendWebhookResponse {
    pub tx: MonotonicAppender,
    pub body_ty: ColumnType,
    pub header_tys: WebhookHeaders,
    pub validator: Option<AppendWebhookValidator>,
}

impl fmt::Debug for AppendWebhookResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppendWebhookResponse")
            .field("tx", &self.tx)
            .field("body_ty", &self.body_ty)
            .field("header_tys", &self.header_tys)
            .field("validate_expr", &"(...)")
            .finish()
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

/// Evaluates any calls to `UnmaterializableFunc::CurrentTimestamp`, replacing it with the current
/// time.
fn eval_current_time(
    mut validation_expr: MirScalarExpr,
    now: u64,
) -> Result<MirScalarExpr, anyhow::Error> {
    validation_expr.try_visit_mut_post(&mut |e| {
        if let MirScalarExpr::CallUnmaterializable(f @ UnmaterializableFunc::CurrentTimestamp) = e {
            let now = to_datetime(now);
            let now: Datum = now.try_into()?;

            let const_expr = MirScalarExpr::literal_ok(now, f.output_type().scalar_type);
            *e = const_expr;
        }
        Ok::<_, anyhow::Error>(())
    })?;

    Ok(validation_expr)
}

#[cfg(test)]
mod test {
    use super::WebhookConcurrencyLimiter;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn smoke_test_concurrency_limiter() {
        let mut limiter = WebhookConcurrencyLimiter::new(10);

        let semaphore_a = limiter.semaphore();
        let _permit_a = semaphore_a.try_acquire_many(10).expect("acquire");

        let semaphore_b = limiter.semaphore();
        assert!(semaphore_b.try_acquire().is_err());

        // Increase our limit.
        limiter.set_limit(15);

        // This should now succeed!
        let _permit_b = semaphore_b.try_acquire().expect("acquire");

        // Decrease our limit.
        limiter.set_limit(5);

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // This should fail again.
        assert!(semaphore_b.try_acquire().is_err());
    }
}
