// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use mz_postgres_util::Client;
use mz_postgres_util::PostgresError;
use mz_repr::GlobalId;
use mz_storage_types::dyncfgs::PG_SCHEMA_VALIDATION_INTERVAL;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::trace;

use crate::source::RawSourceCreationConfig;
use crate::source::postgres::verify_schema;
use crate::source::postgres::{DefiniteError, SourceOutputInfo};

pub(super) enum SchemaValidationError {
    Postgres(PostgresError),
    Schema {
        oid: u32,
        output_index: usize,
        error: DefiniteError,
    },
    /// The upstream's physical-replica status changed out from under us (e.g. a
    /// physical replica was promoted to a primary). `expected` is the status the
    /// source was created against; `actual` is what the upstream reports now.
    PhysicalReplicaChanged {
        expected: bool,
        actual: bool,
    },
}

impl SchemaValidationError {
    /// The `(oid, output_index)` of the one output this error ends, or `None`
    /// if it ends the whole source.
    fn output(&self) -> Option<(u32, usize)> {
        match self {
            Self::Schema {
                oid, output_index, ..
            } => Some((*oid, *output_index)),
            Self::Postgres(_) | Self::PhysicalReplicaChanged { .. } => None,
        }
    }
}

pub(super) fn spawn_schema_validator(
    client: Client,
    config: &RawSourceCreationConfig,
    publication: String,
    table_info: BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>>,
    is_physical_replica: bool,
) -> mpsc::UnboundedReceiver<SchemaValidationError> {
    let (tx, rx) = mpsc::unbounded_channel();
    let source_id = config.id;
    let config_set = Arc::clone(config.config.config_set());

    mz_ore::task::spawn(
        || format!("schema-validator:{}", source_id),
        run_schema_validator(
            tx,
            move || PG_SCHEMA_VALIDATION_INTERVAL.get(&config_set),
            async move || {
                validate_schemas(
                    source_id,
                    &client,
                    &publication,
                    &table_info,
                    is_physical_replica,
                )
                .await
            },
        ),
    );

    rx
}

/// Runs `validate` once per `interval` and sends each failure to `tx` once.
///
/// Stops after the first failure that ends the whole source, or once the
/// receiver is dropped.
async fn run_schema_validator(
    tx: mpsc::UnboundedSender<SchemaValidationError>,
    interval: impl Fn() -> Duration,
    mut validate: impl AsyncFnMut() -> Vec<SchemaValidationError>,
) {
    // The replication loop drains `tx` only when the replication stream yields
    // a keepalive, so while the stream is silent every send stays queued.
    // Sending each failure once bounds the queue by the number of outputs plus
    // one.
    let mut reported = BTreeSet::new();
    while !tx.is_closed() {
        let validation_start = Instant::now();
        for error in validate().await {
            match error.output() {
                Some(output) => {
                    if reported.insert(output) {
                        let _ = tx.send(error);
                    }
                }
                None => {
                    let _ = tx.send(error);
                    return;
                }
            }
        }
        let wait = interval().saturating_sub(validation_start.elapsed());
        tokio::time::sleep(wait).await;
    }
}

/// Validates the upstream against the source's expectations once.
async fn validate_schemas(
    source_id: GlobalId,
    client: &Client,
    publication: &str,
    table_info: &BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>>,
    is_physical_replica: bool,
) -> Vec<SchemaValidationError> {
    trace!(%source_id, "validating schemas");
    let mut errors = Vec::new();

    // Detect a physical replica being promoted to a primary (or vice
    // versa) while the stream is live. The startup check in raw_stream
    // only catches this across a restart, so we re-check periodically
    // here to surface a definite error proactively.
    match mz_postgres_util::get_is_in_recovery(&**client).await {
        Ok(actual) if actual != is_physical_replica => {
            errors.push(SchemaValidationError::PhysicalReplicaChanged {
                expected: is_physical_replica,
                actual,
            });
        }
        Ok(_) => {}
        Err(error) => {
            errors.push(SchemaValidationError::Postgres(error));
            return errors;
        }
    }

    let upstream_info = match mz_postgres_util::publication_info(
        &**client,
        publication,
        Some(&table_info.keys().copied().collect::<Vec<_>>()),
    )
    .await
    {
        Ok(info) => info,
        Err(error) => {
            errors.push(SchemaValidationError::Postgres(error));
            return errors;
        }
    };

    for (&oid, outputs) in table_info.iter() {
        for (&output_index, info) in outputs {
            if let Err(error) = verify_schema(oid, info, &upstream_info) {
                trace!(
                    %source_id,
                    "schema of output index {output_index} for oid {oid} invalid",
                );
                errors.push(SchemaValidationError::Schema {
                    oid,
                    output_index,
                    error,
                });
            } else {
                trace!(
                    %source_id,
                    "schema of output index {output_index} for oid {oid} valid",
                );
            }
        }
    }
    errors
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    fn connection_error() -> SchemaValidationError {
        SchemaValidationError::Postgres(PostgresError::Generic(anyhow::anyhow!(
            "connection closed"
        )))
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn schema_validator_stops_after_source_error() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let rounds = Cell::new(0);
        let validator = run_schema_validator(
            tx,
            || Duration::from_secs(15),
            async || {
                rounds.set(rounds.get() + 1);
                assert!(rounds.get() <= 10, "validator retries without waiting");
                vec![connection_error()]
            },
        );
        let _ = tokio::time::timeout(Duration::from_secs(59), validator).await;
        assert_eq!(rounds.get(), 1);
        assert!(matches!(
            rx.try_recv(),
            Ok(SchemaValidationError::Postgres(_))
        ));
        assert!(rx.try_recv().is_err(), "only one error is queued");
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn schema_validator_reports_each_output_once() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let rounds = Cell::new(0);
        let validator = run_schema_validator(
            tx,
            || Duration::from_secs(15),
            async || {
                rounds.set(rounds.get() + 1);
                // The table leaves the publication, comes back, and leaves again,
                // all before the replication loop drains the queue.
                if rounds.get() % 2 == 1 {
                    vec![SchemaValidationError::Schema {
                        oid: 1,
                        output_index: 0,
                        error: DefiniteError::TableDropped,
                    }]
                } else {
                    Vec::new()
                }
            },
        );
        let _ = tokio::time::timeout(Duration::from_secs(59), validator).await;
        assert_eq!(rounds.get(), 4);
        assert!(matches!(
            rx.try_recv(),
            Ok(SchemaValidationError::Schema {
                oid: 1,
                output_index: 0,
                error: DefiniteError::TableDropped,
            })
        ));
        assert!(rx.try_recv().is_err(), "the output is reported once");
    }
}
