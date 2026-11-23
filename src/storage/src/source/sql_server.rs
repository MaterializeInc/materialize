// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code to render the ingestion dataflow of a [`SqlServerSourceConnection`].

use std::collections::BTreeMap;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;

use itertools::Itertools;
use mz_ore::error::ErrorExt;
use mz_repr::{Diff, GlobalId, Row, RowArena};
use mz_sql_server_util::cdc::Lsn;
use mz_sql_server_util::desc::{SqlServerRowDecoder, SqlServerTableDesc};
use mz_sql_server_util::inspect::{
    ensure_database_cdc_enabled, ensure_sql_server_agent_running, get_latest_restore_history_id,
};
use mz_sql_server_util::{Client, SqlServerCdcMetrics, SqlServerError};
use mz_storage_types::dyncfgs::SQL_SERVER_SOURCE_VALIDATE_RESTORE_HISTORY;
use mz_storage_types::errors::{
    DataflowError, DecodeError, DecodeErrorKind, SourceError, SourceErrorDetails,
};
use mz_storage_types::sources::{
    SourceExport, SourceExportDetails, SourceTimestamp, SqlServerSourceConnection,
};
use mz_timely_util::builder_async::{AsyncOutputHandle, PressOnDropButton};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::operators::vec::{Map, ToStream};
use timely::dataflow::operators::{CapabilitySet, Concat};
use timely::dataflow::{Scope, StreamVec};
use timely::progress::{Antichain, Timestamp};

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::metrics::source::sql_server::SqlServerSourceMetrics;
use crate::source::RawSourceCreationConfig;
use crate::source::types::{
    Probe, ResumeUppers, SharedFuel, SourceMessage, SourceRender, StackedCollection,
};

mod progress;
mod replication;
mod snapshot;

/// The data output ports of an operator, one per source export in output index order.
type DataOutputs = SharedFuel<
    Lsn,
    CapacityContainerBuilder<Vec<(Result<SourceMessage, DataflowError>, Lsn, Diff)>>,
>;

#[derive(Debug, Clone)]
struct SourceOutputInfo {
    /// Name of the capture instance in the upstream SQL Server DB.
    capture_instance: Arc<str>,
    /// Description of the upstream table.
    #[allow(dead_code)]
    upstream_desc: Arc<SqlServerTableDesc>,
    /// Type that can decode (and map) SQL Server rows into Materialize rows.
    decoder: Arc<SqlServerRowDecoder>,
    /// Upper to resume replication from.
    resume_upper: Antichain<Lsn>,
    /// The position of this export's data port among the replication operator's outputs.
    output_index: usize,
    /// The basis for the resumption LSN when snapshotting.
    initial_lsn: Lsn,
}

impl SourceOutputInfo {
    /// The [`Lsn`] this output resumes reading from, or the provided fallback [`Lsn`].
    ///
    /// Panics if `resume_upper` is empty, which would mean the output has no
    /// resumption point at all.
    fn resume_lsn_or(&self, fallback: Lsn) -> Lsn {
        match self.resume_upper.as_option() {
            Some(lsn) if *lsn != Lsn::minimum() => *lsn,
            Some(_) => fallback,
            None => panic!("resume_upper has at least one value"),
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ReplicationError {
    #[error(transparent)]
    Transient(#[from] Rc<TransientError>),
    #[error(transparent)]
    DefiniteError(#[from] Rc<DefiniteError>),
}

#[derive(Debug, thiserror::Error)]
pub enum TransientError {
    #[error("stream ended prematurely")]
    ReplicationEOF,
    #[error(transparent)]
    SqlServer(#[from] SqlServerError),
    #[error(transparent)]
    Generic(#[from] anyhow::Error),
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum DefiniteError {
    #[error("unable to decode: {0}")]
    ValueDecodeError(String),
    #[error("failed to decode row: {0}")]
    Decoding(String),
    #[error("programming error: {0}")]
    ProgrammingError(String),
    #[error("Restore history id changed from {0:?} to {1:?}")]
    RestoreHistoryChanged(Option<i32>, Option<i32>),
    #[error("Incompatible schema change for table {0} capture instance {1}")]
    IncompatibleSchemaChange(String, String),
}

impl From<DefiniteError> for DataflowError {
    fn from(val: DefiniteError) -> Self {
        let msg = val.to_string().into();
        DataflowError::SourceError(Box::new(SourceError {
            error: SourceErrorDetails::Other(msg),
            hint: None,
        }))
    }
}

/// Checks that the upstream database can still serve the source.
///
/// Returns the definite error that ends the source when the database was restored since the
/// source was created.
async fn validate_upstream(
    client: &mut Client,
    source: &SqlServerSourceConnection,
    config: &RawSourceCreationConfig,
) -> Result<Option<DefiniteError>, TransientError> {
    let current_restore_history_id = get_latest_restore_history_id(client).await?;
    if current_restore_history_id != source.extras.restore_history_id {
        if SQL_SERVER_SOURCE_VALIDATE_RESTORE_HISTORY.get(config.config.config_set()) {
            return Ok(Some(DefiniteError::RestoreHistoryChanged(
                source.extras.restore_history_id.clone(),
                current_restore_history_id,
            )));
        }
        tracing::warn!(
            "Restore history mismatch ignored: expected={expected:?} actual={actual:?}",
            expected = source.extras.restore_history_id,
            actual = current_restore_history_id
        );
    }

    // For AOAG, it's possible that the dataflow restarted and is now connected to a
    // different SQL Server, which may not have CDC enabled correctly.
    ensure_database_cdc_enabled(client).await?;
    ensure_sql_server_agent_running(client).await?;
    Ok(None)
}

/// Helper method to return a "definite" error upstream.
async fn return_definite_error(
    err: DefiniteError,
    outputs: impl Iterator<Item = usize>,
    data_outputs: &mut DataOutputs,
    data_cap_sets: &[CapabilitySet<Lsn>],
    errs_handle: &AsyncOutputHandle<Lsn, CapacityContainerBuilder<Vec<ReplicationError>>>,
    errs_capset: &CapabilitySet<Lsn>,
) {
    for output_index in outputs {
        let update = (
            Err(err.clone().into()),
            // Select an LSN that should not conflict with a previously observed LSN.  Ideally
            // we could identify the LSN that resulted in the definite error so that all replicas
            // would emit the same updates for the same times.
            Lsn {
                vlf_id: u32::MAX,
                block_id: u32::MAX,
                record_id: u16::MAX,
            },
            Diff::ONE,
        );
        data_outputs
            .give(output_index, &data_cap_sets[output_index][0], update)
            .await;
    }
    errs_handle.give(
        &errs_capset[0],
        ReplicationError::DefiniteError(Rc::new(err)),
    );
}

/// Helper method to decode a row from a [`tiberius::Row`] (or 2 of them in the case of update)
/// to a [`Row`]. This centralizes the decode and mapping to result.
fn decode(
    decoder: &SqlServerRowDecoder,
    row: &tiberius::Row,
    mz_row: &mut Row,
    arena: &RowArena,
    new_row: Option<&tiberius::Row>,
) -> Result<SourceMessage, DataflowError> {
    match decoder.decode(row, mz_row, arena, new_row) {
        Ok(()) => Ok(SourceMessage {
            key: Row::default(),
            value: mz_row.clone(),
            metadata: Row::default(),
        }),
        Err(e) => {
            let kind = DecodeErrorKind::Text(e.to_string().into());
            // TODO(sql_server2): Get the raw bytes from `tiberius`.
            let raw = format!("{row:?}");
            Err(DataflowError::DecodeError(Box::new(DecodeError {
                kind,
                raw: raw.as_bytes().to_vec(),
            })))
        }
    }
}

/// Provides an implemntation of [`SqlServerCdcMetrics`] that will update [`SqlServerSourceMetrics`]`
struct PrometheusSqlServerCdcMetrics<'a> {
    inner: &'a SqlServerSourceMetrics,
}

impl<'a> SqlServerCdcMetrics for PrometheusSqlServerCdcMetrics<'a> {
    fn snapshot_table_lock_start(&self, table_name: &str) {
        self.inner.update_snapshot_table_lock_count(table_name, 1);
    }

    fn snapshot_table_lock_end(&self, table_name: &str) {
        self.inner.update_snapshot_table_lock_count(table_name, -1);
    }
}

impl SourceRender for SqlServerSourceConnection {
    type Time = Lsn;

    const STATUS_NAMESPACE: StatusNamespace = StatusNamespace::SqlServer;

    fn render<'scope>(
        self,
        scope: Scope<'scope, Lsn>,
        config: &RawSourceCreationConfig,
        resume_uppers: impl futures::Stream<Item = ResumeUppers<Lsn>> + 'static,
        _start_signal: impl Future<Output = ()> + 'static,
    ) -> (
        // Timely Collection for each Source Export defined in the provided `config`.
        BTreeMap<GlobalId, StackedCollection<'scope, Lsn, Result<SourceMessage, DataflowError>>>,
        StreamVec<'scope, Lsn, HealthStatusMessage>,
        StreamVec<'scope, Lsn, Probe<Lsn>>,
        Vec<PressOnDropButton>,
    ) {
        // Collect the source outputs that we will be exporting.
        let mut source_outputs = BTreeMap::new();
        for (idx, (id, export)) in config.source_exports.iter().enumerate() {
            let SourceExport {
                details,
                storage_metadata,
                data_config: _,
            } = export;

            let details = match details {
                SourceExportDetails::SqlServer(details) => details,
                // This is an export that doesn't need any data output to it.
                SourceExportDetails::None => continue,
                other => unreachable!("unexpected source export details: {other:?}"),
            };

            let decoder = details
                .table
                .decoder(&storage_metadata.relation_desc)
                .expect("TODO handle errors");
            let upstream_desc = Arc::new(details.table.clone());
            let resume_upper = config
                .source_resume_uppers
                .get(id)
                .expect("missing resume upper")
                .iter()
                .map(Lsn::decode_row);

            let output_info = SourceOutputInfo {
                capture_instance: Arc::clone(&details.capture_instance),
                upstream_desc,
                decoder: Arc::new(decoder),
                resume_upper: Antichain::from_iter(resume_upper),
                output_index: idx,
                initial_lsn: details.initial_lsn,
            };
            source_outputs.insert(*id, output_info);
        }

        let metrics = config
            .metrics
            .get_sql_server_source_metrics(config.id, config.worker_id);

        let (snapshot_updates, rewinds, snapshot_errs, snapshot_token) = snapshot::render(
            scope.clone(),
            config.clone(),
            source_outputs.clone(),
            self.clone(),
            metrics.clone(),
        );

        let (repl_updates, repl_errs, repl_token) = replication::render(
            scope.clone(),
            config.clone(),
            source_outputs.clone(),
            self.clone(),
            rewinds,
            metrics,
        );

        let (progress_errs, progress_probes, progress_token) = progress::render(
            scope.clone(),
            config.clone(),
            self.connection.clone(),
            source_outputs.clone(),
            resume_uppers,
            self.extras.clone(),
        );

        let data_collections = config
            .source_exports
            .keys()
            .copied()
            .zip_eq(snapshot_updates.into_iter().zip_eq(repl_updates))
            .map(|(id, (snapshot, repl))| (id, snapshot.concat(repl)))
            .collect();

        let export_ids = config.source_exports.keys().copied();
        let health_init = export_ids
            .map(Some)
            .chain(std::iter::once(None))
            .map(|id| HealthStatusMessage {
                id,
                namespace: Self::STATUS_NAMESPACE,
                update: HealthStatusUpdate::Running,
            })
            .collect::<Vec<_>>()
            .to_stream(scope);

        let health_errs = snapshot_errs
            .concat(repl_errs)
            .concat(progress_errs)
            .map(move |err| {
                // This update will cause the dataflow to restart
                let err_string = err.display_with_causes().to_string();
                let update = HealthStatusUpdate::halting(err_string, None);
                // TODO(sql_server2): If the error has anything to do with SSH
                // connections we should use the SSH status namespace.
                let namespace = Self::STATUS_NAMESPACE;

                HealthStatusMessage {
                    id: None,
                    namespace: namespace.clone(),
                    update,
                }
            });
        let health = health_init.concat(health_errs);

        (
            data_collections,
            health,
            progress_probes,
            vec![snapshot_token, repl_token, progress_token],
        )
    }
}
