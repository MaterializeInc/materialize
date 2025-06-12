// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code to render the ingestion dataflow of a [`SqlServerSource`].

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::AsCollection;
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_repr::{Diff, GlobalId};
use mz_sql_server_util::SqlServerError;
use mz_sql_server_util::cdc::Lsn;
use mz_sql_server_util::desc::{SqlServerRowDecoder, SqlServerTableDesc};
use mz_storage_types::errors::{DataflowError, SourceError, SourceErrorDetails};
use mz_storage_types::sources::{
    SourceExport, SourceExportDetails, SourceTimestamp, SqlServerSource,
};
use mz_timely_util::builder_async::PressOnDropButton;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::operators::core::Partition;
use timely::dataflow::operators::{Concat, Map, ToStream};
use timely::dataflow::{Scope, Stream as TimelyStream};
use timely::progress::Antichain;

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::source::RawSourceCreationConfig;
use crate::source::types::{Probe, SourceMessage, SourceRender, StackedCollection};

mod progress;
mod replication;

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
    /// An index to split the timely stream.
    partition_index: u64,
    /// The basis for the resumption LSN when snapshotting.
    initial_lsn: Lsn,
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
}

impl From<DefiniteError> for DataflowError {
    fn from(val: DefiniteError) -> Self {
        let msg = val.to_string().into();
        DataflowError::SourceError(Box::new(SourceError {
            error: SourceErrorDetails::Other(msg),
        }))
    }
}

impl SourceRender for SqlServerSource {
    type Time = Lsn;

    const STATUS_NAMESPACE: StatusNamespace = StatusNamespace::SqlServer;

    fn render<G: Scope<Timestamp = Self::Time>>(
        self,
        scope: &mut G,
        config: &RawSourceCreationConfig,
        resume_uppers: impl futures::Stream<Item = Antichain<Self::Time>> + 'static,
        _start_signal: impl Future<Output = ()> + 'static,
    ) -> (
        // Timely Collection for each Source Export defined in the provided `config`.
        BTreeMap<GlobalId, StackedCollection<G, Result<SourceMessage, DataflowError>>>,
        TimelyStream<G, Infallible>,
        TimelyStream<G, HealthStatusMessage>,
        Option<TimelyStream<G, Probe<Self::Time>>>,
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
                partition_index: u64::cast_from(idx),
                initial_lsn: details.initial_lsn,
            };
            source_outputs.insert(*id, output_info);
        }

        let (repl_updates, uppers, repl_errs, repl_token) = replication::render(
            scope.clone(),
            config.clone(),
            source_outputs.clone(),
            self.clone(),
        );

        let (progress_errs, progress_probes, progress_token) = progress::render(
            scope.clone(),
            config.clone(),
            self.connection.clone(),
            source_outputs.clone(),
            resume_uppers,
            self.extras.clone(),
        );

        let partition_count = u64::cast_from(config.source_exports.len());
        let data_streams: Vec<_> = repl_updates
            .inner
            .partition::<CapacityContainerBuilder<_>, _, _>(
                partition_count,
                move |((partition_idx, data), time, diff): &(
                    (u64, Result<SourceMessage, DataflowError>),
                    Lsn,
                    Diff,
                )| { (*partition_idx, (data.clone(), time.clone(), diff.clone())) },
            );
        let mut data_collections = BTreeMap::new();
        for (id, data_stream) in config.source_exports.keys().zip_eq(data_streams) {
            data_collections.insert(*id, data_stream.as_collection());
        }

        let health_init = std::iter::once(HealthStatusMessage {
            id: None,
            namespace: Self::STATUS_NAMESPACE,
            update: HealthStatusUpdate::Running,
        })
        .to_stream(scope);
        let health_errs = repl_errs.concat(&progress_errs).map(move |err| {
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
        let health = health_init.concat(&health_errs);

        (
            data_collections,
            uppers,
            health,
            Some(progress_probes),
            vec![repl_token, progress_token],
        )
    }
}
