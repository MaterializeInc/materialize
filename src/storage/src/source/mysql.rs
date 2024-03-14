// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code to render the ingestion dataflow of a [`MySqlSourceConnection`].
//!
//! This dataflow is split into Snapshot and Replication operators.
//!
//! # Snapshot
//!
//! The snapshot operator is responsible for taking a consistent snapshot of the tables involved
//! in the ingestion from the MySQL server. Each table that is being ingested is snapshot is
//! assigned a specific worker, which performs a `SELECT * FROM table` and emits updates for all
//! the rows in the given table.
//!
//! For all tables that are snapshotted the snapshot operator also emits a rewind request to
//! the replication operator containing the GTID-set based frontier which will be used to
//! ensure that the requested portion of the replication stream is subtracted from the snapshot.
//!
//! See the [snapshot] module for more information.
//!
//! # Replication
//!
//! The replication operator is responsible for ingesting the MySQL replication stream which must
//! happen from a single worker.
//!
//! See the [replication] module for more information.
//!
//! # Error handling
//!
//! There are two kinds of errors that can happen during ingestion that are represented as two
//! separate error types:
//!
//! [`DefiniteError`]s are errors that happen during processing of a specific collection record.
//! These are the only errors that can ever end up in the error collection of a subsource.
//!
//! [`TransientError`]s are any errors that can happen for reasons that are unrelated to the data
//! itself. This could be authentication failures, connection failures, etc. The only operators
//! that can emit such errors are the `MySqlReplicationReader` and the `MySqlSnapshotReader`
//! operators, which are the ones that talk to the external world. Both of these operators are
//! built with the `AsyncOperatorBuilder::build_fallible` method which allows transient errors
//! to be propagated upwards with the standard `?` operator without risking downgrading the
//! capability and producing bogus frontiers.
//!
//! The error streams from both of those operators are published to the source status and also
//! trigger a restart of the dataflow.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt;
use std::io;
use std::rc::Rc;

use differential_dataflow::Collection;
use mz_storage_types::sources::SourceExport;
use serde::{Deserialize, Serialize};
use timely::dataflow::channels::pushers::TeeCore;
use timely::dataflow::operators::{CapabilitySet, Concat, Map, ToStream};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use uuid::Uuid;

use mz_mysql_util::{
    ensure_full_row_binlog_format, ensure_gtid_consistency, ensure_replication_commit_order,
    MySqlError, MySqlTableDesc,
};
use mz_ore::error::ErrorExt;
use mz_repr::{Diff, Row};
use mz_storage_types::errors::SourceErrorDetails;
use mz_storage_types::sources::mysql::{GtidPartition, GtidState};
use mz_storage_types::sources::{MySqlSourceConnection, SourceTimestamp};
use mz_timely_util::builder_async::{AsyncOutputHandle, PressOnDropButton};
use mz_timely_util::order::Extrema;

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::source::types::{ProgressStatisticsUpdate, SourceRender};
use crate::source::{RawSourceCreationConfig, SourceMessage, SourceReaderError};

mod replication;
mod schemas;
mod snapshot;
mod statistics;

impl SourceRender for MySqlSourceConnection {
    type Time = GtidPartition;

    const STATUS_NAMESPACE: StatusNamespace = StatusNamespace::MySql;

    /// Render the ingestion dataflow. This function only connects things together and contains no
    /// actual processing logic.
    fn render<G: Scope<Timestamp = GtidPartition>>(
        self,
        scope: &mut G,
        config: RawSourceCreationConfig,
        resume_uppers: impl futures::Stream<Item = Antichain<GtidPartition>> + 'static,
        _start_signal: impl std::future::Future<Output = ()> + 'static,
    ) -> (
        Collection<G, (usize, Result<SourceMessage, SourceReaderError>), Diff>,
        Option<Stream<G, Infallible>>,
        Stream<G, HealthStatusMessage>,
        Stream<G, ProgressStatisticsUpdate>,
        Vec<PressOnDropButton>,
    ) {
        // Determine which collections need to be snapshot and which already have been.
        let subsource_resume_uppers: BTreeMap<_, _> = config
            .source_resume_uppers
            .iter()
            .map(|(id, upper)| {
                assert!(
                    config.source_exports.contains_key(id),
                    "all source resume uppers must be present in source exports"
                );

                (
                    *id,
                    Antichain::from_iter(upper.iter().map(GtidPartition::decode_row)),
                )
            })
            .collect();

        // Collect the tables that we will be ingesting.
        let mut table_info = BTreeMap::new();
        for (_id, SourceExport { output_index, .. }) in &config.source_exports {
            // Output index 0 is the primary source which is not a table.
            if *output_index == 0 {
                continue;
            }

            let desc = &self.details.tables[output_index - 1];
            table_info.insert(
                MySqlTableName::new(&desc.schema_name, &desc.name),
                (*output_index, desc.clone()),
            );
        }

        let metrics = config.metrics.get_mysql_source_metrics(config.id);

        let (snapshot_updates, rewinds, snapshot_stats, snapshot_err, snapshot_token) =
            snapshot::render(
                scope.clone(),
                config.clone(),
                self.clone(),
                subsource_resume_uppers.clone(),
                table_info.clone(),
                metrics.snapshot_metrics.clone(),
            );

        let (repl_updates, uppers, repl_err, repl_token) = replication::render(
            scope.clone(),
            config.clone(),
            self.clone(),
            subsource_resume_uppers,
            table_info,
            &rewinds,
            metrics,
        );

        let (stats_stream, stats_err, stats_token) =
            statistics::render(scope.clone(), config, self, resume_uppers);

        let stats_stream = stats_stream.concat(&snapshot_stats);

        let updates = snapshot_updates.concat(&repl_updates).map(|(output, res)| {
            let res = res.map(|row| SourceMessage {
                key: Row::default(),
                value: row,
                metadata: Row::default(),
            });
            (output, res)
        });

        let health_init = std::iter::once(HealthStatusMessage {
            index: 0,
            namespace: Self::STATUS_NAMESPACE,
            update: HealthStatusUpdate::Running,
        })
        .to_stream(scope);

        let health_errs = snapshot_err
            .concat(&repl_err)
            .concat(&stats_err)
            .map(move |err| {
                // This update will cause the dataflow to restart
                let err_string = err.display_with_causes().to_string();
                let update = HealthStatusUpdate::halting(err_string.clone(), None);

                let namespace = match err {
                    ReplicationError::Transient(err)
                        if matches!(&*err, TransientError::MySqlError(MySqlError::Ssh(_))) =>
                    {
                        StatusNamespace::Ssh
                    }
                    _ => Self::STATUS_NAMESPACE,
                };

                HealthStatusMessage {
                    index: 0,
                    namespace: namespace.clone(),
                    update,
                }
            });
        let health = health_init.concat(&health_errs);

        (
            updates,
            Some(uppers),
            health,
            stats_stream,
            vec![snapshot_token, repl_token, stats_token],
        )
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum ReplicationError {
    #[error(transparent)]
    Transient(#[from] Rc<TransientError>),
    #[error(transparent)]
    Definite(#[from] Rc<DefiniteError>),
}

/// A transient error that never ends up in the collection of a specific table.
#[derive(Debug, thiserror::Error)]
pub enum TransientError {
    #[error("couldn't decode binlog row")]
    BinlogRowDecodeError(#[from] mysql_async::binlog::row::BinlogRowToRowError),
    #[error("stream ended prematurely")]
    ReplicationEOF,
    #[error(transparent)]
    IoError(#[from] io::Error),
    #[error("sql client error")]
    SQLClient(#[from] mysql_async::Error),
    #[error("ident decode error")]
    IdentError(#[from] mz_sql_parser::ast::IdentError),
    #[error(transparent)]
    MySqlError(#[from] MySqlError),
    #[error(transparent)]
    Generic(#[from] anyhow::Error),
}

/// A definite error that always ends up in the collection of a specific table.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum DefiniteError {
    #[error("unable to decode: {0}")]
    ValueDecodeError(String),
    #[error("table was truncated: {0}")]
    TableTruncated(String),
    #[error("table was dropped: {0}")]
    TableDropped(String),
    #[error("incompatible schema change: {0}")]
    IncompatibleSchema(String),
    #[error("received a gtid set from the server that violates our requirements: {0}")]
    UnsupportedGtidState(String),
    #[error("received out of order gtids for source {0} at transaction-id {1}")]
    BinlogGtidMonotonicityViolation(String, GtidState),
    #[error("mysql server does not have the binlog available at the requested gtid set")]
    BinlogNotAvailable,
    #[error("mysql server binlog frontier at {0} is beyond required frontier {1}")]
    BinlogMissingResumePoint(String, String),
    #[error("mysql server configuration error: {0}")]
    ServerConfigurationError(String),
}

impl From<DefiniteError> for SourceReaderError {
    fn from(err: DefiniteError) -> Self {
        SourceReaderError {
            inner: SourceErrorDetails::Other(err.to_string()),
        }
    }
}

/// A reference to a MySQL table. (schema_name, table_name)
/// NOTE: We do not use `mz_sql_parser::ast:UnresolvedItemName` because the serialization
/// behavior is not what we need for mysql.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub(crate) struct MySqlTableName(pub(crate) String, pub(crate) String);

impl MySqlTableName {
    pub(crate) fn new(schema_name: &str, table_name: &str) -> Self {
        Self(schema_name.to_string(), table_name.to_string())
    }
}

impl fmt::Display for MySqlTableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "`{}`.`{}`", self.0, self.1)
    }
}

impl From<&MySqlTableDesc> for MySqlTableName {
    fn from(desc: &MySqlTableDesc) -> Self {
        Self::new(&desc.schema_name, &desc.name)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct RewindRequest {
    /// The table that should be rewound.
    pub(crate) table: MySqlTableName,
    /// The frontier of GTIDs that this snapshot represents; all GTIDs that are not beyond this
    /// frontier have been committed by the snapshot operator at timestamp 0.
    pub(crate) snapshot_upper: Antichain<GtidPartition>,
}

async fn return_definite_error(
    err: DefiniteError,
    outputs: &[usize],
    data_handle: &mut AsyncOutputHandle<
        GtidPartition,
        Vec<((usize, Result<Row, DefiniteError>), GtidPartition, i64)>,
        TeeCore<GtidPartition, Vec<((usize, Result<Row, DefiniteError>), GtidPartition, i64)>>,
    >,
    data_cap_set: &CapabilitySet<GtidPartition>,
    definite_error_handle: &mut AsyncOutputHandle<
        GtidPartition,
        Vec<ReplicationError>,
        TeeCore<GtidPartition, Vec<ReplicationError>>,
    >,
    definite_error_cap_set: &CapabilitySet<GtidPartition>,
) -> () {
    for output_index in outputs {
        let update = (
            (*output_index, Err(err.clone())),
            GtidPartition::new_range(Uuid::minimum(), Uuid::maximum(), GtidState::MAX),
            1,
        );
        data_handle.give(&data_cap_set[0], update).await;
    }
    definite_error_handle
        .give(
            &definite_error_cap_set[0],
            ReplicationError::Definite(Rc::new(err)),
        )
        .await;
    ()
}

async fn validate_mysql_repl_settings(conn: &mut mysql_async::Conn) -> Result<(), MySqlError> {
    ensure_gtid_consistency(conn).await?;
    ensure_full_row_binlog_format(conn).await?;
    ensure_replication_commit_order(conn).await?;

    Ok(())
}
