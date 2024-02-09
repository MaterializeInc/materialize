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
use std::io;
use std::rc::Rc;

use differential_dataflow::Collection;
use itertools::Itertools;
use mysql_async::Row as MySqlRow;
use mysql_common::value::convert::from_value;
use mysql_common::Value;
use serde::{Deserialize, Serialize};
use timely::dataflow::channels::pushers::TeeCore;
use timely::dataflow::operators::{CapabilitySet, Concat, Map};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use uuid::Uuid;

use mz_mysql_util::{
    ensure_full_row_binlog_format, ensure_gtid_consistency, ensure_replication_commit_order,
    MySqlError, MySqlTableDesc,
};
use mz_ore::error::ErrorExt;
use mz_repr::{Datum, Diff, Row, ScalarType};
use mz_sql_parser::ast::{Ident, UnresolvedItemName};
use mz_storage_types::errors::SourceErrorDetails;
use mz_storage_types::sources::mysql::{GtidPartition, GtidState};
use mz_storage_types::sources::{MySqlSourceConnection, SourceTimestamp};
use mz_timely_util::builder_async::{AsyncOutputHandle, PressOnDropButton};
use mz_timely_util::order::Extrema;

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::source::types::{ProgressStatisticsUpdate, SourceRender};
use crate::source::{RawSourceCreationConfig, SourceMessage, SourceReaderError};

mod replication;
mod snapshot;

impl SourceRender for MySqlSourceConnection {
    type Time = GtidPartition;

    const STATUS_NAMESPACE: StatusNamespace = StatusNamespace::MySql;

    /// Render the ingestion dataflow. This function only connects things together and contains no
    /// actual processing logic.
    fn render<G: Scope<Timestamp = GtidPartition>>(
        self,
        scope: &mut G,
        config: RawSourceCreationConfig,
        _resume_uppers: impl futures::Stream<Item = Antichain<GtidPartition>> + 'static,
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
        for (i, desc) in self.details.tables.iter().enumerate() {
            table_info.insert(
                table_name(&desc.schema_name, &desc.name).expect("valid idents"),
                (
                    // Index zero maps to the main source
                    i + 1,
                    desc.clone(),
                ),
            );
        }

        let metrics = config.metrics.get_mysql_metrics(config.id);

        let (snapshot_updates, rewinds, snapshot_err, snapshot_token) = snapshot::render(
            scope.clone(),
            config.clone(),
            self.clone(),
            subsource_resume_uppers.clone(),
            table_info.clone(),
            metrics.snapshot_metrics.clone(),
        );

        let (repl_updates, uppers, repl_err, repl_token) = replication::render(
            scope.clone(),
            config,
            self,
            subsource_resume_uppers,
            table_info,
            &rewinds,
            metrics,
        );

        let updates = snapshot_updates.concat(&repl_updates).map(|(output, res)| {
            let res = res.map(|row| SourceMessage {
                key: Row::default(),
                value: row,
                metadata: Row::default(),
            });
            (output, res)
        });

        let health = snapshot_err.concat(&repl_err).map(move |err| {
            // This update will cause the dataflow to restart
            let err_string = err.display_with_causes().to_string();
            let update = HealthStatusUpdate::halting(err_string.clone(), None);
            // TODO: change namespace for SSH errors
            let namespace = Self::STATUS_NAMESPACE.clone();

            HealthStatusMessage {
                index: 0,
                namespace: namespace.clone(),
                update,
            }
        });

        (
            updates,
            Some(uppers),
            health,
            // TODO(guswynn): add progress statistics updates once the core mysql impl is fully
            // fleshed out.
            timely::dataflow::operators::generic::operator::empty(scope),
            vec![snapshot_token, repl_token],
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
    #[error("unsupported type: {0}")]
    UnsupportedDataType(String),
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
    #[error("table was dropped")]
    TableDropped,
    #[error("incompatible schema change: {0}")]
    IncompatibleSchema(String),
    #[error("received a gtid set from the server that violates our requirements: {0}")]
    UnsupportedGtidState(String),
    #[error("received out of order gtids for source {0} at transaction-id {1}")]
    BinlogGtidMonotonicityViolation(String, GtidState),
    #[error("received a null value in a non-null column: {0}")]
    NullValueInNonNullColumn(String),
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct RewindRequest {
    /// The table that should be rewound.
    pub(crate) table: UnresolvedItemName,
    /// The frontier of GTIDs that this snapshot represents; all GTIDs that are not beyond this
    /// frontier have been committed by the snapshot operator at timestamp 0.
    pub(crate) snapshot_upper: Antichain<GtidPartition>,
}

pub(crate) fn table_name(
    schema_name: &str,
    table_name: &str,
) -> Result<UnresolvedItemName, TransientError> {
    Ok(UnresolvedItemName::qualified(&[
        Ident::new(schema_name)?,
        Ident::new(table_name)?,
    ]))
}

pub(crate) fn pack_mysql_row(
    row_container: &mut Row,
    row: MySqlRow,
    table_desc: &MySqlTableDesc,
) -> Result<Result<Row, DefiniteError>, TransientError> {
    let mut packer = row_container.packer();
    let mut temp_bytes = vec![];
    let mut temp_strs = vec![];
    let values = row.unwrap();

    for (col_desc, value) in table_desc.columns.iter().zip_eq(values) {
        let datum = match value {
            Value::NULL => {
                if col_desc.column_type.nullable {
                    Datum::Null
                } else {
                    // produce definite error and stop ingesting this table
                    return Ok(Err(DefiniteError::NullValueInNonNullColumn(
                        col_desc.name.clone(),
                    )));
                }
            }
            value => match &col_desc.column_type.scalar_type {
                ScalarType::Bool => Datum::from(from_value::<bool>(value)),
                ScalarType::UInt16 => Datum::from(from_value::<u16>(value)),
                ScalarType::Int16 => Datum::from(from_value::<i16>(value)),
                ScalarType::UInt32 => Datum::from(from_value::<u32>(value)),
                ScalarType::Int32 => Datum::from(from_value::<i32>(value)),
                ScalarType::UInt64 => Datum::from(from_value::<u64>(value)),
                ScalarType::Int64 => Datum::from(from_value::<i64>(value)),
                ScalarType::Float32 => Datum::from(from_value::<f32>(value)),
                ScalarType::Float64 => Datum::from(from_value::<f64>(value)),
                ScalarType::Char { length: _ } => {
                    temp_strs.push(from_value::<String>(value));
                    Datum::from(temp_strs.last().unwrap().as_str())
                }
                ScalarType::VarChar { max_length: _ } => {
                    temp_strs.push(from_value::<String>(value));
                    Datum::from(temp_strs.last().unwrap().as_str())
                }
                ScalarType::String => {
                    temp_strs.push(from_value::<String>(value));
                    Datum::from(temp_strs.last().unwrap().as_str())
                }
                ScalarType::Bytes => {
                    let data = from_value::<Vec<u8>>(value);
                    temp_bytes.push(data);
                    Datum::from(temp_bytes.last().unwrap().as_slice())
                }
                // TODO(roshan): IMPLEMENT OTHER TYPES
                data_type => Err(TransientError::UnsupportedDataType(format!(
                    "{:?}",
                    data_type
                )))?,
            },
        };
        packer.push(datum);
    }

    Ok(Ok(row_container.clone()))
}

/// Holds the active and future GTID partitions that represent the complete
/// UUID range of all possible GTID source-ids from a MySQL server.
/// The active partitions are all singleton partitions representing a single
/// source-id timestamp, and the future partitions represent the missing
/// UUID ranges that we have not yet seen and are held at timestamp GtidState::Absent.
///
/// This is used to keep track of all partitions and is updated as we receive
/// new GTID updates from the server, and is used to create a full 'frontier'
/// representing the current state of all GTID partitions that we can use
/// to downgrade capabilities for the source.
///
/// We could instead mint capabilities for each individual partition
/// and advance each partition as a frontier separately using its own capabilities,
/// but since this is used inside a fallible operator if we ever hit any errors
/// then all newly minted capabilities would be dropped by the async runtime
/// and we might lose the ability to send any more data.
/// However if we just use the main capabilities provided to the operator, the
/// capabilities externally will be preserved even in the case of an error in
/// the operator, which is why we just manage a single frontier and capability set.
struct GtidReplicationPartitions {
    active: BTreeMap<Uuid, GtidPartition>,
    future: Vec<GtidPartition>,
}

impl From<Antichain<GtidPartition>> for GtidReplicationPartitions {
    fn from(frontier: Antichain<GtidPartition>) -> Self {
        let mut active = BTreeMap::new();
        let mut future = Vec::new();
        for part in frontier.iter() {
            if part.timestamp() == &GtidState::Absent {
                future.push(part.clone());
            } else {
                let source_id = part.interval().singleton().unwrap().clone();
                active.insert(source_id, part.clone());
            }
        }
        Self { active, future }
    }
}

impl GtidReplicationPartitions {
    /// Return an Antichain for the frontier composed of all the
    /// active and future GTID partitions.
    fn frontier(&self) -> Antichain<GtidPartition> {
        Antichain::from_iter(
            self.active
                .values()
                .cloned()
                .chain(self.future.iter().cloned()),
        )
    }

    /// Given a singleton GTID partition, update the timestamp of the existing
    /// active partition with the same UUID
    /// or split the future partitions to remove this new partition and then
    /// insert the new 'active' partition.
    ///
    /// This is used whenever we receive a GTID update from the server and
    /// need to update our state keeping track
    /// of all the active and future GTID partition timestamps.
    /// This call should usually be followed up by downgrading capabilities
    /// using the frontier returned by `self.frontier()`
    fn update(&mut self, new_part: GtidPartition) -> Result<(), DefiniteError> {
        let source_id = new_part.interval().singleton().unwrap();
        // Check if we have an active partition for the GTID UUID
        match self.active.get_mut(source_id) {
            Some(active_part) => {
                // Since we start replication at a specific upper, we
                // should only see GTID transaction-ids
                // in a monotonic order for each source, starting at that upper.
                if active_part.timestamp() > &new_part.timestamp() {
                    let err = DefiniteError::BinlogGtidMonotonicityViolation(
                        source_id.to_string(),
                        new_part.timestamp().clone(),
                    );
                    return Err(err);
                }

                // replace this active partition with the new one
                *active_part = new_part;
            }
            // We've received a GTID for a UUID we don't yet know about
            None => {
                // Extract the future partition whose range encompasses this UUID
                // TODO: Replace with Vec::extract_if() once it's stabilized
                let mut i = 0;
                let mut contained_part = None;
                while i < self.future.len() {
                    if self.future[i].interval().contains(source_id) {
                        contained_part = Some(self.future.remove(i));
                        break;
                    } else {
                        i += 1;
                    }
                }
                let contained_part =
                    contained_part.expect("expected a future partition to contain the UUID");

                // Split the future partition into partitions for before and after this UUID
                // and add back to the future partitions.
                let (before_range, after_range) = contained_part.split(source_id);
                self.future
                    .extend(before_range.into_iter().chain(after_range.into_iter()));

                // Store the new part in our active partitions
                self.active.insert(source_id.clone(), new_part);
            }
        };

        Ok(())
    }
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
