// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code to render the ingestion dataflow of a [`MySqlSourceConnection`].

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::io;
use std::rc::Rc;

use differential_dataflow::Collection;
use itertools::Itertools;
use mysql_async::Row as MySqlRow;
use mysql_common::value::convert::from_value;
use mysql_common::Value;
use mz_mysql_util::MySqlTableDesc;
use serde::{Deserialize, Serialize};
use timely::dataflow::operators::Concat;
use timely::dataflow::operators::Map;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use mz_mysql_util::{GtidSet, MySqlError};
use mz_ore::error::ErrorExt;
use mz_repr::{Datum, Diff, Row, ScalarType};
use mz_sql_parser::ast::{Ident, UnresolvedItemName};
use mz_storage_types::errors::SourceErrorDetails;
use mz_storage_types::sources::MySqlSourceConnection;
use mz_storage_types::sources::SourceTimestamp;
use mz_timely_util::builder_async::PressOnDropButton;

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::source::types::SourceRender;
use crate::source::{RawSourceCreationConfig, SourceMessage, SourceReaderError};

mod replication;
mod snapshot;
mod timestamp;

use timestamp::TransactionId;

impl SourceRender for MySqlSourceConnection {
    type Key = ();
    type Value = Row;
    // TODO: Eventually replace with a Partitioned<Uuid, TransactionId> timestamp
    type Time = TransactionId;

    const STATUS_NAMESPACE: StatusNamespace = StatusNamespace::Postgres;

    /// Render the ingestion dataflow. This function only connects things together and contains no
    /// actual processing logic.
    fn render<G: Scope<Timestamp = TransactionId>>(
        self,
        scope: &mut G,
        config: RawSourceCreationConfig,
        resume_uppers: impl futures::Stream<Item = Antichain<TransactionId>> + 'static,
        _start_signal: impl std::future::Future<Output = ()> + 'static,
    ) -> (
        Collection<G, (usize, Result<SourceMessage<(), Row>, SourceReaderError>), Diff>,
        Option<Stream<G, Infallible>>,
        Stream<G, HealthStatusMessage>,
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
                    Antichain::from_iter(upper.iter().map(TransactionId::decode_row)),
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

        let (snapshot_updates, rewinds, snapshot_err, snapshot_token) = snapshot::render(
            scope.clone(),
            config.clone(),
            self.clone(),
            subsource_resume_uppers.clone(),
            table_info.clone(),
        );

        let (repl_updates, uppers, repl_err, repl_token) = replication::render(
            scope.clone(),
            config,
            self,
            subsource_resume_uppers,
            table_info,
            &rewinds,
            resume_uppers,
        );

        let updates = snapshot_updates.concat(&repl_updates).map(|(output, res)| {
            let res = res.map(|row| SourceMessage {
                key: (),
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
    #[error("received a null value in a non-null column: {0}")]
    NullValueInNonNullColumn(String),
    #[error("mysql server does not have the binlog available at the requested gtid set")]
    BinlogNotAvailable,
    #[error("mysql server binlogs were rotated to {0} past our resume point of {1}")]
    BinlogCompactedPastResumePoint(String, String),
    #[error("server gtid error: {0}")]
    ServerGTIDError(String),
}

impl From<DefiniteError> for SourceReaderError {
    fn from(err: DefiniteError) -> Self {
        SourceReaderError {
            inner: SourceErrorDetails::Other(err.to_string()),
        }
    }
}

/// TODO: This should use a partitioned timestamp implementation instead of the snapshot gtid set.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct RewindRequest {
    /// The table that should be rewound.
    pub(crate) table: UnresolvedItemName,
    /// The GTID set at the start of the snapshot, returned by the server's `gtid_executed` system variable.
    pub(crate) snapshot_gtid_set: GtidSet,
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
