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
use mysql_async::Row as MySqlRow;
use mysql_common::value::convert::FromValue;
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
        // Determined which collections need to be snapshot and which already have been.
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
    #[error("mysql server does not have the binlog available at the requested gtid set")]
    BinlogNotAvailable,
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

fn datum_from_scalar<'a, T>(row: &'a mut MySqlRow, col_index: usize, nullable: bool) -> Datum
where
    T: FromValue,
    Datum<'a>: std::convert::From<T> + std::convert::From<Option<T>>,
{
    if nullable {
        Datum::from(row.take::<Option<T>, _>(col_index).unwrap())
    } else {
        Datum::from(row.take::<T, _>(col_index).unwrap())
    }
}

fn datum_from_string<'a>(
    temp: &'a mut Vec<String>,
    row: &mut MySqlRow,
    col_index: usize,
    nullable: bool,
) -> Datum<'a> {
    if nullable {
        if let Some(data) = row.take::<Option<String>, _>(col_index).unwrap() {
            temp.push(data);
            Datum::from(temp.last().unwrap().as_str())
        } else {
            Datum::Null
        }
    } else {
        let data = row.take::<String, _>(col_index).unwrap();
        temp.push(data);
        Datum::from(temp.last().unwrap().as_str())
    }
}

fn datum_from_bytes<'a>(
    temp: &'a mut Vec<Vec<u8>>,
    row: &mut MySqlRow,
    col_index: usize,
    nullable: bool,
) -> Datum<'a> {
    if nullable {
        if let Some(data) = row.take::<Option<Vec<u8>>, _>(col_index).unwrap() {
            temp.push(data);
            Datum::from(temp.last().unwrap().as_slice())
        } else {
            Datum::Null
        }
    } else {
        let data = row.take::<Vec<u8>, _>(col_index).unwrap();
        temp.push(data);
        Datum::from(temp.last().unwrap().as_slice())
    }
}

pub(crate) fn pack_mysql_row(
    row_container: &mut Row,
    row: &mut MySqlRow,
    table_desc: &MySqlTableDesc,
) -> Result<Row, TransientError> {
    let mut packer = row_container.packer();
    let mut temp_bytes = vec![];
    let mut temp_strs = vec![];
    for (col_index, col_desc) in table_desc.columns.iter().enumerate() {
        let nullable = col_desc.column_type.nullable;
        let datum = match col_desc.column_type.scalar_type {
            ScalarType::Bool => datum_from_scalar::<bool>(row, col_index, nullable),
            ScalarType::UInt16 => datum_from_scalar::<u16>(row, col_index, nullable),
            ScalarType::Int16 => datum_from_scalar::<i16>(row, col_index, nullable),
            ScalarType::UInt32 => datum_from_scalar::<u32>(row, col_index, nullable),
            ScalarType::Int32 => datum_from_scalar::<i32>(row, col_index, nullable),
            ScalarType::UInt64 => datum_from_scalar::<u64>(row, col_index, nullable),
            ScalarType::Int64 => datum_from_scalar::<i64>(row, col_index, nullable),
            ScalarType::Float32 => datum_from_scalar::<f32>(row, col_index, nullable),
            ScalarType::Float64 => datum_from_scalar::<f64>(row, col_index, nullable),
            ScalarType::Char { length: _ } => {
                datum_from_string(&mut temp_strs, row, col_index, nullable)
            }
            ScalarType::VarChar { max_length: _ } => {
                datum_from_string(&mut temp_strs, row, col_index, nullable)
            }
            ScalarType::String => datum_from_string(&mut temp_strs, row, col_index, nullable),
            ScalarType::Bytes => datum_from_bytes(&mut temp_bytes, row, col_index, nullable),
            // TODO(roshan): IMPLEMENT OTHER TYPES
            ref data_type => Err(TransientError::UnsupportedDataType(format!(
                "{:?}",
                data_type
            )))?,
        };
        packer.push(datum);
    }

    Ok(row_container.clone())
}
