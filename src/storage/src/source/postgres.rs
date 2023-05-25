// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code to render the ingestion dataflow of a [`PostgresSourceConnection`]. The dataflow consists
//! of multiple operators in order to take advantage of all the available workers.
//!
//! # Snapshot
//!
//! One part of the dataflow deals with snapshotting the tables involved in the ingestion. Each
//! table that needs a snapshot is assigned to a specific worker which performs a `COPY` query
//! and distributes the raw COPY bytes to all workers to decode the text encoded rows.
//!
//! For all tables that ended up being snapshotted the snapshot reader also emits a rewind request
//! to the replication reader which will ensure that the requested portion of the replication
//! stream is subtracted from the snapshot.
//!
//! See the [snapshot] module for more information on the snapshot strategy.
//!
//! # Replication
//!
//! The other part of the dataflow deals with reading the logical replication slot, which must
//! happen from a single worker. The minimum amount of processing is performed from that worker
//! and the data is then distributed among all workers for decoding.
//!
//! See the [replication] module for more information on the replication strategy.
//!
//! # Error handling
//!
//! There are two kinds of errors that can happen during ingestion that are represented as two
//! separate error types:
//!
//! [`DefiniteError`]s are errors that happen during processing of a specific
//! collection record at a specific LSN. These are the only errors that can ever end up in the
//! error collection of a subsource.
//!
//! Transient errors are any errors that can happen for reasons that are unrelated to the data
//! itself. This could be authentication failures, connection failures, etc. The only operators
//! that can emit such errors are the `TableReader` and the `ReplicationReader` operators, which
//! are the ones that talk to the external world. Both of these operators are built with the
//! `AsyncOperatorBuilder::build_fallible` method which allows transient errors to be propagated
//! upwards with the standard `?` operator without risking downgrading the capability and producing
//! bogus frontiers.
//!
//! The error streams from both of those operators are published to the source status and also
//! trigger a restart of the dataflow.
//!
//! ```text
//!    ┏━━━━━━━━━━━━━━┓
//!    ┃    table     ┃
//!    ┃    reader    ┃
//!    ┗━┯━━━━━━━━━━┯━┛
//!      │          │rewind
//!      │          │requests
//!      │          ╰────╮
//!      │             ┏━v━━━━━━━━━━━┓
//!      │             ┃ replication ┃
//!      │             ┃   reader    ┃
//!      │             ┗━┯━━━━━━━━━┯━┛
//!  COPY│           slot│         │
//!  data│           data│         │
//! ┏━━━━v━━━━━┓ ┏━━━━━━━v━━━━━┓   │
//! ┃  COPY    ┃ ┃ replication ┃   │
//! ┃ decoder  ┃ ┃   decoder   ┃   │
//! ┗━━━━┯━━━━━┛ ┗━━━━━┯━━━━━━━┛   │
//!      │snapshot     │replication│
//!      │updates      │updates    │
//!      ╰────╮    ╭───╯           │
//!          ╭┴────┴╮              │
//!          │concat│              │
//!          ╰──┬───╯              │
//!             │ data             │progress
//!             │ output           │output
//!             v                  v
//! ```

use std::any::Any;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::rc::Rc;
use std::time::Duration;

use differential_dataflow::Collection;
use mz_cluster_client::errors::SourceErrorDetails;
use mz_expr::{EvalError, MirScalarExpr};
use mz_ore::error::ErrorExt;
use mz_postgres_util::desc::PostgresTableDesc;
use mz_postgres_util::PostgresError;
use mz_repr::{Datum, Diff, Row};
use mz_sql_parser::ast::{display::AstDisplay, Ident};
use mz_storage_client::types::connections::ConnectionContext;
use mz_storage_client::types::sources::{MzOffset, PostgresSourceConnection, SourceTimestamp};
use serde::{Deserialize, Serialize};
use timely::dataflow::operators::{Concat, Map};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, SimpleQueryMessage, SimpleQueryRow};

use crate::source::types::{HealthStatus, HealthStatusUpdate, SourceRender};
use crate::source::{RawSourceCreationConfig, SourceMessage, SourceReaderError};

mod metrics;
mod replication;
mod snapshot;

impl SourceRender for PostgresSourceConnection {
    type Key = ();
    type Value = Row;
    type Time = MzOffset;

    /// Render the ingestion dataflow. This function only connects things together and contains no
    /// actual processing logic.
    fn render<G: Scope<Timestamp = MzOffset>>(
        self,
        scope: &mut G,
        config: RawSourceCreationConfig,
        context: ConnectionContext,
        resume_uppers: impl futures::Stream<Item = Antichain<MzOffset>> + 'static,
    ) -> (
        Collection<G, (usize, Result<SourceMessage<(), Row>, SourceReaderError>), Diff>,
        Option<Stream<G, Infallible>>,
        Stream<G, (usize, HealthStatusUpdate)>,
        Rc<dyn Any>,
    ) {
        let resume_upper =
            Antichain::from_iter(config.source_resume_upper.iter().map(MzOffset::decode_row));

        // Collect the tables that we will be ingesting.
        let mut table_info = BTreeMap::new();
        let mut subsource_outputs = vec![];
        for (i, desc) in self.publication_details.tables.iter().enumerate() {
            // Index zero maps to the main source
            let output_index = i + 1;
            // The publication might contain more tables than the user has selected to ingest (via
            // a restricted FOR TABLES <..>). The tables that are to be ingested will be present in
            // the table_casts map and so we can filter the publication tables based on whether or
            // not we have casts for it.
            if let Some(casts) = self.table_casts.get(&output_index) {
                table_info.insert(desc.oid, (output_index, desc.clone(), casts.clone()));
                subsource_outputs.push(output_index);
            }
        }

        let (snapshot_updates, rewinds, snapshot_err, snapshot_token) = snapshot::render(
            scope.clone(),
            config.clone(),
            self.clone(),
            context.clone(),
            resume_upper.clone(),
            table_info.clone(),
        );

        let (repl_updates, uppers, repl_err, repl_token) = replication::render(
            scope.clone(),
            config,
            self,
            context,
            resume_upper,
            table_info,
            &rewinds,
            resume_uppers,
        );

        let updates = snapshot_updates.concat(&repl_updates).map(|(output, res)| {
            let res = res.map(|row| SourceMessage {
                upstream_time_millis: None,
                key: (),
                value: row,
                headers: None,
            });
            (output, res)
        });

        let health = snapshot_err.concat(&repl_err).flat_map(move |err| {
            let update = HealthStatus::StalledWithError {
                error: err.display_with_causes().to_string(),
                hint: None,
            };
            // This update will cause the dataflow to restart
            let halt_status = HealthStatusUpdate {
                update: update.clone(),
                should_halt: true,
            };
            let mut statuses = vec![(0, halt_status)];

            // But we still want to report the transient error for all subsources
            statuses.extend(subsource_outputs.iter().map(|index| {
                let status = HealthStatusUpdate {
                    update: update.clone(),
                    should_halt: false,
                };
                (*index, status)
            }));
            statuses
        });

        let token = Rc::new((snapshot_token, repl_token));
        (updates, Some(uppers), health, token)
    }
}

/// A transient error that never ends up in the collection of a specific table.
#[derive(Debug, thiserror::Error)]
pub enum TransientError {
    #[error("replication slot mysteriously missing")]
    MissingReplicationSlot,
    #[error("slot overcompacted. Requested LSN {requested_lsn} but only LSNs >= {available_lsn} are available")]
    OvercompactedReplicationSlot {
        requested_lsn: MzOffset,
        available_lsn: MzOffset,
    },
    #[error("stream ended prematurely")]
    ReplicationEOF,
    #[error("unexpected replication message")]
    UnknownReplicationMessage,
    #[error("unexpected logical replication message")]
    UnknownLogicalReplicationMessage,
    #[error("malformed logical replication message")]
    MalformedReplicationMessage(#[source] std::io::Error),
    #[error("transaction begun without BEGIN")]
    UnmatchedTransaction,
    #[error("received replication event outside of transaction")]
    BareTransactionEvent,
    #[error("lsn mismatch between BEGIN and COMMIT")]
    InvalidTransaction,
    #[error("BEGIN within existing BEGIN stream")]
    NestedTransaction,
    #[error("query returned more rows than expected")]
    UnexpectedRow,
    #[error("recoverable errors should crash the process during snapshots")]
    SyntheticError,
    #[error("sql client error")]
    SQLClient(#[from] tokio_postgres::Error),
    #[error(transparent)]
    PostgresError(#[from] PostgresError),
    #[error(transparent)]
    Generic(#[from] anyhow::Error),
}

/// A definite error that always ends up in the collection of a specific table.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum DefiniteError {
    #[error("table was truncated")]
    TableTruncated,
    #[error("table was dropped")]
    TableDropped,
    #[error("publication {0:?} does not exist")]
    PublicationDropped(String),
    #[error("unexpected number of columns while parsing COPY output")]
    MissingColumn,
    #[error("failed to parse COPY protocol")]
    InvalidCopyInput,
    #[error("TOASTed value missing from old row. Did you forget to set REPLICA IDENTITY to FULL for your table?")]
    MissingToast,
    #[error("old row missing from replication stream. Did you forget to set REPLICA IDENTITY to FULL for your table?")]
    DefaultReplicaIdentity,
    #[error("incompatible schema change: {0}")]
    // TODO: proper error variants for all the expected schema violations
    IncompatibleSchema(String),
    #[error("invalid UTF8 string: {0:?}")]
    InvalidUTF8(Vec<u8>),
    #[error("failed to cast raw column: {0}")]
    CastError(#[source] EvalError),
}

impl From<DefiniteError> for SourceReaderError {
    fn from(err: DefiniteError) -> Self {
        SourceReaderError {
            inner: SourceErrorDetails::Other(err.to_string()),
        }
    }
}

/// Ensures the replication slot of this connection is created.
async fn ensure_replication_slot(client: &Client, slot: &str) -> Result<(), TransientError> {
    let slot = Ident::from(slot).to_ast_string();
    let query = format!("CREATE_REPLICATION_SLOT {slot} LOGICAL \"pgoutput\" NOEXPORT_SNAPSHOT");
    match simple_query_opt(client, &query).await {
        Ok(_) => Ok(()),
        // If the slot already exists that's still ok
        Err(TransientError::SQLClient(err)) if err.code() == Some(&SqlState::DUPLICATE_OBJECT) => {
            Ok(())
        }
        Err(err) => Err(err),
    }
}

/// Fetches the minimum LSN at which this slot can safely resume.
async fn fetch_slot_resume_lsn(client: &Client, slot: &str) -> Result<MzOffset, TransientError> {
    loop {
        let query = format!(
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{slot}'"
        );
        let Some(row) = simple_query_opt(client, &query).await? else {
            return Err(TransientError::MissingReplicationSlot);
        };

        match row.get("confirmed_flush_lsn") {
            // For postgres, `confirmed_flush_lsn` means that the slot is able to produce
            // all transactions that happen at tx_lsn > confirmed_flush_lsn. Therefore the
            // upper is confirmed_flush_lsn + 1
            Some(flush_lsn) => return Ok(MzOffset::from(flush_lsn.parse::<PgLsn>().unwrap()) + 1),
            // It can happen that confirmed_flush_lsn is NULL as the slot initializes
            None => tokio::time::sleep(Duration::from_millis(500)).await,
        };
    }
}

// Ensures that the table with oid `oid` and expected schema `expected_schema` is still compatible
// with the current upstream schema `upstream_info`.
fn verify_schema(
    oid: u32,
    expected_desc: &PostgresTableDesc,
    upstream_info: &BTreeMap<u32, PostgresTableDesc>,
) -> Result<(), DefiniteError> {
    let current_desc = upstream_info.get(&oid).ok_or(DefiniteError::TableDropped)?;

    match expected_desc.determine_compatibility(current_desc) {
        Ok(()) => Ok(()),
        Err(err) => Err(DefiniteError::IncompatibleSchema(err.to_string())),
    }
}

/// Runs the given query using the client and expects at most a single row to be returned.
async fn simple_query_opt(
    client: &Client,
    query: &str,
) -> Result<Option<SimpleQueryRow>, TransientError> {
    let result = client.simple_query(query).await?;
    let mut rows = result.into_iter().filter_map(|msg| match msg {
        SimpleQueryMessage::Row(row) => Some(row),
        _ => None,
    });
    match (rows.next(), rows.next()) {
        (Some(row), None) => Ok(Some(row)),
        (None, None) => Ok(None),
        _ => Err(TransientError::UnexpectedRow),
    }
}

/// Casts a text row into the target types
fn cast_row(
    casts: &[MirScalarExpr],
    datums: &[Datum<'_>],
    row: &mut Row,
) -> Result<(), DefiniteError> {
    let arena = mz_repr::RowArena::new();
    let mut packer = row.packer();
    for column_cast in casts {
        let datum = column_cast
            .eval(datums, &arena)
            .map_err(DefiniteError::CastError)?;
        packer.push(datum);
    }
    Ok(())
}

/// Converts raw bytes that are expected to be UTF8 encoded into a `Datum::String`
fn decode_utf8_text(bytes: &[u8]) -> Result<Datum<'_>, DefiniteError> {
    match std::str::from_utf8(bytes) {
        Ok(text) => Ok(Datum::String(text)),
        Err(_) => Err(DefiniteError::InvalidUTF8(bytes.to_vec())),
    }
}
