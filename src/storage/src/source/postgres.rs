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

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::rc::Rc;
use std::time::Duration;

use differential_dataflow::AsCollection;
use itertools::Itertools as _;
use mz_expr::{EvalError, MirScalarExpr};
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_postgres_util::desc::PostgresTableDesc;
use mz_postgres_util::{Client, PostgresError, simple_query_opt};
use mz_repr::{Datum, Diff, GlobalId, Row};
use mz_sql_parser::ast::Ident;
use mz_sql_parser::ast::display::AstDisplay;
use mz_storage_types::errors::{DataflowError, SourceError, SourceErrorDetails};
use mz_storage_types::sources::postgres::CastType;
use mz_storage_types::sources::{
    MzOffset, PostgresSourceConnection, SourceExport, SourceExportDetails, SourceTimestamp,
};
use mz_timely_util::builder_async::PressOnDropButton;
use serde::{Deserialize, Serialize};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::operators::core::Partition;
use timely::dataflow::operators::{Concat, Map, ToStream};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::PgLsn;

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::source::types::{Probe, SourceRender, StackedCollection};
use crate::source::{RawSourceCreationConfig, SourceMessage};

mod replication;
mod snapshot;

impl SourceRender for PostgresSourceConnection {
    type Time = MzOffset;

    const STATUS_NAMESPACE: StatusNamespace = StatusNamespace::Postgres;

    /// Render the ingestion dataflow. This function only connects things together and contains no
    /// actual processing logic.
    fn render<G: Scope<Timestamp = MzOffset>>(
        self,
        scope: &mut G,
        config: &RawSourceCreationConfig,
        resume_uppers: impl futures::Stream<Item = Antichain<MzOffset>> + 'static,
        _start_signal: impl std::future::Future<Output = ()> + 'static,
    ) -> (
        BTreeMap<GlobalId, StackedCollection<G, Result<SourceMessage, DataflowError>>>,
        Stream<G, Infallible>,
        Stream<G, HealthStatusMessage>,
        Option<Stream<G, Probe<MzOffset>>>,
        Vec<PressOnDropButton>,
    ) {
        // Collect the source outputs that we will be exporting into a per-table map.
        let mut table_info = BTreeMap::new();
        for (idx, (id, export)) in config.source_exports.iter().enumerate() {
            let SourceExport {
                details,
                storage_metadata: _,
                data_config: _,
            } = export;
            let details = match details {
                SourceExportDetails::Postgres(details) => details,
                // This is an export that doesn't need any data output to it.
                SourceExportDetails::None => continue,
                _ => panic!("unexpected source export details: {:?}", details),
            };
            let desc = details.table.clone();
            let casts = details.column_casts.clone();
            let resume_upper = Antichain::from_iter(
                config
                    .source_resume_uppers
                    .get(id)
                    .expect("all source exports must be present in source resume uppers")
                    .iter()
                    .map(MzOffset::decode_row),
            );
            let output = SourceOutputInfo {
                desc,
                casts,
                resume_upper,
                export_id: id.clone(),
            };
            table_info
                .entry(output.desc.oid)
                .or_insert_with(BTreeMap::new)
                .insert(idx, output);
        }

        let metrics = config.metrics.get_postgres_source_metrics(config.id);

        let (snapshot_updates, rewinds, slot_ready, snapshot_err, snapshot_token) =
            snapshot::render(
                scope.clone(),
                config.clone(),
                self.clone(),
                table_info.clone(),
                metrics.snapshot_metrics.clone(),
            );

        let (repl_updates, uppers, probe_stream, repl_err, repl_token) = replication::render(
            scope.clone(),
            config.clone(),
            self,
            table_info,
            &rewinds,
            &slot_ready,
            resume_uppers,
            metrics,
        );

        let updates = snapshot_updates.concat(&repl_updates);
        let partition_count = u64::cast_from(config.source_exports.len());
        let data_streams: Vec<_> = updates
            .inner
            .partition::<CapacityContainerBuilder<_>, _, _>(
                partition_count,
                |((output, data), time, diff): &(
                    (usize, Result<SourceMessage, DataflowError>),
                    MzOffset,
                    Diff,
                )| {
                    let output = u64::cast_from(*output);
                    (output, (data.clone(), time.clone(), diff.clone()))
                },
            );
        let mut data_collections = BTreeMap::new();
        for (id, data_stream) in config.source_exports.keys().zip_eq(data_streams) {
            data_collections.insert(*id, data_stream.as_collection());
        }

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

        // N.B. Note that we don't check ssh tunnel statuses here. We could, but immediately on
        // restart we are going to set the status to an ssh error correctly, so we don't do this
        // extra work.
        let errs = snapshot_err.concat(&repl_err).map(move |err| {
            // This update will cause the dataflow to restart
            let err_string = err.display_with_causes().to_string();
            let update = HealthStatusUpdate::halting(err_string.clone(), None);

            let namespace = match err {
                ReplicationError::Transient(err)
                    if matches!(
                        &*err,
                        TransientError::PostgresError(PostgresError::Ssh(_))
                            | TransientError::PostgresError(PostgresError::SshIo(_))
                    ) =>
                {
                    StatusNamespace::Ssh
                }
                _ => Self::STATUS_NAMESPACE,
            };

            HealthStatusMessage {
                id: None,
                namespace: namespace.clone(),
                update,
            }
        });

        let health = health_init.concat(&errs);

        (
            data_collections,
            uppers,
            health,
            probe_stream,
            vec![snapshot_token, repl_token],
        )
    }
}

#[derive(Clone, Debug)]
struct SourceOutputInfo {
    desc: PostgresTableDesc,
    casts: Vec<(CastType, Option<MirScalarExpr>)>,
    resume_upper: Antichain<MzOffset>,
    export_id: GlobalId,
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
    #[error("replication slot mysteriously missing")]
    MissingReplicationSlot,
    #[error(
        "slot overcompacted. Requested LSN {requested_lsn} but only LSNs >= {available_lsn} are available"
    )]
    OvercompactedReplicationSlot {
        requested_lsn: MzOffset,
        available_lsn: MzOffset,
    },
    #[error("replication slot already exists")]
    ReplicationSlotAlreadyExists,
    #[error("stream ended prematurely")]
    ReplicationEOF,
    #[error("unexpected replication message")]
    UnknownReplicationMessage,
    #[error("unexpected logical replication message")]
    UnknownLogicalReplicationMessage,
    #[error("received replication event outside of transaction")]
    BareTransactionEvent,
    #[error("lsn mismatch between BEGIN and COMMIT")]
    InvalidTransaction,
    #[error("BEGIN within existing BEGIN stream")]
    NestedTransaction,
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
    #[error("slot compacted past snapshot point. snapshot consistent point={0} resume_lsn={1}")]
    SlotCompactedPastResumePoint(MzOffset, MzOffset),
    #[error("table was truncated")]
    TableTruncated,
    #[error("table was dropped")]
    TableDropped,
    #[error("publication {0:?} does not exist")]
    PublicationDropped(String),
    #[error("replication slot has been invalidated because it exceeded the maximum reserved size")]
    InvalidReplicationSlot,
    #[error("unexpected number of columns while parsing COPY output")]
    MissingColumn,
    #[error("failed to parse COPY protocol")]
    InvalidCopyInput,
    #[error(
        "unsupported action: database restored from point-in-time backup. Expected timeline ID {expected} but got {actual}"
    )]
    InvalidTimelineId { expected: u64, actual: u64 },
    #[error(
        "TOASTed value missing from old row. Did you forget to set REPLICA IDENTITY to FULL for your table?"
    )]
    MissingToast,
    #[error(
        "old row missing from replication stream. Did you forget to set REPLICA IDENTITY to FULL for your table?"
    )]
    DefaultReplicaIdentity,
    #[error("incompatible schema change: {0}")]
    // TODO: proper error variants for all the expected schema violations
    IncompatibleSchema(String),
    #[error("invalid UTF8 string: {0:?}")]
    InvalidUTF8(Vec<u8>),
    #[error("failed to cast raw column: {0}")]
    CastError(#[source] EvalError),
    #[error("unexpected binary data in replication stream")]
    UnexpectedBinaryData,
}

impl From<DefiniteError> for DataflowError {
    fn from(err: DefiniteError) -> Self {
        let m = err.to_string().into();
        DataflowError::SourceError(Box::new(SourceError {
            error: match &err {
                DefiniteError::SlotCompactedPastResumePoint(_, _) => SourceErrorDetails::Other(m),
                DefiniteError::TableTruncated => SourceErrorDetails::Other(m),
                DefiniteError::TableDropped => SourceErrorDetails::Other(m),
                DefiniteError::PublicationDropped(_) => SourceErrorDetails::Initialization(m),
                DefiniteError::InvalidReplicationSlot => SourceErrorDetails::Initialization(m),
                DefiniteError::MissingColumn => SourceErrorDetails::Other(m),
                DefiniteError::InvalidCopyInput => SourceErrorDetails::Other(m),
                DefiniteError::InvalidTimelineId { .. } => SourceErrorDetails::Initialization(m),
                DefiniteError::MissingToast => SourceErrorDetails::Other(m),
                DefiniteError::DefaultReplicaIdentity => SourceErrorDetails::Other(m),
                DefiniteError::IncompatibleSchema(_) => SourceErrorDetails::Other(m),
                DefiniteError::InvalidUTF8(_) => SourceErrorDetails::Other(m),
                DefiniteError::CastError(_) => SourceErrorDetails::Other(m),
                DefiniteError::UnexpectedBinaryData => SourceErrorDetails::Other(m),
            },
        }))
    }
}

async fn ensure_replication_slot(client: &Client, slot: &str) -> Result<(), TransientError> {
    // Note: Using unchecked here is okay because we're using it in a SQL query.
    let slot = Ident::new_unchecked(slot).to_ast_string_simple();
    let query = format!("CREATE_REPLICATION_SLOT {slot} LOGICAL \"pgoutput\" NOEXPORT_SNAPSHOT");
    match simple_query_opt(client, &query).await {
        Ok(_) => Ok(()),
        // If the slot already exists that's still ok
        Err(PostgresError::Postgres(err)) if err.code() == Some(&SqlState::DUPLICATE_OBJECT) => {
            tracing::trace!("replication slot {slot} already existed");
            Ok(())
        }
        Err(err) => Err(TransientError::PostgresError(err)),
    }
}

/// The state of a replication slot.
struct SlotMetadata {
    /// The process ID of the session using this slot if the slot is currently actively being used.
    /// None if inactive.
    active_pid: Option<i32>,
    /// The address (LSN) up to which the logical slot's consumer has confirmed receiving data.
    /// Data corresponding to the transactions committed before this LSN is not available anymore.
    confirmed_flush_lsn: MzOffset,
}

/// Fetches the minimum LSN at which this slot can safely resume.
async fn fetch_slot_metadata(
    client: &Client,
    slot: &str,
    interval: Duration,
) -> Result<SlotMetadata, TransientError> {
    loop {
        let query = "SELECT active_pid, confirmed_flush_lsn
                FROM pg_replication_slots WHERE slot_name = $1";
        let Some(row) = client.query_opt(query, &[&slot]).await? else {
            return Err(TransientError::MissingReplicationSlot);
        };

        match row.get::<_, Option<PgLsn>>("confirmed_flush_lsn") {
            // For postgres, `confirmed_flush_lsn` means that the slot is able to produce
            // all transactions that happen at tx_lsn >= confirmed_flush_lsn. Therefore this value
            // already has "upper" semantics.
            Some(lsn) => {
                return Ok(SlotMetadata {
                    confirmed_flush_lsn: MzOffset::from(lsn),
                    active_pid: row.get("active_pid"),
                });
            }
            // It can happen that confirmed_flush_lsn is NULL as the slot initializes
            // This could probably be a `tokio::time::interval`, but its only is called twice,
            // so its fine like this.
            None => tokio::time::sleep(interval).await,
        };
    }
}

/// Fetch the `pg_current_wal_lsn`, used to report metrics.
async fn fetch_max_lsn(client: &Client) -> Result<MzOffset, TransientError> {
    let query = "SELECT pg_current_wal_lsn()";
    let row = simple_query_opt(client, query).await?;

    match row.and_then(|row| {
        row.get("pg_current_wal_lsn")
            .map(|lsn| lsn.parse::<PgLsn>().unwrap())
    }) {
        // Based on the documentation, it appears that `pg_current_wal_lsn` has
        // the same "upper" semantics of `confirmed_flush_lsn`:
        // <https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADMIN-BACKUP>
        // We may need to revisit this and use `pg_current_wal_flush_lsn`.
        Some(lsn) => Ok(MzOffset::from(lsn)),
        None => Err(TransientError::Generic(anyhow::anyhow!(
            "pg_current_wal_lsn() mysteriously has no value"
        ))),
    }
}

// Ensures that the table with oid `oid` and expected schema `expected_schema` is still compatible
// with the current upstream schema `upstream_info`.
fn verify_schema(
    oid: u32,
    expected_desc: &PostgresTableDesc,
    upstream_info: &BTreeMap<u32, PostgresTableDesc>,
    casts: &[(CastType, Option<MirScalarExpr>)],
) -> Result<(), DefiniteError> {
    let current_desc = upstream_info.get(&oid).ok_or(DefiniteError::TableDropped)?;

    let allow_oids_to_change_by_col_num = expected_desc
        .columns
        .iter()
        .zip_eq(casts.iter())
        .flat_map(|(col, (cast_type, _))| match cast_type {
            CastType::Exclude | CastType::Text => Some(col.col_num),
            CastType::Natural => None,
        })
        .collect();

    match expected_desc.determine_compatibility(current_desc, &allow_oids_to_change_by_col_num) {
        Ok(()) => Ok(()),
        Err(err) => Err(DefiniteError::IncompatibleSchema(err.to_string())),
    }
}

/// Casts a text row into the target types
fn cast_row(
    casts: &[(CastType, Option<MirScalarExpr>)],
    datums: &[Datum<'_>],
    row: &mut Row,
) -> Result<(), DefiniteError> {
    let arena = mz_repr::RowArena::new();
    let mut packer = row.packer();
    for (_, column_cast) in casts {
        if let Some(column_cast) = column_cast {
            let datum = column_cast
                .eval(datums, &arena)
                .map_err(DefiniteError::CastError)?;
            packer.push(datum);
        }
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
