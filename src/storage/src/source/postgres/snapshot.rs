// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Renders the table snapshot side of the [`PostgresSourceConnection`] ingestion dataflow.
//!
//! # Snapshot reading
//!
//! Depending on the resumption LSNs the table reader decides which tables need to be snapshotted
//! and performs a simple `COPY` query on them in order to get a snapshot. There are a few subtle
//! points about this operation, described in the following sections.
//!
//! ## Consistent LSN point for snapshot transactions
//!
//! Given that all our ingestion is based on correctly timestamping updates with the LSN they
//! happened at it is important that we run the `COPY` query at a specific LSN point that is
//! relatable with the LSN numbers we receive from the replication stream. Such point does not
//! necessarily exist for a normal SQL transaction. To achieve this we must force postgres to
//! produce a consistent point and let us know of the LSN number of that by creating a replication
//! slot as the first statement in a transaction.
//!
//! This is a temporary dummy slot that is only used to put our snapshot transaction on a
//! consistent LSN point. Unfortunately no lighterweight method exists for doing this. See this
//! [postgres thread] for more details.
//!
//! One might wonder why we don't use the actual real slot to provide us with the snapshot point
//! which would automatically be at the correct LSN. The answer is that it's possible that we crash
//! and restart after having already created the slot but before having finished the snapshot. In
//! that case the restarting process will have lost its opportunity to run queries at the slot's
//! consistent point as that opportunity only exists in the ephemeral transaction that created the
//! slot and that is long gone. Additionally there are good reasons of why we'd like to move the
//! slot creation much earlier, e.g during purification, in which case the slot will always be
//! pre-created.
//!
//! [postgres thread]: https://www.postgresql.org/message-id/flat/CAMN0T-vzzNy6TV1Jvh4xzNQdAvCLBQK_kh6_U7kAXgGU3ZFg-Q%40mail.gmail.com
//!
//! ## Reusing the consistent point among all workers
//!
//! Creating replication slots is potentially expensive so the code makes is such that all workers
//! cooperate and reuse one consistent snapshot among them. In order to do so we make use the
//! "export transaction" feature of postgres. This feature allows one SQL session to create an
//! identifier for the transaction (a string identifier) it is currently in, which can be used by
//! other sessions to enter the same "snapshot".
//!
//! We accomplish this by picking one worker at random to function as the transaction leader. The
//! transaction leader is responsible for starting a SQL session, creating a temporary replication
//! slot in a transaction, exporting the transaction id, and broadcasting the transaction
//! information to all other workers via a broadcasted feedback edge.
//!
//! During this phase the follower workers are simply waiting to hear on the feedback edge,
//! effectively synchronizing with the leader. Once all workers have received the snapshot
//! information they can all start to perform their assigned COPY queries.
//!
//! The leader and follower steps described above are accomplished by the [`export_snapshot`] and
//! [`use_snapshot`] functions respectively.
//!
//! ## Coordinated transaction COMMIT
//!
//! When follower workers are done with snapshotting they commit their transaction, close their
//! session, and then drop their snapshot feedback capability. When the leader worker is done with
//! snapshotting it drops its snapshot feedback capability and waits until it observes the
//! snapshot input advancing to the empty frontier. This allows the leader to COMMIT its
//! transaction last, which is the transaction that exported the snapshot.
//!
//! It's unclear if this is strictly necessary, but having the frontiers made it easy enough that I
//! added the synchronization.
//!
//! ## Snapshot rewinding
//!
//! Ingestion dataflows must produce definite data, including the snapshot. What this means
//! practically is that whenever we deem it necessary to snapshot a table we must do so at the same
//! LSN. However, the method for running a transaction described above doesn't let us choose the
//! LSN, it could be an LSN in the future chosen by PostgresSQL while it creates the temporary
//! replication slot.
//!
//! The definition of differential collections states that a collection at some time `t_snapshot`
//! is defined to be the accumulation of all updates that happen at `t <= t_snapshot`, where `<=`
//! is the partial order. In this case we are faced with the problem of knowing the state of a
//! table at `t_snapshot` but actually wanting to know the snapshot at `t_slot <= t_snapshot`.
//!
//! From the definition we can see that the snapshot at `t_slot` is related to the snapshot at
//! `t_snapshot` with the following equations:
//!
//!```text
//! sum(update: t <= t_snapshot) = sum(update: t <= t_slot) + sum(update: t_slot <= t <= t_snapshot)
//!                                         |
//!                                         V
//! sum(update: t <= t_slot) = sum(update: t <= snapshot) - sum(update: t_slot <= t <= t_snapshot)
//! ```
//!
//! Therefore, if we manage to recover the `sum(update: t_slot <= t <= t_snapshot)` term we will be
//! able to "rewind" the snapshot we obtained at `t_snapshot` to `t_slot` by emitting all updates
//! that happen between these two points with their diffs negated.
//!
//! It turns out that this term is exactly what the main replication slot provides us with and we
//! can rewind snapshot at arbitrary points! In order to do this the snapshot dataflow emits rewind
//! requests to the replication reader which informs it that a certain range of updates must be
//! emitted at LSN 0 (by convention) with their diffs negated. These negated diffs are consolidated
//! with the diffs taken at `t_snapshot` that were also emitted at LSN 0 (by convention) and we end
//! up with a TVC that at LSN 0 contains the snapshot at `t_slot`.
//!
//! # Snapshot decoding
//!
//! The expectation is that tables will most likely be skewed on the number of rows they contain so
//! while a `COPY` query for any given table runs on a single worker the decoding of the COPY
//! stream is distributed to all workers.
//!
//!
//! ```text
//!                 ╭──────────────────╮
//!    ┏━━━━━━━━━━━━v━┓                │ exported
//!    ┃    table     ┃   ╭─────────╮  │ snapshot id
//!    ┃    reader    ┠─>─┤broadcast├──╯
//!    ┗━┯━━━━━━━━━━┯━┛   ╰─────────╯
//!   raw│          │
//!  COPY│          │
//!  data│          │
//! ╭────┴─────╮    │
//! │distribute│    │
//! ╰────┬─────╯    │
//! ┏━━━━┷━━━━┓     │
//! ┃  COPY   ┃     │
//! ┃ decoder ┃     │
//! ┗━━━━┯━━━━┛     │
//!      │ snapshot │rewind
//!      │ updates  │requests
//!      v          v
//! ```

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::pin::pin;
use std::rc::Rc;

use differential_dataflow::{AsCollection, Collection};
use futures::TryStreamExt;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Broadcast, ConnectLoop, Feedback};
use timely::dataflow::{Scope, Stream};
use timely::progress::{Antichain, Timestamp};
use tokio_postgres::types::PgLsn;
use tokio_postgres::Client;
use tracing::trace;

use mz_expr::MirScalarExpr;
use mz_ore::result::ResultExt;
use mz_postgres_util::desc::PostgresTableDesc;
use mz_repr::{Datum, DatumVec, Diff, GlobalId, Row};
use mz_sql_parser::ast::{display::AstDisplay, Ident};
use mz_storage_client::types::connections::ConnectionContext;
use mz_storage_client::types::sources::{MzOffset, PostgresSourceConnection};
use mz_timely_util::antichain::AntichainExt;
use mz_timely_util::builder_async::{Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder};
use mz_timely_util::operator::StreamExt as TimelyStreamExt;

use crate::source::postgres::replication::RewindRequest;
use crate::source::postgres::{simple_query_opt, verify_schema, DefiniteError, TransientError};
use crate::source::types::SourceReaderError;
use crate::source::RawSourceCreationConfig;

/// Renders the snapshot dataflow. See the module documentation for more information.
pub(crate) fn render<G: Scope<Timestamp = MzOffset>>(
    mut scope: G,
    config: RawSourceCreationConfig,
    connection: PostgresSourceConnection,
    context: ConnectionContext,
    subsource_resume_uppers: BTreeMap<GlobalId, Antichain<MzOffset>>,
    table_info: BTreeMap<u32, (usize, PostgresTableDesc, Vec<MirScalarExpr>)>,
) -> (
    Collection<G, (usize, Result<Row, SourceReaderError>), Diff>,
    Stream<G, RewindRequest>,
    Stream<G, Rc<TransientError>>,
    Rc<dyn Any>,
) {
    let op_name = format!("TableReader({})", config.id);
    let mut builder = AsyncOperatorBuilder::new(op_name, scope.clone());

    let (feedback_handle, feedback_data) = scope.feedback(Default::default());

    let (mut raw_handle, raw_data) = builder.new_output();
    let (mut rewinds_handle, rewinds) = builder.new_output();
    let (mut snapshot_handle, snapshot) = builder.new_output();

    // This operator needs to broadcast data to itself in order to synchronize the transaction
    // snapshot. However, none of the feedback capabilities result in output messages and for the
    // feedback edge specifically having a default conncetion would result in a loop.
    let disconnected = vec![Antichain::new(); 3];
    let mut snapshot_input = builder.new_input_connection(&feedback_data, Pipeline, disconnected);

    // The export id must be sent to all workes, so we broadcast the feedback connection
    snapshot.broadcast().connect_loop(feedback_handle);

    let is_snapshot_leader = config.responsible_for("snapshot_leader");

    // A global view of all exports that need to be snapshot by all workers. Note that this affects
    // `reader_snapshot_table_info` but must be kept separate from it because each worker needs to
    // understand if any worker is snapshotting any subsource.
    let exports_to_snapshot: BTreeSet<_> = subsource_resume_uppers
        .into_iter()
        .filter_map(|(id, upper)| {
            // Determined which collections need to be snapshot and which already have been.
            if id != config.id && *upper == [MzOffset::minimum()] {
                // Convert from `GlobalId` to output index.
                Some(config.source_exports[&id].output_index)
            } else {
                None
            }
        })
        .collect();

    // A filtered table info containing only the tables that this worker should snapshot.
    let reader_snapshot_table_info: BTreeMap<_, _> = table_info
        .iter()
        .filter(|(oid, (output_index, _, _))| {
            mz_ore::soft_assert!(
                *output_index != 0,
                "primary collection should not be represented in table info"
            );
            exports_to_snapshot.contains(output_index) && config.responsible_for(oid)
        })
        .map(|(k, v)| (*k, v.clone()))
        .collect();

    let (button, errors) = builder.build_fallible(move |caps| {
        Box::pin(async move {
            let id = config.id;
            let worker_id = config.worker_id;

            let [data_cap, rewind_cap, snapshot_cap]: &mut [_; 3] = caps.try_into().unwrap();
            let data_cap = data_cap.as_mut().unwrap();
            trace!(
                %id,
                "timely-{worker_id} initializing table reader with {} and {} tables to snapshot",
                config.resume_upper.pretty(),
                reader_snapshot_table_info.len()
            );

            // Nothing needs to be snapshot.
            if exports_to_snapshot.is_empty() {
                return Ok(());
            }

            let connection_config = connection
                .connection
                .config(&*context.secrets_reader)
                .await?
                .replication_timeouts(config.params.pg_replication_timeouts.clone());

            let client = connection_config.connect_replication().await?;
            if is_snapshot_leader {
                // The main slot must be created *before* we start snapshotting so that we can be
                // certain that the temporarly slot created for the snapshot start at an LSN that
                // is greater than or equal to that of the main slot.
                super::ensure_replication_slot(&client, &connection.publication_details.slot).await?;

                let snapshot_info = export_snapshot(&client).await?;
                trace!(%id, "timely-{worker_id} exporting snapshot info {snapshot_info:?}");
                let cap = snapshot_cap.as_ref().unwrap();
                snapshot_handle.give(cap, snapshot_info).await;
            }

            let (snapshot, snapshot_lsn) = loop {
                match snapshot_input.next_mut().await {
                    Some(AsyncEvent::Data(_, data)) => break data.pop().expect("snapshot sent above"),
                    Some(AsyncEvent::Progress(_)) => continue,
                    None => panic!("feedback closed before sending snapshot info"),
                }
            };
            // Snapshot leader is already in identified transaction but all other workers need to enter it.
            if !is_snapshot_leader {
                trace!(%id, "timely-{worker_id} using snapshot id {snapshot:?}");
                use_snapshot(&client, &snapshot).await?;
            }

            // We have established a snapshot LSN so we can broadcast the rewind requests
            for &oid in reader_snapshot_table_info.keys() {
                trace!(%id, "timely-{worker_id} producing rewind request for {oid}");
                let req = RewindRequest { oid, snapshot_lsn };
                rewinds_handle.give(rewind_cap.as_ref().unwrap(), req).await;
            }
            *rewind_cap = None;

            let upstream_info = mz_postgres_util::publication_info(
                &connection_config,
                &connection.publication,
                None,
            )
            .await?;
            let upstream_info = upstream_info.into_iter().map(|t| (t.oid, t)).collect();

            for (&oid, (_, expected_desc, _)) in reader_snapshot_table_info.iter() {
                let desc = match verify_schema(oid, expected_desc, &upstream_info) {
                    Ok(()) => expected_desc,
                    Err(err) => {
                        raw_handle.give(data_cap, ((oid, Err(err)), MzOffset::minimum(), 1)).await;
                        continue;
                    }
                };

                trace!(%id, "timely-{worker_id} snapshotting table {:?}({oid}) @ {snapshot_lsn}", desc.name);
                // To handle quoted/keyword names, we can use `Ident`'s AST printing, which
                // emulate's PG's rules for name formatting.
                let query = format!(
                    "COPY {}.{} TO STDOUT (FORMAT TEXT, DELIMITER '\t')",
                    Ident::from(desc.namespace.clone()).to_ast_string(),
                    Ident::from(desc.name.clone()).to_ast_string(),
                );
                let mut stream = pin!(client.copy_out_simple(&query).await?);

                while let Some(bytes) = stream.try_next().await? {
                    raw_handle.give(data_cap, ((oid, Ok(bytes)), MzOffset::minimum(), 1)).await;
                }
            }
            // Failure scenario after we have produced the snapshot, but before a successful COMMIT
            fail::fail_point!("pg_snapshot_failure", |_| Err(TransientError::SyntheticError));

            // The exporting worker should wait for all the other workers to commit before dropping
            // its client since this is what holds the exported transaction alive.
            if is_snapshot_leader {
                trace!(%id, "timely-{worker_id} waiting for all workers to finish");
                *snapshot_cap = None;
                while snapshot_input.next().await.is_some() {}
                trace!(%id, "timely-{worker_id} (leader) comitting COPY transaction");
                client.simple_query("COMMIT").await?;
            } else {
                trace!(%id, "timely-{worker_id} comitting COPY transaction");
                client.simple_query("COMMIT").await?;
                *snapshot_cap = None;
            }
            drop(client);
            Ok(())
        })
    });

    // Distribute the raw COPY data to all workers and turn it into a collection
    let raw_collection = raw_data.distribute().as_collection();

    // We now decode the COPY protocol and apply the cast expressions
    let mut text_row = Row::default();
    let mut final_row = Row::default();
    let mut datum_vec = DatumVec::new();
    let snapshot_updates = raw_collection.map(move |(oid, event)| {
        let (output_index, _, casts) = &table_info[&oid];

        let event = event.and_then(|bytes| {
            decode_copy_row(&bytes, casts.len(), &mut text_row)?;
            let datums = datum_vec.borrow_with(&text_row);
            super::cast_row(casts, &datums, &mut final_row)?;
            Ok(final_row.clone())
        });

        (*output_index, event.err_into())
    });

    (
        snapshot_updates,
        rewinds,
        errors,
        Rc::new(button.press_on_drop()),
    )
}

/// Starts a read-only transaction on the SQL session of `client` at a consistent LSN point by
/// creating a temporary replication slot. Returns a snapshot identifier that can be imported in
/// other SQL session and the LSN of the consistent point.
async fn export_snapshot(client: &Client) -> Result<(String, MzOffset), TransientError> {
    client
        .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
        .await?;
    // A temporary replication slot is the only way to get the tx in a consistent LSN point
    let slot = format!("mzsnapshot_{}", uuid::Uuid::new_v4()).replace('-', "");
    let query =
        format!("CREATE_REPLICATION_SLOT {slot:?} TEMPORARY LOGICAL \"pgoutput\" USE_SNAPSHOT");
    let row = simple_query_opt(client, &query).await?.unwrap();
    let consistent_point: PgLsn = row.get("consistent_point").unwrap().parse().unwrap();

    let row = simple_query_opt(client, "SELECT pg_export_snapshot();")
        .await?
        .unwrap();
    let snapshot = row.get("pg_export_snapshot").unwrap().to_owned();

    Ok((snapshot, MzOffset::from(consistent_point)))
}

/// Starts a read-only transaction on the SQL session of `client` at a the consistent LSN point of
/// `snapshot`.
async fn use_snapshot(client: &Client, snapshot: &str) -> Result<(), TransientError> {
    client
        .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
        .await?;
    let query = format!("SET TRANSACTION SNAPSHOT '{snapshot}';");
    client.simple_query(&query).await?;
    Ok(())
}

/// Decodes a row of `col_len` columns obtained from a text encoded COPY query into `row`.
fn decode_copy_row(data: &[u8], col_len: usize, row: &mut Row) -> Result<(), DefiniteError> {
    let mut packer = row.packer();
    let row_parser = mz_pgcopy::CopyTextFormatParser::new(data, "\t", "\\N");
    let mut column_iter = row_parser.iter_raw_truncating(col_len);
    for _ in 0..col_len {
        let value = match column_iter.next() {
            Some(Ok(value)) => value,
            Some(Err(_)) => return Err(DefiniteError::InvalidCopyInput),
            None => return Err(DefiniteError::MissingColumn),
        };
        let datum = value.map(super::decode_utf8_text).transpose()?;
        packer.push(datum.unwrap_or(Datum::Null));
    }
    Ok(())
}
