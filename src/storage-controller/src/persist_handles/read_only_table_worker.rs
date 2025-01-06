// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A tokio tasks (and support machinery) for dealing with the persist handles
//! that the storage controller needs to hold.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::ControlFlow;

use differential_dataflow::lattice::Lattice;
use futures::FutureExt;
use mz_persist_client::write::WriteHandle;
use mz_persist_types::Codec64;
use mz_repr::{Diff, GlobalId, TimestampManipulation};
use mz_storage_client::client::{TableData, Update};
use mz_storage_types::controller::InvalidUpper;
use mz_storage_types::sources::SourceData;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tracing::Span;

use crate::persist_handles::{append_work, PersistTableWriteCmd};
use crate::StorageError;

/// Handles table updates in read only mode.
///
/// In read only mode, we write to tables outside of the txn-wal system. This is
/// a gross hack, but it is a quick fix to allow us to perform migrations of the
/// built-in tables in the new generation during a deployment. We need to write
/// to the new shards for migrated built-in tables so that dataflows that depend
/// on those tables can catch up, but we don't want to register them into the
/// existing txn-wal shard, as that would mutate the state of the old generation
/// while it's still running. We could instead create a new txn shard in the new
/// generation for *just* system catalog tables, but then we'd have to do a
/// complicated dance to move the system catalog tables back to the original txn
/// shard during promotion, without ever losing track of a shard or registering
/// it in two txn shards simultaneously.
///
/// This code is a nearly line-for-line reintroduction of the code that managed
/// writing to tables before the txn-wal system. This code can (again) be
/// deleted when we switch to using native persist schema migrations to perform
/// mgirations of built-in tables.
pub(crate) async fn read_only_mode_table_worker<
    T: Timestamp + Lattice + Codec64 + TimestampManipulation,
>(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<(Span, PersistTableWriteCmd<T>)>,
    txns_handle: WriteHandle<SourceData, (), T, Diff>,
) {
    let mut write_handles = BTreeMap::<GlobalId, WriteHandle<SourceData, (), T, Diff>>::new();

    let gen_upper_future = |mut handle: WriteHandle<SourceData, (), T, i64>| {
        let fut = async move {
            let current_upper = handle.shared_upper();
            handle.wait_for_upper_past(&current_upper).await;
            let new_upper = handle.shared_upper();
            (handle, new_upper)
        };

        fut.boxed()
    };

    let mut txns_upper_future = {
        let txns_upper_future = gen_upper_future(txns_handle);
        txns_upper_future
    };

    let shutdown_reason = loop {
        tokio::select! {
            (handle, upper) = &mut txns_upper_future => {
                tracing::debug!("new upper from txns shard: {:?}, advancing upper of migrated builtin tables", upper);
                advance_uppers(&mut write_handles, upper).await;

                let fut = gen_upper_future(handle);
                txns_upper_future = fut;
            }
            cmd = rx.recv() => {
                let Some(cmd) = cmd else {
                    break "command rx closed".to_string();
                };

                // Peel off all available commands.
                // We do this in case we can consolidate commands.
                // It would be surprising to receive multiple concurrent `Append` commands,
                // but we might receive multiple *empty* `Append` commands.
                let mut commands = VecDeque::new();
                commands.push_back(cmd);
                while let Ok(cmd) = rx.try_recv() {
                    commands.push_back(cmd);
                }

                let result = handle_commands(&mut write_handles, commands).await;

                match result {
                    ControlFlow::Continue(_) => {
                        continue;
                    }
                    ControlFlow::Break(msg) => {
                        break msg;
                    }
                }

            }
        }
    };

    tracing::info!(%shutdown_reason, "PersistTableWriteWorker shutting down");
}

/// Handles the given commands.
async fn handle_commands<T>(
    write_handles: &mut BTreeMap<GlobalId, WriteHandle<SourceData, (), T, Diff>>,
    mut commands: VecDeque<(Span, PersistTableWriteCmd<T>)>,
) -> ControlFlow<String>
where
    T: Timestamp + Lattice + Codec64 + TimestampManipulation,
{
    let mut shutdown = false;

    // Accumulated updates and upper frontier.
    let mut all_updates = BTreeMap::default();
    let mut all_responses = Vec::default();

    while let Some((span, command)) = commands.pop_front() {
        match command {
            PersistTableWriteCmd::Register(_register_ts, ids_handles, tx) => {
                for (id, write_handle) in ids_handles {
                    // As of today, we can only migrate builtin (system) tables.
                    assert!(id.is_system(), "trying to register non-system id {id}");

                    let previous = write_handles.insert(id, write_handle);
                    if previous.is_some() {
                        panic!("already registered a WriteHandle for collection {:?}", id);
                    }
                }
                // We don't care if our waiter has gone away.
                let _ = tx.send(());
            }
            PersistTableWriteCmd::Update {
                existing_collection,
                new_collection,
                handle,
                forget_ts: _,
                register_ts: _,
                tx,
            } => {
                write_handles.remove(&existing_collection);
                write_handles.insert(new_collection, handle).expect(
                    "PersistTableWriteCmd::Update only valid for updating extant write handles",
                );
                // We don't care if our waiter has gone away.
                let _ = tx.send(());
            }
            PersistTableWriteCmd::DropHandles {
                forget_ts: _,
                ids,
                tx,
            } => {
                // n.b. this should only remove the
                // handle from the persist worker and
                // not take any additional action such
                // as closing the shard it's connected
                // to because dataflows might still be
                // using it.
                for id in ids {
                    write_handles.remove(&id);
                }
                // We don't care if our waiter has gone away.
                let _ = tx.send(());
            }
            PersistTableWriteCmd::Append {
                write_ts,
                advance_to,
                updates,
                tx,
            } => {
                let mut ids = BTreeSet::new();
                for (id, updates_no_ts) in updates {
                    ids.insert(id);
                    let (old_span, updates, _expected_upper, old_new_upper) =
                        all_updates.entry(id).or_insert_with(|| {
                            (
                                span.clone(),
                                Vec::default(),
                                Antichain::from_elem(write_ts.clone()),
                                Antichain::from_elem(T::minimum()),
                            )
                        });

                    if old_span.id() != span.id() {
                        // Link in any spans for `Append` operations that we
                        // lump together by doing this. This is not ideal,
                        // because we only have a true tracing history for
                        // the "first" span that we process, but it's better
                        // than nothing.
                        old_span.follows_from(span.id());
                    }
                    let updates_with_ts = updates_no_ts.into_iter().flat_map(|x| match x {
                        TableData::Rows(rows) => {
                            let iter = rows.into_iter().map(|(row, diff)| Update {
                                row,
                                timestamp: write_ts.clone(),
                                diff,
                            });
                            itertools::Either::Left(iter)
                        }
                        TableData::Batches(_) => {
                            // TODO(cf1): Handle Batches of updates in ReadOnlyTableWorker.
                            mz_ore::soft_panic_or_log!(
                                "handle Batches of updates in the ReadOnlyTableWorker"
                            );
                            itertools::Either::Right(std::iter::empty())
                        }
                    });
                    updates.extend(updates_with_ts);
                    old_new_upper.join_assign(&Antichain::from_elem(advance_to.clone()));
                }
                all_responses.push((ids, tx));
            }
            PersistTableWriteCmd::Shutdown => shutdown = true,
        }
    }

    let result = append_work(write_handles, all_updates).await;

    for (ids, response) in all_responses {
        let result = match &result {
            Err(bad_ids) => {
                let filtered: Vec<_> = bad_ids
                    .iter()
                    .filter(|(id, _)| ids.contains(id))
                    .cloned()
                    .map(|(id, current_upper)| InvalidUpper { id, current_upper })
                    .collect();
                if filtered.is_empty() {
                    Ok(())
                } else {
                    Err(StorageError::InvalidUppers(filtered))
                }
            }
            Ok(()) => Ok(()),
        };
        // It is not an error for the other end to hang up.
        let _ = response.send(result);
    }

    if shutdown {
        ControlFlow::Break("received a shutdown command".to_string())
    } else {
        ControlFlow::Continue(())
    }
}

/// Advances the upper of all registered tables (which are only the migrated
/// builtin tables) to the given `upper`.
async fn advance_uppers<T>(
    write_handles: &mut BTreeMap<GlobalId, WriteHandle<SourceData, (), T, Diff>>,
    upper: Antichain<T>,
) where
    T: Timestamp + Lattice + Codec64 + TimestampManipulation,
{
    let mut all_updates = BTreeMap::default();

    for (id, write_handle) in write_handles.iter_mut() {
        // As of today, we can only migrate builtin (system) tables, and so only
        // expect to register those in this read-only table worker.
        assert!(id.is_system(), "trying to register non-system id {id}");

        // This business of continually advancing the upper is expensive, but
        // we're a) only doing it when in read-only mode, and b) only doing it
        // for each migrated builtin table, of which there usually aren't many.
        let expected_upper = write_handle.fetch_recent_upper().await.to_owned();

        // Avoid advancing the upper until the coordinator has a chance to back-fill the shard.
        if expected_upper.elements() == &[T::minimum()] {
            continue;
        }

        if PartialOrder::less_equal(&upper, &expected_upper) {
            // Nothing to do, and append_work doesn't like being called with a
            // new upper that is less_equal the current upper.
            continue;
        }

        all_updates.insert(
            *id,
            (Span::none(), Vec::new(), expected_upper, upper.clone()),
        );
    }

    let result = append_work(write_handles, all_updates).await;
    tracing::debug!(?result, "advanced upper of migrated builtin tables");
}
