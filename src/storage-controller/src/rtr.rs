// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of real-time recency.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;

use differential_dataflow::lattice::Lattice;
use mz_ore::now::EpochMillis;
use mz_persist_client::read::{ListenEvent, Subscribe};
use mz_persist_types::Codec64;
use mz_repr::{GlobalId, Row};
use mz_storage_types::StorageDiff;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::sources::{
    GenericSourceConnection, SourceConnection, SourceData, SourceTimestamp,
};
use mz_timely_util::antichain::AntichainExt;
use timely::PartialOrder;
use timely::order::TotalOrder;
use timely::progress::Antichain;
use timely::progress::frontier::MutableAntichain;

use crate::StorageError;
use crate::Timestamp;

/// Determine the "real-time recency" timestamp for `self`.
///
/// Real-time recency is defined as the minimum value of `T` that `id` can
/// be queried at to return all data visible in the upstream system the
/// query was issued. In this case, "the upstream system" is the external
/// system which `self` connects to.
///
/// # Panics
/// - If `self` is a [`GenericSourceConnection::LoadGenerator`]. Load
///   generator sources do not yet (or might never) support real-time
///   recency. You can avoid this panic by choosing to not call this
///   function on load generator sources.
pub(super) async fn real_time_recency_ts<
    T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + Sync,
>(
    connection: GenericSourceConnection,
    id: GlobalId,
    config: StorageConfiguration,
    as_of: Antichain<T>,
    remap_subscribe: Subscribe<SourceData, (), T, StorageDiff>,
) -> Result<T, StorageError<T>> {
    match connection {
        GenericSourceConnection::Kafka(kafka) => {
            let external_frontier = kafka
                .fetch_write_frontier(&config)
                .await
                .map_err(StorageError::Generic)?;

            decode_remap_data_until_geq_external_frontier(
                id,
                external_frontier,
                as_of,
                remap_subscribe,
            )
            .await
        }
        GenericSourceConnection::Postgres(pg) => {
            let external_frontier = pg
                .fetch_write_frontier(&config)
                .await
                .map_err(StorageError::Generic)?;

            decode_remap_data_until_geq_external_frontier(
                id,
                external_frontier,
                as_of,
                remap_subscribe,
            )
            .await
        }
        GenericSourceConnection::MySql(my_sql) => {
            let external_frontier = my_sql
                .fetch_write_frontier(&config)
                .await
                .map_err(StorageError::Generic)?;

            decode_remap_data_until_geq_external_frontier(
                id,
                external_frontier,
                as_of,
                remap_subscribe,
            )
            .await
        }
        GenericSourceConnection::SqlServer(sql_server) => {
            let external_frontier = sql_server
                .fetch_write_frontier(&config)
                .await
                .map_err(StorageError::Generic)?;

            decode_remap_data_until_geq_external_frontier(
                id,
                external_frontier,
                as_of,
                remap_subscribe,
            )
            .await
        }
        // Load generator sources have no "external system" to reach out to,
        // so it's unclear what RTR would mean for them.
        s @ GenericSourceConnection::LoadGenerator(_) => unreachable!(
            "do not try to determine RTR timestamp on {} source",
            s.name()
        ),
    }
}

async fn decode_remap_data_until_geq_external_frontier<
    FromTime: SourceTimestamp,
    T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + Sync,
>(
    id: GlobalId,
    external_frontier: timely::progress::Antichain<FromTime>,
    as_of: Antichain<T>,
    mut remap_subscribe: Subscribe<SourceData, (), T, StorageDiff>,
) -> Result<T, StorageError<T>> {
    tracing::debug!(
        ?id,
        "fetched real time recency frontier: {}",
        external_frontier.pretty()
    );

    let external_frontier = external_frontier.borrow();
    let mut native_upper = MutableAntichain::new();

    let mut remap_frontier = Antichain::from_elem(T::minimum());
    let mut pending_remap = BinaryHeap::new();

    let mut min_ts = T::minimum();
    min_ts.advance_by(as_of.borrow());
    let mut min_ts = Some(min_ts);

    // First we must read the snapshot so that we can accumulate the minimum timestamp
    // correctly.
    while !remap_frontier.is_empty() {
        // Receive binding updates
        for event in remap_subscribe.fetch_next().await {
            match event {
                ListenEvent::Updates(updates) => {
                    for ((k, v), into_ts, diff) in updates {
                        let row: Row = k.0.expect("invalid binding");
                        let _v: () = v;

                        let from_ts: FromTime = SourceTimestamp::decode_row(&row);
                        pending_remap.push(Reverse((into_ts, from_ts, diff)));
                    }
                }

                ListenEvent::Progress(frontier) => remap_frontier = frontier,
            }
        }
        // Only process the data if we can correctly accumulate timestamps beyond the as_of
        if as_of.iter().any(|t| !remap_frontier.less_equal(t)) {
            loop {
                // The next timestamp that must be considered is either the minimum timestamp,
                // if we haven't yet checked it, or the next timestamp of the pending remap
                // updates that is not beyond the frontier.
                let binding_ts = match min_ts.take() {
                    Some(min_ts) => min_ts,
                    None => {
                        let Some(Reverse((into_ts, _, _))) = pending_remap.peek() else {
                            break;
                        };
                        if !remap_frontier.less_equal(into_ts) {
                            into_ts.clone()
                        } else {
                            break;
                        }
                    }
                };
                // Create an iterator with all the updates that are less than or equal to
                // binding_ts
                let binding_updates = std::iter::from_fn(|| {
                    let update = pending_remap.peek_mut()?;
                    if PartialOrder::less_equal(&update.0.0, &binding_ts) {
                        let Reverse((_, from_ts, diff)) = PeekMut::pop(update);
                        Some((from_ts, diff))
                    } else {
                        None
                    }
                });
                native_upper.update_iter(binding_updates);

                // Check if at binding_ts the source contained all of the data visible in the
                // external system. If so, we're done.
                if PartialOrder::less_equal(&external_frontier, &native_upper.frontier()) {
                    tracing::trace!("real-time recency ts for {id}: {binding_ts:?}");
                    return Ok(binding_ts);
                }
            }
        }
    }
    // Collection dropped before we ingested external frontier.
    Err(StorageError::RtrDropFailure(id))
}
