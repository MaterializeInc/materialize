// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::input::Input;
use differential_dataflow::{Collection, Hashable};
use timely::dataflow::Scope;
use timely::progress::Antichain;
use tokio::runtime::Handle as TokioHandle;
use tokio::sync::Mutex;

use mz_persist_client::cache::PersistClientCache;
use mz_repr::GlobalId;
use mz_repr::{Diff, Row, Timestamp};
use mz_storage::controller::CollectionMetadata;
use mz_storage::types::sources::SourceData;

use crate::compute_state::ComputeState;

pub(crate) fn persist_sink<G>(
    target_id: GlobalId,
    target: &CollectionMetadata,
    compute_state: &mut ComputeState,
    desired_collection: Collection<G, Row, Diff>,
) where
    G: Scope<Timestamp = Timestamp>,
{
    // The target storage collection might still contain data written by a
    // previous incarnation of the replica. Empty it, so we don't end up with
    // stale data that never gets retracted.
    // TODO(teskje,lh): Remove the truncation step once/if #13740 gets merged.
    //
    // We need to ensure that only a single timely worker tries to perform the
    // truncate, to avoid retracting the old data multiple times.
    let scope = desired_collection.scope();
    let is_active_worker = (target_id.hashed() as usize) % scope.peers() == scope.index();
    if is_active_worker {
        let persist_clients = Arc::clone(&compute_state.persist_clients);
        TokioHandle::current().block_on(truncate_storage_collection(target, persist_clients));
    }

    let (_, err_collection) = desired_collection.scope().new_collection();

    let token = crate::sink::persist_sink(
        target_id,
        target,
        desired_collection,
        err_collection,
        compute_state,
    );

    // Report frontier of collection back to coord
    compute_state
        .reported_frontiers
        .insert(target_id, Antichain::from_elem(0));

    // We don't allow these dataflows to be dropped, so the tokens could
    // be stored anywhere.
    compute_state.sink_tokens.insert(
        target_id,
        crate::compute_state::SinkToken {
            token: Box::new(token),
            is_tail: false,
        },
    );
}

async fn truncate_storage_collection(
    collection: &CollectionMetadata,
    persist_clients: Arc<Mutex<PersistClientCache>>,
) {
    let client = persist_clients
        .lock()
        .await
        .open(collection.persist_location.clone())
        .await
        .expect("could not open persist client");

    let (mut write, read) = client
        .open::<SourceData, (), Timestamp, Diff>(collection.data_shard)
        .await
        .expect("could not open persist shard");

    let upper = write.upper().clone();
    let upper_ts = upper[0];
    if let Some(ts) = upper_ts.checked_sub(1) {
        let as_of = Antichain::from_elem(ts);

        let mut snapshot_iter = read
            .snapshot(as_of)
            .await
            .expect("cannot serve requested as_of");

        let mut updates = Vec::new();
        while let Some(next) = snapshot_iter.next().await {
            updates.extend(next);
        }
        snapshot_iter.expire().await;

        consolidate_updates(&mut updates);

        let retractions = updates
            .into_iter()
            .map(|((k, v), _ts, diff)| ((k.unwrap(), v.unwrap()), upper_ts, diff * -1));

        let new_upper = Antichain::from_elem(upper_ts + 1);
        write
            .compare_and_append(retractions, upper, new_upper)
            .await
            .expect("external durability failure")
            .expect("invalid usage")
            .expect("unexpected upper");
    }

    write.expire().await;
    read.expire().await;
}
