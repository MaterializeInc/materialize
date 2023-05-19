// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Healthcheck common

use std::sync::Arc;

use differential_dataflow::lattice::Lattice;
use mz_ore::now::NowFn;
use mz_persist_client::{PersistClient, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{GlobalId, RelationDesc, Timestamp};
use mz_storage_client::types::sources::SourceData;
use timely::progress::Antichain;

pub async fn write_to_persist(
    collection_id: GlobalId,
    new_status: &str,
    new_error: Option<&str>,
    now: NowFn,
    client: &PersistClient,
    status_shard: ShardId,
    relation_desc: &RelationDesc,
    hint: Option<&str>,
) {
    let now_ms = now();
    let row = mz_storage_client::healthcheck::pack_status_row(
        collection_id,
        new_status,
        new_error,
        now_ms,
        hint,
    );

    let mut handle = client
        .open_writer(
            status_shard,
            &format!("healthcheck::write_to_persist {}", collection_id),
            Arc::new(relation_desc.clone()),
            Arc::new(UnitSchema),
        )
        .await
        .expect(
            "Invalid usage of the persist \
            client for collection {collection_id} status history shard",
        );

    let mut recent_upper = handle.upper().clone();
    let mut append_ts = Timestamp::from(now_ms);
    'retry_loop: loop {
        // We don't actually care so much about the timestamp we append at; it's best-effort.
        // Ensure that the append timestamp is not less than the current upper, and the new upper
        // is past that timestamp. (Unless we've advanced to the empty antichain, but in
        // that case we're already in trouble.)
        for t in recent_upper.elements() {
            append_ts.join_assign(t);
        }
        let mut new_upper = Antichain::from_elem(append_ts.step_forward());
        new_upper.join_assign(&recent_upper);

        let updates = vec![((SourceData(Ok(row.clone())), ()), append_ts, 1i64)];
        let cas_result = handle
            .compare_and_append(updates, recent_upper.clone(), new_upper)
            .await;
        match cas_result {
            Ok(Ok(())) => break 'retry_loop,
            Ok(Err(mismatch)) => {
                recent_upper = mismatch.current;
            }
            Err(e) => {
                panic!("Invalid usage of the persist client for collection {collection_id} status history shard: {e:?}");
            }
        }
    }

    handle.expire().await
}
