// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Healthcheck common

use chrono::{DateTime, NaiveDateTime, Utc};
use differential_dataflow::lattice::Lattice;
use mz_ore::now::NowFn;
use timely::progress::Antichain;

use mz_persist_client::{PersistClient, ShardId, Upper};
use mz_repr::{Datum, GlobalId, Row, Timestamp};
use mz_storage_client::types::sources::SourceData;
use tracing::trace;

pub async fn write_to_persist(
    collection_id: GlobalId,
    new_status: &str,
    new_error: Option<&str>,
    now: NowFn,
    client: &PersistClient,
    status_shard: ShardId,
) {
    let now_ms = now();
    let row = {
        let timestamp = NaiveDateTime::from_timestamp_opt(
            (now_ms / 1000)
                .try_into()
                .expect("timestamp seconds does not fit into i64"),
            (now_ms % 1000 * 1_000_000)
                .try_into()
                .expect("timestamp millis does not fit into a u32"),
        )
        .unwrap();
        let timestamp = Datum::TimestampTz(
            DateTime::from_utc(timestamp, Utc)
                .try_into()
                .expect("must fit"),
        );
        let collection_id = collection_id.to_string();
        let collection_id = Datum::String(&collection_id);
        let status = Datum::String(new_status);
        let error = new_error.into();
        let metadata = Datum::Null;
        Row::pack_slice(&[timestamp, collection_id, status, error, metadata])
    };

    let mut handle = client.open_writer(status_shard, &format!("healthcheck::write_to_persist {}", collection_id)).await.expect(
        "Invalid usage of the persist client for collection {collection_id} status history shard",
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
            Ok(Ok(Ok(()))) => break 'retry_loop,
            Ok(Ok(Err(Upper(upper)))) => {
                recent_upper = upper;
            }
            Ok(Err(e)) => {
                panic!("Invalid usage of the persist client for collection {collection_id} status history shard: {e:?}");
            }
            Err(e) => {
                trace!("compare_and_append in update_status failed: {e}");
            }
        }
    }

    handle.expire().await
}
