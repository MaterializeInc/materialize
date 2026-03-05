// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Startup recovery: loads snapshot then replays WAL entries.

use std::collections::HashMap;

use bytes::Bytes;
use tracing::info;

use mz_persist::generated::consensus_service::proto_wal_op;

use crate::actor::{ShardState, VersionedEntry};
use crate::s3_wal::{WalWriter, deserialize_snapshot};

/// Recovers shard state from S3 on startup.
///
/// Returns `(shards, next_batch_number)` — the actor should start writing
/// at `next_batch_number`.
pub async fn recover<W: WalWriter>(
    wal_writer: &W,
) -> (HashMap<String, ShardState>, u64) {
    // 1. Try loading snapshot.
    let (mut shards, last_batch) = match wal_writer.read_snapshot().await {
        Ok(Some(snapshot)) => {
            let (shards, through_batch) = deserialize_snapshot(&snapshot);
            info!(
                through_batch,
                shards = shards.len(),
                "loaded snapshot"
            );
            (shards, through_batch)
        }
        Ok(None) => {
            info!("no snapshot found, starting fresh");
            (HashMap::new(), 0)
        }
        Err(e) => {
            // If we can't read the snapshot, start from scratch and try WAL
            // from the beginning.
            tracing::warn!("failed to read snapshot: {}, starting from batch 0", e);
            (HashMap::new(), 0)
        }
    };

    // 2. Replay WAL entries after the snapshot.
    let mut next = if shards.is_empty() && last_batch == 0 {
        // No snapshot: try from batch 0.
        0
    } else {
        last_batch + 1
    };

    loop {
        match wal_writer.read_batch(next).await {
            Ok(Some(batch)) => {
                info!(batch_number = batch.batch_number, ops = batch.ops.len(), "replaying WAL batch");
                for wal_op in &batch.ops {
                    match &wal_op.op {
                        Some(proto_wal_op::Op::Write(w)) => {
                            let shard = shards.entry(w.key.clone()).or_default();
                            shard.entries.push(VersionedEntry {
                                seqno: w.seqno,
                                data: Bytes::from(w.data.clone()),
                            });
                        }
                        Some(proto_wal_op::Op::Truncate(t)) => {
                            if let Some(shard) = shards.get_mut(&t.key) {
                                shard.entries.retain(|e| e.seqno >= t.seqno);
                            }
                        }
                        None => {
                            tracing::warn!("WAL op with no payload, skipping");
                        }
                    }
                }
                next += 1;
            }
            Ok(None) => {
                // No more WAL entries.
                break;
            }
            Err(e) => {
                tracing::warn!("error reading WAL batch {}: {}, stopping replay", next, e);
                break;
            }
        }
    }

    (shards, next)
}
