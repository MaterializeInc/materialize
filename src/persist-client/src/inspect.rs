// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! CLI introspection tools for persist

use std::fmt::Debug;
use std::sync::Arc;

use crate::internal::gc::{GarbageCollector, GcReq};
use crate::internal::machine::Machine;
use crate::internal::paths::PartialRollupKey;
use crate::internal::state::{ProtoStateDiff, ProtoStateRollup};
use crate::internal::state_versions::StateVersions;
use crate::{Metrics, PersistConfig, ShardId};
use anyhow::anyhow;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_build_info::{BuildInfo, DUMMY_BUILD_INFO};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist::cfg::{BlobConfig, ConsensusConfig};
use mz_persist::location::SeqNo;
use mz_persist_types::{Codec, Codec64};
use prost::Message;
use timely::progress::Timestamp;

/// Fetches the latest rollup of a given shard
pub async fn fetch_latest_rollup(
    shard_id: ShardId,
    consensus_uri: &str,
    blob_uri: &str,
) -> Result<impl serde::Serialize, anyhow::Error> {
    let cfg = PersistConfig::new(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone());
    let metrics = Metrics::new(&cfg, &MetricsRegistry::new());
    let consensus =
        ConsensusConfig::try_from(&consensus_uri, 1, metrics.postgres_consensus).await?;
    let consensus = consensus.clone().open().await?;
    let blob = BlobConfig::try_from(&blob_uri).await?;
    let blob = blob.clone().open().await?;

    if let Some(diff_buf) = consensus.head(&shard_id.to_string()).await? {
        let diff = ProtoStateDiff::decode(diff_buf.data).expect("invalid encoded diff");
        let rollup_key = PartialRollupKey(diff.latest_rollup_key);
        let rollup_buf = blob
            .get(&rollup_key.complete(&shard_id))
            .await
            .unwrap()
            .unwrap();
        let proto = ProtoStateRollup::decode(rollup_buf.as_slice()).expect("invalid encoded state");
        return Ok(proto);
    }

    Err(anyhow!("unknown shard"))
}

/// Fetches each state in a shard
pub async fn fetch_state_diffs(
    shard_id: ShardId,
    consensus_uri: &str,
) -> Result<Vec<impl serde::Serialize>, anyhow::Error> {
    let cfg = PersistConfig::new(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone());
    let metrics = Metrics::new(&cfg, &MetricsRegistry::new());
    let consensus =
        ConsensusConfig::try_from(&consensus_uri, 1, metrics.postgres_consensus).await?;
    let consensus = consensus.clone().open().await?;

    let mut states = vec![];
    for state in consensus
        .scan(&shard_id.to_string(), SeqNo::minimum())
        .await?
    {
        states.push(ProtoStateRollup::decode(state.data).expect("invalid encoded state"));
    }

    Ok(states)
}

/// WIP
pub async fn run_gc<K, V, T, D>(
    shard_id: ShardId,
    consensus_uri: &str,
    blob_uri: &str,
    old_seqno_since: SeqNo,
    new_seqno_since: SeqNo,
) -> Result<(), anyhow::Error>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    let build_info = BuildInfo {
        version: "0.27.0-alpha.20".into(),
        sha: "".into(),
        time: "".into(),
    };
    let cfg = PersistConfig::new(&build_info, SYSTEM_TIME.clone());
    let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
    let consensus =
        ConsensusConfig::try_from(&consensus_uri, 1, metrics.postgres_consensus.clone()).await?;
    let consensus = consensus.clone().open().await?;
    let blob = BlobConfig::try_from(&blob_uri).await?;
    let blob = blob.clone().open().await?;
    let state_versions = Arc::new(StateVersions::new(
        cfg.clone(),
        consensus,
        blob,
        Arc::clone(&metrics),
    ));
    let mut machine = Machine::<K, V, T, D>::new(cfg, shard_id, metrics, state_versions).await?;

    let req = GcReq {
        shard_id,
        old_seqno_since,
        new_seqno_since,
    };
    GarbageCollector::gc_and_truncate(&mut machine, req).await;
    Ok(())
}
