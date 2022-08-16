// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! CLI introspection tools for persist

use crate::r#impl::state::ProtoStateRollup;
use crate::{Metrics, ShardId};
use anyhow::anyhow;
use mz_ore::metrics::MetricsRegistry;
use mz_persist::cfg::ConsensusConfig;
use mz_persist::location::SeqNo;
use prost::Message;

/// Fetches the current state of a given shard
pub async fn fetch_current_state(
    shard_id: ShardId,
    consensus_uri: &str,
) -> Result<impl serde::Serialize, anyhow::Error> {
    let metrics = Metrics::new(&MetricsRegistry::new());
    let consensus =
        ConsensusConfig::try_from(&consensus_uri, 1, metrics.postgres_consensus).await?;
    let consensus = consensus.clone().open().await?;

    if let Some(data) = consensus.head(&shard_id.to_string()).await? {
        let proto = ProtoStateRollup::decode(data.data).expect("invalid encoded state");
        return Ok(proto);
    }

    Err(anyhow!("unknown shard"))
}

/// Fetches each state in a shard
pub async fn fetch_state_diffs(
    shard_id: ShardId,
    consensus_uri: &str,
) -> Result<Vec<impl serde::Serialize>, anyhow::Error> {
    let metrics = Metrics::new(&MetricsRegistry::new());
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
