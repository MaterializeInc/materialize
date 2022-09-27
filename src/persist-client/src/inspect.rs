// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! CLI introspection tools for persist

use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use bytes::BufMut;
use differential_dataflow::difference::Semigroup;
use prost::Message;

use mz_build_info::BuildInfo;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist::cfg::{BlobConfig, ConsensusConfig};
use mz_persist_types::{Codec, Codec64};
use mz_proto::RustType;

use crate::internal::paths::PartialRollupKey;
use crate::internal::state::{ProtoStateDiff, ProtoStateRollup};
use crate::{Metrics, PersistConfig, ShardId, StateVersions};

const READ_ALL_BUILD_INFO: BuildInfo = BuildInfo {
    version: "10000000.0.0+test",
    sha: "0000000000000000000000000000000000000000",
    time: "",
};

/// Fetches the current state of a given shard
pub async fn fetch_latest_state(
    shard_id: ShardId,
    consensus_uri: &str,
    blob_uri: &str,
) -> Result<impl serde::Serialize, anyhow::Error> {
    let cfg = PersistConfig::new(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
    let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
    let consensus =
        ConsensusConfig::try_from(&consensus_uri, 1, metrics.postgres_consensus.clone())?;
    let consensus = consensus.clone().open().await?;
    let blob = BlobConfig::try_from(&blob_uri).await?;
    let blob = blob.clone().open().await?;

    let state_versions = StateVersions::new(cfg, consensus, blob, Arc::clone(&metrics));
    let versions = state_versions.fetch_live_diffs(&shard_id).await;

    let state = match state_versions
        .fetch_current_state::<K, V, u64, D>(&shard_id, versions.clone())
        .await
    {
        Ok(s) => s.into_proto(),
        Err(codec) => {
            {
                let mut kvtd = KVTD_CODECS.lock().expect("lockable");
                *kvtd = codec.actual;
            }
            state_versions
                .fetch_current_state::<K, V, u64, D>(&shard_id, versions)
                .await
                .expect("codecs match")
                .into_proto()
        }
    };

    Ok(state)
}

/// Fetches the current state rollup of a given shard
pub async fn fetch_latest_state_rollup(
    shard_id: ShardId,
    consensus_uri: &str,
    blob_uri: &str,
) -> Result<impl serde::Serialize, anyhow::Error> {
    let cfg = PersistConfig::new(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
    let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
    let consensus =
        ConsensusConfig::try_from(&consensus_uri, 1, metrics.postgres_consensus.clone())?;
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
    blob_uri: &str,
) -> Result<Vec<impl serde::Serialize>, anyhow::Error> {
    let cfg = PersistConfig::new(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
    let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
    let consensus =
        ConsensusConfig::try_from(&consensus_uri, 1, metrics.postgres_consensus.clone())?;
    let consensus = consensus.clone().open().await?;
    let blob = BlobConfig::try_from(&blob_uri).await?;
    let blob = blob.clone().open().await?;

    let state_versions = StateVersions::new(cfg, consensus, blob, Arc::clone(&metrics));

    let mut live_states = vec![];
    let mut state_iter = match state_versions
        .fetch_live_states::<K, V, u64, D>(&shard_id)
        .await
    {
        Ok(state_iter) => state_iter,
        Err(codec) => {
            {
                let mut kvtd = KVTD_CODECS.lock().expect("lockable");
                *kvtd = codec.actual;
            }
            state_versions
                .fetch_live_states::<K, V, u64, D>(&shard_id)
                .await?
        }
    };

    while let Some(v) = state_iter.next() {
        live_states.push(v.into_proto());
    }

    Ok(live_states)
}

/// The following is a very terrible hack that no one should draw inspiration from. Currently State
/// is generic over <K, V, T, D>, with KVD being represented as phantom data for type safety and to
/// detect persisted codec mismatches. However, reading persisted States does not require actually
/// decoding KVD, so we only need their codec _names_ to match, not the full types. For the purposes
/// of `persistcli inspect`, which only wants to read the persistent data, we create new types that
/// return static Codec names, and rebind the names if/when we get a CodecMismatch, so we can convince
/// the type system and our safety checks that we really can read the data.

#[derive(Debug)]
struct K;
#[derive(Debug)]
struct V;
#[derive(Debug)]
struct T;
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
struct D(i64);

static KVTD_CODECS: Mutex<(String, String, String, String)> =
    Mutex::new((String::new(), String::new(), String::new(), String::new()));

impl Codec for K {
    fn codec_name() -> String {
        KVTD_CODECS.lock().expect("lockable").0.clone()
    }

    fn encode<B>(&self, _buf: &mut B)
    where
        B: BufMut,
    {
    }

    fn decode(_buf: &[u8]) -> Result<Self, String> {
        Ok(Self)
    }
}

impl Codec for V {
    fn codec_name() -> String {
        KVTD_CODECS.lock().expect("lockable").1.clone()
    }

    fn encode<B>(&self, _buf: &mut B)
    where
        B: BufMut,
    {
    }

    fn decode(_buf: &[u8]) -> Result<Self, String> {
        Ok(Self)
    }
}

impl Codec for T {
    fn codec_name() -> String {
        KVTD_CODECS.lock().expect("lockable").2.clone()
    }

    fn encode<B>(&self, _buf: &mut B)
    where
        B: BufMut,
    {
    }

    fn decode(_buf: &[u8]) -> Result<Self, String> {
        Ok(Self)
    }
}

impl Codec64 for D {
    fn codec_name() -> String {
        KVTD_CODECS.lock().expect("lockable").3.clone()
    }

    fn encode(&self) -> [u8; 8] {
        todo!()
    }

    fn decode(_buf: [u8; 8]) -> Self {
        Self(0)
    }
}

impl Codec for D {
    fn codec_name() -> String {
        KVTD_CODECS.lock().expect("lockable").3.clone()
    }

    fn encode<B>(&self, _buf: &mut B)
    where
        B: BufMut,
    {
    }

    fn decode(_buf: &[u8]) -> Result<Self, String> {
        Ok(Self(0))
    }
}

impl Semigroup for D {
    fn plus_equals(&mut self, _rhs: &Self) {}

    fn is_zero(&self) -> bool {
        false
    }
}
