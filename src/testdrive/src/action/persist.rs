// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;
use std::sync::Arc;

use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::ShardId;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{RelationDesc, ScalarType, Timestamp};
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::SourceData;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

pub async fn run_force_compaction(
    mut cmd: BuiltinCommand,
    state: &State,
) -> Result<ControlFlow, anyhow::Error> {
    let shard_id = cmd.args.string("shard-id")?;
    cmd.args.done()?;

    let shard_id = ShardId::from_str(&shard_id).expect("invalid shard id");
    let cfg = PersistConfig::new_default_configs(state.build_info, SYSTEM_TIME.clone());

    let metrics_registry = MetricsRegistry::new();

    let Some(consensus_url) = state.persist_consensus_url.as_ref() else {
        anyhow::bail!("Missing persist consensus URL");
    };
    let Some(blob_url) = state.persist_blob_url.as_ref() else {
        anyhow::bail!("Missing persist blob URL");
    };

    let relation_desc = RelationDesc::builder()
        .with_column("key", ScalarType::String.nullable(true))
        .with_column("f1", ScalarType::String.nullable(true))
        .with_column("f2", ScalarType::Int64.nullable(true))
        .finish();

    mz_persist_client::cli::admin::force_compaction::<SourceData, (), Timestamp, StorageDiff>(
        cfg,
        &metrics_registry,
        shard_id,
        consensus_url,
        blob_url,
        Arc::new(relation_desc),
        Arc::new(UnitSchema),
        true,
        None,
    )
    .await?;

    Ok(ControlFlow::Continue)
}
