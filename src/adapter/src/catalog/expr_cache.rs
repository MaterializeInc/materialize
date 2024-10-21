// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A cache for optimized expressions.

use mz_catalog::durable::expression_cache_shard_id;
use mz_compute_types::dataflows::DataflowDescription;
use mz_durable_cache::{DurableCache, DurableCacheCodec};
use mz_expr::OptimizedMirRelationExpr;
use mz_persist_client::PersistClient;
use mz_persist_types::Codec;
use mz_repr::adt::jsonb::Jsonb;
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{GlobalId, RelationDesc, ScalarType};
use mz_storage_types::sources::SourceData;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::notice::OptimizerNotice;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use smallvec::SmallVec;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use timely::Container;
use uuid::Uuid;

/// That data that is cached per catalog object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Expressions {
    local_mir: OptimizedMirRelationExpr,
    global_mir: DataflowDescription<OptimizedMirRelationExpr>,
    physical_plan: DataflowDescription<mz_compute_types::plan::Plan>,
    dataflow_metainfos: DataflowMetainfo<Arc<OptimizerNotice>>,
    notices: SmallVec<[Arc<OptimizerNotice>; 4]>,
    optimizer_feature: OptimizerFeatures,
}

/// The key used to serialize cached expressions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct CacheKey {
    deploy_generation: u64,
    id: GlobalId,
}

#[derive(Debug)]
struct ExpressionCodec;

impl DurableCacheCodec for ExpressionCodec {
    type Key = CacheKey;
    type Val = Expressions;
    type KeyCodec = SourceData;
    type ValCodec = SourceData;

    fn schemas() -> (
        <Self::KeyCodec as Codec>::Schema,
        <Self::ValCodec as Codec>::Schema,
    ) {
        (
            RelationDesc::builder()
                .with_column("key", ScalarType::Jsonb.nullable(false))
                .finish(),
            RelationDesc::builder()
                .with_column("val", ScalarType::Jsonb.nullable(false))
                .finish(),
        )
    }

    fn encode_key(key: &Self::Key) -> Self::KeyCodec {
        let serde_value = serde_json::to_value(key).expect("valid json");
        encode_serde_value(serde_value)
    }

    fn encode_val(val: &Self::Val) -> Self::ValCodec {
        let serde_value = serde_json::to_value(val).expect("valid json");
        encode_serde_value(serde_value)
    }

    fn decode_key(key: Self::KeyCodec) -> Self::Key {
        let serde_value = decode_source_data(key);
        serde_json::from_value(serde_value).expect("jsonb should roundtrip")
    }

    fn decode_val(val: Self::ValCodec) -> Self::Val {
        let serde_value = decode_source_data(val);
        serde_json::from_value(serde_value).expect("jsonb should roundtrip")
    }
}

fn encode_serde_value(serde_value: Value) -> SourceData {
    let jsonb = Jsonb::from_serde_json(serde_value).expect("contained integers should fit in f64");
    let row = jsonb.into_row();
    SourceData(Ok(row))
}

fn decode_source_data(source_data: SourceData) -> Value {
    let row = source_data
        .0
        .expect("only Ok values stored in expression cache");
    let jsonb = Jsonb::from_row(row);
    jsonb.as_ref().to_serde_json()
}

/// Configuration needed to initialize an [`ExpressionCache`].
pub(crate) struct ExpressionCacheConfig<'a> {
    deploy_generation: u64,
    persist: &'a PersistClient,
    organization_id: Uuid,
}

/// A durable cache of optimized expressions.
pub(crate) struct ExpressionCache {
    deploy_generation: u64,
    durable_cache: DurableCache<ExpressionCodec>,
}

impl ExpressionCache {
    /// Creates a new [`ExpressionCache`].
    pub(crate) async fn new(
        ExpressionCacheConfig {
            deploy_generation,
            persist,
            organization_id,
        }: ExpressionCacheConfig<'_>,
    ) -> Self {
        let shard_id = expression_cache_shard_id(organization_id);
        let durable_cache = DurableCache::new(persist, shard_id, "expression cache").await;
        Self {
            deploy_generation,
            durable_cache,
        }
    }

    /// Reconciles all entries in current deploy generation with the current objects, `current_ids`,
    /// and current optimizer features, `optimizer_features`.
    ///
    /// If `remove_prior_gens` is `true`, all previous generations are durably removed from the
    /// cache.
    ///
    /// Returns all cached expressions in the current deploy generation, after reconciliation.
    pub(crate) async fn open(
        &mut self,
        current_ids: &BTreeSet<GlobalId>,
        optimizer_features: &OptimizerFeatures,
        remove_prior_gens: bool,
    ) -> Vec<(GlobalId, Expressions)> {
        while self
            .try_open(current_ids, optimizer_features, remove_prior_gens)
            .await
            .is_err()
        {}

        self.durable_cache
            .entries_local()
            .map(|(key, expressions)| (key.id.clone(), expressions.clone()))
            .collect()
    }

    async fn try_open(
        &mut self,
        current_ids: &BTreeSet<GlobalId>,
        optimizer_features: &OptimizerFeatures,
        remove_prior_gens: bool,
    ) -> Result<(), mz_durable_cache::Error> {
        let mut keys_to_remove = Vec::new();

        for (key, expressions) in self.durable_cache.entries_local() {
            if key.deploy_generation == self.deploy_generation {
                // Add dropped IDs.
                if !current_ids.contains(&key.id) {
                    keys_to_remove.push((key.clone(), None));
                }

                // Add expressions that were cached with different features.
                if expressions.optimizer_feature != *optimizer_features {
                    keys_to_remove.push((key.clone(), None));
                }
            } else if remove_prior_gens {
                // Add expressions from previous generations.
                keys_to_remove.push((key.clone(), None));
            }
        }

        let keys_to_remove: Vec<_> = keys_to_remove
            .iter()
            .map(|(key, expressions)| (key, expressions.as_ref()))
            .collect();
        self.durable_cache.try_set_many(&keys_to_remove).await
    }

    /// Durably removes all entries given by `invalidate_ids` and inserts `new_entries` into
    /// current deploy generation.
    ///
    /// If there is a duplicate ID in both `invalidate_ids` and `new_entries`, then the final value
    /// will be taken from `new_entries`.
    pub(crate) async fn insert_expressions(
        &mut self,
        new_entries: Vec<(GlobalId, Expressions)>,
        invalidate_ids: BTreeSet<GlobalId>,
    ) {
        let mut entries = BTreeMap::new();
        // Important to do `invalidate_ids` first
        for id in invalidate_ids {
            entries.insert(
                CacheKey {
                    id,
                    deploy_generation: self.deploy_generation,
                },
                None,
            );
        }
        for (id, expressions) in new_entries {
            entries.insert(
                CacheKey {
                    id,
                    deploy_generation: self.deploy_generation,
                },
                Some(expressions),
            );
        }
        let entries: Vec<_> = entries
            .iter()
            .map(|(key, expressions)| (key, expressions.as_ref()))
            .collect();
        self.durable_cache.set_many(&entries).await
    }
}
