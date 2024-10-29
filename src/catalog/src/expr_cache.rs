// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A cache for optimized expressions.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;

use mz_compute_types::dataflows::DataflowDescription;
use mz_durable_cache::{DurableCache, DurableCacheCodec};
use mz_expr::OptimizedMirRelationExpr;
use mz_ore::channel::trigger;
use mz_ore::task::spawn;
use mz_persist_client::PersistClient;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::Codec;
use mz_repr::adt::jsonb::{JsonbPacker, JsonbRef};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{Datum, GlobalId, RelationDesc, Row, ScalarType};
use mz_storage_types::sources::SourceData;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::notice::OptimizerNotice;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use timely::Container;
use tokio::sync::mpsc;
use tracing::debug;
use uuid::Uuid;

use crate::durable::expression_cache_shard_id;

/// The data that is cached per catalog object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Expressions {
    local_mir: OptimizedMirRelationExpr,
    global_mir: DataflowDescription<OptimizedMirRelationExpr>,
    physical_plan: DataflowDescription<mz_compute_types::plan::Plan>,
    dataflow_metainfos: DataflowMetainfo<Arc<OptimizerNotice>>,
    notices: SmallVec<[Arc<OptimizerNotice>; 4]>,
    optimizer_features: OptimizerFeatures,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Arbitrary)]
struct CacheKey {
    deploy_generation: u64,
    id: GlobalId,
}

#[derive(Debug)]
struct ExpressionCodec;

impl DurableCacheCodec for ExpressionCodec {
    type Key = CacheKey;
    // We use a raw JSON string instead of `Expressions` so that there is no backwards compatibility
    // requirement on `Expressions` between versions.
    type Val = String;
    type KeyCodec = SourceData;
    type ValCodec = ();

    fn schemas() -> (
        <Self::KeyCodec as Codec>::Schema,
        <Self::ValCodec as Codec>::Schema,
    ) {
        (
            RelationDesc::builder()
                .with_column("key", ScalarType::Jsonb.nullable(false))
                .with_column("val", ScalarType::String.nullable(false))
                .finish(),
            UnitSchema::default(),
        )
    }

    fn encode(key: &Self::Key, val: &Self::Val) -> (Self::KeyCodec, Self::ValCodec) {
        let mut row = Row::default();
        let mut packer = row.packer();

        let serde_key = serde_json::to_value(key).expect("valid json");
        JsonbPacker::new(&mut packer)
            .pack_serde_json(serde_key)
            .expect("contained integers should fit in f64");

        packer.push(Datum::String(val));

        let source_data = SourceData(Ok(row));
        (source_data, ())
    }

    fn decode(key: Self::KeyCodec, (): Self::ValCodec) -> (Self::Key, Self::Val) {
        let row = key.0.expect("only Ok values stored in expression cache");
        let datums = row.unpack();
        assert_eq!(datums.len(), 2, "Row should have 2 columns: {datums:?}");

        let key_json = JsonbRef::from_datum(datums[0]);
        let serde_key = key_json.to_serde_json();
        let key = serde_json::from_value(serde_key).expect("jsonb should roundtrip");

        let val = datums[1].unwrap_str().to_string();

        (key, val)
    }
}

/// Configuration needed to initialize an [`ExpressionCache`].
#[derive(Debug, Clone)]
pub struct ExpressionCacheConfig<'a> {
    deploy_generation: u64,
    persist: &'a PersistClient,
    organization_id: Uuid,
    current_ids: &'a BTreeSet<GlobalId>,
    optimizer_features: &'a OptimizerFeatures,
    remove_prior_gens: bool,
}

/// A durable cache of optimized expressions.
struct ExpressionCache {
    deploy_generation: u64,
    durable_cache: DurableCache<ExpressionCodec>,
}

impl ExpressionCache {
    /// Creates a new [`ExpressionCache`] and reconciles all entries in current deploy generation.
    /// Reconciliation will remove all entries that are not in `current_ids` and remove all
    /// entries that have optimizer features that are not equal to `optimizer_features`.
    ///
    /// If `remove_prior_gens` is `true`, all previous generations are durably removed from the
    /// cache.
    ///
    /// Returns all cached expressions in the current deploy generation, after reconciliation.
    async fn open(
        ExpressionCacheConfig {
            deploy_generation,
            persist,
            organization_id,
            current_ids,
            optimizer_features,
            remove_prior_gens,
        }: ExpressionCacheConfig<'_>,
    ) -> (Self, BTreeMap<GlobalId, Expressions>) {
        let shard_id = expression_cache_shard_id(organization_id);
        let durable_cache = DurableCache::new(persist, shard_id, "expressions").await;
        let mut cache = Self {
            deploy_generation,
            durable_cache,
        };

        const RETRIES: usize = 100;
        for _ in 0..RETRIES {
            match cache
                .try_open(current_ids, optimizer_features, remove_prior_gens)
                .await
            {
                Ok(contents) => return (cache, contents),
                Err(err) => debug!("failed to open cache: {err} ... retrying"),
            }
        }

        panic!("Unable to open expression cache after {RETRIES} retries");
    }

    async fn try_open(
        &mut self,
        current_ids: &BTreeSet<GlobalId>,
        optimizer_features: &OptimizerFeatures,
        remove_prior_gens: bool,
    ) -> Result<BTreeMap<GlobalId, Expressions>, mz_durable_cache::Error> {
        let mut keys_to_remove = Vec::new();
        let mut current_contents = BTreeMap::new();

        for (key, expressions) in self.durable_cache.entries_local() {
            if key.deploy_generation == self.deploy_generation {
                // Only deserialize the current generation.
                let expressions: Expressions =
                    serde_json::from_str(expressions).expect("expressions should roundtrip");

                // Remove dropped IDs and expressions that were cached with different features.
                if !current_ids.contains(&key.id)
                    || expressions.optimizer_features != *optimizer_features
                {
                    keys_to_remove.push((key.clone(), None));
                } else {
                    current_contents.insert(key.id, expressions);
                }
            } else if remove_prior_gens {
                // Remove expressions from previous generations.
                keys_to_remove.push((key.clone(), None));
            }
        }

        let keys_to_remove: Vec<_> = keys_to_remove
            .iter()
            .map(|(key, expressions)| (key, expressions.as_ref()))
            .collect();
        self.durable_cache.try_set_many(&keys_to_remove).await?;

        Ok(current_contents)
    }

    /// Durably removes all entries given by `invalidate_ids` and inserts `new_entries` into
    /// current deploy generation.
    ///
    /// If there is a duplicate ID in both `invalidate_ids` and `new_entries`, then the final value
    /// will be taken from `new_entries`.
    async fn insert_expressions(
        &mut self,
        new_entries: Vec<(GlobalId, Expressions)>,
        invalidate_ids: BTreeSet<GlobalId>,
    ) {
        let mut entries = BTreeMap::new();
        // Important to do `invalidate_ids` first, so that `new_entries` overwrites duplicate keys.
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
            let expressions = serde_json::to_string(&expressions).expect("valid json");
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

/// Operations to perform on the cache.
enum CacheOperation {
    /// See [`ExpressionCache::insert_expressions`].
    Insert {
        new_entries: Vec<(GlobalId, Expressions)>,
        invalidate_ids: BTreeSet<GlobalId>,
        trigger: trigger::Trigger,
    },
}

struct ExpressionCacheHandle {
    tx: mpsc::UnboundedSender<CacheOperation>,
}

impl ExpressionCacheHandle {
    /// Spawns a task responsible for managing the expression cache. See [`ExpressionCache::open`].
    ///
    /// Returns a handle to interact with the cache and the initial contents of the cache.
    async fn spawn_expression_cache(
        config: ExpressionCacheConfig<'_>,
    ) -> (Self, BTreeMap<GlobalId, Expressions>) {
        let (mut cache, entries) = ExpressionCache::open(config).await;
        let (tx, mut rx) = mpsc::unbounded_channel();
        spawn(|| "expression-cache-task", async move {
            loop {
                while let Some(op) = rx.recv().await {
                    match op {
                        CacheOperation::Insert {
                            new_entries,
                            invalidate_ids,
                            trigger: _trigger,
                        } => cache.insert_expressions(new_entries, invalidate_ids).await,
                    }
                }
            }
        });

        (Self { tx }, entries)
    }

    fn insert_expressions(
        &self,
        new_entries: Vec<(GlobalId, Expressions)>,
        invalidate_ids: BTreeSet<GlobalId>,
    ) -> impl Future<Output = ()> {
        let (trigger, trigger_rx) = trigger::channel();
        let op = CacheOperation::Insert {
            new_entries,
            invalidate_ids,
            trigger,
        };
        // If the send fails, then we must be shutting down.
        let _ = self.tx.send(op);
        trigger_rx
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use mz_compute_types::dataflows::{DataflowDescription, DataflowExpirationDesc};
    use mz_durable_cache::DurableCacheCodec;
    use mz_expr::{MirRelationExpr, OptimizedMirRelationExpr};
    use mz_persist_client::PersistClient;
    use mz_repr::optimize::OptimizerFeatures;
    use mz_repr::{GlobalId, RelationType};
    use mz_transform::dataflow::DataflowMetainfo;
    use mz_transform::notice::OptimizerNotice;
    use proptest::arbitrary::{any, Arbitrary};
    use proptest::prelude::{BoxedStrategy, ProptestConfig};
    use proptest::proptest;
    use proptest::strategy::Strategy;
    use proptest::test_runner::TestRunner;
    use smallvec::SmallVec;
    use timely::progress::Antichain;
    use uuid::Uuid;

    use crate::expr_cache::{
        CacheKey, ExpressionCacheConfig, ExpressionCacheHandle, ExpressionCodec, Expressions,
    };

    impl Arbitrary for Expressions {
        type Parameters = ();
        fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
            // It would be better to implement `Arbitrary for these types, but that would be extremely
            // painful, so we just manually construct very simple instances.
            let local_mir = OptimizedMirRelationExpr(MirRelationExpr::Constant {
                rows: Ok(Vec::new()),
                typ: RelationType::empty(),
            });
            let global_mir = DataflowDescription::new("gmir".to_string());
            let physical_plan = DataflowDescription {
                source_imports: Default::default(),
                index_imports: Default::default(),
                objects_to_build: Vec::new(),
                index_exports: Default::default(),
                sink_exports: Default::default(),
                as_of: Default::default(),
                until: Antichain::new(),
                initial_storage_as_of: None,
                refresh_schedule: None,
                debug_name: "pp".to_string(),
                dataflow_expiration_desc: DataflowExpirationDesc::default(),
            };

            let dataflow_metainfos = any::<DataflowMetainfo<Arc<OptimizerNotice>>>();
            let notices = any::<[Arc<OptimizerNotice>; 4]>();
            let optimizer_feature = any::<OptimizerFeatures>();

            (dataflow_metainfos, notices, optimizer_feature)
                .prop_map(move |(dataflow_metainfos, notices, optimizer_feature)| {
                    let local_mir = local_mir.clone();
                    let global_mir = global_mir.clone();
                    let physical_plan = physical_plan.clone();
                    let notices = SmallVec::from_const(notices);
                    Expressions {
                        local_mir,
                        global_mir,
                        physical_plan,
                        dataflow_metainfos,
                        notices,
                        optimizer_features: optimizer_feature,
                    }
                })
                .boxed()
        }

        type Strategy = BoxedStrategy<Self>;
    }

    fn generate_expressions() -> Expressions {
        Expressions::arbitrary()
            .new_tree(&mut TestRunner::default())
            .expect("valid expression")
            .current()
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn expression_cache() {
        let first_deploy_generation = 0;
        let second_deploy_generation = 1;
        let persist = &PersistClient::new_for_tests().await;
        let organization_id = Uuid::new_v4();

        let current_ids = &mut BTreeSet::new();
        let optimizer_features = &mut OptimizerFeatures::default();
        let mut remove_prior_gens = false;

        let mut next_id = 0;

        let mut exps = {
            // Open a new empty cache.
            let (cache, entries) =
                ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                    deploy_generation: first_deploy_generation,
                    persist,
                    organization_id,
                    current_ids,
                    optimizer_features,
                    remove_prior_gens,
                })
                .await;
            assert_eq!(entries, BTreeMap::new(), "new cache should be empty");

            // Insert some expressions into the cache.
            let mut exps = BTreeMap::new();
            for _ in 0..5 {
                let mut exp = (GlobalId::User(next_id), generate_expressions());
                next_id += 1;
                exp.1.optimizer_features = optimizer_features.clone();
                cache
                    .insert_expressions(vec![exp.clone()], BTreeSet::new())
                    .await;
                current_ids.insert(exp.0);
                exps.insert(exp.0, exp.1);
            }
            exps
        };

        {
            // Re-open the cache.
            let (cache, entries) =
                ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                    deploy_generation: first_deploy_generation,
                    persist,
                    organization_id,
                    current_ids,
                    optimizer_features,
                    remove_prior_gens,
                })
                .await;
            assert_eq!(
                entries, exps,
                "re-opening the cache should recover the expressions"
            );

            // Insert an expression with non-matching optimizer features.
            let mut exp = (GlobalId::User(next_id), generate_expressions());
            next_id += 1;
            let mut optimizer_features = optimizer_features.clone();
            optimizer_features.enable_eager_delta_joins =
                !optimizer_features.enable_eager_delta_joins;
            exp.1.optimizer_features = optimizer_features.clone();
            cache
                .insert_expressions(vec![exp.clone()], BTreeSet::new())
                .await;
            current_ids.insert(exp.0);
        }

        {
            // Re-open the cache.
            let (_cache, entries) =
                ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                    deploy_generation: first_deploy_generation,
                    persist,
                    organization_id,
                    current_ids,
                    optimizer_features,
                    remove_prior_gens,
                })
                .await;
            assert_eq!(
                entries, exps,
                "expression with non-matching optimizer features should be removed during reconciliation"
            );
        }

        {
            // Simulate dropping an object.
            let id_to_remove = exps.keys().next().expect("not empty").clone();
            current_ids.remove(&id_to_remove);
            let _removed_exp = exps.remove(&id_to_remove);

            // Re-open the cache.
            let (_cache, entries) =
                ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                    deploy_generation: first_deploy_generation,
                    persist,
                    organization_id,
                    current_ids,
                    optimizer_features,
                    remove_prior_gens,
                })
                .await;
            assert_eq!(
                entries, exps,
                "dropped objects should be removed during reconciliation"
            );
        }

        let new_gen_exps = {
            // Open the cache at a new generation.
            let (cache, entries) =
                ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                    deploy_generation: second_deploy_generation,
                    persist,
                    organization_id,
                    current_ids,
                    optimizer_features,
                    remove_prior_gens,
                })
                .await;
            assert_eq!(entries, BTreeMap::new(), "new generation should be empty");

            // Insert some expressions at the new generation.
            let mut exps = BTreeMap::new();
            for _ in 0..5 {
                let mut exp = (GlobalId::User(next_id), generate_expressions());
                next_id += 1;
                exp.1.optimizer_features = optimizer_features.clone();
                cache
                    .insert_expressions(vec![exp.clone()], BTreeSet::new())
                    .await;
                current_ids.insert(exp.0);
                exps.insert(exp.0, exp.1);
            }
            exps
        };

        {
            // Re-open the cache at the first generation.
            let (_cache, entries) =
                ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                    deploy_generation: first_deploy_generation,
                    persist,
                    organization_id,
                    current_ids,
                    optimizer_features,
                    remove_prior_gens,
                })
                .await;
            assert_eq!(
                entries, exps,
                "Previous generation expressions should still exist"
            );
        }

        {
            // Open the cache at a new generation and clear previous generations.
            remove_prior_gens = true;
            let (_cache, entries) =
                ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                    deploy_generation: second_deploy_generation,
                    persist,
                    organization_id,
                    current_ids,
                    optimizer_features,
                    remove_prior_gens,
                })
                .await;
            assert_eq!(
                entries, new_gen_exps,
                "new generation expressions should be persisted"
            );
        }

        {
            // Re-open the cache at the first generation.
            let (_cache, entries) =
                ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                    deploy_generation: first_deploy_generation,
                    persist,
                    organization_id,
                    current_ids,
                    optimizer_features,
                    remove_prior_gens,
                })
                .await;
            assert_eq!(
                entries,
                BTreeMap::new(),
                "Previous generation expressions should be cleared"
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn expr_cache_roundtrip((key, val) in any::<(CacheKey, Expressions)>()) {
            let serde_val = serde_json::to_string(&val).expect("valid json");
            let (encoded_key, encoded_val) = ExpressionCodec::encode(&key, &serde_val);
            let (decoded_key, decoded_val) = ExpressionCodec::decode(encoded_key, encoded_val);
            let decoded_val: Expressions = serde_json::from_str(&decoded_val).expect("expressions should roundtrip");

            assert_eq!(key, decoded_key);
            assert_eq!(val, decoded_val);
        }
    }
}
