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
use mz_dyncfg::ConfigSet;
use mz_expr::OptimizedMirRelationExpr;
use mz_ore::channel::trigger;
use mz_ore::task::spawn;
use mz_persist_client::cli::admin::{
    EXPRESSION_CACHE_FORCE_COMPACTION_FUEL, EXPRESSION_CACHE_FORCE_COMPACTION_WAIT,
};
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
use timely::Container;
use tokio::sync::mpsc;
use tracing::debug;
use uuid::Uuid;

use crate::durable::expression_cache_shard_id;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Arbitrary)]
enum ExpressionType {
    Local,
    Global,
}

/// The data that is cached per catalog object as a result of local optimizations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalExpressions {
    local_mir: OptimizedMirRelationExpr,
    optimizer_features: OptimizerFeatures,
}

/// The data that is cached per catalog object as a result of global optimizations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalExpressions {
    global_mir: DataflowDescription<OptimizedMirRelationExpr>,
    physical_plan: DataflowDescription<mz_compute_types::plan::Plan>,
    dataflow_metainfos: DataflowMetainfo<Arc<OptimizerNotice>>,
    optimizer_features: OptimizerFeatures,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Arbitrary)]
struct CacheKey {
    deploy_generation: u64,
    id: GlobalId,
    expr_type: ExpressionType,
}

#[derive(Debug, PartialEq, Eq)]
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
            .expect("valid json");

        packer.push(Datum::String(val));

        let source_data = SourceData(Ok(row));
        (source_data, ())
    }

    fn decode(key: &Self::KeyCodec, (): &Self::ValCodec) -> (Self::Key, Self::Val) {
        let row = key
            .0
            .as_ref()
            .expect("only Ok values stored in expression cache");
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
    compact_shard: bool,
    dyncfgs: &'a ConfigSet,
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
    /// If `remove_prior_gens` is `true`, then all previous generations are durably removed from the
    /// cache.
    ///
    /// If `compact_shard` is `true`, then this function will block on fully compacting the backing
    /// persist shard.
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
            compact_shard,
            dyncfgs,
        }: ExpressionCacheConfig<'_>,
    ) -> (
        Self,
        BTreeMap<GlobalId, LocalExpressions>,
        BTreeMap<GlobalId, GlobalExpressions>,
    ) {
        let shard_id = expression_cache_shard_id(organization_id);
        let durable_cache = DurableCache::new(persist, shard_id, "expressions").await;
        let mut cache = Self {
            deploy_generation,
            durable_cache,
        };

        const RETRIES: usize = 100;
        for _ in 0..RETRIES {
            match cache
                .try_open(
                    current_ids,
                    optimizer_features,
                    remove_prior_gens,
                    compact_shard,
                    dyncfgs,
                )
                .await
            {
                Ok((local_expressions, global_expressions)) => {
                    return (cache, local_expressions, global_expressions)
                }
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
        compact_shard: bool,
        dyncfgs: &ConfigSet,
    ) -> Result<
        (
            BTreeMap<GlobalId, LocalExpressions>,
            BTreeMap<GlobalId, GlobalExpressions>,
        ),
        mz_durable_cache::Error,
    > {
        let mut keys_to_remove = Vec::new();
        let mut local_expressions = BTreeMap::new();
        let mut global_expressions = BTreeMap::new();

        for (key, expressions) in self.durable_cache.entries_local() {
            if key.deploy_generation == self.deploy_generation {
                // Only deserialize the current generation.
                match key.expr_type {
                    ExpressionType::Local => {
                        let expressions: LocalExpressions = serde_json::from_str(expressions)
                            .expect("local expressions should roundtrip");
                        // Remove dropped IDs and local expressions that were cached with different
                        // features.
                        if !current_ids.contains(&key.id)
                            || expressions.optimizer_features != *optimizer_features
                        {
                            keys_to_remove.push((key.clone(), None));
                        } else {
                            local_expressions.insert(key.id, expressions);
                        }
                    }
                    ExpressionType::Global => {
                        let expressions: GlobalExpressions = serde_json::from_str(expressions)
                            .expect("global expressions should roundtrip");
                        // Remove dropped IDs and global expressions that were cached with different
                        // features.
                        if !current_ids.contains(&key.id)
                            || expressions.optimizer_features != *optimizer_features
                        {
                            keys_to_remove.push((key.clone(), None));
                        } else {
                            global_expressions.insert(key.id, expressions);
                        }
                    }
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

        if compact_shard {
            let fuel = EXPRESSION_CACHE_FORCE_COMPACTION_FUEL.handle(dyncfgs);
            let wait = EXPRESSION_CACHE_FORCE_COMPACTION_WAIT.handle(dyncfgs);
            self.durable_cache
                .dangerous_compact_shard(move || fuel.get(), move || wait.get())
                .await;
        }

        Ok((local_expressions, global_expressions))
    }

    /// Durably removes all entries given by `invalidate_ids` and inserts `new_local_expressions`
    /// and `new_global_expressions` into current deploy generation.
    ///
    /// If there is a duplicate ID in both `invalidate_ids` and one of the new expressions vector,
    /// then the final value will be taken from the new expressions vector.
    async fn update(
        &mut self,
        new_local_expressions: Vec<(GlobalId, LocalExpressions)>,
        new_global_expressions: Vec<(GlobalId, GlobalExpressions)>,
        invalidate_ids: BTreeSet<GlobalId>,
    ) {
        let mut entries = BTreeMap::new();
        // Important to do `invalidate_ids` first, so that `new_X_expressions` overwrites duplicate
        // keys.
        for id in invalidate_ids {
            entries.insert(
                CacheKey {
                    id,
                    deploy_generation: self.deploy_generation,
                    expr_type: ExpressionType::Local,
                },
                None,
            );
            entries.insert(
                CacheKey {
                    id,
                    deploy_generation: self.deploy_generation,
                    expr_type: ExpressionType::Global,
                },
                None,
            );
        }
        for (id, expressions) in new_local_expressions {
            let expressions = serde_json::to_string(&expressions).expect("valid json");
            entries.insert(
                CacheKey {
                    id,
                    deploy_generation: self.deploy_generation,
                    expr_type: ExpressionType::Local,
                },
                Some(expressions),
            );
        }
        for (id, expressions) in new_global_expressions {
            let expressions = serde_json::to_string(&expressions).expect("valid json");
            entries.insert(
                CacheKey {
                    id,
                    deploy_generation: self.deploy_generation,
                    expr_type: ExpressionType::Global,
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
    /// See [`ExpressionCache::update`].
    Update {
        new_local_expressions: Vec<(GlobalId, LocalExpressions)>,
        new_global_expressions: Vec<(GlobalId, GlobalExpressions)>,
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
    ) -> (
        Self,
        BTreeMap<GlobalId, LocalExpressions>,
        BTreeMap<GlobalId, GlobalExpressions>,
    ) {
        let (mut cache, local_expressions, global_expressions) =
            ExpressionCache::open(config).await;
        let (tx, mut rx) = mpsc::unbounded_channel();
        spawn(|| "expression-cache-task", async move {
            loop {
                while let Some(op) = rx.recv().await {
                    match op {
                        CacheOperation::Update {
                            new_local_expressions,
                            new_global_expressions,
                            invalidate_ids,
                            trigger: _trigger,
                        } => {
                            cache
                                .update(
                                    new_local_expressions,
                                    new_global_expressions,
                                    invalidate_ids,
                                )
                                .await
                        }
                    }
                }
            }
        });

        (Self { tx }, local_expressions, global_expressions)
    }

    fn update(
        &self,
        new_local_expressions: Vec<(GlobalId, LocalExpressions)>,
        new_global_expressions: Vec<(GlobalId, GlobalExpressions)>,
        invalidate_ids: BTreeSet<GlobalId>,
    ) -> impl Future<Output = ()> {
        let (trigger, trigger_rx) = trigger::channel();
        let op = CacheOperation::Update {
            new_local_expressions,
            new_global_expressions,
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

    use mz_compute_types::dataflows::DataflowDescription;
    use mz_durable_cache::DurableCacheCodec;
    use mz_dyncfg::ConfigSet;
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
    use timely::progress::Antichain;
    use uuid::Uuid;

    use crate::expr_cache::{
        CacheKey, ExpressionCacheConfig, ExpressionCacheHandle, ExpressionCodec, GlobalExpressions,
        LocalExpressions,
    };

    impl Arbitrary for LocalExpressions {
        type Parameters = ();

        fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
            // It would be better to implement `Arbitrary for this type, but that would be extremely
            // painful, so we just manually construct a very simple instance.
            let local_mir = OptimizedMirRelationExpr(MirRelationExpr::Constant {
                rows: Ok(Vec::new()),
                typ: RelationType::empty(),
            });
            let optimizer_features = any::<OptimizerFeatures>();
            optimizer_features
                .prop_map(move |optimizer_features| {
                    let local_mir = local_mir.clone();

                    LocalExpressions {
                        local_mir,
                        optimizer_features,
                    }
                })
                .boxed()
        }

        type Strategy = BoxedStrategy<Self>;
    }

    impl Arbitrary for GlobalExpressions {
        type Parameters = ();
        fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
            // It would be better to implement `Arbitrary for these types, but that would be extremely
            // painful, so we just manually construct very simple instances.
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
                time_dependence: None,
            };

            let dataflow_metainfos = any::<DataflowMetainfo<Arc<OptimizerNotice>>>();
            let optimizer_features = any::<OptimizerFeatures>();

            (dataflow_metainfos, optimizer_features)
                .prop_map(move |(dataflow_metainfos, optimizer_features)| {
                    let global_mir = global_mir.clone();
                    let physical_plan = physical_plan.clone();
                    GlobalExpressions {
                        global_mir,
                        physical_plan,
                        dataflow_metainfos,
                        optimizer_features,
                    }
                })
                .boxed()
        }

        type Strategy = BoxedStrategy<Self>;
    }

    fn generate_local_expressions() -> LocalExpressions {
        LocalExpressions::arbitrary()
            .new_tree(&mut TestRunner::default())
            .expect("valid expression")
            .current()
    }

    fn generate_global_expressions() -> GlobalExpressions {
        GlobalExpressions::arbitrary()
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
        let mut compact_shard = false;
        let dyncfgs = &mz_persist_client::cfg::all_dyncfgs(ConfigSet::default());

        let mut next_id = 0;

        let (mut local_exps, mut global_exps) = {
            // Open a new empty cache.
            let (cache, local_exprs, global_exprs) =
                ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                    deploy_generation: first_deploy_generation,
                    persist,
                    organization_id,
                    current_ids,
                    optimizer_features,
                    remove_prior_gens,
                    compact_shard,
                    dyncfgs,
                })
                .await;
            assert_eq!(local_exprs, BTreeMap::new(), "new cache should be empty");
            assert_eq!(global_exprs, BTreeMap::new(), "new cache should be empty");

            // Insert some expressions into the cache.
            let mut local_exps = BTreeMap::new();
            let mut global_exps = BTreeMap::new();
            for _ in 0..5 {
                let id = GlobalId::User(next_id);
                let mut local_exp = generate_local_expressions();
                local_exp.optimizer_features = optimizer_features.clone();
                let mut global_exp = generate_global_expressions();
                global_exp.optimizer_features = optimizer_features.clone();

                cache
                    .update(
                        vec![(id, local_exp.clone())],
                        vec![(id, global_exp.clone())],
                        BTreeSet::new(),
                    )
                    .await;

                current_ids.insert(id);
                local_exps.insert(id, local_exp);
                global_exps.insert(id, global_exp);

                next_id += 1;
            }
            (local_exps, global_exps)
        };

        {
            // Re-open the cache.
            let (cache, local_entries, global_entries) =
                ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                    deploy_generation: first_deploy_generation,
                    persist,
                    organization_id,
                    current_ids,
                    optimizer_features,
                    remove_prior_gens,
                    compact_shard,
                    dyncfgs,
                })
                .await;
            assert_eq!(
                local_entries, local_exps,
                "re-opening the cache should recover the local expressions"
            );
            assert_eq!(
                global_entries, global_exps,
                "re-opening the cache should recover the global expressions"
            );

            // Insert an expression with non-matching optimizer features.
            let id = GlobalId::User(next_id);
            let mut local_exp = generate_local_expressions();
            let mut global_exp = generate_global_expressions();
            next_id += 1;
            let mut optimizer_features = optimizer_features.clone();
            optimizer_features.enable_eager_delta_joins =
                !optimizer_features.enable_eager_delta_joins;
            local_exp.optimizer_features = optimizer_features.clone();
            global_exp.optimizer_features = optimizer_features.clone();
            cache
                .update(
                    vec![(id, local_exp.clone())],
                    vec![(id, global_exp.clone())],
                    BTreeSet::new(),
                )
                .await;
            current_ids.insert(id);
        }

        {
            // Re-open the cache.
            let (_cache, local_entries, global_entries) =
                ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                    deploy_generation: first_deploy_generation,
                    persist,
                    organization_id,
                    current_ids,
                    optimizer_features,
                    remove_prior_gens,
                    compact_shard,
                    dyncfgs,
                })
                .await;
            assert_eq!(
                local_entries, local_exps,
                "local expression with non-matching optimizer features should be removed during reconciliation"
            );
            assert_eq!(
                global_entries, global_exps,
                "global expression with non-matching optimizer features should be removed during reconciliation"
            );
        }

        {
            // Simulate dropping an object.
            let id_to_remove = local_exps.keys().next().expect("not empty").clone();
            current_ids.remove(&id_to_remove);
            let _removed_local_exp = local_exps.remove(&id_to_remove);
            let _removed_global_exp = global_exps.remove(&id_to_remove);

            // Re-open the cache.
            let (_cache, local_entries, global_entries) =
                ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                    deploy_generation: first_deploy_generation,
                    persist,
                    organization_id,
                    current_ids,
                    optimizer_features,
                    remove_prior_gens,
                    compact_shard,
                    dyncfgs,
                })
                .await;
            assert_eq!(
                local_entries, local_exps,
                "dropped local objects should be removed during reconciliation"
            );
            assert_eq!(
                global_entries, global_exps,
                "dropped global objects should be removed during reconciliation"
            );
        }

        let (new_gen_local_exps, new_gen_global_exps) = {
            // Open the cache at a new generation.
            let (cache, local_entries, global_entries) =
                ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                    deploy_generation: second_deploy_generation,
                    persist,
                    organization_id,
                    current_ids,
                    optimizer_features,
                    remove_prior_gens,
                    compact_shard,
                    dyncfgs,
                })
                .await;
            assert_eq!(
                local_entries,
                BTreeMap::new(),
                "new generation should be empty"
            );
            assert_eq!(
                global_entries,
                BTreeMap::new(),
                "new generation should be empty"
            );

            // Insert some expressions at the new generation.
            let mut local_exps = BTreeMap::new();
            let mut global_exps = BTreeMap::new();
            for _ in 0..5 {
                let id = GlobalId::User(next_id);
                let mut local_exp = generate_local_expressions();
                local_exp.optimizer_features = optimizer_features.clone();
                let mut global_exp = generate_global_expressions();
                global_exp.optimizer_features = optimizer_features.clone();

                cache
                    .update(
                        vec![(id, local_exp.clone())],
                        vec![(id, global_exp.clone())],
                        BTreeSet::new(),
                    )
                    .await;

                current_ids.insert(id);
                local_exps.insert(id, local_exp);
                global_exps.insert(id, global_exp);

                next_id += 1;
            }
            (local_exps, global_exps)
        };

        {
            // Re-open the cache at the first generation.
            let (_cache, local_entries, global_entries) =
                ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                    deploy_generation: first_deploy_generation,
                    persist,
                    organization_id,
                    current_ids,
                    optimizer_features,
                    remove_prior_gens,
                    compact_shard,
                    dyncfgs,
                })
                .await;
            assert_eq!(
                local_entries, local_exps,
                "Previous generation local expressions should still exist"
            );
            assert_eq!(
                global_entries, global_exps,
                "Previous generation global expressions should still exist"
            );
        }

        {
            // Open the cache at a new generation and clear previous generations.
            remove_prior_gens = true;
            let (_cache, local_entries, global_entries) =
                ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                    deploy_generation: second_deploy_generation,
                    persist,
                    organization_id,
                    current_ids,
                    optimizer_features,
                    remove_prior_gens,
                    compact_shard,
                    dyncfgs,
                })
                .await;
            assert_eq!(
                local_entries, new_gen_local_exps,
                "new generation local expressions should be persisted"
            );
            assert_eq!(
                global_entries, new_gen_global_exps,
                "new generation global expressions should be persisted"
            );
        }

        {
            // Re-open the cache at the first generation.
            let (_cache, local_entries, global_entries) =
                ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                    deploy_generation: first_deploy_generation,
                    persist,
                    organization_id,
                    current_ids,
                    optimizer_features,
                    remove_prior_gens,
                    compact_shard,
                    dyncfgs,
                })
                .await;
            assert_eq!(
                local_entries,
                BTreeMap::new(),
                "Previous generation local expressions should be cleared"
            );
            assert_eq!(
                global_entries,
                BTreeMap::new(),
                "Previous generation global expressions should be cleared"
            );
        }

        {
            // Re-open the cache and compact the shard.
            compact_shard = true;
            let (_cache, _local_entries, _global_entries) =
                ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                    deploy_generation: first_deploy_generation,
                    persist,
                    organization_id,
                    current_ids,
                    optimizer_features,
                    remove_prior_gens,
                    compact_shard,
                    dyncfgs,
                })
                .await;
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn local_expr_cache_roundtrip((key, val) in any::<(CacheKey, LocalExpressions)>()) {
            let serde_val = serde_json::to_string(&val).expect("valid json");
            let (encoded_key, encoded_val) = ExpressionCodec::encode(&key, &serde_val);
            let (decoded_key, decoded_val) = ExpressionCodec::decode(&encoded_key, &encoded_val);
            let decoded_val: LocalExpressions = serde_json::from_str(&decoded_val).expect("local expressions should roundtrip");

            assert_eq!(key, decoded_key);
            assert_eq!(val, decoded_val);
        }

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn global_expr_cache_roundtrip((key, val) in any::<(CacheKey, GlobalExpressions)>()) {
            let serde_val = serde_json::to_string(&val).expect("valid json");
            let (encoded_key, encoded_val) = ExpressionCodec::encode(&key, &serde_val);
            let (decoded_key, decoded_val) = ExpressionCodec::decode(&encoded_key, &encoded_val);
            let decoded_val: GlobalExpressions = serde_json::from_str(&decoded_val).expect("global expressions should roundtrip");

            assert_eq!(key, decoded_key);
            assert_eq!(val, decoded_val);
        }
    }
}
