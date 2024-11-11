// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A cache for optimized expressions.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;

use mz_compute_types::dataflows::DataflowDescription;
use mz_durable_cache::{DurableCache, DurableCacheCodec};
use mz_dyncfg::ConfigSet;
use mz_expr::OptimizedMirRelationExpr;
use mz_ore::channel::trigger;
use mz_ore::soft_panic_or_log;
use mz_ore::task::spawn;
use mz_persist_client::cli::admin::{
    EXPRESSION_CACHE_FORCE_COMPACTION_FUEL, EXPRESSION_CACHE_FORCE_COMPACTION_WAIT,
};
use mz_persist_client::PersistClient;
use mz_persist_types::codec_impls::VecU8Schema;
use mz_persist_types::Codec;
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::GlobalId;
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
    pub local_mir: OptimizedMirRelationExpr,
    pub optimizer_features: OptimizerFeatures,
}

/// The data that is cached per catalog object as a result of global optimizations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalExpressions {
    pub global_mir: DataflowDescription<OptimizedMirRelationExpr>,
    pub physical_plan: DataflowDescription<mz_compute_types::plan::Plan>,
    pub dataflow_metainfos: DataflowMetainfo<Arc<OptimizerNotice>>,
    pub optimizer_features: OptimizerFeatures,
}

impl GlobalExpressions {
    fn index_imports(&self) -> impl Iterator<Item = &GlobalId> {
        self.global_mir
            .index_imports
            .keys()
            .chain(self.physical_plan.index_imports.keys())
    }
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
    // We use a raw bytes instead of `Expressions` so that there is no backwards compatibility
    // requirement on `Expressions` between versions.
    type Val = Vec<u8>;
    type KeyCodec = Vec<u8>;
    type ValCodec = Vec<u8>;

    fn schemas() -> (
        <Self::KeyCodec as Codec>::Schema,
        <Self::ValCodec as Codec>::Schema,
    ) {
        (VecU8Schema::default(), VecU8Schema::default())
    }

    fn encode(key: &Self::Key, val: &Self::Val) -> (Self::KeyCodec, Self::ValCodec) {
        let key = bincode::serialize(key).expect("must serialize");
        (key, val.clone())
    }

    fn decode(key: &Self::KeyCodec, val: &Self::ValCodec) -> (Self::Key, Self::Val) {
        let key = bincode::deserialize(key).expect("must deserialize");
        (key, val.clone())
    }
}

/// Configuration needed to initialize an [`ExpressionCache`].
#[derive(Debug, Clone)]
pub struct ExpressionCacheConfig {
    pub deploy_generation: u64,
    pub persist: PersistClient,
    pub organization_id: Uuid,
    pub current_ids: BTreeSet<GlobalId>,
    pub remove_prior_gens: bool,
    pub compact_shard: bool,
    pub dyncfgs: ConfigSet,
}

/// A durable cache of optimized expressions.
pub struct ExpressionCache {
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
    pub async fn open(
        ExpressionCacheConfig {
            deploy_generation,
            persist,
            organization_id,
            current_ids,
            remove_prior_gens,
            compact_shard,
            dyncfgs,
        }: ExpressionCacheConfig,
    ) -> (
        Self,
        BTreeMap<GlobalId, LocalExpressions>,
        BTreeMap<GlobalId, GlobalExpressions>,
    ) {
        let shard_id = expression_cache_shard_id(organization_id);
        let durable_cache = DurableCache::new(&persist, shard_id, "expressions").await;
        let mut cache = Self {
            deploy_generation,
            durable_cache,
        };

        const RETRIES: usize = 100;
        for _ in 0..RETRIES {
            match cache
                .try_open(&current_ids, remove_prior_gens, compact_shard, &dyncfgs)
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
                        let expressions: LocalExpressions = match bincode::deserialize(expressions)
                        {
                            Ok(expressions) => expressions,
                            Err(err) => {
                                soft_panic_or_log!(
                                    "unable to deserialize local expressions: {expressions:?}: {err:?}"
                                );
                                continue;
                            }
                        };
                        // Remove dropped IDs.
                        if !current_ids.contains(&key.id) {
                            keys_to_remove.push((key.clone(), None));
                        } else {
                            local_expressions.insert(key.id, expressions);
                        }
                    }
                    ExpressionType::Global => {
                        let expressions: GlobalExpressions = match bincode::deserialize(expressions)
                        {
                            Ok(expressions) => expressions,
                            Err(err) => {
                                soft_panic_or_log!(
                                    "unable to deserialize global expressions: {expressions:?}: {err:?}"
                                );
                                continue;
                            }
                        };
                        // Remove dropped IDs and expressions that rely on dropped indexes.
                        let index_dependencies: BTreeSet<_> =
                            expressions.index_imports().cloned().collect();
                        if !current_ids.contains(&key.id)
                            || !index_dependencies.is_subset(current_ids)
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
            let expressions = match bincode::serialize(&expressions) {
                Ok(expressions) => expressions,
                Err(err) => {
                    soft_panic_or_log!(
                        "unable to serialize local expressions: {expressions:?}: {err:?}"
                    );
                    continue;
                }
            };
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
            let expressions = match bincode::serialize(&expressions) {
                Ok(expressions) => expressions,
                Err(err) => {
                    soft_panic_or_log!(
                        "unable to serialize global expressions: {expressions:?}: {err:?}"
                    );
                    continue;
                }
            };
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

#[derive(Debug, Clone)]
pub struct ExpressionCacheHandle {
    tx: mpsc::UnboundedSender<CacheOperation>,
}

impl ExpressionCacheHandle {
    /// Spawns a task responsible for managing the expression cache. See [`ExpressionCache::open`].
    ///
    /// Returns a handle to interact with the cache and the initial contents of the cache.
    pub async fn spawn_expression_cache(
        config: ExpressionCacheConfig,
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

    pub fn update(
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
    use mz_expr::OptimizedMirRelationExpr;
    use mz_persist_client::PersistClient;
    use mz_repr::optimize::OptimizerFeatures;
    use mz_repr::GlobalId;
    use mz_transform::dataflow::DataflowMetainfo;
    use mz_transform::notice::OptimizerNotice;
    use proptest::arbitrary::{any, Arbitrary};
    use proptest::prelude::{BoxedStrategy, ProptestConfig};
    use proptest::proptest;
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;
    use uuid::Uuid;

    use crate::expr_cache::{
        CacheKey, ExpressionCacheConfig, ExpressionCacheHandle, ExpressionCodec, GlobalExpressions,
        LocalExpressions,
    };

    impl Arbitrary for LocalExpressions {
        type Parameters = ();

        fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
            (
                any::<OptimizedMirRelationExpr>(),
                any::<OptimizerFeatures>(),
            )
                .prop_map(|(local_mir, optimizer_features)| LocalExpressions {
                    local_mir,
                    optimizer_features,
                })
                .boxed()
        }

        type Strategy = BoxedStrategy<Self>;
    }

    impl Arbitrary for GlobalExpressions {
        type Parameters = ();
        fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
            (
                any::<DataflowDescription<OptimizedMirRelationExpr>>(),
                any::<DataflowDescription<mz_compute_types::plan::Plan>>(),
                any::<DataflowMetainfo<Arc<OptimizerNotice>>>(),
                any::<OptimizerFeatures>(),
            )
                .prop_map(
                    |(global_mir, physical_plan, dataflow_metainfos, optimizer_features)| {
                        GlobalExpressions {
                            global_mir,
                            physical_plan,
                            dataflow_metainfos,
                            optimizer_features,
                        }
                    },
                )
                .boxed()
        }

        type Strategy = BoxedStrategy<Self>;
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn expression_cache() {
        let local_tree = LocalExpressions::arbitrary()
            .new_tree(&mut TestRunner::default())
            .expect("valid expression");
        let global_tree = GlobalExpressions::arbitrary()
            .new_tree(&mut TestRunner::default())
            .expect("valid expression");
        let generate_local_expressions = move || local_tree.current();
        let generate_global_expressions = move || global_tree.current();

        let first_deploy_generation = 0;
        let second_deploy_generation = 1;
        let persist = PersistClient::new_for_tests().await;
        let organization_id = Uuid::new_v4();

        let mut current_ids = BTreeSet::new();
        let mut remove_prior_gens = false;
        let mut compact_shard = false;
        let dyncfgs = &mz_persist_client::cfg::all_dyncfgs(ConfigSet::default());

        let mut next_id = 0;

        let (mut local_exps, mut global_exps) = {
            // Open a new empty cache.
            let (cache, local_exprs, global_exprs) =
                ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                    deploy_generation: first_deploy_generation,
                    persist: persist.clone(),
                    organization_id,
                    current_ids: current_ids.clone(),
                    remove_prior_gens,
                    compact_shard,
                    dyncfgs: dyncfgs.clone(),
                })
                .await;
            assert_eq!(local_exprs, BTreeMap::new(), "new cache should be empty");
            assert_eq!(global_exprs, BTreeMap::new(), "new cache should be empty");

            // Insert some expressions into the cache.
            let mut local_exps = BTreeMap::new();
            let mut global_exps = BTreeMap::new();
            for _ in 0..4 {
                let id = GlobalId::User(next_id);
                let local_exp = generate_local_expressions();
                let global_exp = generate_global_expressions();

                cache
                    .update(
                        vec![(id, local_exp.clone())],
                        vec![(id, global_exp.clone())],
                        BTreeSet::new(),
                    )
                    .await;

                current_ids.insert(id);
                current_ids.extend(global_exp.index_imports());
                local_exps.insert(id, local_exp);
                global_exps.insert(id, global_exp);

                next_id += 1;
            }
            (local_exps, global_exps)
        };

        {
            // Re-open the cache.
            let (_cache, local_entries, global_entries) =
                ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                    deploy_generation: first_deploy_generation,
                    persist: persist.clone(),
                    organization_id,
                    current_ids: current_ids.clone(),
                    remove_prior_gens,
                    compact_shard,
                    dyncfgs: dyncfgs.clone(),
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
                    persist: persist.clone(),
                    organization_id,
                    current_ids: current_ids.clone(),
                    remove_prior_gens,
                    compact_shard,
                    dyncfgs: dyncfgs.clone(),
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

        {
            // Simulate dropping an object dependency.
            let global_exp_to_remove = global_exps.keys().next().expect("not empty").clone();
            let removed_global_exp = global_exps
                .remove(&global_exp_to_remove)
                .expect("known to exist");
            let dependency_to_remove = removed_global_exp
                .index_imports()
                .next()
                .expect("arbitrary impl always makes non-empty vecs");
            current_ids.remove(&dependency_to_remove);

            // If the dependency is also tracked in the cache remove it.
            let _removed_local_exp = local_exps.remove(&dependency_to_remove);
            let _removed_global_exp = global_exps.remove(&dependency_to_remove);
            // Remove any other exps that depend on dependency.
            global_exps.retain(|_, exp| {
                let index_imports: BTreeSet<_> = exp.index_imports().collect();
                !index_imports.contains(&dependency_to_remove)
            });

            // Re-open the cache.
            let (_cache, local_entries, global_entries) =
                ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                    deploy_generation: first_deploy_generation,
                    persist: persist.clone(),
                    organization_id,
                    current_ids: current_ids.clone(),
                    remove_prior_gens,
                    compact_shard,
                    dyncfgs: dyncfgs.clone(),
                })
                .await;
            assert_eq!(
                local_entries, local_exps,
                "dropped object dependencies should NOT remove local expressions"
            );
            assert_eq!(
                global_entries, global_exps,
                "dropped object dependencies should remove global expressions"
            );
        }

        let (new_gen_local_exps, new_gen_global_exps) = {
            // Open the cache at a new generation.
            let (cache, local_entries, global_entries) =
                ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                    deploy_generation: second_deploy_generation,
                    persist: persist.clone(),
                    organization_id,
                    current_ids: current_ids.clone(),
                    remove_prior_gens,
                    compact_shard,
                    dyncfgs: dyncfgs.clone(),
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
            for _ in 0..3 {
                let id = GlobalId::User(next_id);
                let local_exp = generate_local_expressions();
                let global_exp = generate_global_expressions();

                cache
                    .update(
                        vec![(id, local_exp.clone())],
                        vec![(id, global_exp.clone())],
                        BTreeSet::new(),
                    )
                    .await;

                current_ids.insert(id);
                current_ids.extend(global_exp.index_imports());
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
                    persist: persist.clone(),
                    organization_id,
                    current_ids: current_ids.clone(),
                    remove_prior_gens,
                    compact_shard,
                    dyncfgs: dyncfgs.clone(),
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
                    persist: persist.clone(),
                    organization_id,
                    current_ids: current_ids.clone(),
                    remove_prior_gens,
                    compact_shard,
                    dyncfgs: dyncfgs.clone(),
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
                    persist: persist.clone(),
                    organization_id,
                    current_ids: current_ids.clone(),
                    remove_prior_gens,
                    compact_shard,
                    dyncfgs: dyncfgs.clone(),
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
                    persist: persist.clone(),
                    organization_id,
                    current_ids: current_ids.clone(),
                    remove_prior_gens,
                    compact_shard,
                    dyncfgs: dyncfgs.clone(),
                })
                .await;
        }
    }

    proptest! {
        // Generating the expression structs can take an extremely long amount of time because
        // they are recursive, which can cause test timeouts.
        #![proptest_config(ProptestConfig::with_cases(1))]

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn local_expr_cache_roundtrip((key, val) in any::<(CacheKey, LocalExpressions)>()) {
            let bincode_val = bincode::serialize(&val).expect("must serialize");
            let (encoded_key, encoded_val) = ExpressionCodec::encode(&key, &bincode_val);
            let (decoded_key, decoded_val) = ExpressionCodec::decode(&encoded_key, &encoded_val);
            let decoded_val: LocalExpressions = bincode::deserialize(&decoded_val).expect("local expressions should roundtrip");

            assert_eq!(key, decoded_key);
            assert_eq!(val, decoded_val);
        }

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn global_expr_cache_roundtrip((key, val) in any::<(CacheKey, GlobalExpressions)>()) {
            let bincode_val = bincode::serialize(&val).expect("must serialize");
            let (encoded_key, encoded_val) = ExpressionCodec::encode(&key, &bincode_val);
            let (decoded_key, decoded_val) = ExpressionCodec::decode(&encoded_key, &encoded_val);
            let decoded_val: GlobalExpressions = bincode::deserialize(&decoded_val).expect("global expressions should roundtrip");

            assert_eq!(key, decoded_key);
            assert_eq!(val, decoded_val);
        }
    }
}
