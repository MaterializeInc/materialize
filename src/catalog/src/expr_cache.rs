// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimized expressions and immutable, catalog-selected maintained plans.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use mz_compute_types::dataflows::DataflowDescription;
use mz_durable_cache::{DurableCache, DurableCacheCodec};
use mz_dyncfg::ConfigSet;
use mz_expr::OptimizedMirRelationExpr;
use mz_ore::channel::trigger;
use mz_ore::soft_panic_or_log;
use mz_ore::task::spawn;
use mz_persist_client::PersistClient;
use mz_persist_client::cli::admin::{
    EXPRESSION_CACHE_FORCE_COMPACTION_FUEL, EXPRESSION_CACHE_FORCE_COMPACTION_WAIT,
};
use mz_persist_types::codec_impls::VecU8Schema;
use mz_persist_types::{Codec, ShardId};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{GlobalId, RelationVersion};
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::notice::OptimizerNotice;
use semver::Version;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};
use uuid::Uuid;

/// Identifies an exact build for cache isolation and deployment admission.
pub fn expression_build_version(build_info: &mz_build_info::BuildInfo) -> Version {
    if build_info.is_dev() {
        build_info
            .semver_version_build()
            .expect("build ID is not available on this platform")
    } else {
        build_info.semver_version()
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize
)]
enum ExpressionType {
    Local,
    Global,
    Written { revision: Uuid },
}

/// The data that is cached per catalog object as a result of local optimizations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalExpressions {
    pub local_mir: OptimizedMirRelationExpr,
    pub optimizer_features: OptimizerFeatures,
    /// The owning item's latest version, see [`ExpressionCache::open`].
    pub item_version: RelationVersion,
}

/// Global optimization output, including the plan and its optimizer notices.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalExpressions {
    pub global_mir: DataflowDescription<OptimizedMirRelationExpr>,
    pub physical_plan: DataflowDescription<mz_compute_types::plan::LirRelationExpr>,
    pub dataflow_metainfos: DataflowMetainfo<Arc<OptimizerNotice>>,
    pub optimizer_features: OptimizerFeatures,
    /// The owning item's latest version, see [`ExpressionCache::open`].
    pub item_version: RelationVersion,
}

/// Failure to durably write or read an immutable plan revision.
#[derive(Debug, thiserror::Error)]
pub enum PlanStoreError {
    #[error("written plan codec error: {0}")]
    Codec(#[from] bincode::Error),
    #[error("written plan revision {revision} for {id} already has different contents")]
    RevisionConflict { id: GlobalId, revision: Uuid },
    #[error("expression store shut down")]
    Shutdown,
}

impl GlobalExpressions {
    /// Catalog collections required by the written MIR and physical plan.
    pub fn collection_imports(&self) -> impl Iterator<Item = &GlobalId> {
        self.index_imports()
            .chain(self.global_mir.source_imports.keys())
            .chain(self.physical_plan.source_imports.keys())
    }

    fn index_imports(&self) -> impl Iterator<Item = &GlobalId> {
        self.global_mir
            .index_imports
            .keys()
            .chain(self.physical_plan.index_imports.keys())
    }
}

/// The item version to record with an item's cached expressions, the latest of its `versions`.
pub fn latest_item_version(versions: &BTreeMap<RelationVersion, GlobalId>) -> RelationVersion {
    versions
        .last_key_value()
        .map_or_else(RelationVersion::root, |(version, _)| *version)
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize
)]
struct CacheKey {
    build_version: String,
    id: GlobalId,
    expr_type: ExpressionType,
}

#[derive(Debug, PartialEq, Eq)]
struct ExpressionCodec;

impl DurableCacheCodec for ExpressionCodec {
    type Key = CacheKey;
    // We use a raw bytes instead of `Expressions` so that there is no backwards compatibility
    // requirement on `Expressions` between build versions.
    type Val = Bytes;
    type KeyCodec = Bytes;
    type ValCodec = Bytes;

    fn schemas() -> (
        <Self::KeyCodec as Codec>::Schema,
        <Self::ValCodec as Codec>::Schema,
    ) {
        (VecU8Schema::default(), VecU8Schema::default())
    }

    fn encode(key: &Self::Key, val: &Self::Val) -> (Self::KeyCodec, Self::ValCodec) {
        let key = bincode::serialize(key).expect("must serialize");
        (Bytes::from(key), val.clone())
    }

    fn decode(key: &Self::KeyCodec, val: &Self::ValCodec) -> (Self::Key, Self::Val) {
        let key = bincode::deserialize(key).expect("must deserialize");
        (key, val.clone())
    }
}

/// The native prototype assumes identical committed-plan encodings across its
/// demonstration builds. Replace this assumption with pinned-plan version support.
/// This namespace is never a deployment build identity.
pub const PROTOTYPE_SHARED_PLAN_NAMESPACE: &str = "prototype-shared-plans-v1";

/// Configuration needed to initialize an [`ExpressionCache`].
#[derive(Debug, Clone)]
pub struct ExpressionCacheConfig {
    pub build_version: Version,
    /// Namespace for immutable committed plans, separate from best-effort caches.
    pub written_plan_namespace: String,
    pub persist: PersistClient,
    pub shard_id: ShardId,
    /// Every `GlobalId` that may have entries, mapped to the latest version of the item it
    /// belongs to. All ids of an item map to that same version, which is the one its entries
    /// record, rather than the version the id itself denotes.
    pub current_items: BTreeMap<GlobalId, RelationVersion>,
    /// Whether to advance the backing shard to the Persist client's version target.
    pub upgrade_shard: bool,
    pub compact_shard: bool,
    pub dyncfgs: ConfigSet,
}

/// A durable cache of optimized expressions.
pub struct ExpressionCache {
    build_version: Version,
    written_plan_namespace: String,
    durable_cache: DurableCache<ExpressionCodec>,
}

impl ExpressionCache {
    /// Creates a new [`ExpressionCache`] and reconciles all entries in current build version.
    /// Reconciliation removes entries whose id is not in `current_items` or whose recorded
    /// item version differs from the current one, and global entries that import an index
    /// that is not in `current_items`.
    ///
    /// An entry records the item version it was optimized for, because applying a materialized
    /// view replacement changes an item's definition while the item keeps its `GlobalId`s and
    /// bumps its [`RelationVersion`]. Dropping entries recorded at another version therefore
    /// catches stale entries whichever process wrote them. That includes the replacement's own
    /// entries: they are recorded at its root version, and the apply makes its id a later
    /// version of the target.
    ///
    /// The apply also invalidates the target's entries, but that alone would not do: it only
    /// reaches entries under the applying process's build version. Other deployments
    /// must validate their own entries when reopening.
    ///
    /// Other build namespaces remain intact: another executable or a coexisting
    /// deployment may still use them. Opening a cache does not authorize their reclamation.
    ///
    /// If `upgrade_shard` is `true`, advance the backing shard to the Persist client's
    /// version target without changing expression-build isolation.
    ///
    /// If `compact_shard` is `true`, then this function will block on fully compacting the backing
    /// persist shard.
    ///
    /// Returns all cached expressions in the current build version, after reconciliation.
    pub async fn open(
        ExpressionCacheConfig {
            build_version,
            written_plan_namespace,
            persist,
            shard_id,
            current_items,
            upgrade_shard,
            compact_shard,
            dyncfgs,
        }: ExpressionCacheConfig,
    ) -> (
        Self,
        BTreeMap<GlobalId, LocalExpressions>,
        BTreeMap<GlobalId, GlobalExpressions>,
    ) {
        let durable_cache = DurableCache::new(&persist, shard_id, "expressions").await;
        let mut cache = Self {
            build_version,
            written_plan_namespace,
            durable_cache,
        };

        const RETRIES: usize = 100;
        for _ in 0..RETRIES {
            match cache
                .try_open(&current_items, upgrade_shard, compact_shard, &dyncfgs)
                .await
            {
                Ok((local_expressions, global_expressions)) => {
                    return (cache, local_expressions, global_expressions);
                }
                Err(err) => debug!("failed to open cache: {err} ... retrying"),
            }
        }

        panic!("Unable to open expression cache after {RETRIES} retries");
    }

    async fn try_open(
        &mut self,
        current_items: &BTreeMap<GlobalId, RelationVersion>,
        upgrade_shard: bool,
        compact_shard: bool,
        dyncfgs: &ConfigSet,
    ) -> Result<
        (
            BTreeMap<GlobalId, LocalExpressions>,
            BTreeMap<GlobalId, GlobalExpressions>,
        ),
        mz_durable_cache::Error,
    > {
        let (local_expressions, global_expressions, keys_to_remove) =
            self.select_cached_expressions(current_items, true);
        let keys_to_remove: Vec<_> = keys_to_remove.iter().map(|key| (key, None)).collect();
        self.durable_cache.try_set_many(&keys_to_remove).await?;

        if upgrade_shard {
            self.durable_cache.upgrade_version().await;
        }
        if compact_shard {
            let fuel = EXPRESSION_CACHE_FORCE_COMPACTION_FUEL.handle(dyncfgs);
            let wait = EXPRESSION_CACHE_FORCE_COMPACTION_WAIT.handle(dyncfgs);
            self.durable_cache
                .dangerous_compact_shard(move || fuel.get(), move || wait.get())
                .await;
        }
        Ok((local_expressions, global_expressions))
    }

    /// Selects expressions for the caller's catalog prefix without changing the store.
    /// A lagging reader may reject a newer entry but must not delete it.
    fn select_cached_expressions(
        &self,
        current_items: &BTreeMap<GlobalId, RelationVersion>,
        include_global: bool,
    ) -> (
        BTreeMap<GlobalId, LocalExpressions>,
        BTreeMap<GlobalId, GlobalExpressions>,
        Vec<CacheKey>,
    ) {
        let mut keys_to_remove = Vec::new();
        let mut local_expressions = BTreeMap::new();
        let mut global_expressions = BTreeMap::new();

        for (key, expressions) in self.durable_cache.entries_local() {
            // Reclaiming a build's written entries requires fencing its owner. A
            // cache owner's catalog snapshot proves no such fence, and an entry
            // may precede the transaction that selects it.
            if matches!(key.expr_type, ExpressionType::Written { .. }) {
                continue;
            }
            let build_version = match key.build_version.parse::<Version>() {
                Ok(build_version) => build_version,
                Err(err) => {
                    warn!("unable to parse build version: {key:?}: {err:?}");
                    continue;
                }
            };
            if build_version == self.build_version {
                // Only deserialize the current build version.
                match key.expr_type {
                    ExpressionType::Local => {
                        let expressions: LocalExpressions = match bincode::deserialize(expressions)
                        {
                            Ok(expressions) => expressions,
                            Err(err) => {
                                soft_panic_or_log!(
                                    "unable to deserialize local expressions: ({key:?}, {expressions:?}): {err:?}"
                                );
                                continue;
                            }
                        };
                        // A missing id means the item is gone, a different item version means the
                        // entry was optimized for another definition of it.
                        if current_items.get(&key.id) != Some(&expressions.item_version) {
                            keys_to_remove.push(key.clone());
                        } else {
                            local_expressions.insert(key.id, expressions);
                        }
                    }
                    ExpressionType::Written { .. } => unreachable!("handled above"),
                    ExpressionType::Global if !include_global => continue,
                    ExpressionType::Global => {
                        let expressions: GlobalExpressions = match bincode::deserialize(expressions)
                        {
                            Ok(expressions) => expressions,
                            Err(err) => {
                                soft_panic_or_log!(
                                    "unable to deserialize global expressions: ({key:?}, {expressions:?}): {err:?}"
                                );
                                continue;
                            }
                        };
                        // As above, plus expressions that import a dropped index: `DROP INDEX` is
                        // allowed under a running consumer, whose plan then has to be re-optimized
                        // at the next boot.
                        if current_items.get(&key.id) != Some(&expressions.item_version)
                            || !expressions
                                .index_imports()
                                .all(|id| current_items.contains_key(id))
                        {
                            keys_to_remove.push(key.clone());
                        } else {
                            global_expressions.insert(key.id, expressions);
                        }
                    }
                }
            }
        }
        (local_expressions, global_expressions, keys_to_remove)
    }

    async fn reclaim_unused_releases(
        &mut self,
        live_releases: &[Version],
    ) -> Result<(), mz_durable_cache::Error> {
        // A peer may have populated a retired namespace since our last command.
        // Incorporate that prefix incrementally before deciding there is no work.
        self.durable_cache.synchronize().await;
        let keys: Vec<_> = self
            .durable_cache
            .entries_local()
            .filter(|(key, _)| {
                matches!(
                    key.expr_type,
                    ExpressionType::Local | ExpressionType::Global
                ) && key.build_version.parse::<Version>().is_ok_and(|version| {
                    !live_releases
                        .iter()
                        .any(|live| live.cmp_precedence(&version).is_eq())
                })
            })
            .map(|(key, _)| key.clone())
            .collect();
        if keys.is_empty() {
            return Ok(());
        }
        let removals: Vec<_> = keys.iter().map(|key| (key, None)).collect();
        self.durable_cache.try_set_many(&removals).await
    }

    fn get_global(&self, id: GlobalId) -> Option<GlobalExpressions> {
        let key = CacheKey {
            build_version: self.build_version.to_string(),
            id,
            expr_type: ExpressionType::Global,
        };
        let expressions = self.durable_cache.get_local(&key)?;
        match bincode::deserialize(expressions) {
            Ok(expressions) => Some(expressions),
            Err(err) => {
                soft_panic_or_log!(
                    "unable to deserialize global expressions: ({key:?}, {expressions:?}): {err:?}"
                );
                None
            }
        }
    }

    async fn write_plans(
        &mut self,
        plans: Vec<(GlobalId, Uuid, GlobalExpressions)>,
    ) -> Result<(), PlanStoreError> {
        let mut entries = BTreeMap::new();
        for (id, revision, plan) in plans {
            let key = CacheKey {
                build_version: self.written_plan_namespace.clone(),
                id,
                expr_type: ExpressionType::Written { revision },
            };
            let bytes = Bytes::from(bincode::serialize(&plan)?);
            if let Some(previous) = entries.insert(key, bytes.clone())
                && previous != bytes
            {
                return Err(PlanStoreError::RevisionConflict { id, revision });
            }
        }
        loop {
            let mut writes = Vec::new();
            for (key, bytes) in &entries {
                match self.durable_cache.get_local(key) {
                    Some(existing) if existing != bytes => {
                        let ExpressionType::Written { revision } = key.expr_type else {
                            unreachable!("only written plans are inserted");
                        };
                        return Err(PlanStoreError::RevisionConflict {
                            id: key.id,
                            revision,
                        });
                    }
                    Some(_) => {}
                    None => writes.push((key, Some(bytes))),
                }
            }
            if writes.is_empty() {
                return Ok(());
            }
            // A failed CAS synchronizes the cache. Recheck immutability against that
            // state rather than letting set_many overwrite another writer's revision.
            if self.durable_cache.try_set_many(&writes).await.is_ok() {
                return Ok(());
            }
        }
    }

    async fn read_plans(
        &mut self,
        revisions: Vec<(GlobalId, Uuid)>,
    ) -> Result<BTreeMap<(GlobalId, Uuid), GlobalExpressions>, PlanStoreError> {
        // Written revisions are immutable. Catalog selection establishes their
        // authority, so a local hit needs no freshness check. Do not cache misses:
        // another writer may publish the requested revision before the next read.
        if revisions.iter().any(|(id, revision)| {
            self.durable_cache
                .get_local(&CacheKey {
                    build_version: self.written_plan_namespace.clone(),
                    id: *id,
                    expr_type: ExpressionType::Written {
                        revision: *revision,
                    },
                })
                .is_none()
        }) {
            self.durable_cache.synchronize().await;
        }
        revisions
            .into_iter()
            .filter_map(|(id, revision)| {
                let key = CacheKey {
                    build_version: self.written_plan_namespace.clone(),
                    id,
                    expr_type: ExpressionType::Written { revision },
                };
                self.durable_cache.get_local(&key).map(|bytes| {
                    bincode::deserialize(bytes)
                        .map(|plan| ((id, revision), plan))
                        .map_err(PlanStoreError::from)
                })
            })
            .collect()
    }

    /// Durably removes all entries given by `invalidate_ids` and inserts `new_local_expressions`
    /// and `new_global_expressions` into current build version.
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
        let build_version = self.build_version.to_string();
        // Important to do `invalidate_ids` first, so that `new_X_expressions` overwrites duplicate
        // keys.
        for id in invalidate_ids {
            entries.insert(
                CacheKey {
                    id,
                    build_version: build_version.clone(),
                    expr_type: ExpressionType::Local,
                },
                None,
            );
            entries.insert(
                CacheKey {
                    id,
                    build_version: build_version.clone(),
                    expr_type: ExpressionType::Global,
                },
                None,
            );
        }
        for (id, expressions) in new_local_expressions {
            let expressions = match bincode::serialize(&expressions) {
                Ok(expressions) => Bytes::from(expressions),
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
                    build_version: build_version.clone(),
                    expr_type: ExpressionType::Local,
                },
                Some(expressions),
            );
        }
        for (id, expressions) in new_global_expressions {
            let expressions = match bincode::serialize(&expressions) {
                Ok(expressions) => Bytes::from(expressions),
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
                    build_version: build_version.clone(),
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
    ReclaimUnusedReleases {
        live_releases: Vec<Version>,
    },
    WritePlans {
        plans: Vec<(GlobalId, Uuid, GlobalExpressions)>,
        tx: oneshot::Sender<Result<(), PlanStoreError>>,
    },
    ReadPlans {
        revisions: Vec<(GlobalId, Uuid)>,
        tx: oneshot::Sender<Result<BTreeMap<(GlobalId, Uuid), GlobalExpressions>, PlanStoreError>>,
    },
    GetGlobal {
        id: GlobalId,
        tx: oneshot::Sender<Option<GlobalExpressions>>,
    },
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
        let (cache, local_expressions, global_expressions) = ExpressionCache::open(config).await;
        (Self::spawn(cache), local_expressions, global_expressions)
    }

    /// Opens the plan store and reads valid local expressions without cache maintenance.
    /// The supplied build identifies the paired writer, including its development
    /// binary identity. Written-plan compatibility already authorizes its MIR encoding.
    /// A stale catalog prefix may reject entries but must not delete them.
    pub async fn open_plan_store_with_local_expressions(
        build_version: Version,
        written_plan_namespace: String,
        persist: &PersistClient,
        shard_id: ShardId,
        current_items: &BTreeMap<GlobalId, RelationVersion>,
    ) -> (Self, BTreeMap<GlobalId, LocalExpressions>) {
        let cache = ExpressionCache {
            build_version,
            written_plan_namespace,
            durable_cache: DurableCache::new(persist, shard_id, "expressions").await,
        };
        let (local, _, _) = cache.select_cached_expressions(current_items, false);
        (Self::spawn(cache), local)
    }

    /// Opens the per-build plan store without cache reconciliation or speculative deletion.
    /// Only binaries supporting written plans may share this shard.
    pub async fn open_plan_store(
        build_version: Version,
        persist: &PersistClient,
        shard_id: ShardId,
    ) -> Self {
        let namespace = build_version.to_string();
        Self::open_plan_store_in_namespace(build_version, namespace, persist, shard_id).await
    }

    /// Opens immutable plans under an explicitly authorized namespace. Ordinary
    /// local/global expression caches still belong to the exact writer build. Sharing a
    /// namespace asserts compatible encodings, not deployment admission.
    pub async fn open_plan_store_in_namespace(
        build_version: Version,
        written_plan_namespace: String,
        persist: &PersistClient,
        shard_id: ShardId,
    ) -> Self {
        Self::spawn(ExpressionCache {
            build_version,
            written_plan_namespace,
            durable_cache: DurableCache::new(persist, shard_id, "expressions").await,
        })
    }

    fn spawn(mut cache: ExpressionCache) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel();
        spawn(|| "expression-cache-task", async move {
            let mut pending_reclamation = None;
            let mut retry_at = tokio::time::Instant::now();
            loop {
                let op = tokio::select! {
                    op = rx.recv() => match op {
                        Some(op) => op,
                        None => break,
                    },
                    _ = tokio::time::sleep_until(retry_at), if pending_reclamation.is_some() => {
                        CacheOperation::ReclaimUnusedReleases {
                            live_releases: pending_reclamation.take().expect("pending reclamation"),
                        }
                    },
                };
                match op {
                    CacheOperation::ReclaimUnusedReleases { live_releases } => {
                        match cache.reclaim_unused_releases(&live_releases).await {
                            Ok(()) => pending_reclamation = None,
                            Err(error) => {
                                // The failed CAS incrementally refreshed the cache. Recompute
                                // removals on the next turn, serving plan requests and shutdown
                                // during backoff rather than dropping the cleanup request.
                                debug!(%error, "expression cache reclamation deferred");
                                pending_reclamation = Some(live_releases);
                                retry_at = tokio::time::Instant::now()
                                    + crate::retry::periodic_delay(
                                        std::time::Duration::from_millis(100),
                                    );
                            }
                        }
                    }
                    CacheOperation::WritePlans { plans, tx } => {
                        let _ = tx.send(cache.write_plans(plans).await);
                    }
                    CacheOperation::ReadPlans { revisions, tx } => {
                        let _ = tx.send(cache.read_plans(revisions).await);
                    }
                    CacheOperation::GetGlobal { id, tx } => {
                        let _ = tx.send(cache.get_global(id));
                    }
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
        });

        Self { tx }
    }

    /// Enqueues reclamation using committed admission membership.
    /// Retains every build variant of a live release (ignoring build metadata),
    /// unknown namespaces, and all Written revisions. Call only after writable
    /// catalog admission or successful retirement, never from replica opens.
    ///
    /// Membership can change before this runs. Concurrent admission may lose
    /// disposable cache hits, never authoritative plans. No completion is required
    /// for admission, publication, or retirement. The cache owner retries contention
    /// between serving commands and exits when its handles are dropped.
    pub fn reclaim_unused_releases(&self, live_releases: Vec<Version>) {
        let _ = self
            .tx
            .send(CacheOperation::ReclaimUnusedReleases { live_releases });
    }

    /// Writes immutable revisions and acknowledges durable completion. A catalog transaction
    /// must select a revision before it becomes authoritative. Reusing a revision with different
    /// contents fails, including when another writer wins the expression-shard CAS.
    pub async fn write_plans(
        &self,
        plans: Vec<(GlobalId, Uuid, GlobalExpressions)>,
    ) -> Result<(), PlanStoreError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(CacheOperation::WritePlans { plans, tx })
            .map_err(|_| PlanStoreError::Shutdown)?;
        rx.await.map_err(|_| PlanStoreError::Shutdown)?
    }

    /// Reads immutable revisions locally, synchronizing with other writers if any
    /// requested revision is missing. Missing entries are omitted, never generated.
    /// Callers select revisions from their committed catalog.
    pub async fn read_plans(
        &self,
        revisions: Vec<(GlobalId, Uuid)>,
    ) -> Result<BTreeMap<(GlobalId, Uuid), GlobalExpressions>, PlanStoreError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(CacheOperation::ReadPlans { revisions, tx })
            .map_err(|_| PlanStoreError::Shutdown)?;
        rx.await.map_err(|_| PlanStoreError::Shutdown)?
    }

    /// Returns best-effort local cache contents for the current build version, or `None`
    /// if absent or the cache task shuts down. Reads follow prior updates on this handle
    /// without synchronizing with other cache owners. Callers must validate the item version,
    /// optimizer features, and dependencies against their catalog/compute snapshot.
    pub async fn get_global(&self, id: GlobalId) -> Option<GlobalExpressions> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(CacheOperation::GetGlobal { id, tx }).ok()?;
        rx.await.ok().flatten()
    }

    pub fn update(
        &self,
        new_local_expressions: Vec<(GlobalId, LocalExpressions)>,
        new_global_expressions: Vec<(GlobalId, GlobalExpressions)>,
        invalidate_ids: BTreeSet<GlobalId>,
    ) -> impl Future<Output = ()> + use<> {
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

    use bytes::Bytes;
    use itertools::Itertools;
    use mz_compute_types::dataflows::{DataflowDescription, IndexDesc, IndexImport};
    use mz_durable_cache::DurableCacheCodec;
    use mz_dyncfg::ConfigSet;
    use mz_expr::{MirRelationExpr, OptimizedMirRelationExpr};
    use mz_persist_client::PersistClient;
    use mz_persist_types::ShardId;
    use mz_repr::{Datum, GlobalId, ReprRelationType, ReprScalarType};
    use semver::Version;

    use super::*;

    #[mz_ore::test(tokio::test)]
    async fn reclaim_unused_release_namespaces() {
        use std::sync::atomic::{AtomicBool, Ordering};

        use mz_persist::location::{
            CaSResult, Consensus, ExternalError, ResultStream, SeqNo, VersionedData,
        };
        use mz_persist::mem::{MemBlob, MemConsensus};
        use mz_persist_client::async_runtime::IsolatedRuntime;
        use mz_persist_client::cache::{PersistClientCache, StateCache};
        use mz_persist_client::rpc::PubSubClientConnection;
        use tokio::sync::Notify;

        // Only the owner's next CAS is gated. Separate state caches and noop
        // pubsub keep the peer's advance from bypassing the backend conflict.
        #[derive(Debug)]
        struct GatedConsensus {
            inner: Arc<MemConsensus>,
            shard_key: String,
            armed: AtomicBool,
            entered: Notify,
            release: Notify,
            lost: AtomicBool,
        }

        #[async_trait::async_trait]
        impl Consensus for GatedConsensus {
            fn list_keys(&self) -> ResultStream<'_, String> {
                self.inner.list_keys()
            }

            async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
                self.inner.head(key).await
            }

            async fn compare_and_set(
                &self,
                key: &str,
                new: VersionedData,
            ) -> Result<CaSResult, ExternalError> {
                let gated = key == self.shard_key && self.armed.swap(false, Ordering::SeqCst);
                if gated {
                    self.entered.notify_one();
                    self.release.notified().await;
                }
                let result = self.inner.compare_and_set(key, new).await;
                if gated {
                    assert_eq!(
                        result.as_ref().expect("gated consensus operation"),
                        &CaSResult::ExpectationMismatch
                    );
                    self.lost.store(true, Ordering::SeqCst);
                }
                result
            }

            async fn scan(
                &self,
                key: &str,
                from: SeqNo,
                limit: usize,
            ) -> Result<Vec<VersionedData>, ExternalError> {
                self.inner.scan(key, from, limit).await
            }

            async fn truncate(
                &self,
                key: &str,
                seqno: SeqNo,
            ) -> Result<Option<usize>, ExternalError> {
                self.inner.truncate(key, seqno).await
            }
        }

        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let shard = ShardId::new();
            let consensus = Arc::new(MemConsensus::default());
            let blob = Arc::new(MemBlob::open(Default::default()));
            let gate = Arc::new(GatedConsensus {
                inner: Arc::clone(&consensus),
                shard_key: shard.to_string(),
                armed: AtomicBool::new(false),
                entered: Notify::new(),
                release: Notify::new(),
                lost: AtomicBool::new(false),
            });
            let new_client = |consensus: Arc<dyn Consensus>| {
                let cache = PersistClientCache::new_no_metrics();
                let cfg = cache.cfg().clone();
                let metrics = Arc::clone(cache.metrics());
                let sender = PubSubClientConnection::noop().sender;
                let states = Arc::new(StateCache::new(
                    &cfg,
                    Arc::clone(&metrics),
                    Arc::clone(&sender),
                ));
                PersistClient::new(
                    cfg,
                    Arc::<MemBlob>::clone(&blob),
                    consensus,
                    metrics,
                    Arc::new(IsolatedRuntime::new_for_tests()),
                    states,
                    sender,
                )
                .expect("test persist client")
            };
            let persist = new_client(Arc::<GatedConsensus>::clone(&gate));
            let peer = new_client(consensus);
            let id = GlobalId::User(1);
            let revision = Uuid::new_v4();
            let namespaces = [
                "0.1.0",
                "0.2.0-dev.0+adapter",
                "0.2.0-dev.0+controller",
                "0.2.0-dev.0",
                "0.2.0-dev.1+unused",
                "0.3.0+live",
                "0.4.0+unused",
                "unknown",
                PROTOTYPE_SHARED_PLAN_NAMESPACE,
            ];
            let mut expected = BTreeMap::new();
            for namespace in namespaces {
                for expr_type in [
                    ExpressionType::Local,
                    ExpressionType::Global,
                    ExpressionType::Written { revision },
                ] {
                    let value = match expr_type {
                        ExpressionType::Local => bincode::serialize(&gen_local_expressions()),
                        _ => bincode::serialize(&gen_global_expressions()),
                    }
                    .expect("serialize expression");
                    expected.insert(
                        CacheKey {
                            build_version: namespace.into(),
                            id,
                            expr_type,
                        },
                        Bytes::from(value),
                    );
                }
            }
            let mut durable = DurableCache::<ExpressionCodec>::new(&peer, shard, "test").await;
            durable
                .set_many(
                    &expected
                        .iter()
                        .map(|(key, value)| (key, Some(value)))
                        .collect::<Vec<_>>(),
                )
                .await;
            let cache = ExpressionCacheHandle::open_plan_store(
                "0.3.0+live".parse().expect("live build version"),
                &persist,
                shard,
            )
            .await;
            // Retiring one of two deployments of a release must preserve every dev
            // build of that release, not just the exact build recorded by the survivor.
            let mut members = BTreeMap::from([
                (
                    7,
                    "0.2.0-dev.0+retiring"
                        .parse::<Version>()
                        .expect("retiring build version"),
                ),
                (
                    8,
                    "0.2.0-dev.0+surviving"
                        .parse()
                        .expect("surviving build version"),
                ),
                (9, "0.3.0".parse().expect("live release version")),
            ]);
            members.remove(&7);
            gate.armed.store(true, Ordering::SeqCst);
            cache.reclaim_unused_releases(members.values().cloned().collect());
            gate.entered.notified().await;
            // Reclamation has synchronized and submitted its removal CAS. Advancing
            // the peer now forces an upper conflict, not just a stale initial read.
            durable.set_many(&[]).await;
            gate.release.notify_one();
            assert_eq!(
                cache.get_global(id).await,
                Some(
                    bincode::deserialize(
                        &expected[&CacheKey {
                            build_version: "0.3.0+live".into(),
                            id,
                            expr_type: ExpressionType::Global,
                        }]
                    )
                    .expect("decode seeded global expression")
                ),
                "the actor must still serve commands after contention"
            );
            assert!(gate.lost.load(Ordering::SeqCst));
            // No further startup or retirement request may be needed for cleanup.
            expected.retain(|key, _| {
                matches!(key.expr_type, ExpressionType::Written { .. })
                    || !matches!(
                        key.build_version.as_str(),
                        "0.1.0" | "0.2.0-dev.1+unused" | "0.4.0+unused"
                    )
            });
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                loop {
                    durable.synchronize().await;
                    let actual = durable
                        .entries_local()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect::<BTreeMap<_, _>>();
                    if actual == expected {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("reclamation must retry peer contention");

            // Once the last member leaves, its ordinary caches are disposable but
            // its immutable revisions remain, including revisions in foreign namespaces.
            members.remove(&8);
            cache.reclaim_unused_releases(members.values().cloned().collect());
            let _ = cache.get_global(GlobalId::User(999)).await;
            expected.retain(|key, _| {
                matches!(key.expr_type, ExpressionType::Written { .. })
                    || !key.build_version.starts_with("0.2.0-dev.0")
            });
            durable.synchronize().await;
            assert_eq!(
                durable
                    .entries_local()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect::<BTreeMap<_, _>>(),
                expected
            );

            // The actor's local cache now contains no removable keys. A peer can
            // still publish a disposable namespace before retirement is observed.
            let late = CacheKey {
                build_version: "0.5.0+late".into(),
                id,
                expr_type: ExpressionType::Local,
            };
            let value = Bytes::from(
                bincode::serialize(&gen_local_expressions()).expect("serialize peer expression"),
            );
            durable.set(&late, Some(&value)).await;
            cache.reclaim_unused_releases(members.values().cloned().collect());
            let _ = cache.get_global(GlobalId::User(999)).await;
            durable.synchronize().await;
            assert_eq!(
                durable
                    .entries_local()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect::<BTreeMap<_, _>>(),
                expected,
                "reclamation must discover peer entries even when its local selection was empty"
            );
        })
        .await
        .expect("reclamation and actor commands must complete");
    }

    #[mz_ore::test(tokio::test)]
    async fn written_plan_revisions() {
        let registry = mz_ore::metrics::MetricsRegistry::new();
        let cache = mz_persist_client::cache::PersistClientCache::new(
            mz_persist_client::cfg::PersistConfig::new_for_tests(),
            &registry,
            |_, _| mz_persist_client::rpc::PubSubClientConnection::noop(),
        );
        let persist = cache
            .open(mz_persist_client::PersistLocation::new_in_mem())
            .await
            .expect("persist client");
        let upper_reads = || {
            registry
                .gather()
                .into_iter()
                .find(|family| family.name() == "mz_persist_cmd_fetch_upper_count")
                .expect("upper read counter")
                .get_metric()[0]
                .get_counter()
                .value()
        };
        let shard = ShardId::new();
        let build = Version::new(0, 1, 0);
        let writer = ExpressionCacheHandle::open_plan_store(build.clone(), &persist, shard).await;
        let reader = ExpressionCacheHandle::open_plan_store(build.clone(), &persist, shard).await;
        let id = GlobalId::User(1);
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let absent = Uuid::new_v4();
        let plan = gen_global_expressions();
        let mut changed = plan.clone();
        changed.physical_plan.debug_name = "another plan".into();

        let before = upper_reads();
        assert!(
            reader
                .read_plans(Vec::new())
                .await
                .expect("empty read")
                .is_empty()
        );
        assert_eq!(upper_reads(), before, "empty requests do not synchronize");

        assert!(
            reader
                .read_plans(vec![(id, first)])
                .await
                .expect("read")
                .is_empty()
        );
        writer
            .write_plans(vec![
                (id, first, plan.clone()),
                (id, second, changed.clone()),
            ])
            .await
            .expect("write revisions");
        let before = upper_reads();
        assert_eq!(
            reader
                .read_plans(vec![(id, first), (id, second), (id, absent)])
                .await
                .expect("synchronized read"),
            BTreeMap::from([((id, first), plan.clone()), ((id, second), changed.clone())])
        );
        assert_eq!(
            upper_reads(),
            before + 1.0,
            "misses synchronize once per batch"
        );
        let before = upper_reads();
        assert_eq!(
            reader
                .read_plans(vec![(id, first), (id, second)])
                .await
                .expect("local revisions"),
            BTreeMap::from([((id, first), plan.clone()), ((id, second), changed.clone())])
        );
        assert_eq!(upper_reads(), before, "immutable hits do not synchronize");
        reader
            .write_plans(vec![(id, first, plan.clone())])
            .await
            .expect("idempotent write");

        // The handle has not observed this entry. Its failed CAS must reveal the
        // conflict rather than retrying an overwrite, including the rest of the batch.
        let stale = ExpressionCacheHandle::open_plan_store(build.clone(), &persist, shard).await;
        let raced = Uuid::new_v4();
        writer
            .write_plans(vec![(id, raced, plan.clone())])
            .await
            .expect("winner");
        assert!(matches!(
            stale
                .write_plans(vec![
                    (id, absent, plan.clone()),
                    (id, raced, changed.clone())
                ])
                .await,
            Err(PlanStoreError::RevisionConflict { .. })
        ));
        assert_eq!(
            reader
                .read_plans(vec![(id, raced), (id, absent)])
                .await
                .expect("read winner"),
            BTreeMap::from([((id, raced), plan.clone())])
        );

        let other_build =
            ExpressionCacheHandle::open_plan_store(Version::new(0, 2, 0), &persist, shard).await;
        assert!(
            other_build
                .read_plans(vec![(id, first)])
                .await
                .expect("other build")
                .is_empty()
        );
        other_build
            .write_plans(vec![(id, first, changed.clone())])
            .await
            .expect("independent build");

        // Cache reconciliation has no authority over immutable entries, including
        // another build's entries and revisions not yet selected by the catalog.
        let (cache, _, _) = ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
            build_version: build.clone(),
            written_plan_namespace: build.to_string(),
            persist: persist.clone(),
            shard_id: shard,
            current_items: BTreeMap::new(),
            upgrade_shard: true,
            compact_shard: false,
            dyncfgs: mz_persist_client::cfg::all_dyncfgs(ConfigSet::default()),
        })
        .await;
        cache
            .update(Vec::new(), Vec::new(), BTreeSet::from([id]))
            .await;
        let restarted = ExpressionCacheHandle::open_plan_store(build, &persist, shard).await;
        assert_eq!(
            restarted
                .read_plans(vec![(id, first)])
                .await
                .expect("restart"),
            BTreeMap::from([((id, first), plan)])
        );
        assert_eq!(
            other_build
                .read_plans(vec![(id, first)])
                .await
                .expect("foreign build retained"),
            BTreeMap::from([((id, first), changed)])
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn prototype_shared_written_plans() {
        let persist = PersistClient::new_for_tests().await;
        let shard = ShardId::new();
        let old_build = Version::new(0, 1, 0);
        let new_build = Version::new(0, 2, 0);
        let namespace = PROTOTYPE_SHARED_PLAN_NAMESPACE.to_owned();
        let old = ExpressionCacheHandle::open_plan_store_in_namespace(
            old_build.clone(),
            namespace.clone(),
            &persist,
            shard,
        )
        .await;
        let new = ExpressionCacheHandle::open_plan_store_in_namespace(
            new_build.clone(),
            namespace.clone(),
            &persist,
            shard,
        )
        .await;
        let id = GlobalId::User(1);
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let plan = gen_global_expressions();
        let mut changed = plan.clone();
        changed.physical_plan.debug_name = "new writer".into();
        old.write_plans(vec![(id, first, plan.clone())])
            .await
            .expect("old writes");
        assert_eq!(
            new.read_plans(vec![(id, first)])
                .await
                .expect("new follows"),
            BTreeMap::from([((id, first), plan.clone())])
        );
        new.write_plans(vec![(id, second, changed.clone())])
            .await
            .expect("new writes");
        assert_eq!(
            old.read_plans(vec![(id, second)])
                .await
                .expect("old follows"),
            BTreeMap::from([((id, second), changed.clone())])
        );
        assert!(matches!(
            new.write_plans(vec![(id, first, changed.clone())]).await,
            Err(PlanStoreError::RevisionConflict { .. })
        ));

        // Sharing does not expose bytes to exact-build readers. Nor does ordinary
        // cache reconciliation remove shared immutable entries.
        let (exact, _, globals) =
            ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                build_version: new_build.clone(),
                written_plan_namespace: new_build.to_string(),
                persist: persist.clone(),
                shard_id: shard,
                current_items: BTreeMap::new(),
                upgrade_shard: true,
                compact_shard: false,
                dyncfgs: mz_persist_client::cfg::all_dyncfgs(ConfigSet::default()),
            })
            .await;
        assert!(globals.is_empty());
        assert!(
            exact
                .read_plans(vec![(id, first), (id, second)])
                .await
                .expect("exact namespace")
                .is_empty()
        );
        drop(old);
        drop(new);
        let restarted = ExpressionCacheHandle::open_plan_store_in_namespace(
            old_build, namespace, &persist, shard,
        )
        .await;
        assert_eq!(
            restarted
                .read_plans(vec![(id, first), (id, second)])
                .await
                .expect("restart"),
            BTreeMap::from([((id, first), plan), ((id, second), changed)])
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn written_plan_errors_are_not_misses() {
        let (tx, rx) = mpsc::unbounded_channel();
        drop(rx);
        let stopped = ExpressionCacheHandle { tx };
        assert!(matches!(
            stopped.write_plans(Vec::new()).await,
            Err(PlanStoreError::Shutdown)
        ));
        assert!(matches!(
            stopped.read_plans(Vec::new()).await,
            Err(PlanStoreError::Shutdown)
        ));

        let persist = PersistClient::new_for_tests().await;
        let shard = ShardId::new();
        let build = Version::new(0, 1, 0);
        let store = ExpressionCacheHandle::open_plan_store(build.clone(), &persist, shard).await;
        let id = GlobalId::User(1);
        let revision = Uuid::new_v4();
        let mut raw =
            DurableCache::<ExpressionCodec>::new(&persist, shard, "corrupt plan fixture").await;
        raw.set(
            &CacheKey {
                build_version: build.to_string(),
                id,
                expr_type: ExpressionType::Written { revision },
            },
            Some(&Bytes::from_static(b"not a plan")),
        )
        .await;
        assert!(matches!(
            store.read_plans(vec![(id, revision)]).await,
            Err(PlanStoreError::Codec(_))
        ));
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn cache_reads_do_not_reconcile_peer_entries() {
        let persist = PersistClient::new_for_tests().await;
        let shard_id = ShardId::new();
        let build: Version = "0.1.0+writer".parse().expect("valid writer build version");
        let id = GlobalId::User(1_000_000);
        let local = gen_local_expressions();
        let global = gen_global_expressions();
        let writer =
            ExpressionCacheHandle::open_plan_store(build.clone(), &persist, shard_id).await;
        writer
            .update(vec![(id, local.clone())], Vec::new(), BTreeSet::new())
            .await;
        let revision = Uuid::new_v4();
        writer
            .write_plans(vec![(id, revision, global.clone())])
            .await
            .expect("can write selected plan");
        let current = BTreeMap::from([(id, RelationVersion::root())]);
        for (reader_build, items) in [
            (build.clone(), BTreeMap::new()),
            (
                build.clone(),
                BTreeMap::from([(id, RelationVersion::root().bump())]),
            ),
            (
                "0.1.0+other".parse().expect("valid foreign build version"),
                current.clone(),
            ),
        ] {
            let (_, locals) = ExpressionCacheHandle::open_plan_store_with_local_expressions(
                reader_build,
                build.to_string(),
                &persist,
                shard_id,
                &items,
            )
            .await;
            assert!(locals.is_empty());
        }
        // Stale and foreign readers did not remove the writer's entries. The
        // retained handle also serves immutable revisions without another open.
        let (reader, locals) = ExpressionCacheHandle::open_plan_store_with_local_expressions(
            build.clone(),
            build.to_string(),
            &persist,
            shard_id,
            &current,
        )
        .await;
        assert_eq!(locals, BTreeMap::from([(id, local)]));
        assert_eq!(
            reader
                .read_plans(vec![(id, revision)])
                .await
                .expect("can read selected plan after cache invalidation"),
            BTreeMap::from([((id, revision), global)])
        );
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn expression_cache() {
        let first_build_version = Version::new(0, 1, 0);
        let second_build_version = Version::new(0, 2, 0);
        let persist = PersistClient::new_for_tests().await;
        let shard_id = ShardId::new();

        let mut current_items = BTreeMap::new();
        let mut upgrade_shard = false;
        // Compacting the shard takes too long, so we leave it to integration tests.
        let compact_shard = false;
        let dyncfgs = &mz_persist_client::cfg::all_dyncfgs(ConfigSet::default());
        let spawn = |build_version: &Version,
                     current_items: &BTreeMap<GlobalId, RelationVersion>,
                     upgrade_shard: bool| {
            ExpressionCacheHandle::spawn_expression_cache(ExpressionCacheConfig {
                build_version: build_version.clone(),
                written_plan_namespace: build_version.to_string(),
                persist: persist.clone(),
                shard_id,
                current_items: current_items.clone(),
                upgrade_shard,
                compact_shard,
                dyncfgs: dyncfgs.clone(),
            })
        };

        let mut next_id = 0;

        let (mut local_exps, mut global_exps) = {
            // Open a new empty cache.
            let (cache, local_exprs, global_exprs) =
                spawn(&first_build_version, &current_items, upgrade_shard).await;
            assert_eq!(local_exprs, BTreeMap::new(), "new cache should be empty");
            assert_eq!(global_exprs, BTreeMap::new(), "new cache should be empty");
            assert_eq!(cache.get_global(GlobalId::User(next_id)).await, None);

            // Insert some expressions into the cache.
            let mut local_exps = BTreeMap::new();
            let mut global_exps = BTreeMap::new();
            for _ in 0..4 {
                let id = GlobalId::User(next_id);
                let local_exp = gen_local_expressions();
                let global_exp = gen_global_expressions();

                // The read must observe the update even without awaiting its completion.
                drop(cache.update(
                    vec![(id, local_exp.clone())],
                    vec![(id, global_exp.clone())],
                    BTreeSet::new(),
                ));
                assert_eq!(cache.get_global(id).await, Some(global_exp.clone()));

                current_items.insert(id, RelationVersion::root());
                current_items.extend(
                    global_exp
                        .index_imports()
                        .map(|id| (*id, RelationVersion::root())),
                );
                local_exps.insert(id, local_exp);
                global_exps.insert(id, global_exp);

                next_id += 1;
            }
            (local_exps, global_exps)
        };

        {
            // Re-open the cache.
            let (_cache, local_entries, global_entries) =
                spawn(&first_build_version, &current_items, upgrade_shard).await;
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
            current_items.remove(&id_to_remove);
            let _removed_local_exp = local_exps.remove(&id_to_remove);
            let _removed_global_exp = global_exps.remove(&id_to_remove);

            // Re-open the cache.
            let (_cache, local_entries, global_entries) =
                spawn(&first_build_version, &current_items, upgrade_shard).await;
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
            // Simulate applying a replacement: the item keeps its id at a bumped version.
            let id_to_bump = global_exps.keys().next_back().expect("not empty").clone();
            current_items.insert(id_to_bump, RelationVersion::root().bump());
            let _removed_local_exp = local_exps.remove(&id_to_bump);
            let _removed_global_exp = global_exps.remove(&id_to_bump);

            // Re-open the cache.
            let (_cache, local_entries, global_entries) =
                spawn(&first_build_version, &current_items, upgrade_shard).await;
            assert_eq!(
                local_entries, local_exps,
                "local expressions of an earlier item version should be removed during reconciliation"
            );
            assert_eq!(
                global_entries, global_exps,
                "global expressions of an earlier item version should be removed during reconciliation"
            );

            // The removal is durable: restoring the item version does not bring the entries back.
            current_items.insert(id_to_bump, RelationVersion::root());
            let (_cache, local_entries, global_entries) =
                spawn(&first_build_version, &current_items, upgrade_shard).await;
            assert_eq!(
                local_entries, local_exps,
                "local expressions of an earlier item version should be durably removed"
            );
            assert_eq!(
                global_entries, global_exps,
                "global expressions of an earlier item version should be durably removed"
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
                .expect("generator always makes non-empty index imports");
            current_items.remove(dependency_to_remove);

            // If the dependency is also tracked in the cache remove it.
            let _removed_local_exp = local_exps.remove(dependency_to_remove);
            let _removed_global_exp = global_exps.remove(dependency_to_remove);
            // Remove any other exps that depend on dependency.
            global_exps.retain(|_, exp| {
                let index_imports: BTreeSet<_> = exp.index_imports().collect();
                !index_imports.contains(&dependency_to_remove)
            });

            // Re-open the cache.
            let (_cache, local_entries, global_entries) =
                spawn(&first_build_version, &current_items, upgrade_shard).await;
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
            // Open the cache at a new build version.
            let (cache, local_entries, global_entries) =
                spawn(&second_build_version, &current_items, upgrade_shard).await;
            assert_eq!(
                local_entries,
                BTreeMap::new(),
                "new build version should be empty"
            );
            assert_eq!(
                global_entries,
                BTreeMap::new(),
                "new build version should be empty"
            );

            // Insert some expressions at the new build version.
            let mut local_exps = BTreeMap::new();
            let mut global_exps = BTreeMap::new();
            for _ in 0..2 {
                let id = GlobalId::User(next_id);
                let local_exp = gen_local_expressions();
                let global_exp = gen_global_expressions();

                cache
                    .update(
                        vec![(id, local_exp.clone())],
                        vec![(id, global_exp.clone())],
                        BTreeSet::new(),
                    )
                    .await;

                current_items.insert(id, RelationVersion::root());
                current_items.extend(
                    global_exp
                        .index_imports()
                        .map(|id| (*id, RelationVersion::root())),
                );
                local_exps.insert(id, local_exp);
                global_exps.insert(id, global_exp);

                next_id += 1;
            }
            (local_exps, global_exps)
        };

        {
            // Re-open the cache at the first build version.
            let (cache, local_entries, global_entries) =
                spawn(&first_build_version, &current_items, upgrade_shard).await;
            for id in new_gen_global_exps.keys() {
                assert_eq!(cache.get_global(*id).await, None);
            }
            assert_eq!(
                local_entries, local_exps,
                "Previous build version local expressions should still exist"
            );
            assert_eq!(
                global_entries, global_exps,
                "Previous build version global expressions should still exist"
            );
        }

        {
            // Upgrading the Persist shard must not evict another build namespace.
            upgrade_shard = true;
            let (cache, local_entries, global_entries) =
                spawn(&second_build_version, &current_items, upgrade_shard).await;
            assert_eq!(
                local_entries, new_gen_local_exps,
                "new build version local expressions should be persisted"
            );
            assert_eq!(
                global_entries, new_gen_global_exps,
                "new build version global expressions should be persisted"
            );
            let (&id, expressions) = new_gen_global_exps.first_key_value().expect("not empty");
            assert_eq!(cache.get_global(id).await, Some(expressions.clone()));
            drop(cache.update(Vec::new(), Vec::new(), BTreeSet::from([id])));
            assert_eq!(cache.get_global(id).await, None);
        }

        {
            // Re-open the cache at the first build version.
            let (_cache, local_entries, global_entries) =
                spawn(&first_build_version, &current_items, upgrade_shard).await;
            assert_eq!(
                local_entries, local_exps,
                "Previous build version local expressions must survive shard upgrade"
            );
            assert_eq!(
                global_entries, global_exps,
                "Previous build version global expressions must survive shard upgrade"
            );
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn executable_namespaces_survive_each_others_startup() {
        let persist = PersistClient::new_for_tests().await;
        let shard_id = ShardId::new();
        let id = GlobalId::System(1);
        let builds = [
            Version::parse("0.1.0-dev.0+adapter").expect("valid adapter build version"),
            Version::parse("0.1.0-dev.0+controller").expect("valid controller build version"),
        ];
        let expressions = [gen_local_expressions(), gen_local_expressions()];
        let config = |build: &Version| ExpressionCacheConfig {
            build_version: build.clone(),
            written_plan_namespace: "shared-written-plans".into(),
            persist: persist.clone(),
            shard_id,
            current_items: BTreeMap::from([(id, RelationVersion::root())]),
            upgrade_shard: true,
            compact_shard: false,
            dyncfgs: mz_persist_client::cfg::all_dyncfgs(ConfigSet::default()),
        };

        // Both executables cache the same builtin ID, but neither can consume
        // the other's optimized expression or discard its namespace on open.
        for (build, expression) in builds.iter().zip_eq(&expressions) {
            let (cache, local, global) =
                ExpressionCacheHandle::spawn_expression_cache(config(build)).await;
            assert!(local.is_empty());
            assert!(global.is_empty());
            cache
                .update(vec![(id, expression.clone())], Vec::new(), BTreeSet::new())
                .await;
        }
        for (build, expression) in builds.iter().zip_eq(&expressions) {
            let (_cache, local, global) =
                ExpressionCacheHandle::spawn_expression_cache(config(build)).await;
            assert_eq!(local, BTreeMap::from([(id, expression.clone())]));
            assert!(global.is_empty());
        }
    }

    #[mz_ore::test]
    fn local_expr_cache_roundtrip() {
        let key = CacheKey {
            id: GlobalId::User(1),
            build_version: "1.2.3".into(),
            expr_type: ExpressionType::Local,
        };
        let val = gen_local_expressions();

        let bincode_val = Bytes::from(bincode::serialize(&val).expect("must serialize"));
        let (encoded_key, encoded_val) = ExpressionCodec::encode(&key, &bincode_val);
        let (decoded_key, decoded_val) = ExpressionCodec::decode(&encoded_key, &encoded_val);
        let decoded_val: LocalExpressions =
            bincode::deserialize(&decoded_val).expect("local expressions should roundtrip");

        assert_eq!(key, decoded_key);
        assert_eq!(val, decoded_val);
    }

    #[mz_ore::test]
    fn global_expr_cache_roundtrip() {
        let key = CacheKey {
            id: GlobalId::User(1),
            build_version: "1.2.3".into(),
            expr_type: ExpressionType::Global,
        };
        let val = gen_global_expressions();

        let bincode_val = Bytes::from(bincode::serialize(&val).expect("must serialize"));
        let (encoded_key, encoded_val) = ExpressionCodec::encode(&key, &bincode_val);
        let (decoded_key, decoded_val) = ExpressionCodec::decode(&encoded_key, &encoded_val);
        let decoded_val: GlobalExpressions =
            bincode::deserialize(&decoded_val).expect("global expressions should roundtrip");

        assert_eq!(key, decoded_key);
        assert_eq!(val, decoded_val);
    }

    /// Generate a random [`LocalExpressions`] value.
    ///
    /// The returned values are mostly hardcoded and only differ in a single randomized number.
    /// That's sufficient for the expr cache tests, since the cache mostly treats expressions as
    /// opaque objects.
    fn gen_local_expressions() -> LocalExpressions {
        let datum = Datum::UInt64(rand::random());

        LocalExpressions {
            local_mir: OptimizedMirRelationExpr(MirRelationExpr::constant(
                vec![vec![datum]],
                ReprRelationType::new(vec![ReprScalarType::UInt64.nullable(false)]),
            )),
            optimizer_features: Default::default(),
            item_version: RelationVersion::root(),
        }
    }

    /// Generate a random [`GlobalExpressions`] value.
    ///
    /// The returned values are mostly hardcoded and only differ in a single randomized string.
    /// That's sufficient for the expr cache tests, since the cache mostly treats expressions as
    /// opaque objects.
    fn gen_global_expressions() -> GlobalExpressions {
        let name = format!("test-{}", rand::random::<u64>());

        let mut global_mir = DataflowDescription::new(name.clone());
        let mut physical_plan = DataflowDescription::new(name);

        // Add pieces expected by tests.
        let index_imports = BTreeMap::from_iter([(
            GlobalId::User(2),
            IndexImport {
                desc: IndexDesc {
                    on_id: GlobalId::User(1),
                    key: Default::default(),
                },
                typ: ReprRelationType::empty(),
                monotonic: false,
                with_snapshot: true,
            },
        )]);
        global_mir.index_imports = index_imports.clone();
        physical_plan.index_imports = index_imports;

        GlobalExpressions {
            global_mir,
            physical_plan,
            dataflow_metainfos: Default::default(),
            optimizer_features: Default::default(),
            item_version: RelationVersion::root(),
        }
    }
}
