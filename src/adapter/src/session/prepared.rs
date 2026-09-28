// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Session-local ownership of optional prepared execution programs.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use mz_controller_types::ClusterId;
use mz_expr::RowSetFinishing;
use mz_ore::cast::CastFrom;
use mz_repr::SqlRelationType;
use mz_repr::optimize::OverrideFrom;
use mz_sql::plan::{Params, Plan, QueryWhen, SelectPlan};

use super::{PreparedQuery, Session};
use crate::AdapterError;
use crate::catalog::Catalog;
use crate::coord::{TargetCluster, catalog_serving};
use crate::metrics::PreparedCacheMetrics;
use crate::optimize::OptimizerConfig;
use crate::optimize::dataflows::{ComputeInstanceSnapshot, DataflowBuilder};
use crate::optimize::prepared::{FinishingTemplate, IndexedQueryTemplate, LinearQueryTemplate};

const MAX_ENTRIES: usize = 64;
const MAX_CHARGED_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug)]
struct Usage {
    bytes: AtomicUsize,
    entries: AtomicUsize,
    metrics: PreparedCacheMetrics,
}

#[derive(Debug)]
struct Charge {
    usage: Arc<Usage>,
    bytes: usize,
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.usage.bytes.fetch_sub(self.bytes, Ordering::Relaxed);
        self.usage.entries.fetch_sub(1, Ordering::Relaxed);
        self.usage
            .metrics
            .estimated_bytes
            .sub(u64::cast_from(self.bytes));
        self.usage.metrics.compiled_entries.dec();
    }
}

#[derive(Debug)]
pub(crate) struct PreparedExecution {
    pub template: IndexedQueryTemplate,
    pub typ: SqlRelationType,
    pub cluster: ClusterId,
    pub config: OptimizerConfig,
    finishing: FinishingTemplate,
    _charge: Charge,
}

#[derive(Debug)]
struct Entry {
    query: Weak<PreparedQuery>,
    cluster: ClusterId,
    config: OptimizerConfig,
    plan: Option<Arc<PreparedExecution>>,
}

#[derive(Debug)]
pub(super) struct PreparedPlanCache {
    entries: VecDeque<Entry>,
    usage: Arc<Usage>,
}

impl PreparedPlanCache {
    pub(super) fn new(metrics: PreparedCacheMetrics) -> Self {
        Self {
            entries: VecDeque::new(),
            usage: Arc::new(Usage {
                bytes: AtomicUsize::new(0),
                entries: AtomicUsize::new(0),
                metrics,
            }),
        }
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
    }

    pub(super) fn prune(&mut self) {
        let before = self.entries.len();
        self.entries.retain(|entry| entry.query.strong_count() > 0);
        let removed = before - self.entries.len();
        if removed > 0 {
            self.usage.metrics.orphaned.inc_by(u64::cast_from(removed));
        }
    }

    fn get(
        &mut self,
        query: &Arc<PreparedQuery>,
        cluster: ClusterId,
        config: &OptimizerConfig,
    ) -> Option<Option<Arc<PreparedExecution>>> {
        self.prune();
        let position = self.entries.iter().position(|entry| {
            entry.query.as_ptr() == Arc::as_ptr(query)
                && entry.cluster == cluster
                && &entry.config == config
        })?;
        let entry = self.entries.remove(position).expect("found entry");
        let plan = entry.plan.clone();
        self.entries.push_back(entry);
        Some(plan)
    }

    fn reserve(&mut self, bytes: usize) -> Option<Charge> {
        if bytes > MAX_CHARGED_BYTES {
            return None;
        }
        while self
            .usage
            .bytes
            .load(Ordering::Relaxed)
            .saturating_add(bytes)
            > MAX_CHARGED_BYTES
            || self.usage.entries.load(Ordering::Relaxed) >= MAX_ENTRIES
        {
            self.evict()?;
        }
        // Only the session admits entries. A charge can be released by a task
        // still executing an evicted program, so accounting follows Arc lifetime.
        self.usage.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.usage.entries.fetch_add(1, Ordering::Relaxed);
        self.usage
            .metrics
            .estimated_bytes
            .add(u64::cast_from(bytes));
        self.usage.metrics.compiled_entries.inc();
        Some(Charge {
            usage: Arc::clone(&self.usage),
            bytes,
        })
    }

    fn insert(
        &mut self,
        query: &Arc<PreparedQuery>,
        cluster: ClusterId,
        config: OptimizerConfig,
        plan: Option<Arc<PreparedExecution>>,
    ) {
        while self.entries.len() >= MAX_ENTRIES {
            self.evict();
        }
        self.entries.push_back(Entry {
            query: Arc::downgrade(query),
            cluster,
            config,
            plan,
        });
    }

    fn evict(&mut self) -> Option<()> {
        self.entries.pop_front()?;
        self.usage.metrics.evictions.inc();
        Some(())
    }
}

impl Session {
    /// Returns a policy-checking SELECT and its reusable execution program.
    /// The SELECT retains unbound HIR and must not enter the custom optimizer.
    pub(crate) fn try_prepared_execution(
        &mut self,
        catalog: &Catalog,
        query: &Arc<PreparedQuery>,
        params: &Params,
    ) -> Result<Option<(Plan, Arc<PreparedExecution>)>, AdapterError> {
        if !catalog.system_config().enable_prepared_query_reuse()
            || !catalog.system_config().enable_prepared_query_templates()
            || !query.is_valid(catalog, self)
            || query.select.has_as_of()
            || self.vars().emit_plan_insights_notice()
            || params.execute_types != params.expected_types
            || params.expected_types != query.parameter_types
        {
            return Ok(None);
        }
        let mut plan = Plan::Select(SelectPlan {
            source: query.select.source().clone(),
            finishing: RowSetFinishing::trivial(query.select.source().arity()),
            when: QueryWhen::Immediately,
            copy_to: None,
            select: None,
        });
        let conn_catalog = catalog.for_session(self);
        mz_sql::plan::check_unsafe_functions(&conn_catalog, &query.resolved_ids)?;
        let target = self.transaction().cluster().map_or_else(
            || catalog_serving::catalog_server_target(&conn_catalog, self, &plan),
            TargetCluster::Transaction,
        );
        let cluster = catalog.resolve_target_cluster(target, self)?;
        let cluster_id = cluster.id;
        let config = OptimizerConfig::from(catalog.system_config())
            .override_from(&cluster.config.features())
            .override_from(
                &catalog
                    .state()
                    .cluster_scoped_optimizer_overrides(cluster_id),
            );
        if config.no_fast_path {
            return Ok(None);
        }
        let compiled = match self.prepared_plans.get(query, cluster_id, &config) {
            Some(compiled) => {
                if compiled.is_some() {
                    self.metrics().prepared.template_hit.inc();
                } else {
                    self.metrics().prepared.unsupported_hit.inc();
                }
                compiled
            }
            None => {
                let _timer = self.metrics().prepared.compile_seconds.start_timer();
                let mut admission_declined = false;
                let compiled = (|| -> Result<_, AdapterError> {
                    self.metrics().prepared.template_compile.inc();
                    let Some(linear) = LinearQueryTemplate::compile(
                        query.select.source(),
                        &params.expected_types,
                        (&config).into(),
                    )?
                    else {
                        return Ok(None);
                    };
                    let builder = DataflowBuilder::new(
                        catalog.state(),
                        ComputeInstanceSnapshot::new_without_collections(cluster_id),
                    );
                    let Some(template) = builder
                        .indexes_on(linear.collection_id())
                        .find_map(|(id, index)| linear.for_index(id, &index.keys))
                    else {
                        return Ok(None);
                    };
                    let Some(finishing) = FinishingTemplate::compile(
                        query.select.finishing(),
                        params.expected_types.len(),
                    )?
                    else {
                        return Ok(None);
                    };
                    let types = params
                        .expected_types
                        .iter()
                        .cloned()
                        .enumerate()
                        .map(|(i, typ)| (i + 1, typ))
                        .collect();
                    let typ = query.select.source().typ(&[], &types);
                    // Charge serialized payload plus structural overhead. This is
                    // an admission estimate, not a measurement of allocator RSS.
                    let serialized = serde_json::to_vec(&(&template, &finishing, &typ))
                        .map_err(|e| AdapterError::Internal(e.to_string()))?;
                    let bytes = serialized.len().saturating_mul(8).saturating_add(4096);
                    let Some(charge) = self.prepared_plans.reserve(bytes) else {
                        self.metrics().prepared.admission_declined.inc();
                        admission_declined = true;
                        return Ok(None);
                    };
                    Ok(Some(Arc::new(PreparedExecution {
                        template,
                        finishing,
                        typ,
                        cluster: cluster_id,
                        config: config.clone(),
                        _charge: charge,
                    })))
                })()?;
                // Compilation is synchronous with session ownership and contains
                // no await. Analysis context was checked against this catalog.
                if !admission_declined {
                    self.prepared_plans
                        .insert(query, cluster_id, config, compiled.clone());
                }
                compiled
            }
        };
        let Some(compiled) = compiled else {
            return Ok(None);
        };
        let Some(finishing) = compiled.finishing.instantiate(params) else {
            return Ok(None);
        };
        let Plan::Select(select) = &mut plan else {
            unreachable!()
        };
        select.finishing = finishing;
        Ok(Some((plan, compiled)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_ore::assert_none;
    use mz_ore::collections::CollectionExt;
    use mz_repr::GlobalId;

    fn cache() -> PreparedPlanCache {
        let metrics =
            crate::metrics::Metrics::register_into(&mz_ore::metrics::MetricsRegistry::new());
        PreparedPlanCache::new(metrics.prepared.cache)
    }

    #[mz_ore::test]
    fn charges_follow_executions_past_cache_clear() {
        let mut cache = cache();
        let charge = cache
            .reserve(MAX_CHARGED_BYTES)
            .expect("test fixture must be valid");
        cache.clear();
        assert_eq!(cache.usage.bytes.load(Ordering::Relaxed), MAX_CHARGED_BYTES);
        assert_none!(cache.reserve(1));
        drop(charge);
        assert_eq!(cache.usage.bytes.load(Ordering::Relaxed), 0);
        assert_eq!(cache.usage.entries.load(Ordering::Relaxed), 0);
        assert_eq!(cache.usage.metrics.estimated_bytes.get(), 0);
        assert_eq!(cache.usage.metrics.compiled_entries.get(), 0);
        assert!(cache.reserve(MAX_CHARGED_BYTES).is_some());
        assert_none!(cache.reserve(MAX_CHARGED_BYTES + 1));
    }

    #[mz_ore::test]
    fn pinned_entry_limit_can_recover_after_release() {
        let mut cache = cache();
        let mut charges: Vec<_> = (0..MAX_ENTRIES)
            .map(|_| cache.reserve(1).expect("test fixture must be valid"))
            .collect();
        assert_none!(cache.reserve(1));
        drop(charges.pop());
        let charge = cache.reserve(1).expect("test fixture must be valid");
        assert_eq!(cache.usage.entries.load(Ordering::Relaxed), MAX_ENTRIES);
        drop(charges);
        assert_eq!(cache.usage.bytes.load(Ordering::Relaxed), 1);
        drop(charge);
        assert_eq!(cache.usage.entries.load(Ordering::Relaxed), 0);
    }

    #[mz_ore::test(tokio::test)]
    async fn lru_eviction_preserves_active_programs_and_context_keys() {
        Catalog::with_debug(|catalog| async move {
            let mut session = Session::dummy();
            let conn_catalog = catalog.for_system_session();
            let make_query = || {
                let stmt =
                    mz_sql::parse::parse("SELECT id FROM mz_catalog.mz_tables WHERE id = $1")
                        .expect("test fixture must be valid")
                        .into_element()
                        .ast;
                let (stmt, ids) = mz_sql::names::resolve(&conn_catalog, stmt)
                    .expect("test fixture must be valid");
                let analysis = mz_sql::plan::describe_analyzed(
                    &mz_sql::plan::PlanContext::zero(),
                    &conn_catalog,
                    stmt,
                    &[],
                )
                .expect("test fixture must be valid");
                Arc::new(PreparedQuery::new(
                    &catalog,
                    &session,
                    analysis.select.expect("test fixture must be valid"),
                    ids,
                    analysis.sql_impl_ids,
                    analysis.desc.param_types,
                ))
            };
            let mut cache = cache();
            let config = OptimizerConfig::from(catalog.system_config());
            let cluster = ClusterId::User(1);
            let make_program = |cache: &mut PreparedPlanCache, query: &PreparedQuery| {
                let template = LinearQueryTemplate::compile(
                    query.select.source(),
                    &query.parameter_types,
                    (&config).into(),
                )
                .expect("test fixture must be valid")
                .expect("test fixture must be valid")
                .for_index(GlobalId::User(1), &[mz_expr::MirScalarExpr::column(0)])
                .expect("test fixture must be valid");
                let finishing = FinishingTemplate::compile(
                    query.select.finishing(),
                    query.parameter_types.len(),
                )
                .expect("test fixture must be valid")
                .expect("test fixture must be valid");
                let types = query
                    .parameter_types
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(i, typ)| (i + 1, typ))
                    .collect();
                Arc::new(PreparedExecution {
                    template,
                    finishing,
                    typ: query.select.source().typ(&[], &types),
                    cluster,
                    config: config.clone(),
                    _charge: cache
                        .reserve(MAX_CHARGED_BYTES / 2)
                        .expect("test fixture must be valid"),
                })
            };
            let first = make_query();
            let second = make_query();
            let third = make_query();
            for query in [&first, &second] {
                let program = make_program(&mut cache, query);
                cache.insert(query, cluster, config.clone(), Some(program));
            }
            assert!(
                cache
                    .get(&first, cluster, &config)
                    .expect("test fixture must be valid")
                    .is_some()
            );
            let program = make_program(&mut cache, &third);
            cache.insert(&third, cluster, config.clone(), Some(program));
            assert_none!(cache.get(&second, cluster, &config));
            assert_eq!(cache.usage.metrics.evictions.get(), 1);
            let pinned = cache
                .get(&first, cluster, &config)
                .expect("test fixture must be valid")
                .expect("test fixture must be valid");
            assert_none!(cache.get(&first, ClusterId::User(2), &config));
            let mut changed = config.clone();
            changed.no_fast_path = true;
            assert_none!(cache.get(&first, cluster, &changed));
            assert!(Arc::ptr_eq(
                &pinned,
                &cache
                    .get(&first, cluster, &config)
                    .expect("test fixture must be valid")
                    .expect("test fixture must be valid"),
            ));

            cache.clear();
            assert_eq!(cache.usage.metrics.compiled_entries.get(), 1);
            assert_eq!(
                cache.usage.metrics.estimated_bytes.get(),
                u64::cast_from(MAX_CHARGED_BYTES / 2)
            );
            assert_none!(cache.reserve(MAX_CHARGED_BYTES));
            drop(pinned);
            assert_eq!(cache.usage.metrics.compiled_entries.get(), 0);
            assert_eq!(cache.usage.metrics.estimated_bytes.get(), 0);

            cache.insert(&second, cluster, config, None);
            drop(second);
            cache.prune();
            assert!(cache.entries.is_empty());
            assert_eq!(cache.usage.metrics.orphaned.get(), 1);
            // SQL SET ROLE is not implemented, but retained analysis must guard
            // every role component if the execution context changes internally.
            let original_roles = session.role_metadata.clone();
            for component in 0..3 {
                session.role_metadata = original_roles.clone();
                let roles = session
                    .role_metadata
                    .as_mut()
                    .expect("test fixture must be valid");
                let changed_role = mz_repr::role_id::RoleId::User(42);
                match component {
                    0 => roles.authenticated_role = changed_role,
                    1 => roles.session_role = changed_role,
                    2 => roles.current_role = changed_role,
                    _ => unreachable!(),
                }
                assert!(matches!(
                    first.invalidation_reason(&catalog, &session),
                    Some(super::super::PreparedInvalidation::Roles),
                ));
            }
            drop(conn_catalog);
            catalog.expire().await;
        })
        .await;
    }
}
