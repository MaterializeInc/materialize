// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Index and metric-sink commands validate semantic and physical inputs at commit.

use std::sync::Arc;

use maplit::btreemap;
use mz_catalog::expr_cache::GlobalExpressions;
use mz_catalog::memory::objects::{CatalogItem, Index, MetricSink};
use mz_compute_client::controller::ComputeController;
use mz_controller_types::ClusterId;
use mz_expr::MirScalarExpr;
use mz_repr::explain::{ExprHumanizerExt, TransientItem};
use mz_repr::global_id::TransientIdGen;
use mz_repr::optimize::OverrideFrom;
use mz_repr::{CatalogItemId, GlobalId, RelationVersion};
use mz_sql::ast::{Raw, Statement};
use mz_sql::names::{FullItemName, ResolvedIds};
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_sql::plan::{Params, Plan, PlanNotice};
use mz_transform::notice::RawOptimizerNotice;

use crate::AdapterError;
use crate::catalog::{Catalog, CatalogState, Op};
use crate::coord::id_bundle::CollectionIdBundle;
use crate::optimize::dataflows::{
    ComputeInstanceSnapshot, DataflowBuilder, dataflow_import_id_bundle,
};
use crate::optimize::{Optimize, OptimizerConfig};
use crate::session::SessionMeta;

use super::{CommandExecution, CommandExecutionError, CommandWork};

#[derive(Clone, Debug)]
pub struct ComputeStatement {
    statement: Arc<Statement<Raw>>,
    params: Params,
    session: Arc<SessionMeta>,
    create_id: Option<(CatalogItemId, GlobalId)>,
    prepared: Option<(ComputePreparation, PreparedCompute)>,
    notices: Vec<PlanNotice>,
    skipped_name: Option<String>,
}

pub(crate) struct ComputeCompilation {
    pub plan: Plan,
    resolved_ids: ResolvedIds,
    notices: Vec<PlanNotice>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ComputePreparation {
    global_id: GlobalId,
    on: GlobalId,
    definition: ComputeDefinition,
    cluster_id: ClusterId,
    name: FullItemName,
    config: OptimizerConfig,
    inputs: CollectionIdBundle,
}

#[derive(Clone, Debug, PartialEq)]
enum ComputeDefinition {
    Index { keys: Vec<MirScalarExpr> },
    MetricSink { prefix: String },
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedCompute {
    expressions: GlobalExpressions,
    notices: Vec<RawOptimizerNotice>,
}

#[derive(Debug)]
pub(crate) struct ComputePreparationResponse {
    request: ComputePreparation,
    result: Result<PreparedCompute, AdapterError>,
}

#[derive(Debug)]
pub(crate) struct ComputeWork {
    request: ComputePreparation,
    catalog: Arc<CatalogState>,
    compute: ComputeInstanceSnapshot,
    name: mz_sql::names::QualifiedItemName,
    prepared: Option<PreparedCompute>,
}

impl ComputeStatement {
    pub(crate) fn new(
        statement: Arc<Statement<Raw>>,
        params: Params,
        session: SessionMeta,
    ) -> Self {
        Self {
            statement,
            params,
            session: Arc::new(session),
            create_id: None,
            prepared: None,
            notices: Vec::new(),
            skipped_name: None,
        }
    }

    pub(crate) fn compile_create(
        &self,
        state: &CatalogState,
    ) -> Result<ComputeCompilation, AdapterError> {
        let (plan, resolved_ids, notices) =
            super::plan_command(state, &self.statement, &self.params, &self.session)?;
        if !matches!(plan, Plan::CreateIndex(_) | Plan::CreateMetricSink(_)) {
            return Err(AdapterError::Unsupported("non-compute creation intent"));
        }
        Ok(ComputeCompilation {
            plan,
            resolved_ids,
            notices,
        })
    }

    pub(crate) fn allocate_create(&mut self, ids: (CatalogItemId, GlobalId)) {
        assert!(
            self.create_id.replace(ids).is_none(),
            "compute IDs allocated once"
        );
    }

    pub(super) fn compile(
        &mut self,
        state: &CatalogState,
        compute: &ComputeController,
        command_index: usize,
        response: Option<ComputePreparationResponse>,
    ) -> Result<CommandExecution<'_>, CommandExecutionError> {
        let compiled = self.compile_create(state)?;
        let (id, global_id) = self.create_id.expect("compute IDs allocated at admission");
        let (cluster_id, name, on, definition) = match &compiled.plan {
            Plan::CreateIndex(plan) => (
                plan.index.cluster_id,
                &plan.name,
                plan.index.on,
                ComputeDefinition::Index {
                    keys: plan.index.keys.clone(),
                },
            ),
            Plan::CreateMetricSink(plan) => {
                ensure_metric_sink_prefix_is_free(
                    state,
                    &plan.name,
                    plan.metric_sink.cluster_id,
                    &plan.metric_sink.prefix,
                )?;
                (
                    plan.metric_sink.cluster_id,
                    &plan.name,
                    plan.metric_sink.from,
                    ComputeDefinition::MetricSink {
                        prefix: plan.metric_sink.prefix.clone(),
                    },
                )
            }
            _ => unreachable!("compile_create checks the plan kind"),
        };
        let snapshot = ComputeInstanceSnapshot::new_from_parts(
            cluster_id,
            compute
                .collection_ids(cluster_id)
                .map_err(|error| AdapterError::Unstructured(error.into()))?
                .collect(),
        );
        let config = OptimizerConfig::from(state.system_config())
            .override_from(&state.get_cluster(cluster_id).config.features())
            .override_from(&state.cluster_scoped_optimizer_overrides(cluster_id));
        let request = ComputePreparation {
            global_id,
            on,
            definition,
            cluster_id,
            name: state.resolve_full_name(name, None),
            config,
            inputs: DataflowBuilder::new(state, snapshot.clone()).sufficient_collections([on]),
        };
        if let Some(response) = response {
            if response.request == request {
                self.prepared = Some((request.clone(), response.result?));
            }
        }
        let mut prepared = self
            .prepared
            .take()
            .filter(|(previous, _)| previous == &request);
        let refresh_cache = prepared
            .as_mut()
            .is_some_and(|(_, prepared)| prepared.refresh_notices(state, &request));
        if prepared.is_none() || refresh_cache {
            // Each new preparation or metadata refresh must update the durable cache
            // before publication. Do not retain an older artifact across that write.
            return Err(CommandExecutionError::Pending {
                index: command_index,
                work: CommandWork::Compute(Box::new(ComputeWork {
                    request,
                    catalog: Arc::new(state.clone()),
                    compute: snapshot,
                    name: name.clone(),
                    prepared: prepared.map(|(_, prepared)| prepared),
                })),
            });
        }
        self.prepared = prepared;
        let (_, prepared) = self.prepared.as_ref().expect("preparation checked above");
        let (name, if_not_exists, owner_id, item) = match compiled.plan {
            Plan::CreateIndex(plan) => {
                let index = plan.index;
                let owner_id = *state.get_entry_by_global_id(&index.on).owner_id();
                (
                    plan.name,
                    plan.if_not_exists,
                    owner_id,
                    CatalogItem::Index(Index {
                        create_sql: index.create_sql,
                        global_id,
                        keys: index.keys.into(),
                        on: index.on,
                        conn_id: None,
                        resolved_ids: compiled.resolved_ids,
                        cluster_id,
                        is_retained_metrics_object: false,
                        custom_logical_compaction_window: index.compaction_window,
                        optimized_plan: Some(Arc::new(prepared.expressions.global_mir.clone())),
                        physical_plan: Some(Arc::new(prepared.expressions.physical_plan.clone())),
                        dataflow_metainfo: Some(prepared.expressions.dataflow_metainfos.clone()),
                    }),
                )
            }
            Plan::CreateMetricSink(plan) => {
                let sink = plan.metric_sink;
                (
                    plan.name,
                    plan.if_not_exists,
                    *self.session.current_role_id(),
                    CatalogItem::MetricSink(MetricSink {
                        create_sql: sink.create_sql,
                        global_id,
                        from: sink.from,
                        resolved_ids: compiled.resolved_ids,
                        cluster_id,
                        prefix: sink.prefix,
                        optimized_plan: Some(Arc::new(prepared.expressions.global_mir.clone())),
                        physical_plan: Some(Arc::new(prepared.expressions.physical_plan.clone())),
                        dataflow_metainfo: Some(prepared.expressions.dataflow_metainfos.clone()),
                    }),
                )
            }
            _ => unreachable!("compile_create checks the plan kind"),
        };
        let ops = vec![Op::CreateItem {
            id,
            name: name.clone(),
            item,
            owner_id,
        }];
        use mz_sql::session::metadata::SessionMetadata;
        state.validate_resource_limits(&ops, self.session.conn_id())?;
        self.notices = compiled.notices;
        Ok(CommandExecution::create_item(
            ops,
            id,
            name.item,
            if_not_exists,
            &mut self.skipped_name,
        ))
    }

    pub(crate) fn object_type(&self) -> &'static str {
        match &*self.statement {
            Statement::CreateIndex(_) => "index",
            Statement::CreateMetricSink(_) => "metric sink",
            _ => unreachable!("validated compute statement"),
        }
    }

    pub(crate) fn notices(&self) -> &[PlanNotice] {
        &self.notices
    }
    pub(crate) fn skipped_name(&self) -> Option<&str> {
        self.skipped_name.as_deref()
    }
    pub(crate) fn optimizer_notices(&self) -> &[RawOptimizerNotice] {
        if self.skipped_name.is_some() {
            return &[];
        }
        self.prepared
            .as_ref()
            .map_or(&[], |(_, prepared)| &prepared.notices)
    }
}

impl PreparedCompute {
    fn refresh_notices(&mut self, state: &CatalogState, request: &ComputePreparation) -> bool {
        let metadata = &self.expressions.dataflow_metainfos;
        let Some(first) = metadata.optimizer_notices.first() else {
            return false;
        };
        let on = state.get_entry_by_global_id(&request.on);
        let desc = on
            .relation_desc()
            .expect("compute input has a relation description");
        let transient = btreemap! { request.global_id => TransientItem::new(
            Some(request.name.clone().into_parts()),
            Some(desc.iter_names().map(|c| c.to_string()).collect()),
        ) };
        let system = state.for_system_session();
        let humanizer = ExprHumanizerExt::new(transient, &system);
        let raw = mz_transform::dataflow::DataflowMetainfo {
            optimizer_notices: self.notices.clone(),
            index_usage_types: metadata.index_usage_types.clone(),
        };
        let rendered = CatalogState::render_notices_core(
            &humanizer,
            first.created_at,
            &raw,
            metadata
                .optimizer_notices
                .iter()
                .map(|notice| notice.id)
                .collect(),
            Some(request.global_id),
        );
        if &rendered == metadata {
            false
        } else {
            // Renames can change notice text without invalidating the physical plan.
            self.expressions.dataflow_metainfos = rendered;
            true
        }
    }
}

impl ComputeWork {
    pub(crate) fn phases(&self, repeated: bool) -> (&'static str, &'static str) {
        let metric_sink = matches!(
            self.request.definition,
            ComputeDefinition::MetricSink { .. }
        );
        let phase = match (metric_sink, self.prepared.is_some(), repeated) {
            (false, true, _) => "index_notice_refresh",
            (false, false, true) => "index_repreparation",
            (false, false, false) => "index_preparation",
            (true, true, _) => "metric_sink_notice_refresh",
            (true, false, true) => "metric_sink_repreparation",
            (true, false, false) => "metric_sink_preparation",
        };
        (
            phase,
            if metric_sink {
                "metric_sink_preparation_canceled"
            } else {
                "index_preparation_canceled"
            },
        )
    }

    pub(crate) fn execute(
        self,
        catalog: &Catalog,
        metrics: OptimizerMetrics,
        ids: Arc<TransientIdGen>,
    ) -> impl std::future::Future<Output = ComputePreparationResponse> + Send + use<> {
        let cache = catalog.expr_cache_handle.clone();
        let now = catalog.config().now.clone();
        async move {
            let request = self.request.clone();
            let metric_sink = matches!(request.definition, ComputeDefinition::MetricSink { .. });
            let span = tracing::Span::current();
            let result = mz_ore::task::spawn_blocking(|| "optimize compute command", move || {
                span.in_scope(|| {
                    if let Some(prepared) = self.prepared {
                        return Ok(prepared);
                    }
                    let optimize = || -> Result<PreparedCompute, AdapterError> {
                        let (global_mir, physical_plan, metadata) = match self.request.definition {
                            ComputeDefinition::Index { keys } => {
                                let mut optimizer = crate::optimize::index::Optimizer::new(
                                    Arc::<CatalogState>::clone(&self.catalog),
                                    self.compute, self.request.global_id,
                                    self.request.config.clone(), metrics,
                                );
                                let input = crate::optimize::index::Index::new(
                                    self.name, self.request.on, keys,
                                );
                                let mir = optimizer.catch_unwind_optimize(input)?;
                                let lir = optimizer.catch_unwind_optimize(mir.clone())?;
                                let (physical, metadata) = lir.unapply();
                                (mir.df_desc().clone(), physical, metadata)
                            }
                            ComputeDefinition::MetricSink { prefix } => {
                                let mut optimizer = crate::optimize::metric_sink::Optimizer::new(
                                    Arc::<CatalogState>::clone(&self.catalog),
                                    self.compute, ids.allocate_id().1,
                                    self.request.global_id, self.request.config.clone(), metrics,
                                );
                                let input = crate::optimize::metric_sink::MetricSink::new(
                                    self.request.name.to_string(),
                                    crate::optimize::metric_sink::MetricSinkFrom::Id(
                                        self.request.on,
                                    ),
                                    prefix, None,
                                );
                                let mir = optimizer.catch_unwind_optimize(input)?;
                                let lir = optimizer.catch_unwind_optimize(mir.clone())?;
                                let (physical, metadata) = lir.unapply();
                                (mir.df_desc().clone(), physical, metadata)
                            }
                        };
                        let imports = dataflow_import_id_bundle(
                            &physical_plan, self.request.cluster_id,
                        );
                        if !imports.difference(&self.request.inputs).is_empty() {
                            return Err(AdapterError::Internal("compute plan imports unvalidated collections".into()));
                        }
                        let on = self.catalog.get_entry_by_global_id(&self.request.on);
                        let desc = on.relation_desc().expect("compute input has a relation description");
                        let transient = btreemap! { self.request.global_id => TransientItem::new(
                            Some(self.request.name.into_parts()),
                            Some(desc.iter_names().map(|c| c.to_string()).collect()),
                        ) };
                        let system = self.catalog.for_system_session();
                        let humanizer = ExprHumanizerExt::new(transient, &system);
                        let notice_ids = metadata.optimizer_notices.iter()
                            .map(|_| ids.allocate_id().1).collect();
                        let rendered = CatalogState::render_notices_core(
                            &humanizer, now(), &metadata, notice_ids,
                            Some(self.request.global_id),
                        );
                        Ok(PreparedCompute {
                            expressions: GlobalExpressions {
                                global_mir,
                                physical_plan,
                                dataflow_metainfos: rendered,
                                optimizer_features: self.request.config.features,
                                item_version: RelationVersion::root(),
                            },
                            notices: metadata.optimizer_notices,
                        })
                    };
                    let result = optimize();
                    if metric_sink { fail::fail_point!("catalog_metric_sink_optimize_ready"); } else { fail::fail_point!("catalog_index_optimize_ready"); }
                    result
                })
            }).await;
            if let (Some(cache), Ok(prepared)) = (cache, &result) {
                cache
                    .update(
                        Vec::new(),
                        vec![(request.global_id, prepared.expressions.clone())],
                        Default::default(),
                    )
                    .await;
                if metric_sink {
                    fail::fail_point!("catalog_metric_sink_expression_cached");
                } else {
                    fail::fail_point!("catalog_index_expression_cached");
                }
            }
            ComputePreparationResponse { request, result }
        }
    }
}

fn ensure_metric_sink_prefix_is_free(
    state: &CatalogState,
    name: &mz_sql::names::QualifiedItemName,
    cluster_id: ClusterId,
    prefix: &str,
) -> Result<(), AdapterError> {
    let cluster = state.get_cluster(cluster_id);
    for item_id in &cluster.bound_objects {
        let entry = state.get_entry(item_id);
        let CatalogItem::MetricSink(existing) = entry.item() else {
            continue;
        };
        if entry.name() == name {
            continue;
        }
        if existing.prefix.starts_with(prefix) || prefix.starts_with(&existing.prefix) {
            return Err(AdapterError::Unstructured(anyhow::anyhow!(
                "metric sink prefix {:?} conflicts with prefix {:?} of metric sink {} on \
                     cluster {}",
                prefix,
                existing.prefix,
                entry.name().item,
                cluster.name,
            )));
        }
    }
    Ok(())
}
