// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Writer admission for replica-owned curated sinks. No controller effects run here.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use mz_adapter_types::connection::ConnectionId;
use mz_catalog::durable::objects::ReplicaPlanOwner;
use mz_catalog::expr_cache::GlobalExpressions;
use mz_cluster_client::ReplicaId;
use mz_controller_types::ClusterId;
use mz_ore::cast::CastFrom;
use mz_repr::optimize::OverrideFrom;
use mz_repr::{GlobalId, RelationVersion, Timestamp};
use mz_sql::catalog::SessionCatalog;
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_sql::plan::validate_metric_sink_prefix;
use mz_sql::session::vars::ENABLE_METRIC_SINK;

use super::{CURATED, CuratedMetricSink, ensure_reads_only_logs};
use crate::AdapterError;
use crate::catalog::{Catalog, CatalogState, Op};
use crate::coord::Coordinator;
use crate::optimize::dataflows::ComputeInstanceSnapshot;
use crate::optimize::{self, Optimize, OptimizerConfig};

impl Coordinator {
    pub(in crate::coord) fn replica_owned_metric_sinks(&self) -> bool {
        self.controller.replica_owned_compute()
            && self.catalog().state().catalog_read_protection_enabled()
    }

    /// Admit only replicas created by this transaction. A flag change alone must
    /// neither backfill existing replicas nor withdraw their selections.
    pub(in crate::coord) async fn prepare_replica_metric_sinks(
        &self,
        conn_id: Option<&ConnectionId>,
        ops: &mut Vec<Op>,
        write_ts: Timestamp,
    ) -> Result<(), AdapterError> {
        if !self.replica_owned_metric_sinks() || self.read_only_controllers {
            return Ok(());
        }
        let replicas: BTreeSet<_> = ops
            .iter()
            .filter_map(|op| match op {
                Op::CreateClusterReplica { replica_id, .. } => Some(*replica_id),
                _ => None,
            })
            .collect();
        if replicas.is_empty() {
            return Ok(());
        }
        let revision = self.catalog().transient_revision();
        let conn = conn_id.map(|id| self.active_conns.get(id).expect("connection exists"));
        let (candidate, _) = self
            .catalog()
            .transact_incremental_dry_run(self.catalog().state(), ops.clone(), conn, None, write_ts)
            .await?;
        let selections = prepare_selections(
            self.catalog(),
            Arc::new(candidate),
            Some(&replicas),
            write_ts,
            self.optimizer_metrics(),
        )
        .await?;
        if self.catalog().transient_revision() != revision {
            return Err(AdapterError::DDLTransactionRace);
        }
        ops.extend(selections);
        Ok(())
    }

    /// Bootstrap reconciles all replicas under the current flag, before replica
    /// effects are installed. Savepoints cannot admit durable work.
    pub(in crate::coord) async fn bootstrap_replica_metric_sink_selections(
        &mut self,
    ) -> Result<Vec<Op>, AdapterError> {
        if !self.replica_owned_metric_sinks() || self.read_only_controllers {
            return Ok(Vec::new());
        }
        let revision = self.catalog().transient_revision();
        let write_ts = self.get_catalog_write_ts().await;
        let selections = prepare_selections(
            self.catalog(),
            Arc::new(self.catalog().state().clone()),
            None,
            write_ts,
            self.optimizer_metrics(),
        )
        .await?;
        if self.catalog().transient_revision() != revision {
            return Err(AdapterError::DDLTransactionRace);
        }
        Ok(selections)
    }
}

/// `replicas == None` denotes bootstrap. Otherwise only those new replicas are
/// eligible, including replicas of clusters born in the same transaction.
async fn prepare_selections(
    catalog: &Catalog,
    candidate: Arc<CatalogState>,
    replicas: Option<&BTreeSet<ReplicaId>>,
    write_ts: Timestamp,
    metrics: OptimizerMetrics,
) -> Result<Vec<Op>, AdapterError> {
    {
        let storage = catalog.storage().await;
        if storage.is_savepoint() || storage.is_read_only() {
            return Ok(Vec::new());
        }
    }
    let build = Catalog::expression_build_version(candidate.config().build_info).to_string();
    let enabled = ENABLE_METRIC_SINK.enabled(candidate.system_config());
    let owned: BTreeMap<_, _> = candidate
        .written_plans()
        .iter()
        .filter(|((_, version), selection)| version == &build && selection.replica_owner.is_some())
        .map(|((id, _), selection)| (*id, selection))
        .collect();
    if !enabled {
        return Ok(if replicas.is_none() {
            owned
                .into_iter()
                .map(|(id, selection)| Op::SetWrittenPlan {
                    id,
                    build_version: build.clone(),
                    expected_revision: Some(selection.revision),
                    revision: None,
                    imports: BTreeSet::new(),
                    replica_owner: selection.replica_owner.clone(),
                })
                .collect()
        } else {
            Vec::new()
        });
    }
    let existing: BTreeSet<_> = owned
        .values()
        .map(|selection| {
            let owner = selection
                .replica_owner
                .as_ref()
                .expect("filtered owned selection");
            (owner.replica_id, owner.name.as_str())
        })
        .collect();
    let mut missing = Vec::new();
    for replica in candidate.for_system_session().get_cluster_replicas() {
        let replica_id = replica.replica_id();
        if replicas.is_some_and(|replicas| !replicas.contains(&replica_id)) {
            continue;
        }
        for definition in CURATED {
            if !existing.contains(&(replica_id, definition.name)) {
                missing.push((replica.cluster_id(), replica_id, definition));
            }
        }
    }
    if missing.is_empty() {
        return Ok(Vec::new());
    }
    // The durable user allocator is the high-water mark, even after every
    // selection has been dropped. These IDs are never SQL catalog items.
    // Allocate directly, without entering coordinator DDL to refill a pool.
    let mut ids = catalog
        .allocate_user_ids(u64::cast_from(missing.len()) * 2, write_ts)
        .await?
        .into_iter()
        .map(|(_, id)| match id {
            GlobalId::User(n) => GlobalId::Transient(n),
            _ => unreachable!("user allocator returns user IDs"),
        });
    let work: Vec<_> = missing
        .into_iter()
        .map(|(cluster, replica, definition)| {
            (
                cluster,
                replica,
                definition,
                ids.next().expect("export ID"),
                ids.next().expect("view ID"),
            )
        })
        .collect();
    let (plans, owners) = mz_ore::task::spawn_blocking(
        || "plan replica metric sinks",
        move || {
            let mut plans = BTreeMap::new();
            let mut owners = BTreeMap::new();
            for (cluster, replica, definition, export, view) in work {
                plans.insert(
                    export,
                    plan_definition(
                        Arc::clone(&candidate),
                        cluster,
                        replica,
                        definition,
                        export,
                        view,
                        metrics.clone(),
                    )?,
                );
                owners.insert(
                    export,
                    ReplicaPlanOwner {
                        replica_id: replica,
                        name: definition.name.into(),
                    },
                );
            }
            Ok::<_, AdapterError>((plans, owners))
        },
    )
    .await?;
    // Immutable bytes must be durable before their selections can be committed.
    let mut selections = catalog.write_plans(plans).await?;
    for op in &mut selections {
        if let Op::SetWrittenPlan {
            id, replica_owner, ..
        } = op
        {
            *replica_owner = Some(owners[id].clone());
        }
    }
    Ok(selections)
}

fn plan_definition(
    catalog: Arc<CatalogState>,
    cluster_id: ClusterId,
    replica_id: ReplicaId,
    definition: &CuratedMetricSink,
    export: GlobalId,
    view: GlobalId,
    metrics: OptimizerMetrics,
) -> Result<GlobalExpressions, AdapterError> {
    validate_metric_sink_prefix(definition.prefix)?;
    let (expr, desc, dependencies) = definition.plan_source(&catalog.for_system_session())?;
    ensure_reads_only_logs(&catalog, &dependencies)?;
    let cluster = catalog.get_cluster(cluster_id);
    let logs: BTreeSet<_> = cluster.log_indexes.values().copied().collect();
    let config = OptimizerConfig::from(catalog.system_config())
        .override_from(&cluster.config.features())
        .override_from(&catalog.cluster_scoped_optimizer_overrides(cluster_id));
    let mut optimizer = optimize::metric_sink::Optimizer::new(
        catalog,
        ComputeInstanceSnapshot::new_from_parts(cluster_id, logs.clone()),
        view,
        export,
        config.clone(),
        metrics,
    );
    let mir = optimizer.catch_unwind_optimize(optimize::metric_sink::MetricSink::new(
        format!("metric-sink-{}-{replica_id}", definition.name),
        optimize::metric_sink::MetricSinkFrom::Query { expr, desc },
        definition.prefix.into(),
        Some(definition.name.into()),
    ))?;
    let global_mir = mir.df_desc().clone();
    let (physical_plan, metainfo) = optimizer.catch_unwind_optimize(mir)?.unapply();
    let plan = GlobalExpressions {
        global_mir,
        physical_plan,
        // Curated sinks have no SQL item to own optimizer notices.
        dataflow_metainfos: mz_transform::dataflow::DataflowMetainfo {
            optimizer_notices: Vec::new(),
            index_usage_types: metainfo.index_usage_types,
        },
        optimizer_features: config.features,
        item_version: RelationVersion::root(),
    };
    if !plan.global_mir.source_imports.is_empty()
        || !plan.physical_plan.source_imports.is_empty()
        || plan.collection_imports().any(|id| !logs.contains(id))
    {
        return Err(AdapterError::internal(
            "plan replica metric sinks",
            "plan imports non-local logs",
        ));
    }
    Ok(plan)
}

#[cfg(test)]
mod tests;
