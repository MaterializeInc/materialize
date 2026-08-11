// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Coordinator-installed metric sinks, the curated counterpart to `CREATE METRIC SINK`.
//!
//! A curated metric sink is a [`CURATED`] entry rendered on every replica, publishing its series
//! into that replica's process-local Prometheus registry. Unlike a user's `CREATE METRIC SINK` it
//! is not a catalog item: it gets a transient [`GlobalId`], targets one replica rather than a
//! cluster, and is re-created from the static list on every boot. Modelling the curated set this
//! way keeps it out of the catalog, so adding or removing a definition needs no builtin migration.
//!
//! # Lifecycle
//!
//! * After a new replica is created, the coordinator calls `install_metric_sinks` to install every
//!   definition on it. `bootstrap_metric_sinks` does the same for the replicas that already exist
//!   when the coordinator starts.
//! * Before a replica is dropped, the coordinator calls `drop_metric_sinks` to drop the sinks
//!   installed on it.
//! * A replica that disconnects and reconnects (a crash, an OOM) has its dataflows re-rendered from
//!   the controller's state, so unlike an introspection subscribe there is nothing to reinstall.
//!
//! This mirrors [`crate::coord::introspection`], which installs introspection subscribes on the
//! same triggers.

use std::collections::BTreeSet;

use anyhow::{anyhow, bail};
use mz_cluster_client::ReplicaId;
use mz_controller_types::ClusterId;
use mz_ore::collections::CollectionExt;
use mz_ore::{instrument, soft_panic_or_log};
use mz_repr::optimize::OverrideFrom;
use mz_repr::{CatalogItemId, GlobalId, RelationDesc};
use mz_sql::catalog::SessionCatalog;
use mz_sql::plan::{
    HirRelationExpr, Params, Plan, PlanContext, SelectPlan, validate_metric_sink_desc,
};
use mz_sql::session::user::{MZ_SYSTEM_ROLE_ID, RoleMetadata};
use mz_sql::session::vars::ENABLE_METRIC_SINK;
use tracing::{Span, info};

use crate::coord::{
    Coordinator, Message, MetricSinkFinish, MetricSinkOptimize, MetricSinkStage, PlanValidity,
    StageResult, Staged,
};
use crate::optimize::Optimize;
use crate::optimize::dataflows::dataflow_import_id_bundle;
use crate::{AdapterError, ExecuteResponse, optimize};

/// A curated metric sink: SQL producing the canonical metric-sink columns, plus the name it is
/// known by in logs.
#[derive(Debug)]
pub(super) struct CuratedMetricSink {
    /// Identifies the definition in logs, in [`Coordinator::metric_sinks`], in the assembled
    /// dataflow's debug name, and as the `sink` label on the operator's health gauges (the sink's
    /// `GlobalId` is transient, so the name is what stays stable across boots). Must be unique
    /// within [`CURATED`].
    name: &'static str,
    /// A `SELECT` producing the canonical metric-sink columns (`metric_name`, `metric_type`,
    /// `labels`, `value`, `help`), the contract `mz_sql::plan::validate_metric_sink_desc` checks.
    ///
    /// The query must read only introspection relations. A catalog-backed relation would put
    /// envd's write frontier on the sink's emission path, which is exactly the coupling these
    /// sinks exist to avoid: the sink would stall whenever envd did, taking the freshness signal
    /// with it.
    source_sql: &'static str,
}

/// The curated metric sinks, installed on every replica.
const CURATED: &[CuratedMetricSink] = &[];

/// A [`CuratedMetricSink`] installed on one replica.
#[derive(Debug)]
pub(super) struct InstalledMetricSink {
    /// The cluster the replica belongs to, needed to drop the sink's compute collection.
    cluster_id: ClusterId,
    /// The transient id of the sink's compute export.
    sink_id: GlobalId,
}

impl Coordinator {
    /// Installs the curated metric sinks on all existing replicas.
    ///
    /// Meant to be invoked during coordinator bootstrapping.
    pub(super) async fn bootstrap_metric_sinks(&mut self) {
        let mut cluster_replicas = Vec::new();
        for cluster in self.catalog.clusters() {
            for replica in cluster.replicas() {
                cluster_replicas.push((cluster.id, replica.replica_id));
            }
        }

        for (cluster_id, replica_id) in cluster_replicas {
            self.install_metric_sinks(cluster_id, replica_id).await;
        }
    }

    /// Installs the curated metric sinks on the given replica.
    ///
    /// Turning `enable_metric_sink` off stops installing on replicas created from then on. It does
    /// not tear down what is already installed: those keep running until their replica is dropped
    /// or envd restarts. A replica that merely reconnects re-renders them from the controller's
    /// command history, so a replica restart does not clear them either.
    pub(super) async fn install_metric_sinks(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) {
        if !ENABLE_METRIC_SINK.enabled(self.catalog().system_config()) {
            return;
        }

        // TODO: Skip replicas created with introspection disabled. Their logging dataflows never
        // run, so the introspection relations every `source_sql` reads stay empty there.
        for definition in CURATED {
            self.install_metric_sink(cluster_id, replica_id, definition)
                .await;
        }
    }

    async fn install_metric_sink(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        definition: &'static CuratedMetricSink,
    ) {
        let (_, sink_id) = self.allocate_transient_id();
        info!(%sink_id, %replica_id, name = definition.name, "installing metric sink");

        let catalog = self.catalog().for_system_session();
        let (expr, desc, dependencies) = match definition.plan_source(&catalog) {
            Ok(planned) => planned,
            Err(err) => {
                // The curated SQL is ours, so this is a bug in a definition rather than something
                // an operator can fix. Give up on this one sink instead of failing the boot.
                soft_panic_or_log!(
                    "invalid curated metric sink (name={}): {err}",
                    definition.name
                );
                return;
            }
        };

        let validity = PlanValidity::new(
            &self.catalog,
            dependencies,
            Some(cluster_id),
            Some(replica_id),
            RoleMetadata::new(MZ_SYSTEM_ROLE_ID),
        );
        let stage = MetricSinkStage::Optimize(MetricSinkOptimize {
            validity,
            definition,
            sink_id,
            expr,
            desc,
            cluster_id,
            replica_id,
        });
        self.sequence_staged((), Span::current(), stage).await;
    }

    #[instrument]
    fn metric_sink_optimize(
        &self,
        stage: MetricSinkOptimize,
    ) -> Result<StageResult<Box<MetricSinkStage>>, AdapterError> {
        let MetricSinkOptimize {
            validity,
            definition,
            sink_id,
            expr,
            desc,
            cluster_id,
            replica_id,
        } = stage;

        let compute_instance = self
            .instance_snapshot(cluster_id)
            .expect("compute instance exists");
        // A transient id for the view the optimizer builds to shape the source rows, scoped to this
        // dataflow. See `optimize::metric_sink::shape_metric_sink_source`.
        let (_, view_id) = self.allocate_transient_id();

        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config())
            .override_from(&self.catalog.get_cluster(cluster_id).config.features())
            .override_from(&self.cluster_scoped_optimizer_overrides(cluster_id));

        let mut optimizer = optimize::metric_sink::Optimizer::new(
            self.owned_catalog(),
            compute_instance,
            view_id,
            sink_id,
            optimizer_config,
            self.optimizer_metrics(),
        );

        let span = Span::current();
        Ok(StageResult::Handle(mz_ore::task::spawn_blocking(
            || "optimize metric sink",
            move || {
                span.in_scope(|| {
                    let metric_sink = optimize::metric_sink::MetricSink::new(
                        format!("metric-sink-{}-{replica_id}", definition.name),
                        optimize::metric_sink::MetricSinkFrom::Query { expr, desc },
                        Some(definition.name.to_string()),
                    );

                    // Both steps run inside one closure so either failure hits the same log.
                    // `sequence_staged` has no session to report to for a coordinator-driven
                    // install, so an error would otherwise vanish.
                    let global_lir_plan = (|| {
                        // MIR ⇒ MIR optimization (global)
                        let global_mir_plan = optimizer.catch_unwind_optimize(metric_sink)?;
                        // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
                        optimizer.catch_unwind_optimize(global_mir_plan)
                    })()
                    .inspect_err(|err| {
                        soft_panic_or_log!(
                            "curated metric sink failed to optimize (name={}): {err}",
                            definition.name
                        )
                    })?;

                    let stage = MetricSinkStage::Finish(MetricSinkFinish {
                        validity,
                        definition,
                        sink_id,
                        global_lir_plan,
                        cluster_id,
                        replica_id,
                    });
                    Ok(Box::new(stage))
                })
            },
        )))
    }

    #[instrument]
    async fn metric_sink_finish(
        &mut self,
        stage: MetricSinkFinish,
    ) -> Result<StageResult<Box<MetricSinkStage>>, AdapterError> {
        let MetricSinkFinish {
            validity: _,
            definition,
            sink_id,
            global_lir_plan,
            cluster_id,
            replica_id,
        } = stage;

        // `sequence_staged` rechecked validity before this stage ran, so the replica still exists.
        // The coordinator handles one message at a time, so no replica drop runs between that check
        // and the ship below.

        // The metainfo is dropped rather than persisted: a curated sink is not a catalog item, so
        // there is nothing for `mz_optimizer_notices` to hang its notices off.
        let (mut df_desc, _df_meta) = global_lir_plan.unapply();

        // Hold a read on the imports across shipping, so their since cannot advance past the as-of
        // just picked. Compute takes its own holds during `create_dataflow`.
        let id_bundle = dataflow_import_id_bundle(&df_desc, cluster_id);
        let read_holds = self.acquire_read_holds(&id_bundle);
        df_desc.set_as_of(read_holds.least_valid_read());

        // Record the install now that its dataflow is about to exist, so a definition that fails to
        // plan or optimize leaves nothing behind. `drop_metric_sinks` uses this entry to release
        // the sink's instance-global collection state when the replica is dropped.
        let install = InstalledMetricSink {
            cluster_id,
            sink_id,
        };
        if self
            .metric_sinks
            .insert((replica_id, definition.name), install)
            .is_some()
        {
            soft_panic_or_log!(
                "metric sink installed twice (name={}, replica_id={replica_id})",
                definition.name
            );
        }

        self.ship_dataflow(df_desc, cluster_id, Some(replica_id))
            .await;

        drop(read_holds);
        // Nobody is waiting on this: `StagedContext for ()` drops the result. Reuses the
        // `CREATE METRIC SINK` response rather than adding a variant no client ever sees.
        Ok(StageResult::Response(ExecuteResponse::CreatedMetricSink))
    }

    /// Drops the curated metric sinks installed on the given replica.
    ///
    /// Called before the replica itself is dropped. Dropping the replica would tear the sink
    /// dataflows down anyway, but the controller's collection state for them is instance-global,
    /// so it has to be released explicitly.
    pub(super) fn drop_metric_sinks(&mut self, replica_id: ReplicaId) {
        let to_drop: Vec<_> = self
            .metric_sinks
            .range((replica_id, "")..)
            .take_while(|((id, _), _)| *id == replica_id)
            .map(|((_, name), install)| (*name, install.cluster_id, install.sink_id))
            .collect();

        for (name, cluster_id, sink_id) in to_drop {
            info!(%sink_id, %replica_id, name, "dropping metric sink");
            self.metric_sinks.remove(&(replica_id, name));

            // An entry exists only for a sink whose dataflow was shipped, so its collection
            // exists. The result is ignored defensively, in case the controller already released
            // it.
            let _ = self
                .controller
                .compute
                .drop_collections(cluster_id, vec![sink_id]);
        }
    }
}

impl CuratedMetricSink {
    /// Plans `source_sql` against a session-less catalog, returning the query, its output shape,
    /// and the catalog items it reads.
    fn plan_source(
        &self,
        catalog: &dyn SessionCatalog,
    ) -> Result<(HirRelationExpr, RelationDesc, BTreeSet<CatalogItemId>), anyhow::Error> {
        let parsed = mz_sql::parse::parse(self.source_sql)?.into_element();
        let (stmt, resolved_ids) = mz_sql::names::resolve(catalog, parsed.ast)?;

        let pcx = PlanContext::zero();
        let desc = mz_sql::plan::describe(&pcx, catalog, stmt.clone(), &[])?
            .relation_desc
            .ok_or_else(|| anyhow!("source SQL does not return rows"))?;
        validate_metric_sink_desc(&desc)?;

        let (plan, _sql_impl_ids) =
            mz_sql::plan::plan(Some(&pcx), catalog, stmt, &Params::empty(), &resolved_ids)?;
        let Plan::Select(SelectPlan {
            source, finishing, ..
        }) = plan
        else {
            bail!("source SQL is not a SELECT: {plan:?}");
        };
        // The sink consumes the whole collection continuously, so there is no row set for a
        // finishing to order or limit. Dropping one silently would also desync `desc` from `source`,
        // since a finishing's `project` reorders the output columns and the shaping resolves the
        // canonical columns by index into `desc`.
        if !finishing.is_trivial(desc.arity()) {
            bail!("source SQL must not use ORDER BY, LIMIT, or OFFSET");
        }

        let dependencies = resolved_ids.items().copied().collect();
        Ok((source, desc, dependencies))
    }
}

impl Staged for MetricSinkStage {
    type Ctx = ();

    fn validity(&mut self) -> &mut PlanValidity {
        match self {
            Self::Optimize(stage) => &mut stage.validity,
            Self::Finish(stage) => &mut stage.validity,
        }
    }

    async fn stage(
        self,
        coord: &mut Coordinator,
        _ctx: &mut (),
    ) -> Result<StageResult<Box<Self>>, AdapterError> {
        match self {
            Self::Optimize(stage) => coord.metric_sink_optimize(stage),
            Self::Finish(stage) => coord.metric_sink_finish(stage).await,
        }
    }

    fn message(self, _ctx: (), span: Span) -> Message {
        Message::MetricSinkStageReady { span, stage: self }
    }

    fn cancel_enabled(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::Catalog;
    use crate::coord::metric_sink::CuratedMetricSink;

    /// The five canonical columns, no finishing: the shape a definition must produce.
    const VALID_SOURCE: &str = "SELECT 'n'::text AS metric_name, 'gauge'::text AS metric_type, \
        NULL::map[text=>text] AS labels, NULL::double AS value, 'h'::text AS help";

    /// `VALID_SOURCE` with a finishing appended, which `plan_source` must reject.
    const ORDERED_SOURCE: &str = "SELECT 'n'::text AS metric_name, 'gauge'::text AS metric_type, \
        NULL::map[text=>text] AS labels, NULL::double AS value, 'h'::text AS help ORDER BY 1";

    /// `plan_source` accepts the canonical column contract and rejects the shapes the shaping and
    /// the operator cannot consume: missing columns, and a finishing that would desync `desc` from
    /// `source`.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
    async fn plan_source_enforces_the_metric_sink_contract() {
        Catalog::with_debug(|catalog| async move {
            let session_catalog = catalog.for_system_session();
            let plan = |source_sql: &'static str| {
                CuratedMetricSink {
                    name: "test",
                    source_sql,
                }
                .plan_source(&session_catalog)
            };

            assert!(plan(VALID_SOURCE).is_ok());

            // Missing the canonical columns: rejected by `validate_metric_sink_desc`.
            assert!(plan("SELECT 1 AS foo").is_err());

            // A finishing (ORDER BY/LIMIT/OFFSET) would desync `desc` from `source`.
            assert!(plan(ORDERED_SOURCE).is_err());
        })
        .await
    }
}
