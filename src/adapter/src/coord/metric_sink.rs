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
//! Every replica means every replica of every cluster, user clusters included. Each definition is
//! therefore a dataflow, with its arrangements, on customer compute, charged to that customer's
//! cluster, and the cost scales with `CURATED`. `coord::introspection` already accepts this for its
//! subscribes.
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

use std::collections::{BTreeMap, BTreeSet};

use anyhow::bail;
use mz_catalog::memory::objects::CatalogItem;
use mz_cluster_client::ReplicaId;
use mz_controller_types::ClusterId;
use mz_ore::collections::CollectionExt;
use mz_ore::{instrument, soft_panic_or_log};
use mz_repr::optimize::OverrideFrom;
use mz_repr::{CatalogItemId, GlobalId, RelationDesc};
use mz_sql::catalog::SessionCatalog;
use mz_sql::plan::{
    HirRelationExpr, Params, Plan, SubscribeFrom, SubscribePlan, validate_metric_sink_desc,
    validate_metric_sink_prefix,
};
use mz_sql::session::user::{MZ_SYSTEM_ROLE_ID, RoleMetadata};
use mz_sql::session::vars::ENABLE_METRIC_SINK;
use tracing::{Span, info};

use crate::catalog::Catalog;
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
    /// Stable identifier for the definition: used in logs, as the [`Coordinator::metric_sinks`] key,
    /// and as the `sink` label on the health gauges (the `GlobalId` is transient, the name is not).
    /// Must be unique within [`CURATED`].
    name: &'static str,
    /// A `SELECT` producing the canonical metric-sink columns (`metric_name`, `metric_type`,
    /// `labels`, `value`, `help`), the contract `mz_sql::plan::validate_metric_sink_desc` checks.
    ///
    /// The query must read only introspection relations. A catalog-backed relation would put
    /// envd's write frontier on the sink's emission path, which is exactly the coupling these
    /// sinks exist to avoid: the sink would stall whenever envd did, taking the freshness signal
    /// with it.
    source_sql: &'static str,
    /// Prepended to every row's `metric_name` to form the published name, exactly as a user's
    /// `CREATE METRIC SINK ... WITH (PREFIX = ...)`. Must start with `mz_metric_sink_` so the
    /// published families land in the reserved lane (see `validate_metric_sink_prefix`).
    prefix: &'static str,
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

/// A [`CuratedMetricSink`] planned once and shared across the replicas it installs on. See
/// [`Coordinator::plan_metric_sink`].
#[derive(Clone, Debug)]
pub(super) struct PlannedMetricSink {
    /// The shaped source query.
    expr: HirRelationExpr,
    /// The shape `expr` produces.
    desc: RelationDesc,
    /// The catalog items the source reads.
    dependencies: BTreeSet<CatalogItemId>,
}

impl Coordinator {
    /// Installs the curated metric sinks on all existing replicas.
    ///
    /// Meant to be invoked during coordinator bootstrapping.
    pub(super) async fn bootstrap_metric_sinks(&mut self) {
        for (cluster_id, replica_id) in self.all_cluster_replicas() {
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
        // run, so a `source_sql` reading introspection relations there never advances. That is not
        // just wasted work: the sink publishes its input frontier as its write frontier, so a
        // never-advancing input stalls the sink's frontier at its as-of and pins the read holds it
        // takes on those collections for the replica's whole life (replica-local, released on
        // drop). `coord::introspection` installs subscribes on the same triggers and has the same
        // gap.
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
        // Cheap duplicate check before planning: if the definition is already installed on this
        // replica, there is nothing to do. `metric_sink_finish` keeps a backstop for a double
        // install still in flight (not yet recorded here).
        if self
            .metric_sinks
            .contains_key(&(replica_id, definition.name))
        {
            return;
        }

        let Some(planned) = self.plan_metric_sink(definition) else {
            return;
        };

        let (_, sink_id) = self.allocate_transient_id();
        // Logged only once the definition is known good, so an abandoned install leaves no
        // misleading "installing" line.
        info!(%sink_id, %replica_id, name = definition.name, "installing metric sink");

        let validity = PlanValidity::new(
            &self.catalog,
            planned.dependencies.clone(),
            Some(cluster_id),
            Some(replica_id),
            RoleMetadata::new(MZ_SYSTEM_ROLE_ID),
        );
        let stage = MetricSinkStage::Optimize(MetricSinkOptimize {
            validity,
            definition,
            sink_id,
            expr: planned.expr.clone(),
            desc: planned.desc.clone(),
            cluster_id,
            replica_id,
        });
        self.sequence_staged((), Span::current(), stage).await;
    }

    /// Plans a curated definition once, caching the result in [`Coordinator::metric_sink_plans`].
    ///
    /// The plan depends only on the catalog, never on the replica, so it is shared across every
    /// replica the definition installs on rather than re-planned per replica. Curated sources read
    /// only builtins (enforced by [`ensure_reads_only_logs`]), which do not change while envd runs,
    /// so a cached plan stays valid for envd's lifetime. Returns `None` for an invalid definition,
    /// having soft-panicked.
    fn plan_metric_sink(
        &mut self,
        definition: &'static CuratedMetricSink,
    ) -> Option<PlannedMetricSink> {
        if let Some(planned) = self.metric_sink_plans.get(definition.name) {
            return Some(planned.clone());
        }

        // A user sink's prefix is validated at plan time; a curated one has no such gate, so enforce
        // the same contract here. The prefix keeps published families in the reserved lane and
        // supplies the leading character the row shaping's name validation needs. A failure is a bug
        // in our own definition, hence `soft_panic_or_log!`.
        //
        // NOTE: This checks only the prefix format, not collisions. A curated prefix is not checked
        // against user sinks (`ensure_metric_sink_prefix_is_free` cannot see a non-catalog item) nor
        // against other curated definitions, yet both share the `mz_metric_sink_` lane and could
        // overlap. Deferred until `CURATED` is populated.
        if let Err(err) = validate_metric_sink_prefix(definition.prefix) {
            soft_panic_or_log!(
                "invalid curated metric sink prefix (name={}): {err}",
                definition.name
            );
            return None;
        }

        let catalog = self.catalog().for_system_session();
        let (expr, desc, dependencies) = match definition.plan_source(&catalog) {
            Ok(planned) => planned,
            Err(err) => {
                soft_panic_or_log!(
                    "invalid curated metric sink (name={}): {err}",
                    definition.name
                );
                return None;
            }
        };

        // Enforce the introspection-only contract before any optimization work, against what the
        // definition reads rather than how the optimizer imports it.
        if let Err(err) = ensure_reads_only_logs(&self.catalog, &dependencies) {
            soft_panic_or_log!(
                "invalid curated metric sink (name={}): {err}",
                definition.name
            );
            return None;
        }

        let planned = PlannedMetricSink {
            expr,
            desc,
            dependencies,
        };
        self.metric_sink_plans
            .insert(definition.name, planned.clone());
        Some(planned)
    }

    #[instrument]
    fn metric_sink_optimize(
        &self,
        stage: MetricSinkOptimize,
    ) -> Result<StageResult<Box<MetricSinkStage>>, AdapterError> {
        let MetricSinkOptimize {
            mut validity,
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
        let catalog = self.owned_catalog();

        let span = Span::current();
        Ok(StageResult::Handle(mz_ore::task::spawn_blocking(
            || "optimize metric sink",
            move || {
                span.in_scope(|| {
                    let metric_sink = optimize::metric_sink::MetricSink::new(
                        format!("metric-sink-{}-{replica_id}", definition.name),
                        optimize::metric_sink::MetricSinkFrom::Query { expr, desc },
                        definition.prefix.to_string(),
                        Some(definition.name.to_string()),
                    );

                    // Both steps run inside one closure so either failure hits the same log.
                    // `sequence_staged` has no session to report to for a coordinator-driven
                    // install, so an error would otherwise vanish.
                    let global_lir_plan = (|| {
                        // MIR ⇒ MIR optimization (global)
                        let global_mir_plan = optimizer.catch_unwind_optimize(metric_sink)?;
                        // The optimizer imports indexes the SQL never named. Fold them into
                        // validity so one dropped before the finish stage fails the recheck rather
                        // than shipping a dataflow that imports a gone collection.
                        let id_bundle =
                            dataflow_import_id_bundle(global_mir_plan.df_desc(), cluster_id);
                        let item_ids = id_bundle.iter().map(|id| catalog.resolve_item_id(&id));
                        validity.extend_dependencies(&catalog, item_ids);
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

        let id_bundle = dataflow_import_id_bundle(&df_desc, cluster_id);

        // Backstop for the introspection-only contract; the real gate is `ensure_reads_only_logs`
        // at install time. A log-only source imports only compute collections, so this should never
        // fire, but a storage import would couple the sink's frontier to envd.
        if !id_bundle.storage_ids.is_empty() {
            soft_panic_or_log!(
                "curated metric sink reads non-introspection relations (name={}): {:?}",
                definition.name,
                id_bundle.storage_ids
            );
            return Ok(StageResult::Response(ExecuteResponse::CreatedMetricSink));
        }

        // Hold a read on the imports across shipping, so their since cannot advance past the as-of
        // just picked. Compute takes its own holds during `create_dataflow`.
        let read_holds = self.acquire_read_holds(&id_bundle);
        df_desc.set_as_of(read_holds.least_valid_read());

        // Record the install now that the dataflow is about to ship, so a definition that fails to
        // plan or optimize leaves no entry behind. `drop_metric_sinks` uses this entry to release the
        // sink's instance-global collection state when the replica is dropped. Recording after the
        // ship (an introspection subscribe records before sequencing) is safe: validity was rechecked
        // at this stage and nothing awaits before the ship, so no replica drop can interleave.
        let install = InstalledMetricSink {
            cluster_id,
            sink_id,
        };
        if let Some(previous) = self
            .metric_sinks
            .insert((replica_id, definition.name), install)
        {
            // The key is already taken. `curated_names_are_unique` rules out two definitions
            // colliding, so the reachable cause is `install_metric_sinks` running twice for one
            // replica. Restore the first install and abandon this one: shipping both would leak the
            // first's collection (now unreachable to `drop_metric_sinks`) and register a second
            // collector under the same `sink` label.
            self.metric_sinks
                .insert((replica_id, definition.name), previous);
            soft_panic_or_log!(
                "metric sink installed twice (name={}, replica_id={replica_id})",
                definition.name
            );
            return Ok(StageResult::Response(ExecuteResponse::CreatedMetricSink));
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
        for (name, cluster_id, sink_id) in metric_sinks_on_replica(&self.metric_sinks, replica_id) {
            info!(%sink_id, %replica_id, name, "dropping metric sink");
            self.metric_sinks.remove(&(replica_id, name));

            // The collection exists (the entry is recorded only after the dataflow ships), so this
            // drop succeeds. Result ignored: a failure during replica teardown is not worth a panic.
            let _ = self
                .controller
                .compute
                .drop_collections(cluster_id, vec![sink_id]);
        }
    }
}

/// The registry entries installed on `replica_id`, as `(name, cluster, sink)` in key order.
///
/// The map is keyed replica-first, so a replica's installs are one contiguous range.
fn metric_sinks_on_replica(
    metric_sinks: &BTreeMap<(ReplicaId, &'static str), InstalledMetricSink>,
    replica_id: ReplicaId,
) -> Vec<(&'static str, ClusterId, GlobalId)> {
    metric_sinks
        .range((replica_id, "")..)
        .take_while(|((id, _), _)| *id == replica_id)
        .map(|((_, name), install)| (*name, install.cluster_id, install.sink_id))
        .collect()
}

/// Enforces the introspection-only contract from [`CuratedMetricSink::source_sql`]: every relation
/// the definition reads, walking views transitively, must be a log collection. A storage-backed
/// read would put envd's write frontier on the sink's emission path, the coupling these sinks exist
/// to avoid.
///
/// Checked here against what the definition reads rather than by import kind after optimization: the
/// import split (storage vs index) depends on which indexes the target cluster happens to have, so
/// it gives the same definition different verdicts on different clusters.
fn ensure_reads_only_logs(
    catalog: &Catalog,
    dependencies: &BTreeSet<CatalogItemId>,
) -> Result<(), anyhow::Error> {
    let mut to_visit: Vec<_> = dependencies.iter().copied().collect();
    let mut visited = BTreeSet::new();
    while let Some(id) = to_visit.pop() {
        if !visited.insert(id) {
            continue;
        }
        let entry = catalog.get_entry(&id);
        match entry.item() {
            // The only data leaf allowed.
            CatalogItem::Log(_) => {}
            // Allowed only if everything it reads is, so walk its dependencies.
            CatalogItem::View(_) => to_visit.extend(entry.uses()),
            // No data dependency; a view over logs still references these.
            CatalogItem::Type(_) | CatalogItem::Func(_) => {}
            _ => bail!(
                "curated metric sink reads {}, which is not an introspection log relation \
                 (only logs and views over logs are allowed)",
                catalog.resolve_full_name(entry.name(), None)
            ),
        }
    }
    Ok(())
}

impl CuratedMetricSink {
    /// Plans `source_sql` against a session-less catalog, returning the query, its output shape,
    /// and the catalog items it reads.
    fn plan_source(
        &self,
        catalog: &dyn SessionCatalog,
    ) -> Result<(HirRelationExpr, RelationDesc, BTreeSet<CatalogItemId>), anyhow::Error> {
        // A definition is a single statement. Reject the count explicitly for a clear error.
        let statements = mz_sql::parse::parse(self.source_sql)?;
        if statements.len() != 1 {
            bail!(
                "source SQL must be exactly one statement, got {}",
                statements.len()
            );
        }

        // A metric sink's source is a continuously maintained dataflow, like a SUBSCRIBE, so plan it
        // as one. A maintained lifetime folds any finishing into the expression (an ORDER BY over a
        // maintained collection is dropped, a LIMIT becomes a TopK) rather than leaving it beside the
        // query, so `MetricSinkFrom::Query` gets a self-contained expression whose arity matches its
        // `desc`. This mirrors `coord::introspection`, which plans its specs as subscribes too.
        let subscribe_sql = format!("SUBSCRIBE ({})", self.source_sql);
        let parsed = mz_sql::parse::parse(&subscribe_sql)?.into_element();
        let (stmt, resolved_ids) = mz_sql::names::resolve(catalog, parsed.ast)?;
        let (plan, sql_impl_ids) =
            mz_sql::plan::plan(None, catalog, stmt, &Params::empty(), &resolved_ids)?;
        let Plan::Subscribe(SubscribePlan {
            from: SubscribeFrom::Query { expr, desc },
            ..
        }) = plan
        else {
            bail!("source SQL must be a single SELECT");
        };
        validate_metric_sink_desc(&desc)?;

        // Fold in ids from SQL-implemented function bodies. `plan` keeps them out of `resolved_ids`
        // since a one-shot statement doesn't depend on a function's body, but a metric sink inlines
        // that body into its dataflow, so the body's reads are real imports the gate must check.
        let dependencies = resolved_ids
            .items()
            .chain(sql_impl_ids.items())
            .copied()
            .collect();
        Ok((expr, desc, dependencies))
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
    use std::collections::{BTreeMap, BTreeSet};

    use mz_catalog::memory::objects::CatalogItem;
    use mz_cluster_client::ReplicaId;
    use mz_controller_types::ClusterId;
    use mz_repr::GlobalId;
    use mz_sql::plan::validate_metric_sink_prefix;

    use crate::catalog::Catalog;
    use crate::coord::metric_sink::{
        CURATED, CuratedMetricSink, InstalledMetricSink, ensure_reads_only_logs,
        metric_sinks_on_replica,
    };

    /// `drop_metric_sinks` relies on this range scan returning exactly one replica's installs, with
    /// no bleed into a neighbouring replica's contiguous range.
    #[mz_ore::test]
    fn metric_sinks_on_replica_scans_one_replica() {
        let cluster = ClusterId::user(1).expect("valid cluster id");
        let install = |sink_id| InstalledMetricSink {
            cluster_id: cluster,
            sink_id: GlobalId::Transient(sink_id),
        };
        let r = ReplicaId::User;

        let mut sinks = BTreeMap::new();
        sinks.insert((r(1), "a"), install(10));
        sinks.insert((r(2), "a"), install(20));
        sinks.insert((r(2), "b"), install(21));
        sinks.insert((r(2), "c"), install(22));
        sinks.insert((r(4), "a"), install(40));

        // A replica with several installs: all of them, in key order, and nothing from r(1)/r(4).
        assert_eq!(
            metric_sinks_on_replica(&sinks, r(2)),
            vec![
                ("a", cluster, GlobalId::Transient(20)),
                ("b", cluster, GlobalId::Transient(21)),
                ("c", cluster, GlobalId::Transient(22)),
            ]
        );
        // First and last replicas in the map: the scan stops at each boundary.
        assert_eq!(
            metric_sinks_on_replica(&sinks, r(1)),
            vec![("a", cluster, GlobalId::Transient(10))]
        );
        assert_eq!(
            metric_sinks_on_replica(&sinks, r(4)),
            vec![("a", cluster, GlobalId::Transient(40))]
        );
        // A replica with no installs, whether ordered between present ones (the r(3) gap) or past
        // the end, returns nothing rather than the next replica's range.
        assert!(metric_sinks_on_replica(&sinks, r(3)).is_empty());
        assert!(metric_sinks_on_replica(&sinks, r(5)).is_empty());
    }

    /// Every curated definition's prefix must satisfy the same contract a user's `PREFIX` does.
    /// Unlike the user path, nothing validates a curated prefix at runtime before this guards it,
    /// so a malformed one would escape the reserved lane or break the shaping's name validation.
    #[mz_ore::test]
    fn curated_prefixes_are_valid() {
        for definition in CURATED {
            validate_metric_sink_prefix(definition.prefix).unwrap_or_else(|err| {
                panic!(
                    "curated metric sink {:?} has an invalid prefix {:?}: {err}",
                    definition.name, definition.prefix
                )
            });
        }
    }

    /// The registry is keyed on the name, so a duplicate would make one definition's install
    /// unreachable to teardown and both collectors collide on the `sink` label. Guarded at runtime
    /// (`metric_sink_finish`) too, but caught here at build time before it can ship.
    #[mz_ore::test]
    fn curated_names_are_unique() {
        let mut seen = BTreeSet::new();
        for definition in CURATED {
            assert!(
                seen.insert(definition.name),
                "duplicate curated metric sink name {:?}",
                definition.name
            );
        }
    }

    /// Every curated definition must plan against the system catalog. A definition that does not
    /// would soft-panic at boot, so catch it here as a failing test instead. `CURATED` is empty
    /// today, so this iterates nothing until the first definition lands.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
    async fn curated_definitions_plan() {
        Catalog::with_debug(|catalog| async move {
            let session_catalog = catalog.for_system_session();
            for definition in CURATED {
                definition
                    .plan_source(&session_catalog)
                    .unwrap_or_else(|err| {
                        panic!(
                            "curated metric sink {:?} does not plan: {err}",
                            definition.name
                        )
                    });
            }
        })
        .await
    }

    /// The five canonical columns, no finishing: the shape a definition must produce.
    const VALID_SOURCE: &str = "SELECT 'n'::text AS metric_name, 'gauge'::text AS metric_type, \
        NULL::map[text=>text] AS labels, NULL::double AS value, 'h'::text AS help";

    /// `VALID_SOURCE` with an ORDER BY appended. Maintained-lifetime planning folds it away rather
    /// than rejecting it, since ordering has no meaning for a continuously-consumed collection.
    const ORDERED_SOURCE: &str = "SELECT 'n'::text AS metric_name, 'gauge'::text AS metric_type, \
        NULL::map[text=>text] AS labels, NULL::double AS value, 'h'::text AS help ORDER BY 1";

    /// `plan_source` accepts the canonical column contract (including a source with a finishing,
    /// which maintained-lifetime planning folds in) and rejects a source missing the columns.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
    async fn plan_source_enforces_the_metric_sink_contract() {
        Catalog::with_debug(|catalog| async move {
            let session_catalog = catalog.for_system_session();
            let plan = |source_sql: &'static str| {
                CuratedMetricSink {
                    name: "test",
                    source_sql,
                    prefix: "mz_metric_sink_test_",
                }
                .plan_source(&session_catalog)
            };

            assert!(plan(VALID_SOURCE).is_ok());

            // An ORDER BY is folded away by maintained-lifetime planning, not rejected.
            assert!(plan(ORDERED_SOURCE).is_ok());

            // Missing the canonical columns: rejected by `validate_metric_sink_desc`.
            assert!(plan("SELECT 1 AS foo").is_err());

            // Not exactly one statement: rejected by the explicit count guard.
            assert!(plan("").is_err());
            assert!(plan("SELECT 1; SELECT 2").is_err());
        })
        .await
    }

    /// A SQL-implemented builtin hides its reads: `pg_get_viewdef`'s body reads
    /// `mz_catalog.mz_views`, which the dataflow imports but the statement's resolved ids omit.
    /// `plan_source` must surface those reads so the gate rejects them.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
    async fn ensure_reads_only_logs_sees_sql_impl_function_reads() {
        Catalog::with_debug(|catalog| async move {
            let session_catalog = catalog.for_system_session();
            let (_, _, dependencies) = CuratedMetricSink {
                name: "test",
                source_sql: "SELECT pg_get_viewdef('x') AS metric_name, 'gauge'::text AS metric_type, \
                    NULL::map[text=>text] AS labels, NULL::double AS value, 'h'::text AS help",
                prefix: "mz_metric_sink_test_",
            }
            .plan_source(&session_catalog)
            .expect("plans against the system catalog");
            assert!(ensure_reads_only_logs(&catalog, &dependencies).is_err());
        })
        .await
    }

    /// The introspection-only contract: a log dependency is accepted, a storage-backed one is
    /// rejected. Checked against what the definition reads, so the verdict does not depend on the
    /// target cluster's index layout.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method`
    async fn ensure_reads_only_logs_accepts_logs_rejects_storage() {
        Catalog::with_debug(|catalog| async move {
            let log_id = catalog
                .entries()
                .find(|e| matches!(e.item(), CatalogItem::Log(_)))
                .expect("debug catalog has a builtin log")
                .id();
            assert!(ensure_reads_only_logs(&catalog, &BTreeSet::from([log_id])).is_ok());

            let storage_id = catalog
                .entries()
                .find(|e| matches!(e.item(), CatalogItem::Table(_) | CatalogItem::Source(_)))
                .expect("debug catalog has a builtin table or source")
                .id();
            assert!(ensure_reads_only_logs(&catalog, &BTreeSet::from([storage_id])).is_err());
        })
        .await
    }
}
