// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Wall-clock dependence of selected compute plans and their native storage inputs.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, bail, ensure};
use itertools::Itertools;
use mz_catalog::catalog::Catalog;
use mz_catalog::expr_cache::ExpressionCacheHandle;
use mz_catalog::memory::objects::{CatalogItem, DataSourceDesc, TableDataSource};
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::LirRelationExpr;
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{GlobalId, RelationVersion};
use mz_storage_types::sources::{GenericSourceConnection, Timeline};
use mz_storage_types::time_dependence::TimeDependence;

#[cfg(test)]
mod tests;

#[derive(Clone)]
struct Inputs {
    ids: BTreeSet<GlobalId>,
    schedule: Option<RefreshSchedule>,
}

impl Inputs {
    fn from_plan(plan: &DataflowDescription<LirRelationExpr, ()>) -> Self {
        Self {
            ids: plan.used_import_ids(),
            schedule: plan.refresh_schedule.clone(),
        }
    }
}

enum Definition {
    Known(Option<TimeDependence>),
    Alias(GlobalId),
    Plan(GlobalId, RelationVersion),
}

/// Populate dependence without planning or installing anything. `dataflows` must
/// be the pending written selections for this catalog prefix. Installed values
/// describe running plans and take precedence over selections when resolving imports.
/// Missing prerequisites and cycles are errors, not indeterminate dependence.
/// On error no dataflow is modified. The caller can retain the batch as pending.
pub(super) async fn resolve(
    catalog: &Catalog,
    store: &ExpressionCacheHandle,
    build: &str,
    dataflows: &mut [DataflowDescription<LirRelationExpr, ()>],
    installed: &BTreeMap<GlobalId, Option<TimeDependence>>,
) -> anyhow::Result<()> {
    let mut pending = BTreeMap::new();
    for dataflow in dataflows.iter() {
        let inputs = Inputs::from_plan(dataflow);
        for id in dataflow.export_ids() {
            ensure!(
                pending.insert(id, inputs.clone()).is_none(),
                "multiple pending producers for {id}"
            );
        }
    }
    let mut resolver = Resolver {
        catalog,
        store,
        build,
        pending,
        resolved: installed.clone(),
        visiting: BTreeSet::new(),
    };
    let mut dependencies = Vec::with_capacity(dataflows.len());
    for dataflow in dataflows.iter() {
        dependencies.push(resolver.inputs(Inputs::from_plan(dataflow)).await?);
    }
    for (dataflow, dependence) in dataflows.iter_mut().zip_eq(dependencies) {
        dataflow.time_dependence = dependence;
    }
    Ok(())
}

struct Resolver<'a> {
    catalog: &'a Catalog,
    store: &'a ExpressionCacheHandle,
    build: &'a str,
    pending: BTreeMap<GlobalId, Inputs>,
    resolved: BTreeMap<GlobalId, Option<TimeDependence>>,
    visiting: BTreeSet<GlobalId>,
}

impl Resolver<'_> {
    async fn inputs(&mut self, inputs: Inputs) -> anyhow::Result<Option<TimeDependence>> {
        let mut dependencies = Vec::with_capacity(inputs.ids.len());
        for id in inputs.ids {
            dependencies.push(Box::pin(self.collection(id)).await?);
        }
        Ok(TimeDependence::merge(
            dependencies,
            inputs.schedule.as_ref(),
        ))
    }

    async fn collection(&mut self, id: GlobalId) -> anyhow::Result<Option<TimeDependence>> {
        if let Some(dependence) = self.resolved.get(&id) {
            return Ok(dependence.clone());
        }
        ensure!(self.visiting.insert(id), "cyclic time dependence at {id}");
        let dependence = if let Some(inputs) = self.pending.get(&id).cloned() {
            self.inputs(inputs).await?
        } else {
            match self.definition(id)? {
                Definition::Known(dependence) => dependence,
                Definition::Alias(writer) => Box::pin(self.collection(writer)).await?,
                Definition::Plan(writer, version) => {
                    let revision = self
                        .catalog
                        .state()
                        .written_plan(writer, self.build)
                        .with_context(|| format!("missing written selection for {writer}"))?;
                    let mut plans = self.store.read_plans(vec![(writer, revision)]).await?;
                    let plan = plans
                        .remove(&(writer, revision))
                        .with_context(|| format!("missing written plan {writer}/{revision}"))?;
                    ensure!(
                        plan.item_version == version,
                        "written plan version mismatch for {writer}/{revision}"
                    );
                    ensure!(
                        plan.physical_plan.export_ids().any(|id| id == writer),
                        "written plan {writer}/{revision} does not export its producer"
                    );
                    self.inputs(Inputs::from_plan(&plan.physical_plan)).await?
                }
            }
        };
        self.visiting.remove(&id);
        self.resolved.insert(id, dependence.clone());
        Ok(dependence)
    }

    fn definition(&self, id: GlobalId) -> anyhow::Result<Definition> {
        let entry = self
            .catalog
            .try_get_entry_by_global_id(&id)
            .with_context(|| format!("missing time dependence definition for {id}"))?;
        match entry.item() {
            CatalogItem::MaterializedView(mv) => {
                let writer = mv.global_id_writes();
                // Replacement aliases read the current writer's output. Resolving
                // through its ID also preserves an installed writer's captured value.
                if id != writer {
                    Ok(Definition::Alias(writer))
                } else {
                    let (version, _) = mv
                        .collections
                        .last_key_value()
                        .context("MV has no version")?;
                    Ok(Definition::Plan(writer, *version))
                }
            }
            CatalogItem::MetricSink(sink) => {
                Ok(Definition::Plan(sink.global_id, RelationVersion::root()))
            }
            CatalogItem::Index(index) => {
                let is_log_index = self
                    .catalog
                    .try_get_cluster(index.cluster_id)
                    .is_some_and(|cluster| cluster.log_indexes.values().any(|log| *log == id));
                if is_log_index {
                    Ok(Definition::Known(Some(TimeDependence::default())))
                } else {
                    Ok(Definition::Plan(id, RelationVersion::root()))
                }
            }
            CatalogItem::Log(_) => Ok(Definition::Known(Some(TimeDependence::default()))),
            CatalogItem::Table(table) => match &table.data_source {
                TableDataSource::TableWrites { .. } => {
                    Ok(Definition::Known(Some(TimeDependence::default())))
                }
                TableDataSource::DataSource { desc, timeline } => self.source(desc, timeline),
            },
            CatalogItem::Source(source) => self.source(&source.data_source, &source.timeline),
            CatalogItem::Sink(_) => Ok(Definition::Known(None)),
            _ => bail!("unsupported time dependence definition for {id}"),
        }
    }

    fn source(&self, desc: &DataSourceDesc, timeline: &Timeline) -> anyhow::Result<Definition> {
        if *timeline != Timeline::EpochMilliseconds {
            return Ok(Definition::Known(None));
        }
        let dependence = match desc {
            DataSourceDesc::Ingestion { desc, .. }
            | DataSourceDesc::OldSyntaxIngestion { desc, .. } => match &desc.connection {
                GenericSourceConnection::Kafka(_)
                | GenericSourceConnection::Postgres(_)
                | GenericSourceConnection::MySql(_)
                | GenericSourceConnection::SqlServer(_) => Some(TimeDependence::default()),
                GenericSourceConnection::LoadGenerator(_) => None,
            },
            DataSourceDesc::IngestionExport { ingestion_id, .. } => {
                let ingestion = self
                    .catalog
                    .try_get_entry(ingestion_id)
                    .with_context(|| format!("missing ingestion {ingestion_id}"))?;
                return Ok(Definition::Alias(ingestion.latest_global_id()));
            }
            DataSourceDesc::Introspection(_)
            | DataSourceDesc::Progress
            | DataSourceDesc::Webhook { .. } => Some(TimeDependence::default()),
            // The catalog shard is registered with storage as DataSource::Other.
            DataSourceDesc::Catalog => None,
        };
        Ok(Definition::Known(dependence))
    }
}
