// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Catalog checks for user-provided LIR plans, as in
//! `CREATE MATERIALIZED VIEW ... FROM PLAN '<json>'`.
//!
//! Purification resolves import names to items, and planning resolves items to `GlobalId`s.
//! Both apply the same checks through [`resolve_import`], so a stored plan is checked again
//! when it is planned at boot.

use mz_compute_types::plan::LirRelationNode;
use mz_compute_types::plan::envelope::{EnvelopeImport, ImportSource, LirPlanEnvelope};
use mz_controller_types::ClusterId;
use mz_expr::Id;
use mz_repr::{CatalogItemId, GlobalId, RelationVersion, RelationVersionSelector};

use crate::catalog::{CatalogItem, CatalogItemType, SessionCatalog};
use crate::normalize;
use crate::plan::PlanError;

/// Parses a user-provided envelope and checks that its placeholder ids are transient.
///
/// Every import and object id must be a `GlobalId::Transient`, and every `Get` of a
/// `GlobalId` in an object must name a transient id, so that plans reach catalog relations
/// only through imports.
pub(crate) fn parse_user_envelope(json: &str) -> Result<LirPlanEnvelope, PlanError> {
    let envelope =
        mz_compute_types::plan::envelope::parse_envelope(json).map_err(|e| sql_err!("{e}"))?;
    for import in &envelope.imports {
        if !import.id.is_transient() {
            sql_bail!("import id {} in LIR plan must be transient", import.id);
        }
    }
    for object in &envelope.objects {
        if !object.id.is_transient() {
            sql_bail!("object id {} in LIR plan must be transient", object.id);
        }
        let mut stack = vec![&object.plan];
        while let Some(expr) = stack.pop() {
            if let LirRelationNode::Get {
                id: Id::Global(id), ..
            } = &expr.node
            {
                if !id.is_transient() {
                    sql_bail!(
                        "object {} in LIR plan reads non-transient id {id}, \
                         plans must read catalog relations through imports",
                        object.id
                    );
                }
            }
            stack.extend(expr.node.children());
        }
    }
    Ok(envelope)
}

/// An import resolved against the catalog.
pub(crate) struct ResolvedImport<'a> {
    /// The imported relation.
    pub item: &'a dyn CatalogItem,
    /// The version of the relation to read.
    pub version: RelationVersion,
    /// The collection of the relation at `version`.
    pub global_id: GlobalId,
    /// The imported indexes and their collections, in envelope order.
    pub indexes: Vec<(&'a dyn CatalogItem, GlobalId)>,
}

/// Resolves `import` and checks that the plan may read it.
///
/// The relation must be a table, source, or materialized view, or a view read only through
/// at least one index. Each index must be an index on the relation at the resolved version, installed on
/// `cluster_id`. With `allow_names` false, every source must be an [`ImportSource::Item`].
pub(crate) fn resolve_import<'a>(
    catalog: &'a dyn SessionCatalog,
    import: &EnvelopeImport,
    cluster_id: ClusterId,
    allow_names: bool,
) -> Result<ResolvedImport<'a>, PlanError> {
    let (item, version) = resolve_source(catalog, &import.source, allow_names)?;
    let name = catalog.minimal_qualification(item.name());
    let readable = match item.item_type() {
        CatalogItemType::Table | CatalogItemType::Source | CatalogItemType::MaterializedView => {
            true
        }
        CatalogItemType::View => !import.persist,
        _ => false,
    };
    if !readable {
        let access = if import.persist {
            "read from persist"
        } else {
            "import"
        };
        sql_bail!("LIR plan cannot {access} {} {name}", item.item_type());
    }
    // A view has no collection of its own, so a plan reads it only through its indexes.
    if item.item_type() == CatalogItemType::View && import.indexes.is_empty() {
        sql_bail!("LIR plan must import view {name} through at least one index");
    }
    let global_id = item
        .at_version(version_selector(item, version)?)
        .global_id();

    let mut indexes = Vec::with_capacity(import.indexes.len());
    for index in &import.indexes {
        let (index_item, index_version) = resolve_source(catalog, index, allow_names)?;
        let index_name = catalog.minimal_qualification(index_item.name());
        if !matches!(index_item.index_details(), Some((_, on)) if on == global_id) {
            sql_bail!("{index_name} is not an index on {name}");
        }
        let index_cluster = index_item
            .cluster_id()
            .ok_or_else(|| sql_err!("{index_name} is not installed on a cluster"))?;
        if index_cluster != cluster_id {
            sql_bail!(
                "index {index_name} is on cluster {}, but the materialized view is on cluster {}",
                catalog.get_cluster(index_cluster).name(),
                catalog.get_cluster(cluster_id).name(),
            );
        }
        let index_global_id = index_item
            .at_version(version_selector(index_item, index_version)?)
            .global_id();
        indexes.push((index_item, index_global_id));
    }

    Ok(ResolvedImport {
        item,
        version,
        global_id,
        indexes,
    })
}

/// Looks up the item named by `source`, and the version of it a plan reads.
///
/// A name resolves to the item's latest version.
fn resolve_source<'a>(
    catalog: &'a dyn SessionCatalog,
    source: &ImportSource,
    allow_names: bool,
) -> Result<(&'a dyn CatalogItem, RelationVersion), PlanError> {
    match source {
        ImportSource::Name(name) => {
            if !allow_names {
                sql_bail!("LIR plan import {name} was not resolved to a catalog item");
            }
            let parsed = mz_sql_parser::parser::parse_item_name(name)
                .map_err(|e| sql_err!("invalid import name {name} in LIR plan: {e}"))?;
            let item = catalog.resolve_item(&normalize::unresolved_item_name(parsed)?)?;
            let version = item.latest_version().unwrap_or_else(RelationVersion::root);
            Ok((item, version))
        }
        ImportSource::Item { item_id, version } => {
            let item = catalog
                .try_get_item(item_id)
                .ok_or_else(|| sql_err!("unknown catalog item {item_id} in LIR plan"))?;
            Ok((item, *version))
        }
    }
}

/// The selector for `version` of `item`.
///
/// Only tables have versions a plan can name. Other relations are read at their latest
/// collection, as SQL references to them are, and must name the root version.
pub fn version_selector(
    item: &dyn CatalogItem,
    version: RelationVersion,
) -> Result<RelationVersionSelector, PlanError> {
    match item.latest_version() {
        Some(latest) if version <= latest => Ok(RelationVersionSelector::Specific(version)),
        None if version == RelationVersion::root() => Ok(RelationVersionSelector::Latest),
        _ => sql_bail!("{} has no version {version}", item.name().item),
    }
}

/// The collections an envelope's resolved imports read, for recording the references of a
/// stored statement. Imports that do not resolve are skipped, planning reports them.
pub(crate) fn resolved_import_collections(
    catalog: &dyn SessionCatalog,
    envelope: &LirPlanEnvelope,
) -> Vec<(CatalogItemId, GlobalId)> {
    let mut ids = Vec::new();
    let sources = envelope
        .imports
        .iter()
        .flat_map(|import| std::iter::once(&import.source).chain(&import.indexes));
    for source in sources {
        let ImportSource::Item { item_id, version } = source else {
            continue;
        };
        let Some(item) = catalog.try_get_item(item_id) else {
            continue;
        };
        if let Ok(selector) = version_selector(item, *version) {
            ids.push((*item_id, item.at_version(selector).global_id()));
        }
    }
    ids
}
