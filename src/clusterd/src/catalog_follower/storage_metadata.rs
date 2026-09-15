// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Storage read metadata derived from one committed catalog prefix.

use std::collections::{BTreeMap, BTreeSet};

use mz_catalog::catalog::Catalog;
use mz_catalog::expr_cache::ExpressionCacheHandle;
use mz_catalog::memory::objects::{CatalogItem, TableDataSource};
use mz_compute_types::sinks::ComputeSinkConnection;
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
use mz_persist_types::schema::SchemaId;
use mz_repr::{GlobalId, RelationDesc, RelationVersion, Timestamp};
use mz_sql::catalog::SessionCatalog;
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::sources::SourceData;
use timely::progress::Antichain;
use uuid::Uuid;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Pending {
    Definition,
    UnsupportedDefinition,
    ShardMapping,
    TxnWalShard,
    /// An exact table schema, or any registered schema for an unversioned source.
    Schema(Option<SchemaId>),
    ProducerSelection(GlobalId),
    ProducerBytes(GlobalId, Uuid),
    ProducerVersion(GlobalId),
    ProducerSink(GlobalId),
}

#[derive(Debug, Default)]
pub(super) struct Resolution {
    pub metadata: BTreeMap<GlobalId, CollectionMetadata>,
    pub uppers: BTreeMap<GlobalId, Antichain<Timestamp>>,
    pub pending: BTreeMap<GlobalId, Pending>,
}

enum Schema {
    // Table ALTER maps RelationVersion 1:1 to SchemaId. A missing version must
    // not fall back to the latest schema, which can contain different columns.
    Table(SchemaId),
    Registered,
    Builtin(RelationDesc),
    // MV replacement versions identify writers, not Persist schemas. Retired
    // aliases read their own mapped shard using the latest writer's value_desc.
    MaterializedView {
        writer: GlobalId,
        version: RelationVersion,
    },
}

struct Definition {
    schema: Schema,
    transactional: bool,
}

/// Resolve only `wanted`, including producers in other clusters. All catalog
/// decisions use the native catalog, and plan bytes are read by its immutable selections.
/// Missing prerequisites are pending, while malformed records and Persist errors
/// fail the observation. Metadata and uppers have identical key sets, disjoint
/// from pending, and together cover `wanted`.
///
/// This acquires no protection and registers no reader or writer. Persist's
/// observation APIs can initialize an unused shard. Uppers are observations, not
/// a proof that a collection is readable. The caller owns cancellation/timeouts.
pub(super) async fn resolve(
    catalog: &Catalog,
    wanted: &BTreeSet<GlobalId>,
    store: &ExpressionCacheHandle,
    build: &str,
    persist: &PersistClient,
    location: &PersistLocation,
    txns_shard: Option<ShardId>,
) -> anyhow::Result<Resolution> {
    let definitions = definitions(catalog, wanted);
    let shards = &catalog.state().storage_metadata().collection_metadata;
    let writers: BTreeSet<_> = definitions
        .values()
        .filter_map(|definition| match definition.as_ref().ok()?.schema {
            Schema::MaterializedView { writer, .. } => Some(writer),
            _ => None,
        })
        .collect();
    let selections: BTreeMap<_, _> = writers
        .into_iter()
        .filter_map(|id| {
            catalog
                .state()
                .written_plan(id, build)
                .map(|revision| (id, revision))
        })
        .collect();
    let plans = store
        .read_plans(selections.iter().map(|(id, rev)| (*id, *rev)).collect())
        .await?;
    let mut result = Resolution::default();
    let mut observed_uppers = BTreeMap::new();
    for id in wanted {
        let resolved = (|| {
            let definition = definitions
                .get(id)
                .ok_or(Pending::Definition)?
                .as_ref()
                .map_err(Clone::clone)?;
            let data_shard = *shards.get(id).ok_or(Pending::ShardMapping)?;
            let txns_shard = if definition.transactional {
                Some(txns_shard.ok_or(Pending::TxnWalShard)?)
            } else {
                None
            };
            let desc = match &definition.schema {
                Schema::Builtin(desc) => Some(desc.clone()),
                Schema::MaterializedView { writer, version } => {
                    let revision = selections
                        .get(writer)
                        .ok_or(Pending::ProducerSelection(*writer))?;
                    let plan = plans
                        .get(&(*writer, *revision))
                        .ok_or(Pending::ProducerBytes(*writer, *revision))?;
                    if plan.item_version != *version {
                        return Err(Pending::ProducerVersion(*writer));
                    }
                    let sink = plan
                        .physical_plan
                        .sink_exports
                        .get(writer)
                        .ok_or(Pending::ProducerSink(*writer))?;
                    let ComputeSinkConnection::MaterializedView(connection) = &sink.connection
                    else {
                        return Err(Pending::ProducerSink(*writer));
                    };
                    Some(connection.value_desc.clone())
                }
                Schema::Table(_) | Schema::Registered => None,
            };
            Ok((data_shard, txns_shard, desc))
        })();
        let (data_shard, txns_shard, mut desc) = match resolved {
            Ok(resolved) => resolved,
            Err(pending) => {
                result.pending.insert(*id, pending);
                continue;
            }
        };
        let definition = definitions[id].as_ref().expect("resolved above");
        let schema_id = match definition.schema {
            Schema::Table(schema_id) => Some(Some(schema_id)),
            Schema::Registered => Some(None),
            _ => None,
        };
        if let Some(schema_id) = schema_id {
            let schema = match schema_id {
                Some(schema_id) => persist
                    .get_schema::<SourceData, (), Timestamp, StorageDiff>(
                        data_shard,
                        schema_id,
                        diagnostics(*id),
                    )
                    .await?
                    .map(|(desc, _)| desc),
                None => persist
                    .latest_schema::<SourceData, (), Timestamp, StorageDiff>(
                        data_shard,
                        diagnostics(*id),
                    )
                    .await?
                    .map(|(_, desc, _)| desc),
            };
            let Some(schema) = schema else {
                result.pending.insert(*id, Pending::Schema(schema_id));
                continue;
            };
            desc = Some(schema);
        }
        let metadata = CollectionMetadata {
            persist_location: location.clone(),
            data_shard,
            txns_shard,
            relation_desc: desc.expect("every schema kind resolved above"),
        };
        let upper_shard = txns_shard.unwrap_or(data_shard);
        let upper = match observed_uppers.get(&upper_shard) {
            Some(upper) => upper,
            None => {
                let upper = persist
                    .recent_upper::<SourceData, (), Timestamp, StorageDiff>(
                        upper_shard,
                        diagnostics(*id),
                    )
                    .await?;
                observed_uppers.entry(upper_shard).or_insert(upper)
            }
        };
        result.uppers.insert(*id, upper.clone());
        result.metadata.insert(*id, metadata);
    }
    Ok(result)
}

fn diagnostics(id: GlobalId) -> Diagnostics {
    Diagnostics {
        shard_name: id.to_string(),
        handle_purpose: "catalog follower storage metadata".into(),
    }
}

fn definitions(
    catalog: &Catalog,
    wanted: &BTreeSet<GlobalId>,
) -> BTreeMap<GlobalId, Result<Definition, Pending>> {
    let session = catalog.for_system_session();
    wanted
        .iter()
        .map(|id| {
            let definition = (|| {
                let entry = catalog
                    .try_get_entry_by_global_id(id)
                    .ok_or(Pending::Definition)?;
                let transactional = matches!(entry.item(), CatalogItem::Table(table)
                if matches!(table.data_source, TableDataSource::TableWrites { .. }));
                let schema = match entry.item() {
                    CatalogItem::MaterializedView(mv) => Schema::MaterializedView {
                        writer: mv.global_id_writes(),
                        version: *mv.collections.last_key_value().expect("MV has a version").0,
                    },
                    _ if id.is_system() => {
                        let item = session
                            .try_get_item_by_global_id(id)
                            .ok_or(Pending::Definition)?;
                        Schema::Builtin(
                            item.relation_desc()
                                .ok_or(Pending::UnsupportedDefinition)?
                                .into_owned(),
                        )
                    }
                    CatalogItem::Table(table) if transactional => {
                        let version = table
                            .collections
                            .iter()
                            .find_map(|(version, alias)| (alias == id).then_some(*version))
                            .ok_or(Pending::Definition)?;
                        Schema::Table(version.into())
                    }
                    CatalogItem::Table(_) | CatalogItem::Source(_) | CatalogItem::Sink(_) => {
                        Schema::Registered
                    }
                    _ => return Err(Pending::UnsupportedDefinition),
                };
                Ok(Definition {
                    schema,
                    transactional,
                })
            })();
            (*id, definition)
        })
        .collect()
}
