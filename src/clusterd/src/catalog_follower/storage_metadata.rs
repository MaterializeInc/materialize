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

use anyhow::Context;
use mz_catalog::builtin::{BUILTINS, Builtin};
use mz_catalog::durable::Snapshot;
use mz_catalog::durable::objects::{self, DurableType};
use mz_catalog::expr_cache::ExpressionCacheHandle;
use mz_compute_types::sinks::ComputeSinkConnection;
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
use mz_persist_types::schema::SchemaId;
use mz_proto::RustType;
use mz_repr::{GlobalId, RelationDesc, RelationVersion, Timestamp};
use mz_sql_parser::ast::Statement;
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::sources::SourceData;
use timely::progress::Antichain;
use uuid::Uuid;

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
/// decisions use `snapshot`, and plan bytes are read by its immutable selections.
/// Missing prerequisites are pending, while malformed records and Persist errors
/// fail the observation. Metadata and uppers have identical key sets, disjoint
/// from pending, and together cover `wanted`.
///
/// This acquires no protection and registers no reader or writer. Persist's
/// observation APIs can initialize an unused shard. Uppers are observations, not
/// a proof that a collection is readable. The caller owns cancellation/timeouts.
pub(super) async fn resolve(
    snapshot: &Snapshot,
    wanted: &BTreeSet<GlobalId>,
    store: &ExpressionCacheHandle,
    build: &str,
    persist: &PersistClient,
    location: &PersistLocation,
) -> anyhow::Result<Resolution> {
    let definitions = definitions(snapshot, wanted)?;
    let mut shards = BTreeMap::new();
    for (key, value) in &snapshot.storage_collection_metadata {
        let mapping = objects::StorageCollectionMetadata::from_key_value(
            RustType::from_proto(key.clone())?,
            RustType::from_proto(value.clone())?,
        );
        if wanted.contains(&mapping.id) {
            shards.insert(mapping.id, mapping.shard);
        }
    }
    let writers: BTreeSet<_> = definitions
        .values()
        .filter_map(|definition| match definition.as_ref().ok()?.schema {
            Schema::MaterializedView { writer, .. } => Some(writer),
            _ => None,
        })
        .collect();
    let mut selections = BTreeMap::new();
    for (key, value) in &snapshot.written_plans {
        let selection = objects::WrittenPlan::from_key_value(
            RustType::from_proto(key.clone())?,
            RustType::from_proto(value.clone())?,
        );
        if selection.build_version == build && writers.contains(&selection.id) {
            selections.insert(selection.id, selection.revision);
        }
    }
    let plans = store
        .read_plans(selections.iter().map(|(id, rev)| (*id, *rev)).collect())
        .await?;
    let txns_shard: Option<ShardId> = snapshot
        .txn_wal_shard
        .get(&())
        .map(|value| value.shard.parse().map_err(anyhow::Error::msg))
        .transpose()?;
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
    snapshot: &Snapshot,
    wanted: &BTreeSet<GlobalId>,
) -> anyhow::Result<BTreeMap<GlobalId, Result<Definition, Pending>>> {
    let mut definitions = BTreeMap::new();
    for (key, value) in &snapshot.items {
        let item = objects::Item::from_key_value(
            RustType::from_proto(key.clone())?,
            RustType::from_proto(value.clone())?,
        );
        if item.ephemeral_owner_session.is_some() {
            continue;
        }
        let aliases: Vec<_> = std::iter::once((RelationVersion::root(), item.global_id))
            .chain(
                item.extra_versions
                    .iter()
                    .map(|(version, id)| (*version, *id)),
            )
            .filter(|(_, id)| wanted.contains(id))
            .collect();
        if aliases.is_empty() {
            continue;
        }
        let mut statements = mz_sql_parser::parser::parse_statements(&item.create_sql)
            .with_context(|| format!("classifying storage item {}", item.id))?;
        anyhow::ensure!(
            statements.len() == 1,
            "expected one canonical CREATE statement"
        );
        let statement = statements.remove(0).ast;
        let (version, writer) = item.extra_versions.last_key_value().map_or_else(
            || (RelationVersion::root(), item.global_id),
            |(v, id)| (*v, *id),
        );
        for (alias_version, id) in aliases {
            let definition = match &statement {
                Statement::CreateTable(_) => Ok(Definition {
                    schema: Schema::Table(alias_version.into()),
                    transactional: true,
                }),
                Statement::CreateMaterializedView(_) => Ok(Definition {
                    schema: Schema::MaterializedView { writer, version },
                    transactional: false,
                }),
                Statement::CreateSource(_)
                | Statement::CreateSubsource(_)
                | Statement::CreateTableFromSource(_)
                | Statement::CreateWebhookSource(_)
                    if item.extra_versions.is_empty() =>
                {
                    Ok(Definition {
                        schema: Schema::Registered,
                        transactional: false,
                    })
                }
                _ => Err(Pending::UnsupportedDefinition),
            };
            definitions.insert(id, definition);
        }
    }
    let builtins: BTreeMap<_, _> = BUILTINS::iter()
        .map(|builtin| {
            (
                objects::SystemObjectDescription {
                    schema_name: builtin.schema().into(),
                    object_type: builtin.catalog_item_type(),
                    object_name: builtin.name().into(),
                },
                builtin,
            )
        })
        .collect();
    for (key, value) in &snapshot.system_object_mappings {
        let mapping = objects::SystemObjectMapping::from_key_value(
            RustType::from_proto(key.clone())?,
            RustType::from_proto(value.clone())?,
        );
        let id = mapping.unique_identifier.global_id;
        if !wanted.contains(&id) {
            continue;
        }
        let definition = match builtins.get(&mapping.description) {
            Some(Builtin::Table(table)) => Ok(Definition {
                schema: Schema::Builtin(table.desc.clone()),
                transactional: true,
            }),
            Some(Builtin::Source(source)) => Ok(Definition {
                schema: Schema::Builtin(source.desc.clone()),
                transactional: false,
            }),
            Some(Builtin::MaterializedView(_)) => Ok(Definition {
                schema: Schema::MaterializedView {
                    writer: id,
                    version: RelationVersion::root(),
                },
                transactional: false,
            }),
            Some(_) => Err(Pending::UnsupportedDefinition),
            None => Err(Pending::Definition),
        };
        definitions.insert(id, definition);
    }
    Ok(definitions)
}

#[cfg(test)]
mod tests;
