// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Build an in-memory [`CatalogItem`] for a branched object from its
//! [`BranchDescriptor`] plus the source item.
//!
//! The produced item carries the branch's own identity, never the source's:
//! callers reading a branch item resolve through the branch-local
//! `GlobalId` and [`RelationDesc`] stored on the descriptor, not the source
//! item's. Mutations to the source item after this builder runs cannot
//! reach back into the produced branch item.

use std::collections::BTreeMap;

use mz_proto::{ProtoType, RustType};
use mz_repr::{GlobalId, ProtoRelationDesc, RelationDesc, RelationVersion, VersionedRelationDesc};

use crate::durable::objects::BranchDescriptor;
use crate::memory::objects::{CatalogItem, MaterializedView, Source, Table};

/// Failure modes for [`build_branch_item`].
#[derive(Debug, thiserror::Error)]
pub enum BranchItemError {
    /// The descriptor's snapshotted `relation_desc` failed to decode.
    #[error("invalid relation_desc on branch descriptor: {0}")]
    InvalidRelationDesc(String),
    /// The source catalog item's variant is not supported for branching in
    /// the current implementation.
    #[error("source item variant is not branchable: {0:?}")]
    UnsupportedSourceVariant(&'static str),
}

/// Construct a branch-local [`CatalogItem`] from `descriptor` and the
/// `source` item it forks. The new item:
///
/// * Carries the branch's [`GlobalId`] (`descriptor.branch_global_id`) in
///   place of the source's.
/// * Carries the [`RelationDesc`] snapshotted on the descriptor in place of
///   the source's current desc, so later source ALTERs cannot reshape this
///   item.
/// * Inherits everything else from `source`. Source mutations after this
///   builder runs do not propagate, because the result is owned by the
///   caller.
pub fn build_branch_item(
    descriptor: &BranchDescriptor,
    source: &CatalogItem,
) -> Result<CatalogItem, BranchItemError> {
    let desc = decode_relation_desc(&descriptor.relation_desc)?;
    let branch_gid = descriptor.branch_global_id;

    match source {
        CatalogItem::Table(table) => Ok(CatalogItem::Table(branch_table(
            table,
            branch_gid,
            desc,
            descriptor.fork_shard_id,
        ))),
        CatalogItem::Source(source) => {
            Ok(CatalogItem::Source(branch_source(source, branch_gid, desc)))
        }
        CatalogItem::MaterializedView(mv) => Ok(CatalogItem::MaterializedView(branch_mv(
            mv,
            branch_gid,
            desc,
            descriptor.fork_shard_id,
        ))),
        CatalogItem::Sink(_)
        | CatalogItem::Index(_)
        | CatalogItem::View(_)
        | CatalogItem::Log(_)
        | CatalogItem::Type(_)
        | CatalogItem::Func(_)
        | CatalogItem::Secret(_)
        | CatalogItem::Connection(_) => Err(BranchItemError::UnsupportedSourceVariant(
            source_variant_name(source),
        )),
    }
}

fn decode_relation_desc(bytes: &[u8]) -> Result<RelationDesc, BranchItemError> {
    use prost::Message;
    let proto = ProtoRelationDesc::decode(bytes)
        .map_err(|err| BranchItemError::InvalidRelationDesc(err.to_string()))?;
    proto
        .into_rust()
        .map_err(|err| BranchItemError::InvalidRelationDesc(err.to_string()))
}

fn branch_table(
    source: &Table,
    branch_gid: GlobalId,
    desc: RelationDesc,
    fork_shard_id: mz_persist_client::ShardId,
) -> Table {
    let mut collections = BTreeMap::new();
    collections.insert(RelationVersion::root(), branch_gid);
    Table {
        create_sql: source.create_sql.clone(),
        desc: VersionedRelationDesc::new(desc),
        collections,
        conn_id: source.conn_id.clone(),
        resolved_ids: source.resolved_ids.clone(),
        custom_logical_compaction_window: source.custom_logical_compaction_window,
        is_retained_metrics_object: source.is_retained_metrics_object,
        data_source: source.data_source.clone(),
        branch_target_shard: Some(fork_shard_id),
    }
}

fn branch_source(source: &Source, branch_gid: GlobalId, desc: RelationDesc) -> Source {
    Source {
        create_sql: source.create_sql.clone(),
        global_id: branch_gid,
        data_source: source.data_source.clone(),
        desc,
        timeline: source.timeline.clone(),
        resolved_ids: source.resolved_ids.clone(),
        custom_logical_compaction_window: source.custom_logical_compaction_window,
        is_retained_metrics_object: source.is_retained_metrics_object,
    }
}

fn branch_mv(
    source: &MaterializedView,
    branch_gid: GlobalId,
    desc: RelationDesc,
    fork_shard_id: mz_persist_client::ShardId,
) -> MaterializedView {
    let mut collections = BTreeMap::new();
    collections.insert(RelationVersion::root(), branch_gid);
    let mut branched = source.clone();
    branched.collections = collections;
    branched.desc = VersionedRelationDesc::new(desc);
    branched.branch_target_shard = Some(fork_shard_id);
    branched
}

fn source_variant_name(item: &CatalogItem) -> &'static str {
    match item {
        CatalogItem::Table(_) => "Table",
        CatalogItem::Source(_) => "Source",
        CatalogItem::Log(_) => "Log",
        CatalogItem::View(_) => "View",
        CatalogItem::MaterializedView(_) => "MaterializedView",
        CatalogItem::Sink(_) => "Sink",
        CatalogItem::Index(_) => "Index",
        CatalogItem::Type(_) => "Type",
        CatalogItem::Func(_) => "Func",
        CatalogItem::Secret(_) => "Secret",
        CatalogItem::Connection(_) => "Connection",
    }
}

/// Encode `desc` to the proto-bytes form stored on a [`BranchDescriptor`].
/// Mirror of the decode path in [`build_branch_item`].
pub fn encode_relation_desc(desc: &RelationDesc) -> Vec<u8> {
    use prost::Message;
    desc.into_proto().encode_to_vec()
}

#[cfg(test)]
mod tests {
    use mz_repr::RelationDescBuilder;
    use mz_repr::{SqlColumnType, SqlScalarType};

    use super::*;

    fn make_descriptor(
        branch_gid: GlobalId,
        relation_desc: &RelationDesc,
    ) -> BranchDescriptor {
        BranchDescriptor {
            branch_catalog_id: mz_repr::CatalogItemId::User(99),
            fork_shard_id: mz_persist_types::ShardId::new(),
            branch_ts: 7,
            source_catalog_id: mz_repr::CatalogItemId::User(1),
            branch_global_id: branch_gid,
            relation_desc: encode_relation_desc(relation_desc),
            branch_id: "00000000-0000-0000-0000-000000000001".to_owned(),
            branch_name: "b".to_owned(),
            owner: mz_repr::role_id::RoleId::User(1),
            created_at_ms: 1700000000_000,
        }
    }

    fn make_desc() -> RelationDesc {
        RelationDescBuilder::default()
            .with_column(
                "id",
                SqlColumnType {
                    scalar_type: SqlScalarType::Int32,
                    nullable: false,
                },
            )
            .finish()
    }

    #[mz_ore::test]
    fn relation_desc_round_trips_through_descriptor() {
        let desc = make_desc();
        let descriptor = make_descriptor(GlobalId::User(42), &desc);
        let decoded = decode_relation_desc(&descriptor.relation_desc).expect("decode");
        assert_eq!(decoded, desc);
    }

    #[mz_ore::test]
    fn rejects_invalid_relation_desc() {
        let mut descriptor =
            make_descriptor(GlobalId::User(42), &make_desc());
        descriptor.relation_desc = vec![0xff; 8];
        assert!(matches!(
            decode_relation_desc(&descriptor.relation_desc),
            Err(BranchItemError::InvalidRelationDesc(_))
        ));
    }

    #[mz_ore::test]
    fn descriptor_round_trips_through_durable_type() {
        use crate::durable::objects::DurableType;
        let descriptor = make_descriptor(GlobalId::User(42), &make_desc());
        let (key, value) = descriptor.clone().into_key_value();
        let recovered = BranchDescriptor::from_key_value(key, value);
        assert_eq!(descriptor, recovered);
    }

    #[mz_ore::test]
    fn branched_table_carries_fork_target_shard() {
        // The Table built from a descriptor must record the fork shard so
        // `catalog_transact_inner`'s Op::CreateItem path routes through
        // `storage_collections_to_register` instead of allocating a fresh
        // shard. Without this binding, DML on the branched table would land
        // on a brand-new (empty) shard and the fork's pre-loaded history
        // would be invisible to reads.
        let desc = make_desc();
        let descriptor = make_descriptor(GlobalId::User(42), &desc);
        let source = CatalogItem::Table(Table {
            create_sql: Some("CREATE TABLE t (id int4 NOT NULL)".to_owned()),
            desc: VersionedRelationDesc::new(desc.clone()),
            collections: BTreeMap::from([(RelationVersion::root(), GlobalId::User(1))]),
            conn_id: None,
            resolved_ids: mz_sql::names::ResolvedIds::empty(),
            custom_logical_compaction_window: None,
            is_retained_metrics_object: false,
            data_source: crate::memory::objects::TableDataSource::TableWrites {
                defaults: vec![],
            },
            branch_target_shard: None,
        });
        let built = build_branch_item(&descriptor, &source).expect("build");
        let table = match built {
            CatalogItem::Table(t) => t,
            other => panic!("expected Table, got {other:?}"),
        };
        assert_eq!(
            table.branch_target_shard,
            Some(descriptor.fork_shard_id),
            "branched Table must record the descriptor's fork shard id"
        );
        // Its identity is the branch's, not the source's.
        assert!(table.collections.values().any(|gid| *gid == GlobalId::User(42)));
    }

    #[mz_ore::test]
    fn descriptor_round_trips_through_proto() {
        use mz_catalog_protos::objects as proto;
        use crate::durable::objects::{
            BranchDescriptorKey, BranchDescriptorValue, DurableType,
        };

        let descriptor = make_descriptor(GlobalId::User(42), &make_desc());
        let (key, value) = descriptor.clone().into_key_value();

        let proto_key: proto::BranchDescriptorKey = key.into_proto();
        let proto_value: proto::BranchDescriptorValue = value.into_proto();

        let key_recovered: BranchDescriptorKey =
            proto_key.into_rust().expect("key round-trips");
        let value_recovered: BranchDescriptorValue =
            proto_value.into_rust().expect("value round-trips");

        let recovered =
            BranchDescriptor::from_key_value(key_recovered, value_recovered);
        assert_eq!(descriptor, recovered);
    }
}
