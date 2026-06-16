// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(clippy::unwrap_used)]

use crate::durable::traits::UpgradeFrom;
use crate::durable::traits::UpgradeInto;
use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::json_compatible::JsonCompatible;
use crate::durable::upgrade::objects_v78 as v78;
use crate::durable::upgrade::objects_v79 as v79;
use crate::json_compatible;

json_compatible!(v78::AclMode with v79::AclMode);
json_compatible!(v78::ClusterScheduleRefreshOptions with v79::ClusterScheduleRefreshOptions);
json_compatible!(v78::CommentValue with v79::CommentValue);
json_compatible!(v78::DefaultPrivilegesValue with v79::DefaultPrivilegesValue);
json_compatible!(v78::EpochMillis with v79::EpochMillis);
json_compatible!(v78::OptimizerFeatureOverride with v79::OptimizerFeatureOverride);
json_compatible!(v78::ReplicaConfig with v79::ReplicaConfig);
json_compatible!(v78::ReplicaLogging with v79::ReplicaLogging);
json_compatible!(v78::RoleAttributes with v79::RoleAttributes);
json_compatible!(v78::RoleAuthValue with v79::RoleAuthValue);
json_compatible!(v78::SourceReferencesValue with v79::SourceReferencesValue);
json_compatible!(v78::StorageCollectionMetadataValue with v79::StorageCollectionMetadataValue);
json_compatible!(v78::SystemPrivilegesValue with v79::SystemPrivilegesValue);
json_compatible!(v78::Version with v79::Version);
json_compatible!(v78::state_update_kind::IdAlloc with v79::IdAlloc);
json_compatible!(v78::state_update_kind::ServerConfiguration with v79::ServerConfiguration);
json_compatible!(v78::state_update_kind::TxnWalShard with v79::TxnWalShard);
json_compatible!(v78::state_update_kind::UnfinalizedShard with v79::UnfinalizedShard);

// These types MUST be JSON-compatible because they are de-serialized before we run the migration.
// See `StateUpdateKindJson::is_always_deserializable`.
json_compatible!(v78::state_update_kind::AuditLog with v79::AuditLog);
json_compatible!(v78::state_update_kind::Config with v79::Config);
json_compatible!(v78::state_update_kind::FenceToken with v79::FenceToken);
json_compatible!(v78::state_update_kind::Setting with v79::Setting);

pub fn upgrade(
    snapshot: Vec<v78::StateUpdateKind>,
) -> Vec<MigrationAction<v78::StateUpdateKind, v79::StateUpdateKind>> {
    use v78::state_update_kind::Kind::*;
    use v79::StateUpdateKind as Kind;

    // For JSON-compatible kinds we don't need to perform a migration.
    macro_rules! skip_json_compatible {
        ($old_val:expr, $new_type:ty) => {{
            let _: $new_type = JsonCompatible::convert(&$old_val);
            return None;
        }};
    }

    snapshot
        .into_iter()
        .filter_map(|old| {
            let new = match old.kind.clone().unwrap() {
                Cluster(x) => Kind::Cluster(x.upgrade_into()),
                ClusterIntrospectionSourceIndex(x) => {
                    Kind::ClusterIntrospectionSourceIndex(x.upgrade_into())
                }
                ClusterReplica(x) => Kind::ClusterReplica(x.upgrade_into()),
                Comment(x) => Kind::Comment(x.upgrade_into()),
                Database(x) => Kind::Database(x.upgrade_into()),
                DefaultPrivileges(x) => Kind::DefaultPrivileges(x.upgrade_into()),
                GidMapping(x) => Kind::GidMapping(x.upgrade_into()),
                Item(x) => Kind::Item(x.upgrade_into()),
                NetworkPolicy(x) => Kind::NetworkPolicy(x.upgrade_into()),
                Role(x) => Kind::Role(x.upgrade_into()),
                RoleAuth(x) => Kind::RoleAuth(x.upgrade_into()),
                Schema(x) => Kind::Schema(x.upgrade_into()),
                SourceReferences(x) => Kind::SourceReferences(x.upgrade_into()),
                StorageCollectionMetadata(x) => Kind::StorageCollectionMetadata(x.upgrade_into()),
                SystemPrivileges(x) => Kind::SystemPrivileges(x.upgrade_into()),

                AuditLog(x) => skip_json_compatible!(x, v79::AuditLog),
                Config(x) => skip_json_compatible!(x, v79::Config),
                FenceToken(x) => skip_json_compatible!(x, v79::FenceToken),
                IdAlloc(x) => skip_json_compatible!(x, v79::IdAlloc),
                ServerConfiguration(x) => skip_json_compatible!(x, v79::ServerConfiguration),
                Setting(x) => skip_json_compatible!(x, v79::Setting),
                TxnWalShard(x) => skip_json_compatible!(x, v79::TxnWalShard),
                UnfinalizedShard(x) => skip_json_compatible!(x, v79::UnfinalizedShard),
            };
            Some(MigrationAction::Update(old, new))
        })
        .collect()
}

impl UpgradeFrom<v78::state_update_kind::Cluster> for v79::Cluster {
    fn upgrade_from(old: v78::state_update_kind::Cluster) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::ClusterKey> for v79::ClusterKey {
    fn upgrade_from(old: v78::ClusterKey) -> Self {
        Self {
            id: old.id.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::ClusterValue> for v79::ClusterValue {
    fn upgrade_from(old: v78::ClusterValue) -> Self {
        Self {
            name: old.name,
            owner_id: old.owner_id.unwrap().upgrade_into(),
            privileges: old
                .privileges
                .into_iter()
                .map(UpgradeInto::upgrade_into)
                .collect(),
            config: old.config.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::ClusterId> for v79::ClusterId {
    fn upgrade_from(old: v78::ClusterId) -> Self {
        use v78::cluster_id::Value::*;
        match old.value.unwrap() {
            System(id) => Self::System(id),
            User(id) => Self::User(id),
        }
    }
}

impl UpgradeFrom<v78::RoleId> for v79::RoleId {
    fn upgrade_from(old: v78::RoleId) -> Self {
        use v78::role_id::Value::*;
        match old.value.unwrap() {
            System(id) => Self::System(id),
            User(id) => Self::User(id),
            Public(_) => Self::Public,
            Predefined(id) => Self::Predefined(id),
        }
    }
}

impl UpgradeFrom<v78::MzAclItem> for v79::MzAclItem {
    fn upgrade_from(old: v78::MzAclItem) -> Self {
        Self {
            grantee: old.grantee.unwrap().upgrade_into(),
            grantor: old.grantor.unwrap().upgrade_into(),
            acl_mode: JsonCompatible::convert(&old.acl_mode.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::ClusterConfig> for v79::ClusterConfig {
    fn upgrade_from(old: v78::ClusterConfig) -> Self {
        Self {
            workload_class: old.workload_class,
            variant: old.variant.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::cluster_config::Variant> for v79::ClusterVariant {
    fn upgrade_from(old: v78::cluster_config::Variant) -> Self {
        use v78::cluster_config::Variant::*;
        match old {
            Unmanaged(_) => Self::Unmanaged,
            Managed(m) => Self::Managed(m.upgrade_into()),
        }
    }
}

impl UpgradeFrom<v78::cluster_config::ManagedCluster> for v79::ManagedCluster {
    fn upgrade_from(old: v78::cluster_config::ManagedCluster) -> Self {
        Self {
            size: old.size,
            replication_factor: old.replication_factor,
            availability_zones: old.availability_zones,
            logging: JsonCompatible::convert(&old.logging.unwrap()),
            optimizer_feature_overrides: old
                .optimizer_feature_overrides
                .iter()
                .map(JsonCompatible::convert)
                .collect(),
            schedule: old.schedule.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::ClusterSchedule> for v79::ClusterSchedule {
    fn upgrade_from(old: v78::ClusterSchedule) -> Self {
        use v78::cluster_schedule::Value::*;
        match old.value.unwrap() {
            Manual(_) => Self::Manual,
            Refresh(r) => Self::Refresh(JsonCompatible::convert(&r)),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::ClusterReplica> for v79::ClusterReplica {
    fn upgrade_from(old: v78::state_update_kind::ClusterReplica) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::ClusterReplicaKey> for v79::ClusterReplicaKey {
    fn upgrade_from(old: v78::ClusterReplicaKey) -> Self {
        Self {
            id: old.id.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::ReplicaId> for v79::ReplicaId {
    fn upgrade_from(old: v78::ReplicaId) -> Self {
        use v78::replica_id::Value::*;
        match old.value.unwrap() {
            System(id) => Self::System(id),
            User(id) => Self::User(id),
        }
    }
}

impl UpgradeFrom<v78::ClusterReplicaValue> for v79::ClusterReplicaValue {
    fn upgrade_from(old: v78::ClusterReplicaValue) -> Self {
        Self {
            cluster_id: old.cluster_id.unwrap().upgrade_into(),
            name: old.name,
            config: JsonCompatible::convert(&old.config.unwrap()),
            owner_id: old.owner_id.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::Comment> for v79::Comment {
    fn upgrade_from(old: v78::state_update_kind::Comment) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: JsonCompatible::convert(&old.value.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::CommentKey> for v79::CommentKey {
    fn upgrade_from(old: v78::CommentKey) -> Self {
        Self {
            object: old.object.unwrap().upgrade_into(),
            sub_component: old.sub_component.map(UpgradeInto::upgrade_into),
        }
    }
}

impl UpgradeFrom<v78::comment_key::Object> for v79::CommentObject {
    fn upgrade_from(old: v78::comment_key::Object) -> Self {
        use v78::comment_key::Object::*;
        match old {
            Table(id) => Self::Table(id.upgrade_into()),
            View(id) => Self::View(id.upgrade_into()),
            MaterializedView(id) => Self::MaterializedView(id.upgrade_into()),
            Source(id) => Self::Source(id.upgrade_into()),
            Sink(id) => Self::Sink(id.upgrade_into()),
            Index(id) => Self::Index(id.upgrade_into()),
            Func(id) => Self::Func(id.upgrade_into()),
            Connection(id) => Self::Connection(id.upgrade_into()),
            Type(id) => Self::Type(id.upgrade_into()),
            Secret(id) => Self::Secret(id.upgrade_into()),
            ContinualTask(id) => Self::ContinualTask(id.upgrade_into()),
            Role(id) => Self::Role(id.upgrade_into()),
            Database(id) => Self::Database(id.upgrade_into()),
            Schema(s) => Self::Schema(s.upgrade_into()),
            Cluster(id) => Self::Cluster(id.upgrade_into()),
            ClusterReplica(id) => Self::ClusterReplica(id.upgrade_into()),
            NetworkPolicy(id) => Self::NetworkPolicy(id.upgrade_into()),
        }
    }
}

impl UpgradeFrom<v78::CatalogItemId> for v79::CatalogItemId {
    fn upgrade_from(old: v78::CatalogItemId) -> Self {
        use v78::catalog_item_id::Value::*;
        match old.value.unwrap() {
            System(id) => Self::System(id),
            User(id) => Self::User(id),
            Transient(id) => Self::Transient(id),
            IntrospectionSourceIndex(id) => Self::IntrospectionSourceIndex(id),
        }
    }
}

impl UpgradeFrom<v78::DatabaseId> for v79::DatabaseId {
    fn upgrade_from(old: v78::DatabaseId) -> Self {
        use v78::database_id::Value::*;
        match old.value.unwrap() {
            System(id) => Self::System(id),
            User(id) => Self::User(id),
        }
    }
}

impl UpgradeFrom<v78::ResolvedSchema> for v79::ResolvedSchema {
    fn upgrade_from(old: v78::ResolvedSchema) -> Self {
        Self {
            database: old.database.unwrap().upgrade_into(),
            schema: old.schema.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::ResolvedDatabaseSpecifier> for v79::ResolvedDatabaseSpecifier {
    fn upgrade_from(old: v78::ResolvedDatabaseSpecifier) -> Self {
        use v78::resolved_database_specifier::Spec::*;
        match old.spec.unwrap() {
            Ambient(_) => Self::Ambient,
            Id(id) => Self::Id(id.upgrade_into()),
        }
    }
}

impl UpgradeFrom<v78::SchemaSpecifier> for v79::SchemaSpecifier {
    fn upgrade_from(old: v78::SchemaSpecifier) -> Self {
        use v78::schema_specifier::Spec::*;
        match old.spec.unwrap() {
            Temporary(_) => Self::Temporary,
            Id(id) => Self::Id(id.upgrade_into()),
        }
    }
}

impl UpgradeFrom<v78::SchemaId> for v79::SchemaId {
    fn upgrade_from(old: v78::SchemaId) -> Self {
        use v78::schema_id::Value::*;
        match old.value.unwrap() {
            System(id) => Self::System(id),
            User(id) => Self::User(id),
        }
    }
}

impl UpgradeFrom<v78::ClusterReplicaId> for v79::ClusterReplicaId {
    fn upgrade_from(old: v78::ClusterReplicaId) -> Self {
        Self {
            cluster_id: old.cluster_id.unwrap().upgrade_into(),
            replica_id: old.replica_id.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::NetworkPolicyId> for v79::NetworkPolicyId {
    fn upgrade_from(old: v78::NetworkPolicyId) -> Self {
        use v78::network_policy_id::Value::*;
        match old.value.unwrap() {
            System(id) => Self::System(id),
            User(id) => Self::User(id),
        }
    }
}

impl UpgradeFrom<v78::comment_key::SubComponent> for v79::CommentSubComponent {
    fn upgrade_from(old: v78::comment_key::SubComponent) -> Self {
        use v78::comment_key::SubComponent::*;
        match old {
            ColumnPos(pos) => Self::ColumnPos(pos),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::Database> for v79::Database {
    fn upgrade_from(old: v78::state_update_kind::Database) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::DatabaseKey> for v79::DatabaseKey {
    fn upgrade_from(old: v78::DatabaseKey) -> Self {
        Self {
            id: old.id.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::DatabaseValue> for v79::DatabaseValue {
    fn upgrade_from(old: v78::DatabaseValue) -> Self {
        Self {
            name: old.name,
            owner_id: old.owner_id.unwrap().upgrade_into(),
            privileges: old
                .privileges
                .into_iter()
                .map(UpgradeInto::upgrade_into)
                .collect(),
            oid: old.oid,
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::DefaultPrivileges> for v79::DefaultPrivileges {
    fn upgrade_from(old: v78::state_update_kind::DefaultPrivileges) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: JsonCompatible::convert(&old.value.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::DefaultPrivilegesKey> for v79::DefaultPrivilegesKey {
    fn upgrade_from(old: v78::DefaultPrivilegesKey) -> Self {
        Self {
            role_id: old.role_id.unwrap().upgrade_into(),
            database_id: old.database_id.map(UpgradeInto::upgrade_into),
            schema_id: old.schema_id.map(UpgradeInto::upgrade_into),
            object_type: old.object_type.upgrade_into(),
            grantee: old.grantee.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<i32> for v79::ObjectType {
    fn upgrade_from(value: i32) -> Self {
        match value {
            0 => Self::Unknown,
            1 => Self::Table,
            2 => Self::View,
            3 => Self::MaterializedView,
            4 => Self::Source,
            5 => Self::Sink,
            6 => Self::Index,
            7 => Self::Type,
            8 => Self::Role,
            9 => Self::Cluster,
            10 => Self::ClusterReplica,
            11 => Self::Secret,
            12 => Self::Connection,
            13 => Self::Database,
            14 => Self::Schema,
            15 => Self::Func,
            16 => Self::ContinualTask,
            17 => Self::NetworkPolicy,
            x => panic!("invalid ObjectType: {x}"),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::ClusterIntrospectionSourceIndex>
    for v79::ClusterIntrospectionSourceIndex
{
    fn upgrade_from(old: v78::state_update_kind::ClusterIntrospectionSourceIndex) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::ClusterIntrospectionSourceIndexKey>
    for v79::ClusterIntrospectionSourceIndexKey
{
    fn upgrade_from(old: v78::ClusterIntrospectionSourceIndexKey) -> Self {
        Self {
            cluster_id: old.cluster_id.unwrap().upgrade_into(),
            name: old.name,
        }
    }
}

impl UpgradeFrom<v78::ClusterIntrospectionSourceIndexValue>
    for v79::ClusterIntrospectionSourceIndexValue
{
    fn upgrade_from(old: v78::ClusterIntrospectionSourceIndexValue) -> Self {
        Self {
            catalog_id: v79::IntrospectionSourceIndexCatalogItemId(old.index_id),
            global_id: old.global_id.unwrap().upgrade_into(),
            oid: old.oid,
        }
    }
}

impl UpgradeFrom<v78::IntrospectionSourceIndexGlobalId> for v79::IntrospectionSourceIndexGlobalId {
    fn upgrade_from(old: v78::IntrospectionSourceIndexGlobalId) -> Self {
        Self(old.value)
    }
}

impl UpgradeFrom<v78::state_update_kind::Item> for v79::Item {
    fn upgrade_from(old: v78::state_update_kind::Item) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::ItemKey> for v79::ItemKey {
    fn upgrade_from(old: v78::ItemKey) -> Self {
        Self {
            gid: old.gid.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::ItemValue> for v79::ItemValue {
    fn upgrade_from(old: v78::ItemValue) -> Self {
        Self {
            schema_id: old.schema_id.unwrap().upgrade_into(),
            name: old.name,
            definition: old.definition.unwrap().upgrade_into(),
            owner_id: old.owner_id.unwrap().upgrade_into(),
            privileges: old
                .privileges
                .into_iter()
                .map(UpgradeInto::upgrade_into)
                .collect(),
            oid: old.oid,
            global_id: old.global_id.unwrap().upgrade_into(),
            extra_versions: old
                .extra_versions
                .into_iter()
                .map(UpgradeInto::upgrade_into)
                .collect(),
        }
    }
}

impl UpgradeFrom<v78::CatalogItem> for v79::CatalogItem {
    fn upgrade_from(old: v78::CatalogItem) -> Self {
        use v78::catalog_item::Value::*;
        match old.value.unwrap() {
            V1(v) => Self::V1(v79::CatalogItemV1 {
                create_sql: v.create_sql,
            }),
        }
    }
}

impl UpgradeFrom<v78::GlobalId> for v79::GlobalId {
    fn upgrade_from(old: v78::GlobalId) -> Self {
        use v78::global_id::Value::*;
        match old.value.unwrap() {
            System(id) => Self::System(id),
            User(id) => Self::User(id),
            Transient(id) => Self::Transient(id),
            Explain(_) => Self::Explain,
            IntrospectionSourceIndex(id) => Self::IntrospectionSourceIndex(id),
        }
    }
}

impl UpgradeFrom<v78::ItemVersion> for v79::ItemVersion {
    fn upgrade_from(old: v78::ItemVersion) -> Self {
        Self {
            global_id: old.global_id.unwrap().upgrade_into(),
            version: JsonCompatible::convert(&old.version.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::Role> for v79::Role {
    fn upgrade_from(old: v78::state_update_kind::Role) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::RoleKey> for v79::RoleKey {
    fn upgrade_from(old: v78::RoleKey) -> Self {
        Self {
            id: old.id.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::RoleValue> for v79::RoleValue {
    fn upgrade_from(old: v78::RoleValue) -> Self {
        Self {
            name: old.name,
            attributes: JsonCompatible::convert(&old.attributes.unwrap()),
            membership: old.membership.unwrap().upgrade_into(),
            vars: old.vars.unwrap().upgrade_into(),
            oid: old.oid,
        }
    }
}

impl UpgradeFrom<v78::RoleMembership> for v79::RoleMembership {
    fn upgrade_from(old: v78::RoleMembership) -> Self {
        Self {
            map: old.map.into_iter().map(UpgradeInto::upgrade_into).collect(),
        }
    }
}

impl UpgradeFrom<v78::role_membership::Entry> for v79::RoleMembershipEntry {
    fn upgrade_from(old: v78::role_membership::Entry) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::RoleVars> for v79::RoleVars {
    fn upgrade_from(old: v78::RoleVars) -> Self {
        Self {
            entries: old
                .entries
                .into_iter()
                .map(UpgradeInto::upgrade_into)
                .collect(),
        }
    }
}

impl UpgradeFrom<v78::role_vars::Entry> for v79::RoleVarsEntry {
    fn upgrade_from(old: v78::role_vars::Entry) -> Self {
        Self {
            key: old.key,
            val: old.val.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::role_vars::entry::Val> for v79::RoleVar {
    fn upgrade_from(old: v78::role_vars::entry::Val) -> Self {
        use v78::role_vars::entry::Val::*;
        match old {
            Flat(s) => Self::Flat(s),
            SqlSet(s) => Self::SqlSet(s.entries),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::Schema> for v79::Schema {
    fn upgrade_from(old: v78::state_update_kind::Schema) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::SchemaKey> for v79::SchemaKey {
    fn upgrade_from(old: v78::SchemaKey) -> Self {
        Self {
            id: old.id.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::SchemaValue> for v79::SchemaValue {
    fn upgrade_from(old: v78::SchemaValue) -> Self {
        Self {
            database_id: old.database_id.map(UpgradeInto::upgrade_into),
            name: old.name,
            owner_id: old.owner_id.unwrap().upgrade_into(),
            privileges: old
                .privileges
                .into_iter()
                .map(UpgradeInto::upgrade_into)
                .collect(),
            oid: old.oid,
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::GidMapping> for v79::GidMapping {
    fn upgrade_from(old: v78::state_update_kind::GidMapping) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::GidMappingKey> for v79::GidMappingKey {
    fn upgrade_from(old: v78::GidMappingKey) -> Self {
        Self {
            schema_name: old.schema_name,
            object_type: old.object_type.upgrade_into(),
            object_name: old.object_name,
        }
    }
}

impl UpgradeFrom<i32> for v79::CatalogItemType {
    fn upgrade_from(value: i32) -> Self {
        match value {
            0 => Self::Unknown,
            1 => Self::Table,
            2 => Self::Source,
            3 => Self::Sink,
            4 => Self::View,
            5 => Self::MaterializedView,
            6 => Self::Index,
            7 => Self::Type,
            8 => Self::Func,
            9 => Self::Secret,
            10 => Self::Connection,
            11 => Self::ContinualTask,
            x => panic!("invalid CatalogItemType: {x}"),
        }
    }
}

impl UpgradeFrom<v78::GidMappingValue> for v79::GidMappingValue {
    fn upgrade_from(old: v78::GidMappingValue) -> Self {
        Self {
            catalog_id: v79::SystemCatalogItemId(old.id),
            global_id: old.global_id.unwrap().upgrade_into(),
            fingerprint: old.fingerprint,
        }
    }
}

impl UpgradeFrom<v78::SystemGlobalId> for v79::SystemGlobalId {
    fn upgrade_from(old: v78::SystemGlobalId) -> Self {
        Self(old.value)
    }
}

impl UpgradeFrom<v78::state_update_kind::SystemPrivileges> for v79::SystemPrivileges {
    fn upgrade_from(old: v78::state_update_kind::SystemPrivileges) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: JsonCompatible::convert(&old.value.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::SystemPrivilegesKey> for v79::SystemPrivilegesKey {
    fn upgrade_from(old: v78::SystemPrivilegesKey) -> Self {
        Self {
            grantee: old.grantee.unwrap().upgrade_into(),
            grantor: old.grantor.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::StorageCollectionMetadata>
    for v79::StorageCollectionMetadata
{
    fn upgrade_from(old: v78::state_update_kind::StorageCollectionMetadata) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: JsonCompatible::convert(&old.value.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::StorageCollectionMetadataKey> for v79::StorageCollectionMetadataKey {
    fn upgrade_from(old: v78::StorageCollectionMetadataKey) -> Self {
        Self {
            id: old.id.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::SourceReferences> for v79::SourceReferences {
    fn upgrade_from(old: v78::state_update_kind::SourceReferences) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: JsonCompatible::convert(&old.value.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::SourceReferencesKey> for v79::SourceReferencesKey {
    fn upgrade_from(old: v78::SourceReferencesKey) -> Self {
        Self {
            source: old.source.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::NetworkPolicy> for v79::NetworkPolicy {
    fn upgrade_from(old: v78::state_update_kind::NetworkPolicy) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: old.value.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::NetworkPolicyKey> for v79::NetworkPolicyKey {
    fn upgrade_from(old: v78::NetworkPolicyKey) -> Self {
        Self {
            id: old.id.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::NetworkPolicyValue> for v79::NetworkPolicyValue {
    fn upgrade_from(old: v78::NetworkPolicyValue) -> Self {
        Self {
            name: old.name,
            rules: old
                .rules
                .into_iter()
                .map(UpgradeInto::upgrade_into)
                .collect(),
            owner_id: old.owner_id.unwrap().upgrade_into(),
            privileges: old
                .privileges
                .into_iter()
                .map(UpgradeInto::upgrade_into)
                .collect(),
            oid: old.oid,
        }
    }
}

impl UpgradeFrom<v78::NetworkPolicyRule> for v79::NetworkPolicyRule {
    fn upgrade_from(old: v78::NetworkPolicyRule) -> Self {
        Self {
            name: old.name,
            address: old.address,
            action: old.action.unwrap().upgrade_into(),
            direction: old.direction.unwrap().upgrade_into(),
        }
    }
}

impl UpgradeFrom<v78::network_policy_rule::Action> for v79::NetworkPolicyRuleAction {
    fn upgrade_from(old: v78::network_policy_rule::Action) -> Self {
        use v78::network_policy_rule::Action::*;
        match old {
            Allow(_) => Self::Allow,
        }
    }
}

impl UpgradeFrom<v78::network_policy_rule::Direction> for v79::NetworkPolicyRuleDirection {
    fn upgrade_from(old: v78::network_policy_rule::Direction) -> Self {
        use v78::network_policy_rule::Direction::*;
        match old {
            Ingress(_) => Self::Ingress,
        }
    }
}

impl UpgradeFrom<v78::state_update_kind::RoleAuth> for v79::RoleAuth {
    fn upgrade_from(old: v78::state_update_kind::RoleAuth) -> Self {
        Self {
            key: old.key.unwrap().upgrade_into(),
            value: JsonCompatible::convert(&old.value.unwrap()),
        }
    }
}

impl UpgradeFrom<v78::RoleAuthKey> for v79::RoleAuthKey {
    fn upgrade_from(old: v78::RoleAuthKey) -> Self {
        Self {
            id: old.id.unwrap().upgrade_into(),
        }
    }
}
