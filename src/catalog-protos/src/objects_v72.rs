// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use num_enum::{IntoPrimitive, TryFromPrimitive};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ConfigKey {
    pub key: String,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ConfigValue {
    pub value: u64,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct SettingKey {
    pub name: String,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct SettingValue {
    pub value: String,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct IdAllocKey {
    pub name: String,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct IdAllocValue {
    pub next_id: u64,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct GidMappingKey {
    pub schema_name: String,
    pub object_type: i32,
    pub object_name: String,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct GidMappingValue {
    pub id: u64,
    pub fingerprint: String,
    pub global_id: Option<SystemGlobalId>,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ClusterKey {
    pub id: Option<ClusterId>,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ClusterValue {
    pub name: String,
    pub owner_id: Option<RoleId>,
    pub privileges: Vec<MzAclItem>,
    pub config: Option<ClusterConfig>,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ClusterIntrospectionSourceIndexKey {
    pub cluster_id: Option<ClusterId>,
    pub name: String,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ClusterIntrospectionSourceIndexValue {
    pub index_id: u64,
    pub oid: u32,
    pub global_id: Option<SystemGlobalId>,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ClusterReplicaKey {
    pub id: Option<ReplicaId>,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ClusterReplicaValue {
    pub cluster_id: Option<ClusterId>,
    pub name: String,
    pub config: Option<ReplicaConfig>,
    pub owner_id: Option<RoleId>,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct DatabaseKey {
    pub id: Option<DatabaseId>,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct DatabaseValue {
    pub name: String,
    pub owner_id: Option<RoleId>,
    pub privileges: Vec<MzAclItem>,
    pub oid: u32,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct SchemaKey {
    pub id: Option<SchemaId>,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct SchemaValue {
    pub database_id: Option<DatabaseId>,
    pub name: String,
    pub owner_id: Option<RoleId>,
    pub privileges: Vec<MzAclItem>,
    pub oid: u32,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ItemKey {
    pub gid: Option<CatalogItemId>,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ItemValue {
    pub schema_id: Option<SchemaId>,
    pub name: String,
    pub definition: Option<CatalogItem>,
    pub owner_id: Option<RoleId>,
    pub privileges: Vec<MzAclItem>,
    pub oid: u32,
    pub global_id: Option<GlobalId>,
    pub extra_versions: Vec<ItemVersion>,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ItemVersion {
    pub global_id: Option<GlobalId>,
    pub version: Option<Version>,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct RoleKey {
    pub id: Option<RoleId>,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct RoleValue {
    pub name: String,
    pub attributes: Option<RoleAttributes>,
    pub membership: Option<RoleMembership>,
    pub vars: Option<RoleVars>,
    pub oid: u32,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct NetworkPolicyKey {
    pub id: Option<NetworkPolicyId>,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct NetworkPolicyValue {
    pub name: String,
    pub rules: Vec<NetworkPolicyRule>,
    pub owner_id: Option<RoleId>,
    pub privileges: Vec<MzAclItem>,
    pub oid: u32,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ServerConfigurationKey {
    pub name: String,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ServerConfigurationValue {
    pub value: String,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct AuditLogKey {
    pub event: Option<audit_log_key::Event>,
}

pub mod audit_log_key {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub enum Event {
        V1(AuditLogEventV1),
    }
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct CommentKey {
    pub object: Option<comment_key::Object>,
    pub sub_component: Option<comment_key::SubComponent>,
}

pub mod comment_key {
    use super::*;

    #[derive(
        Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub enum Object {
        Table(CatalogItemId),
        View(CatalogItemId),
        MaterializedView(CatalogItemId),
        Source(CatalogItemId),
        Sink(CatalogItemId),
        Index(CatalogItemId),
        Func(CatalogItemId),
        Connection(CatalogItemId),
        Type(CatalogItemId),
        Secret(CatalogItemId),
        ContinualTask(CatalogItemId),
        Role(RoleId),
        Database(DatabaseId),
        Schema(ResolvedSchema),
        Cluster(ClusterId),
        ClusterReplica(ClusterReplicaId),
        NetworkPolicy(NetworkPolicyId),
    }

    #[derive(
        Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub enum SubComponent {
        ColumnPos(u64),
    }
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct CommentValue {
    pub comment: String,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct SourceReferencesKey {
    pub source: Option<CatalogItemId>,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct SourceReferencesValue {
    pub references: Vec<SourceReference>,
    pub updated_at: Option<EpochMillis>,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct SourceReference {
    pub name: String,
    pub namespace: Option<String>,
    pub columns: Vec<String>,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct StorageCollectionMetadataKey {
    pub id: Option<GlobalId>,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct StorageCollectionMetadataValue {
    pub shard: String,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct UnfinalizedShardKey {
    pub shard: String,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct TxnWalShardValue {
    pub shard: String,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct Empty {}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct StringWrapper {
    pub inner: String,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct Duration {
    pub secs: u64,
    pub nanos: u32,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct EpochMillis {
    pub millis: u64,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct Timestamp {
    pub internal: u64,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct Version {
    pub value: u64,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct CatalogItem {
    pub value: Option<catalog_item::Value>,
}

pub mod catalog_item {
    use super::*;

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct V1 {
        pub create_sql: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub enum Value {
        V1(V1),
    }
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct CatalogItemId {
    pub value: Option<catalog_item_id::Value>,
}

pub mod catalog_item_id {
    use super::*;

    #[derive(
        Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub enum Value {
        System(u64),
        User(u64),
        Transient(u64),
    }
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct SystemCatalogItemId {
    pub value: u64,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct GlobalId {
    pub value: Option<global_id::Value>,
}

pub mod global_id {
    use super::*;

    #[derive(
        Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub enum Value {
        System(u64),
        User(u64),
        Transient(u64),
        Explain(Empty),
    }
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct SystemGlobalId {
    pub value: u64,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ClusterId {
    pub value: Option<cluster_id::Value>,
}

pub mod cluster_id {
    use super::*;

    #[derive(
        Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub enum Value {
        System(u64),
        User(u64),
    }
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct DatabaseId {
    pub value: Option<database_id::Value>,
}

pub mod database_id {
    use super::*;

    #[derive(
        Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub enum Value {
        System(u64),
        User(u64),
    }
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ResolvedDatabaseSpecifier {
    pub spec: Option<resolved_database_specifier::Spec>,
}

pub mod resolved_database_specifier {
    use super::*;

    #[derive(
        Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub enum Spec {
        Ambient(Empty),
        Id(DatabaseId),
    }
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct SchemaId {
    pub value: Option<schema_id::Value>,
}

pub mod schema_id {
    use super::*;

    #[derive(
        Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub enum Value {
        System(u64),
        User(u64),
    }
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct SchemaSpecifier {
    pub spec: Option<schema_specifier::Spec>,
}

pub mod schema_specifier {
    use super::*;

    #[derive(
        Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub enum Spec {
        Temporary(Empty),
        Id(SchemaId),
    }
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ResolvedSchema {
    pub database: Option<ResolvedDatabaseSpecifier>,
    pub schema: Option<SchemaSpecifier>,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ReplicaId {
    pub value: Option<replica_id::Value>,
}

pub mod replica_id {
    use super::*;

    #[derive(
        Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub enum Value {
        System(u64),
        User(u64),
    }
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ClusterReplicaId {
    pub cluster_id: Option<ClusterId>,
    pub replica_id: Option<ReplicaId>,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct NetworkPolicyId {
    pub value: Option<network_policy_id::Value>,
}

pub mod network_policy_id {
    use super::*;

    #[derive(
        Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub enum Value {
        System(u64),
        User(u64),
    }
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ReplicaLogging {
    pub log_logging: bool,
    pub interval: Option<Duration>,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct OptimizerFeatureOverride {
    pub name: String,
    pub value: String,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ClusterScheduleRefreshOptions {
    pub rehydration_time_estimate: Option<Duration>,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ClusterSchedule {
    pub value: Option<cluster_schedule::Value>,
}

pub mod cluster_schedule {
    use super::*;

    #[derive(
        Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub enum Value {
        Manual(Empty),
        Refresh(ClusterScheduleRefreshOptions),
    }
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ClusterConfig {
    pub workload_class: Option<String>,
    pub variant: Option<cluster_config::Variant>,
}

pub mod cluster_config {
    use super::*;

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct ManagedCluster {
        pub size: String,
        pub replication_factor: u32,
        pub availability_zones: Vec<String>,
        pub logging: Option<ReplicaLogging>,
        pub disk: bool,
        pub optimizer_feature_overrides: Vec<OptimizerFeatureOverride>,
        pub schedule: Option<ClusterSchedule>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub enum Variant {
        Unmanaged(Empty),
        Managed(ManagedCluster),
    }
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct ReplicaConfig {
    pub logging: Option<ReplicaLogging>,
    pub location: Option<replica_config::Location>,
}

pub mod replica_config {
    use super::*;

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct UnmanagedLocation {
        pub storagectl_addrs: Vec<String>,
        pub storage_addrs: Vec<String>,
        pub computectl_addrs: Vec<String>,
        pub compute_addrs: Vec<String>,
        pub workers: u64,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct ManagedLocation {
        pub size: String,
        pub availability_zone: Option<String>,
        pub disk: bool,
        pub internal: bool,
        pub billed_as: Option<String>,
        pub pending: bool,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub enum Location {
        Unmanaged(UnmanagedLocation),
        Managed(ManagedLocation),
    }
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct RoleId {
    pub value: Option<role_id::Value>,
}

pub mod role_id {
    use super::*;

    #[derive(
        Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub enum Value {
        System(u64),
        User(u64),
        Public(Empty),
        Predefined(u64),
    }
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct RoleAttributes {
    pub inherit: bool,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct RoleMembership {
    pub map: Vec<role_membership::Entry>,
}

pub mod role_membership {
    use super::*;

    #[derive(
        Clone,
        Copy,
        Debug,
        Default,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Serialize,
        Deserialize,
        Arbitrary,
    )]
    pub struct Entry {
        pub key: Option<RoleId>,
        pub value: Option<RoleId>,
    }
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct RoleVars {
    pub entries: Vec<role_vars::Entry>,
}

pub mod role_vars {
    use super::*;

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct SqlSet {
        pub entries: Vec<String>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct Entry {
        pub key: String,
        pub val: Option<entry::Val>,
    }

    pub mod entry {
        use super::*;

        #[derive(
            Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
        )]
        pub enum Val {
            Flat(String),
            SqlSet(SqlSet),
        }
    }
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct NetworkPolicyRule {
    pub name: String,
    pub address: String,
    pub action: Option<network_policy_rule::Action>,
    pub direction: Option<network_policy_rule::Direction>,
}

pub mod network_policy_rule {
    use super::*;

    #[derive(
        Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub enum Action {
        Allow(Empty),
    }

    #[derive(
        Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub enum Direction {
        Ingress(Empty),
    }
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct AclMode {
    pub bitflags: u64,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct MzAclItem {
    pub grantee: Option<RoleId>,
    pub grantor: Option<RoleId>,
    pub acl_mode: Option<AclMode>,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct DefaultPrivilegesKey {
    pub role_id: Option<RoleId>,
    pub database_id: Option<DatabaseId>,
    pub schema_id: Option<SchemaId>,
    pub object_type: i32,
    pub grantee: Option<RoleId>,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct DefaultPrivilegesValue {
    pub privileges: Option<AclMode>,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct SystemPrivilegesKey {
    pub grantee: Option<RoleId>,
    pub grantor: Option<RoleId>,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct SystemPrivilegesValue {
    pub acl_mode: Option<AclMode>,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct AuditLogEventV1 {
    pub id: u64,
    pub event_type: i32,
    pub object_type: i32,
    pub user: Option<StringWrapper>,
    pub occurred_at: Option<EpochMillis>,
    pub details: Option<audit_log_event_v1::Details>,
}

pub mod audit_log_event_v1 {
    use super::*;

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct IdFullNameV1 {
        pub id: String,
        pub name: Option<FullNameV1>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct FullNameV1 {
        pub database: String,
        pub schema: String,
        pub item: String,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct IdNameV1 {
        pub id: String,
        pub name: String,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct RenameClusterV1 {
        pub id: String,
        pub old_name: String,
        pub new_name: String,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct RenameClusterReplicaV1 {
        pub cluster_id: String,
        pub replica_id: String,
        pub old_name: String,
        pub new_name: String,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct RenameItemV1 {
        pub id: String,
        pub old_name: Option<FullNameV1>,
        pub new_name: Option<FullNameV1>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct CreateClusterReplicaV1 {
        pub cluster_id: String,
        pub cluster_name: String,
        pub replica_id: Option<StringWrapper>,
        pub replica_name: String,
        pub logical_size: String,
        pub disk: bool,
        pub billed_as: Option<String>,
        pub internal: bool,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct CreateClusterReplicaV2 {
        pub cluster_id: String,
        pub cluster_name: String,
        pub replica_id: Option<StringWrapper>,
        pub replica_name: String,
        pub logical_size: String,
        pub disk: bool,
        pub billed_as: Option<String>,
        pub internal: bool,
        pub reason: Option<CreateOrDropClusterReplicaReasonV1>,
        pub scheduling_policies: Option<SchedulingDecisionsWithReasonsV1>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct CreateClusterReplicaV3 {
        pub cluster_id: String,
        pub cluster_name: String,
        pub replica_id: Option<StringWrapper>,
        pub replica_name: String,
        pub logical_size: String,
        pub disk: bool,
        pub billed_as: Option<String>,
        pub internal: bool,
        pub reason: Option<CreateOrDropClusterReplicaReasonV1>,
        pub scheduling_policies: Option<SchedulingDecisionsWithReasonsV2>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct DropClusterReplicaV1 {
        pub cluster_id: String,
        pub cluster_name: String,
        pub replica_id: Option<StringWrapper>,
        pub replica_name: String,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct DropClusterReplicaV2 {
        pub cluster_id: String,
        pub cluster_name: String,
        pub replica_id: Option<StringWrapper>,
        pub replica_name: String,
        pub reason: Option<CreateOrDropClusterReplicaReasonV1>,
        pub scheduling_policies: Option<SchedulingDecisionsWithReasonsV1>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct DropClusterReplicaV3 {
        pub cluster_id: String,
        pub cluster_name: String,
        pub replica_id: Option<StringWrapper>,
        pub replica_name: String,
        pub reason: Option<CreateOrDropClusterReplicaReasonV1>,
        pub scheduling_policies: Option<SchedulingDecisionsWithReasonsV2>,
    }

    #[derive(
        Clone,
        Copy,
        Debug,
        Default,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Serialize,
        Deserialize,
        Arbitrary,
    )]
    pub struct CreateOrDropClusterReplicaReasonV1 {
        pub reason: Option<create_or_drop_cluster_replica_reason_v1::Reason>,
    }

    pub mod create_or_drop_cluster_replica_reason_v1 {
        use super::*;

        #[derive(
            Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
        )]
        pub enum Reason {
            Manual(Empty),
            Schedule(Empty),
            System(Empty),
        }
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct SchedulingDecisionsWithReasonsV1 {
        pub on_refresh: Option<RefreshDecisionWithReasonV1>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct SchedulingDecisionsWithReasonsV2 {
        pub on_refresh: Option<RefreshDecisionWithReasonV2>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct RefreshDecisionWithReasonV1 {
        pub objects_needing_refresh: Vec<String>,
        pub rehydration_time_estimate: String,
        pub decision: Option<refresh_decision_with_reason_v1::Decision>,
    }

    pub mod refresh_decision_with_reason_v1 {
        use super::*;

        #[derive(
            Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
        )]
        pub enum Decision {
            On(Empty),
            Off(Empty),
        }
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct RefreshDecisionWithReasonV2 {
        pub objects_needing_refresh: Vec<String>,
        pub objects_needing_compaction: Vec<String>,
        pub rehydration_time_estimate: String,
        pub decision: Option<refresh_decision_with_reason_v2::Decision>,
    }

    pub mod refresh_decision_with_reason_v2 {
        use super::*;

        #[derive(
            Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
        )]
        pub enum Decision {
            On(Empty),
            Off(Empty),
        }
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct CreateSourceSinkV1 {
        pub id: String,
        pub name: Option<FullNameV1>,
        pub size: Option<StringWrapper>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct CreateSourceSinkV2 {
        pub id: String,
        pub name: Option<FullNameV1>,
        pub size: Option<StringWrapper>,
        pub external_type: String,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct CreateSourceSinkV3 {
        pub id: String,
        pub name: Option<FullNameV1>,
        pub external_type: String,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct CreateSourceSinkV4 {
        pub id: String,
        pub cluster_id: Option<StringWrapper>,
        pub name: Option<FullNameV1>,
        pub external_type: String,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct CreateIndexV1 {
        pub id: String,
        pub cluster_id: String,
        pub name: Option<FullNameV1>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct CreateMaterializedViewV1 {
        pub id: String,
        pub cluster_id: String,
        pub name: Option<FullNameV1>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct AlterSourceSinkV1 {
        pub id: String,
        pub name: Option<FullNameV1>,
        pub old_size: Option<StringWrapper>,
        pub new_size: Option<StringWrapper>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct AlterSetClusterV1 {
        pub id: String,
        pub name: Option<FullNameV1>,
        pub old_cluster: Option<StringWrapper>,
        pub new_cluster: Option<StringWrapper>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct GrantRoleV1 {
        pub role_id: String,
        pub member_id: String,
        pub grantor_id: String,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct GrantRoleV2 {
        pub role_id: String,
        pub member_id: String,
        pub grantor_id: String,
        pub executed_by: String,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct RevokeRoleV1 {
        pub role_id: String,
        pub member_id: String,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct RevokeRoleV2 {
        pub role_id: String,
        pub member_id: String,
        pub grantor_id: String,
        pub executed_by: String,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct UpdatePrivilegeV1 {
        pub object_id: String,
        pub grantee_id: String,
        pub grantor_id: String,
        pub privileges: String,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct AlterDefaultPrivilegeV1 {
        pub role_id: String,
        pub database_id: Option<StringWrapper>,
        pub schema_id: Option<StringWrapper>,
        pub grantee_id: String,
        pub privileges: String,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct UpdateOwnerV1 {
        pub object_id: String,
        pub old_owner_id: String,
        pub new_owner_id: String,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct SchemaV1 {
        pub id: String,
        pub name: String,
        pub database_name: String,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct SchemaV2 {
        pub id: String,
        pub name: String,
        pub database_name: Option<StringWrapper>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct RenameSchemaV1 {
        pub id: String,
        pub database_name: Option<String>,
        pub old_name: String,
        pub new_name: String,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct UpdateItemV1 {
        pub id: String,
        pub name: Option<FullNameV1>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct AlterRetainHistoryV1 {
        pub id: String,
        pub old_history: Option<String>,
        pub new_history: Option<String>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct ToNewIdV1 {
        pub id: String,
        pub new_id: String,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct FromPreviousIdV1 {
        pub id: String,
        pub previous_id: String,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct SetV1 {
        pub name: String,
        pub value: Option<String>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct RotateKeysV1 {
        pub id: String,
        pub name: String,
    }

    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        Arbitrary,
        IntoPrimitive,
        TryFromPrimitive,
    )]
    #[repr(i32)]
    pub enum EventType {
        Unknown = 0,
        Create = 1,
        Drop = 2,
        Alter = 3,
        Grant = 4,
        Revoke = 5,
        Comment = 6,
    }

    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        Arbitrary,
        IntoPrimitive,
        TryFromPrimitive,
    )]
    #[repr(i32)]
    pub enum ObjectType {
        Unknown = 0,
        Cluster = 1,
        ClusterReplica = 2,
        Connection = 3,
        Database = 4,
        Func = 5,
        Index = 6,
        MaterializedView = 7,
        Role = 8,
        Secret = 9,
        Schema = 10,
        Sink = 11,
        Source = 12,
        Table = 13,
        Type = 14,
        View = 15,
        System = 16,
        ContinualTask = 17,
        NetworkPolicy = 18,
    }
    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub enum Details {
        CreateClusterReplicaV1(CreateClusterReplicaV1),
        CreateClusterReplicaV2(CreateClusterReplicaV2),
        CreateClusterReplicaV3(CreateClusterReplicaV3),
        DropClusterReplicaV1(DropClusterReplicaV1),
        DropClusterReplicaV2(DropClusterReplicaV2),
        DropClusterReplicaV3(DropClusterReplicaV3),
        CreateSourceSinkV1(CreateSourceSinkV1),
        CreateSourceSinkV2(CreateSourceSinkV2),
        AlterSourceSinkV1(AlterSourceSinkV1),
        AlterSetClusterV1(AlterSetClusterV1),
        GrantRoleV1(GrantRoleV1),
        GrantRoleV2(GrantRoleV2),
        RevokeRoleV1(RevokeRoleV1),
        RevokeRoleV2(RevokeRoleV2),
        UpdatePrivilegeV1(UpdatePrivilegeV1),
        AlterDefaultPrivilegeV1(AlterDefaultPrivilegeV1),
        UpdateOwnerV1(UpdateOwnerV1),
        IdFullNameV1(IdFullNameV1),
        RenameClusterV1(RenameClusterV1),
        RenameClusterReplicaV1(RenameClusterReplicaV1),
        RenameItemV1(RenameItemV1),
        IdNameV1(IdNameV1),
        SchemaV1(SchemaV1),
        SchemaV2(SchemaV2),
        RenameSchemaV1(RenameSchemaV1),
        UpdateItemV1(UpdateItemV1),
        CreateSourceSinkV3(CreateSourceSinkV3),
        AlterRetainHistoryV1(AlterRetainHistoryV1),
        ToNewIdV1(ToNewIdV1),
        FromPreviousIdV1(FromPreviousIdV1),
        SetV1(SetV1),
        ResetAllV1(Empty),
        RotateKeysV1(RotateKeysV1),
        CreateSourceSinkV4(CreateSourceSinkV4),
        CreateIndexV1(CreateIndexV1),
        CreateMaterializedViewV1(CreateMaterializedViewV1),
    }
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub struct StateUpdateKind {
    pub kind: Option<state_update_kind::Kind>,
}

pub mod state_update_kind {
    use super::*;

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct AuditLog {
        pub key: Option<AuditLogKey>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct Cluster {
        pub key: Option<ClusterKey>,
        pub value: Option<ClusterValue>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct ClusterReplica {
        pub key: Option<ClusterReplicaKey>,
        pub value: Option<ClusterReplicaValue>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct Comment {
        pub key: Option<CommentKey>,
        pub value: Option<CommentValue>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct Config {
        pub key: Option<ConfigKey>,
        pub value: Option<ConfigValue>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct Database {
        pub key: Option<DatabaseKey>,
        pub value: Option<DatabaseValue>,
    }

    #[derive(
        Clone,
        Copy,
        Debug,
        Default,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Serialize,
        Deserialize,
        Arbitrary,
    )]
    pub struct DefaultPrivileges {
        pub key: Option<DefaultPrivilegesKey>,
        pub value: Option<DefaultPrivilegesValue>,
    }

    #[derive(
        Clone,
        Copy,
        Debug,
        Default,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Serialize,
        Deserialize,
        Arbitrary,
    )]
    pub struct FenceToken {
        pub deploy_generation: u64,
        pub epoch: i64,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct IdAlloc {
        pub key: Option<IdAllocKey>,
        pub value: Option<IdAllocValue>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct ClusterIntrospectionSourceIndex {
        pub key: Option<ClusterIntrospectionSourceIndexKey>,
        pub value: Option<ClusterIntrospectionSourceIndexValue>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct Item {
        pub key: Option<ItemKey>,
        pub value: Option<ItemValue>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct Role {
        pub key: Option<RoleKey>,
        pub value: Option<RoleValue>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct NetworkPolicy {
        pub key: Option<NetworkPolicyKey>,
        pub value: Option<NetworkPolicyValue>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct Schema {
        pub key: Option<SchemaKey>,
        pub value: Option<SchemaValue>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct Setting {
        pub key: Option<SettingKey>,
        pub value: Option<SettingValue>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct ServerConfiguration {
        pub key: Option<ServerConfigurationKey>,
        pub value: Option<ServerConfigurationValue>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct SourceReferences {
        pub key: Option<SourceReferencesKey>,
        pub value: Option<SourceReferencesValue>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct GidMapping {
        pub key: Option<GidMappingKey>,
        pub value: Option<GidMappingValue>,
    }

    #[derive(
        Clone,
        Copy,
        Debug,
        Default,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Serialize,
        Deserialize,
        Arbitrary,
    )]
    pub struct SystemPrivileges {
        pub key: Option<SystemPrivilegesKey>,
        pub value: Option<SystemPrivilegesValue>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct StorageCollectionMetadata {
        pub key: Option<StorageCollectionMetadataKey>,
        pub value: Option<StorageCollectionMetadataValue>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct UnfinalizedShard {
        pub key: Option<UnfinalizedShardKey>,
    }

    #[derive(
        Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct TxnWalShard {
        pub value: Option<TxnWalShardValue>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    // Serialize the top-level enum in the persist-backed catalog as internally tagged to set up
    // persist pushdown statistics for success.
    #[serde(tag = "kind")]
    pub enum Kind {
        AuditLog(AuditLog),
        Cluster(Cluster),
        ClusterReplica(ClusterReplica),
        Comment(Comment),
        Config(Config),
        Database(Database),
        DefaultPrivileges(DefaultPrivileges),
        IdAlloc(IdAlloc),
        ClusterIntrospectionSourceIndex(ClusterIntrospectionSourceIndex),
        Item(Item),
        Role(Role),
        Schema(Schema),
        Setting(Setting),
        ServerConfiguration(ServerConfiguration),
        GidMapping(GidMapping),
        SystemPrivileges(SystemPrivileges),
        StorageCollectionMetadata(StorageCollectionMetadata),
        UnfinalizedShard(UnfinalizedShard),
        TxnWalShard(TxnWalShard),
        SourceReferences(SourceReferences),
        FenceToken(FenceToken),
        NetworkPolicy(NetworkPolicy),
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Arbitrary,
    IntoPrimitive,
    TryFromPrimitive,
)]
#[repr(i32)]
pub enum CatalogItemType {
    Unknown = 0,
    Table = 1,
    Source = 2,
    Sink = 3,
    View = 4,
    MaterializedView = 5,
    Index = 6,
    Type = 7,
    Func = 8,
    Secret = 9,
    Connection = 10,
    ContinualTask = 11,
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Arbitrary,
    IntoPrimitive,
    TryFromPrimitive,
)]
#[repr(i32)]
pub enum ObjectType {
    Unknown = 0,
    Table = 1,
    View = 2,
    MaterializedView = 3,
    Source = 4,
    Sink = 5,
    Index = 6,
    Type = 7,
    Role = 8,
    Cluster = 9,
    ClusterReplica = 10,
    Secret = 11,
    Connection = 12,
    Database = 13,
    Schema = 14,
    Func = 15,
    ContinualTask = 16,
    NetworkPolicy = 17,
}
