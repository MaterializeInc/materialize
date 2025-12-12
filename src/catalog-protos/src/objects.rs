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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ConfigKey {
    pub key: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ConfigValue {
    pub value: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct SettingKey {
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct SettingValue {
    pub value: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct IdAllocKey {
    pub name: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct IdAllocValue {
    pub next_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct GidMappingKey {
    pub schema_name: String,
    pub object_type: i32,
    pub object_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct GidMappingValue {
    pub id: u64,
    pub fingerprint: String,
    pub global_id: SystemGlobalId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ClusterKey {
    pub id: ClusterId,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ClusterValue {
    pub name: String,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
    pub config: ClusterConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ClusterIntrospectionSourceIndexKey {
    pub cluster_id: ClusterId,
    pub name: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ClusterIntrospectionSourceIndexValue {
    pub index_id: u64,
    pub oid: u32,
    pub global_id: IntrospectionSourceIndexGlobalId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ClusterReplicaKey {
    pub id: ReplicaId,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ClusterReplicaValue {
    pub cluster_id: ClusterId,
    pub name: String,
    pub config: ReplicaConfig,
    pub owner_id: RoleId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct DatabaseKey {
    pub id: DatabaseId,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct DatabaseValue {
    pub name: String,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
    pub oid: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct SchemaKey {
    pub id: SchemaId,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct SchemaValue {
    pub database_id: Option<DatabaseId>,
    pub name: String,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
    pub oid: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ItemKey {
    pub gid: CatalogItemId,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ItemValue {
    pub schema_id: SchemaId,
    pub name: String,
    pub definition: CatalogItem,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
    pub oid: u32,
    pub global_id: GlobalId,
    pub extra_versions: Vec<ItemVersion>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ItemVersion {
    pub global_id: GlobalId,
    pub version: Version,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct RoleKey {
    pub id: RoleId,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct RoleValue {
    pub name: String,
    pub attributes: RoleAttributes,
    pub membership: RoleMembership,
    pub vars: RoleVars,
    pub oid: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct RoleAuthKey {
    pub id: RoleId,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct RoleAuthValue {
    pub password_hash: Option<String>,
    pub updated_at: EpochMillis,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct NetworkPolicyKey {
    pub id: NetworkPolicyId,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct NetworkPolicyValue {
    pub name: String,
    pub rules: Vec<NetworkPolicyRule>,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
    pub oid: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ServerConfigurationKey {
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ServerConfigurationValue {
    pub value: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct AuditLogKey {
    pub event: AuditLogEvent,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum AuditLogEvent {
    V1(AuditLogEventV1),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct CommentKey {
    pub object: CommentObject,
    pub sub_component: Option<CommentSubComponent>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum CommentObject {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum CommentSubComponent {
    ColumnPos(u64),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct CommentValue {
    pub comment: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct SourceReferencesKey {
    pub source: CatalogItemId,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct SourceReferencesValue {
    pub references: Vec<SourceReference>,
    pub updated_at: EpochMillis,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct SourceReference {
    pub name: String,
    pub namespace: Option<String>,
    pub columns: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct StorageCollectionMetadataKey {
    pub id: GlobalId,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct StorageCollectionMetadataValue {
    pub shard: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct UnfinalizedShardKey {
    pub shard: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct TxnWalShardValue {
    pub shard: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct Empty {}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct StringWrapper {
    pub inner: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct Duration {
    pub secs: u64,
    pub nanos: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct EpochMillis {
    pub millis: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct Timestamp {
    pub internal: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct Version {
    pub value: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum CatalogItem {
    V1(CatalogItemV1),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct CatalogItemV1 {
    pub create_sql: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum CatalogItemId {
    System(u64),
    User(u64),
    Transient(u64),
    IntrospectionSourceIndex(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct SystemCatalogItemId {
    pub value: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct IntrospectionSourceIndexCatalogItemId {
    pub value: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum GlobalId {
    System(u64),
    User(u64),
    Transient(u64),
    Explain,
    IntrospectionSourceIndex(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct SystemGlobalId {
    pub value: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct IntrospectionSourceIndexGlobalId {
    pub value: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum ClusterId {
    System(u64),
    User(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum DatabaseId {
    System(u64),
    User(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum ResolvedDatabaseSpecifier {
    Ambient,
    Id(DatabaseId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum SchemaId {
    System(u64),
    User(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum SchemaSpecifier {
    Temporary,
    Id(SchemaId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ResolvedSchema {
    pub database: ResolvedDatabaseSpecifier,
    pub schema: SchemaSpecifier,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum ReplicaId {
    System(u64),
    User(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ClusterReplicaId {
    pub cluster_id: ClusterId,
    pub replica_id: ReplicaId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum NetworkPolicyId {
    System(u64),
    User(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ReplicaLogging {
    pub log_logging: bool,
    pub interval: Option<Duration>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct OptimizerFeatureOverride {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ClusterScheduleRefreshOptions {
    pub rehydration_time_estimate: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum ClusterSchedule {
    Manual,
    Refresh(ClusterScheduleRefreshOptions),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ClusterConfig {
    pub workload_class: Option<String>,
    pub variant: ClusterVariant,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum ClusterVariant {
    Unmanaged,
    Managed(ManagedCluster),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ManagedCluster {
    pub size: String,
    pub replication_factor: u32,
    pub availability_zones: Vec<String>,
    pub logging: ReplicaLogging,
    pub optimizer_feature_overrides: Vec<OptimizerFeatureOverride>,
    pub schedule: ClusterSchedule,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ReplicaConfig {
    pub logging: ReplicaLogging,
    pub location: ReplicaLocation,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct UnmanagedLocation {
    pub storagectl_addrs: Vec<String>,
    pub computectl_addrs: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ManagedLocation {
    pub size: String,
    pub availability_zone: Option<String>,
    pub internal: bool,
    pub billed_as: Option<String>,
    pub pending: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum ReplicaLocation {
    Unmanaged(UnmanagedLocation),
    Managed(ManagedLocation),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum RoleId {
    System(u64),
    User(u64),
    Public,
    Predefined(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct RoleAttributes {
    pub inherit: bool,
    pub superuser: Option<bool>,
    pub login: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct RoleMembership {
    pub map: Vec<RoleMembershipEntry>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct RoleMembershipEntry {
    pub key: RoleId,
    pub value: RoleId,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct RoleVars {
    pub entries: Vec<RoleVarsEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct RoleVarsEntry {
    pub key: String,
    pub val: RoleVar,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum RoleVar {
    Flat(String),
    SqlSet(Vec<String>),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct NetworkPolicyRule {
    pub name: String,
    pub address: String,
    pub action: NetworkPolicyRuleAction,
    pub direction: NetworkPolicyRuleDirection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum NetworkPolicyRuleAction {
    Allow,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum NetworkPolicyRuleDirection {
    Ingress,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct AclMode {
    pub bitflags: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct MzAclItem {
    pub grantee: RoleId,
    pub grantor: RoleId,
    pub acl_mode: AclMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct DefaultPrivilegesKey {
    pub role_id: RoleId,
    pub database_id: Option<DatabaseId>,
    pub schema_id: Option<SchemaId>,
    pub object_type: i32,
    pub grantee: RoleId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct DefaultPrivilegesValue {
    pub privileges: AclMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct SystemPrivilegesKey {
    pub grantee: RoleId,
    pub grantor: RoleId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct SystemPrivilegesValue {
    pub acl_mode: AclMode,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct AuditLogEventV1 {
    pub id: u64,
    pub event_type: i32,
    pub object_type: i32,
    pub user: Option<StringWrapper>,
    pub occurred_at: EpochMillis,
    pub details: audit_log_event_v1::Details,
}

pub mod audit_log_event_v1 {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct IdFullNameV1 {
        pub id: String,
        pub name: FullNameV1,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct FullNameV1 {
        pub database: String,
        pub schema: String,
        pub item: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct IdNameV1 {
        pub id: String,
        pub name: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct RenameClusterV1 {
        pub id: String,
        pub old_name: String,
        pub new_name: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct RenameClusterReplicaV1 {
        pub cluster_id: String,
        pub replica_id: String,
        pub old_name: String,
        pub new_name: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct RenameItemV1 {
        pub id: String,
        pub old_name: FullNameV1,
        pub new_name: FullNameV1,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
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

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct CreateClusterReplicaV2 {
        pub cluster_id: String,
        pub cluster_name: String,
        pub replica_id: Option<StringWrapper>,
        pub replica_name: String,
        pub logical_size: String,
        pub disk: bool,
        pub billed_as: Option<String>,
        pub internal: bool,
        pub reason: CreateOrDropClusterReplicaReasonV1,
        pub scheduling_policies: Option<SchedulingDecisionsWithReasonsV1>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct CreateClusterReplicaV3 {
        pub cluster_id: String,
        pub cluster_name: String,
        pub replica_id: Option<StringWrapper>,
        pub replica_name: String,
        pub logical_size: String,
        pub disk: bool,
        pub billed_as: Option<String>,
        pub internal: bool,
        pub reason: CreateOrDropClusterReplicaReasonV1,
        pub scheduling_policies: Option<SchedulingDecisionsWithReasonsV2>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct CreateClusterReplicaV4 {
        pub cluster_id: String,
        pub cluster_name: String,
        pub replica_id: Option<StringWrapper>,
        pub replica_name: String,
        pub logical_size: String,
        pub billed_as: Option<String>,
        pub internal: bool,
        pub reason: CreateOrDropClusterReplicaReasonV1,
        pub scheduling_policies: Option<SchedulingDecisionsWithReasonsV2>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct DropClusterReplicaV1 {
        pub cluster_id: String,
        pub cluster_name: String,
        pub replica_id: Option<StringWrapper>,
        pub replica_name: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct DropClusterReplicaV2 {
        pub cluster_id: String,
        pub cluster_name: String,
        pub replica_id: Option<StringWrapper>,
        pub replica_name: String,
        pub reason: CreateOrDropClusterReplicaReasonV1,
        pub scheduling_policies: Option<SchedulingDecisionsWithReasonsV1>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct DropClusterReplicaV3 {
        pub cluster_id: String,
        pub cluster_name: String,
        pub replica_id: Option<StringWrapper>,
        pub replica_name: String,
        pub reason: CreateOrDropClusterReplicaReasonV1,
        pub scheduling_policies: Option<SchedulingDecisionsWithReasonsV2>,
    }

    #[derive(
        Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub struct CreateOrDropClusterReplicaReasonV1 {
        pub reason: CreateOrDropClusterReplicaReasonV1Reason,
    }

    #[derive(
        Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
    )]
    pub enum CreateOrDropClusterReplicaReasonV1Reason {
        Manual(Empty),
        Schedule(Empty),
        System(Empty),
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct SchedulingDecisionsWithReasonsV1 {
        pub on_refresh: RefreshDecisionWithReasonV1,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct SchedulingDecisionsWithReasonsV2 {
        pub on_refresh: RefreshDecisionWithReasonV2,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub enum RefreshDecision {
        On(Empty),
        Off(Empty),
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct RefreshDecisionWithReasonV1 {
        pub objects_needing_refresh: Vec<String>,
        pub rehydration_time_estimate: String,
        pub decision: RefreshDecision,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct RefreshDecisionWithReasonV2 {
        pub objects_needing_refresh: Vec<String>,
        pub objects_needing_compaction: Vec<String>,
        pub rehydration_time_estimate: String,
        pub decision: RefreshDecision,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct CreateSourceSinkV1 {
        pub id: String,
        pub name: FullNameV1,
        pub size: Option<StringWrapper>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct CreateSourceSinkV2 {
        pub id: String,
        pub name: FullNameV1,
        pub size: Option<StringWrapper>,
        pub external_type: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct CreateSourceSinkV3 {
        pub id: String,
        pub name: FullNameV1,
        pub external_type: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct CreateSourceSinkV4 {
        pub id: String,
        pub cluster_id: Option<StringWrapper>,
        pub name: FullNameV1,
        pub external_type: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct CreateIndexV1 {
        pub id: String,
        pub cluster_id: String,
        pub name: FullNameV1,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct CreateMaterializedViewV1 {
        pub id: String,
        pub cluster_id: String,
        pub name: FullNameV1,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct AlterSourceSinkV1 {
        pub id: String,
        pub name: FullNameV1,
        pub old_size: Option<StringWrapper>,
        pub new_size: Option<StringWrapper>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct AlterSetClusterV1 {
        pub id: String,
        pub name: FullNameV1,
        pub old_cluster: Option<StringWrapper>,
        pub new_cluster: Option<StringWrapper>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct GrantRoleV1 {
        pub role_id: String,
        pub member_id: String,
        pub grantor_id: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct GrantRoleV2 {
        pub role_id: String,
        pub member_id: String,
        pub grantor_id: String,
        pub executed_by: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct RevokeRoleV1 {
        pub role_id: String,
        pub member_id: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct RevokeRoleV2 {
        pub role_id: String,
        pub member_id: String,
        pub grantor_id: String,
        pub executed_by: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct UpdatePrivilegeV1 {
        pub object_id: String,
        pub grantee_id: String,
        pub grantor_id: String,
        pub privileges: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct AlterDefaultPrivilegeV1 {
        pub role_id: String,
        pub database_id: Option<StringWrapper>,
        pub schema_id: Option<StringWrapper>,
        pub grantee_id: String,
        pub privileges: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct UpdateOwnerV1 {
        pub object_id: String,
        pub old_owner_id: String,
        pub new_owner_id: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct SchemaV1 {
        pub id: String,
        pub name: String,
        pub database_name: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct SchemaV2 {
        pub id: String,
        pub name: String,
        pub database_name: Option<StringWrapper>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct RenameSchemaV1 {
        pub id: String,
        pub database_name: Option<String>,
        pub old_name: String,
        pub new_name: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct UpdateItemV1 {
        pub id: String,
        pub name: FullNameV1,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct AlterRetainHistoryV1 {
        pub id: String,
        pub old_history: Option<String>,
        pub new_history: Option<String>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct ToNewIdV1 {
        pub id: String,
        pub new_id: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct FromPreviousIdV1 {
        pub id: String,
        pub previous_id: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
    pub struct SetV1 {
        pub name: String,
        pub value: Option<String>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
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
        CreateClusterReplicaV4(CreateClusterReplicaV4),
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

/// The contents of a single state update.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
// Serialize the top-level enum in the persist-backed catalog as internally tagged to set up
// persist pushdown statistics for success.
#[serde(tag = "kind")]
pub enum StateUpdateKind {
    AuditLog(AuditLog),
    Cluster(Cluster),
    ClusterIntrospectionSourceIndex(ClusterIntrospectionSourceIndex),
    ClusterReplica(ClusterReplica),
    Comment(Comment),
    Config(Config),
    Database(Database),
    DefaultPrivileges(DefaultPrivileges),
    FenceToken(FenceToken),
    GidMapping(GidMapping),
    IdAlloc(IdAlloc),
    Item(Item),
    NetworkPolicy(NetworkPolicy),
    Role(Role),
    RoleAuth(RoleAuth),
    Schema(Schema),
    ServerConfiguration(ServerConfiguration),
    Setting(Setting),
    SourceReferences(SourceReferences),
    StorageCollectionMetadata(StorageCollectionMetadata),
    SystemPrivileges(SystemPrivileges),
    TxnWalShard(TxnWalShard),
    UnfinalizedShard(UnfinalizedShard),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct AuditLog {
    pub key: AuditLogKey,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct Cluster {
    pub key: ClusterKey,
    pub value: ClusterValue,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ClusterReplica {
    pub key: ClusterReplicaKey,
    pub value: ClusterReplicaValue,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct Comment {
    pub key: CommentKey,
    pub value: CommentValue,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct Config {
    pub key: ConfigKey,
    pub value: ConfigValue,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct Database {
    pub key: DatabaseKey,
    pub value: DatabaseValue,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct DefaultPrivileges {
    pub key: DefaultPrivilegesKey,
    pub value: DefaultPrivilegesValue,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct FenceToken {
    pub deploy_generation: u64,
    pub epoch: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct IdAlloc {
    pub key: IdAllocKey,
    pub value: IdAllocValue,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ClusterIntrospectionSourceIndex {
    pub key: ClusterIntrospectionSourceIndexKey,
    pub value: ClusterIntrospectionSourceIndexValue,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct Item {
    pub key: ItemKey,
    pub value: ItemValue,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct Role {
    pub key: RoleKey,
    pub value: RoleValue,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct RoleAuth {
    pub key: RoleAuthKey,
    pub value: RoleAuthValue,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct NetworkPolicy {
    pub key: NetworkPolicyKey,
    pub value: NetworkPolicyValue,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct Schema {
    pub key: SchemaKey,
    pub value: SchemaValue,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct Setting {
    pub key: SettingKey,
    pub value: SettingValue,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct ServerConfiguration {
    pub key: ServerConfigurationKey,
    pub value: ServerConfigurationValue,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct SourceReferences {
    pub key: SourceReferencesKey,
    pub value: SourceReferencesValue,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct GidMapping {
    pub key: GidMappingKey,
    pub value: GidMappingValue,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct SystemPrivileges {
    pub key: SystemPrivilegesKey,
    pub value: SystemPrivilegesValue,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct StorageCollectionMetadata {
    pub key: StorageCollectionMetadataKey,
    pub value: StorageCollectionMetadataValue,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct UnfinalizedShard {
    pub key: UnfinalizedShardKey,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct TxnWalShard {
    pub value: TxnWalShardValue,
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
