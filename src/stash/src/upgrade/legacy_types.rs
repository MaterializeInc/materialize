// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Legacy JSON types that we used to serialize in the Stash. These were later (May 2023) replaced
//! with protobuf messages. These types only exist to facilitate the migration to protobuf, and can
//! be deleted post upgrade.

use bitflags::bitflags;
use proptest::arbitrary::any;
use proptest::strategy::{BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize, Serializer};
use timely::progress::Antichain;
use uuid::Uuid;

use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::Duration;

const SYSTEM_CHAR: char = 's';
const USER_CHAR: char = 'u';
const PUBLIC_CHAR: char = 'p';

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct ConfigValue {
    pub value: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct SettingKey {
    pub name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct SettingValue {
    pub value: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct IdAllocKey {
    pub name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct IdAllocValue {
    pub next_id: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct GidMappingKey {
    pub schema_name: String,
    pub object_type: CatalogItemType,
    pub object_name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct GidMappingValue {
    pub id: u64,
    pub fingerprint: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct ClusterKey {
    pub id: ClusterId,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct ClusterValue {
    pub name: String,
    pub linked_object_id: Option<GlobalId>,
    pub owner_id: RoleId,
    pub privileges: Option<Vec<MzAclItem>>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct ClusterReplicaKey {
    pub id: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct ClusterReplicaValue {
    #[serde(rename = "compute_instance_id")] // historical name
    pub cluster_id: ClusterId,
    pub name: String,
    pub config: SerializedReplicaConfig,
    pub owner_id: RoleId,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct SerializedReplicaConfig {
    pub location: SerializedReplicaLocation,
    pub logging: SerializedReplicaLogging,
    pub idle_arrangement_merge_effort: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub enum SerializedReplicaLocation {
    Unmanaged {
        storagectl_addrs: Vec<String>,
        storage_addrs: Vec<String>,
        computectl_addrs: Vec<String>,
        compute_addrs: Vec<String>,
        workers: usize,
    },
    Managed {
        size: String,
        availability_zone: String,
        /// `true` if the AZ was specified by the user and must be respected;
        /// `false` if it was picked arbitrarily by Materialize.
        az_user_specified: bool,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct SerializedReplicaLogging {
    pub log_logging: bool,
    pub interval: Option<Duration>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct ClusterIntrospectionSourceIndexKey {
    #[serde(rename = "compute_id")] // historical name
    pub cluster_id: ClusterId,
    pub name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct ClusterIntrospectionSourceIndexValue {
    pub index_id: u64,
}

#[derive(
    Debug, Default, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary,
)]
pub enum DatabaseNamespace {
    #[default]
    User,
    System,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct DatabaseKey {
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ns: Option<DatabaseNamespace>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct DatabaseValue {
    pub name: String,
    pub owner_id: RoleId,
    pub privileges: Option<Vec<MzAclItem>>,
}

#[derive(
    Debug, Default, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary,
)]
pub enum SchemaNamespace {
    #[default]
    User,
    System,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct SchemaKey {
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ns: Option<SchemaNamespace>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct SchemaValue {
    pub database_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database_ns: Option<DatabaseNamespace>,
    pub name: String,
    pub owner_id: RoleId,
    pub privileges: Option<Vec<MzAclItem>>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct TimestampKey {
    pub id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct TimestampValue {
    pub ts: MzTimestamp,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct ServerConfigurationKey {
    pub name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct ServerConfigurationValue {
    pub value: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct ItemKey {
    pub gid: GlobalId,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub enum SerializedCatalogItem {
    V1 { create_sql: String },
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct ItemValue {
    pub schema_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_ns: Option<SchemaNamespace>,
    pub name: String,
    pub definition: SerializedCatalogItem,
    pub owner_id: RoleId,
    pub privileges: Option<Vec<MzAclItem>>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct RoleKey {
    pub id: RoleId,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct RoleAttributes {
    pub inherit: bool,
    pub create_role: bool,
    pub create_db: bool,
    pub create_cluster: bool,
    _private: (),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
#[serde(into = "BTreeMap<String, RoleId>")]
#[serde(try_from = "BTreeMap<String, RoleId>")]
pub struct RoleMembership {
    pub map: BTreeMap<RoleId, RoleId>,
}

impl From<RoleMembership> for BTreeMap<String, RoleId> {
    fn from(value: RoleMembership) -> Self {
        value
            .map
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    }
}

impl TryFrom<BTreeMap<String, RoleId>> for RoleMembership {
    type Error = anyhow::Error;

    fn try_from(value: BTreeMap<String, RoleId>) -> Result<Self, Self::Error> {
        Ok(RoleMembership {
            map: value
                .into_iter()
                .map(|(k, v)| Ok((RoleId::from_str(&k)?, v)))
                .collect::<Result<_, anyhow::Error>>()?,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct SerializedRole {
    pub name: String,
    pub attributes: Option<RoleAttributes>,
    pub membership: Option<RoleMembership>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct RoleValue {
    #[serde(flatten)]
    pub role: SerializedRole,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct AuditLogKey {
    pub event: VersionedEvent,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub enum VersionedEvent {
    V1(EventV1),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct EventV1 {
    pub id: u64,
    pub event_type: EventType,
    pub object_type: ObjectType,
    pub details: EventDetails,
    pub user: Option<String>,
    pub occurred_at: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
#[serde(rename_all = "kebab-case")]
pub enum EventType {
    Create,
    Drop,
    Alter,
    Grant,
    Revoke,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
#[serde(rename_all = "kebab-case")]
pub enum ObjectType {
    Cluster,
    ClusterReplica,
    Connection,
    Database,
    Func,
    Index,
    MaterializedView,
    Role,
    Secret,
    Schema,
    Sink,
    Source,
    Table,
    Type,
    View,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub enum EventDetails {
    #[serde(rename = "CreateComputeReplicaV1")] // historical name
    CreateClusterReplicaV1(CreateClusterReplicaV1),
    #[serde(rename = "DropComputeReplicaV1")] // historical name
    DropClusterReplicaV1(DropClusterReplicaV1),
    CreateSourceSinkV1(CreateSourceSinkV1),
    CreateSourceSinkV2(CreateSourceSinkV2),
    AlterSourceSinkV1(AlterSourceSinkV1),
    GrantRoleV1(GrantRoleV1),
    GrantRoleV2(GrantRoleV2),
    RevokeRoleV1(RevokeRoleV1),
    RevokeRoleV2(RevokeRoleV2),
    IdFullNameV1(IdFullNameV1),
    RenameItemV1(RenameItemV1),
    IdNameV1(IdNameV1),
    SchemaV1(SchemaV1),
    SchemaV2(SchemaV2),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct IdFullNameV1 {
    pub id: String,
    #[serde(flatten)]
    pub name: FullNameV1,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct FullNameV1 {
    pub database: String,
    pub schema: String,
    pub item: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct IdNameV1 {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct RenameItemV1 {
    pub id: String,
    pub old_name: FullNameV1,
    pub new_name: FullNameV1,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct DropClusterReplicaV1 {
    pub cluster_id: String,
    pub cluster_name: String,
    // Events that predate v0.32.0 will not have this field set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replica_id: Option<String>,
    pub replica_name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct CreateClusterReplicaV1 {
    pub cluster_id: String,
    pub cluster_name: String,
    // Events that predate v0.32.0 will not have this field set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replica_id: Option<String>,
    pub replica_name: String,
    pub logical_size: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct CreateSourceSinkV1 {
    pub id: String,
    #[serde(flatten)]
    pub name: FullNameV1,
    pub size: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct CreateSourceSinkV2 {
    pub id: String,
    #[serde(flatten)]
    pub name: FullNameV1,
    pub size: Option<String>,
    #[serde(rename = "type")]
    pub external_type: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct AlterSourceSinkV1 {
    pub id: String,
    #[serde(flatten)]
    pub name: FullNameV1,
    pub old_size: Option<String>,
    pub new_size: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct GrantRoleV1 {
    pub role_id: String,
    pub member_id: String,
    pub grantor_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct GrantRoleV2 {
    pub role_id: String,
    pub member_id: String,
    pub grantor_id: String,
    pub executed_by: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct RevokeRoleV1 {
    pub role_id: String,
    pub member_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct RevokeRoleV2 {
    pub role_id: String,
    pub member_id: String,
    pub grantor_id: String,
    pub executed_by: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct SchemaV1 {
    pub id: String,
    pub name: String,
    pub database_name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct SchemaV2 {
    pub id: String,
    pub name: String,
    pub database_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct StorageUsageKey {
    pub metric: VersionedStorageUsage,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub enum VersionedStorageUsage {
    V1(StorageUsageV1),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct StorageUsageV1 {
    pub id: u64,
    pub shard_id: Option<String>,
    pub size_bytes: u64,
    pub collection_timestamp: u64,
}

#[derive(Arbitrary, Clone, Debug, PartialEq, PartialOrd, Ord, Eq, Serialize, Deserialize)]
pub struct DurableCollectionMetadata {
    pub remap_shard: Option<ShardId>,
    pub data_shard: ShardId,
}

#[derive(Debug, Clone, Deserialize, Serialize, Arbitrary)]
pub struct DurableExportMetadata {
    pub initial_as_of: SinkAsOf,
}

/// The identifier for a global dataflow.
#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub enum GlobalId {
    System(u64),
    User(u64),
    Transient(u64),
    Explain,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub enum ClusterId {
    System(u64),
    User(u64),
}

/// The identifier for a role.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub enum RoleId {
    System(u64),
    User(u64),
    Public,
}

impl ToString for RoleId {
    fn to_string(&self) -> String {
        match self {
            Self::System(id) => format!("{SYSTEM_CHAR}{id}"),
            Self::User(id) => format!("{USER_CHAR}{id}"),
            Self::Public => PUBLIC_CHAR.to_string(),
        }
    }
}

impl FromStr for RoleId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || anyhow::anyhow!("couldn't parse role id '{s}'");
        match s.chars().next() {
            Some(SYSTEM_CHAR) => {
                if s.len() < 2 {
                    return Err(err());
                }
                let val: u64 = s[1..].parse().map_err(|_| err())?;
                Ok(Self::System(val))
            }
            Some(USER_CHAR) => {
                if s.len() < 2 {
                    return Err(err());
                }
                let val: u64 = s[1..].parse().map_err(|_| err())?;
                Ok(Self::User(val))
            }
            Some(PUBLIC_CHAR) => {
                if s.len() == 1 {
                    Ok(Self::Public)
                } else {
                    Err(err())
                }
            }
            _ => Err(err()),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct MzAclItem {
    /// Role that this item grants privileges to.
    pub grantee: RoleId,
    /// Grantor of privileges.
    pub grantor: RoleId,
    /// Privileges bit flag.
    pub acl_mode: AclMode,
}

bitflags! {
    /// A bit flag representing all the privileges that can be granted to a role.
    ///
    /// Modeled after:
    /// https://github.com/postgres/postgres/blob/7f5b19817eaf38e70ad1153db4e644ee9456853e/src/include/nodes/parsenodes.h#L74-L101
    #[derive(Serialize, Deserialize)]
    pub struct AclMode: u64 {
        const INSERT = 1 << 0;
        const SELECT = 1 << 1;
        const UPDATE = 1 << 2;
        const DELETE = 1 << 3;
        const USAGE = 1 << 8;
        const CREATE = 1 << 9;
    }
}

impl proptest::arbitrary::Arbitrary for AclMode {
    type Strategy = BoxedStrategy<AclMode>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        any::<u64>().prop_map(AclMode::from_bits_truncate).boxed()
    }
}

#[derive(Arbitrary, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ShardId(pub [u8; 16]);

impl std::fmt::Display for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "s{}", Uuid::from_bytes(self.0))
    }
}

impl std::fmt::Debug for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ShardId({})", Uuid::from_bytes(self.0))
    }
}

impl std::str::FromStr for ShardId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_id('s', "ShardId", s).map(ShardId)
    }
}

impl From<ShardId> for String {
    fn from(shard_id: ShardId) -> Self {
        shard_id.to_string()
    }
}

impl TryFrom<String> for ShardId {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

pub(crate) fn parse_id(id_prefix: char, id_type: &str, encoded: &str) -> Result<[u8; 16], String> {
    let uuid_encoded = match encoded.strip_prefix(id_prefix) {
        Some(x) => x,
        None => return Err(format!("invalid {} {}: incorrect prefix", id_type, encoded)),
    };
    let uuid = Uuid::parse_str(uuid_encoded)
        .map_err(|err| format!("invalid {} {}: {}", id_type, encoded, err))?;
    Ok(*uuid.as_bytes())
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SinkAsOf {
    pub frontier: Antichain<MzTimestamp>,
    pub strict: bool,
}

impl proptest::arbitrary::Arbitrary for SinkAsOf {
    type Strategy = BoxedStrategy<SinkAsOf>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        let elements = proptest::collection::vec(any::<MzTimestamp>(), 0..256);
        let strict = any::<bool>();

        (elements, strict)
            .prop_map(|(elements, strict)| {
                let mut frontier = Antichain::new();
                frontier.extend(elements);
                SinkAsOf { frontier, strict }
            })
            .boxed()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub enum CatalogItemType {
    /// A table.
    Table,
    /// A source.
    Source,
    /// A sink.
    Sink,
    /// A view.
    View,
    /// A materialized view.
    MaterializedView,
    /// An index.
    Index,
    /// A type.
    Type,
    /// A func.
    Func,
    /// A secret.
    Secret,
    /// A connection.
    Connection,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Default, Arbitrary)]
pub struct MzTimestamp {
    pub internal: u64,
}

impl timely::PartialOrder for MzTimestamp {
    fn less_equal(&self, other: &Self) -> bool {
        self.internal.less_equal(&other.internal)
    }
}

impl Serialize for MzTimestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.internal.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MzTimestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self {
            internal: u64::deserialize(deserializer)?,
        })
    }
}
