// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Audit log data structures.
//!
//! The audit log is logging that is produced by user actions and consumed
//! by users in the form of the `mz_catalog.mz_audit_events` SQL table and
//! by the cloud management layer for billing and introspection. This crate
//! is designed to make the production and consumption of the logs type
//! safe. Events and their metadata are versioned and the data structures
//! replicated here so that if the data change in some other crate, a
//! new version here can be made. This avoids needing to poke at the data
//! when reading it to determine what it means and should have full backward
//! compatibility. This is its own crate so that production and consumption can
//! be in different processes and production is not allowed to specify private
//! data structures unknown to the reader.

use mz_ore::now::EpochMillis;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

/// New version variants should be added if fields need to be added, changed, or removed.
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub enum VersionedEvent {
    V1(EventV1),
}

impl VersionedEvent {
    /// Create a new event. This function must always require and produce the most
    /// recent variant of VersionedEvent. `id` must be a globally increasing,
    /// ordered number such that sorting by it on all events yields the order
    /// of events by users. It is insufficient to use `occurred_at` (even at
    /// nanosecond precision) due to clock unpredictability.
    pub fn new(
        id: u64,
        event_type: EventType,
        object_type: ObjectType,
        details: EventDetails,
        user: Option<String>,
        occurred_at: EpochMillis,
    ) -> Self {
        Self::V1(EventV1::new(
            id,
            event_type,
            object_type,
            details,
            user,
            occurred_at,
        ))
    }

    // Implement deserialize and serialize so writers and readers don't have to
    // coordinate about which Serializer to use.
    pub fn deserialize(data: &[u8]) -> Result<Self, anyhow::Error> {
        Ok(serde_json::from_slice(data)?)
    }

    pub fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("must serialize")
    }

    /// Returns a globally sortable event order. All event versions must have this
    /// field.
    pub fn sortable_id(&self) -> u64 {
        match self {
            VersionedEvent::V1(ev) => ev.id,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
#[serde(rename_all = "kebab-case")]
pub enum EventType {
    Create,
    Drop,
    Alter,
    Grant,
    Revoke,
}

impl EventType {
    pub fn as_title_case(&self) -> &'static str {
        match self {
            EventType::Create => "Created",
            EventType::Drop => "Dropped",
            EventType::Alter => "Altered",
            EventType::Grant => "Granted",
            EventType::Revoke => "Revoked",
        }
    }
}

serde_plain::derive_display_from_serialize!(EventType);

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
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
    System,
    Table,
    Type,
    View,
}

impl ObjectType {
    pub fn as_title_case(&self) -> &'static str {
        match self {
            ObjectType::Cluster => "Cluster",
            ObjectType::ClusterReplica => "Cluster Replica",
            ObjectType::Connection => "Connection",
            ObjectType::Database => "Database",
            ObjectType::Func => "Function",
            ObjectType::Index => "Index",
            ObjectType::MaterializedView => "Materialized View",
            ObjectType::Role => "Role",
            ObjectType::Schema => "Schema",
            ObjectType::Secret => "Secret",
            ObjectType::Sink => "Sink",
            ObjectType::Source => "Source",
            ObjectType::System => "System",
            ObjectType::Table => "Table",
            ObjectType::Type => "Type",
            ObjectType::View => "View",
        }
    }
}

serde_plain::derive_display_from_serialize!(ObjectType);

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub enum EventDetails {
    #[serde(rename = "CreateComputeReplicaV1")] // historical name
    CreateClusterReplicaV1(CreateClusterReplicaV1),
    #[serde(rename = "DropComputeReplicaV1")] // historical name
    DropClusterReplicaV1(DropClusterReplicaV1),
    CreateSourceSinkV1(CreateSourceSinkV1),
    CreateSourceSinkV2(CreateSourceSinkV2),
    CreateSourceSinkV3(CreateSourceSinkV3),
    AlterSetClusterV1(AlterSetClusterV1),
    AlterSourceSinkV1(AlterSourceSinkV1),
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
    UpdateItemV1(UpdateItemV1),
    RenameSchemaV1(RenameSchemaV1),
    AlterRetainHistoryV1(AlterRetainHistoryV1),
    ToNewIdV1(ToNewIdV1),
    FromPreviousIdV1(FromPreviousIdV1),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct IdFullNameV1 {
    pub id: String,
    #[serde(flatten)]
    pub name: FullNameV1,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct FullNameV1 {
    pub database: String,
    pub schema: String,
    pub item: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct IdNameV1 {
    pub id: String,
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct RenameItemV1 {
    pub id: String,
    pub old_name: FullNameV1,
    pub new_name: FullNameV1,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct RenameClusterV1 {
    pub id: String,
    pub old_name: String,
    pub new_name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct RenameClusterReplicaV1 {
    pub cluster_id: String,
    pub replica_id: String,
    pub old_name: String,
    pub new_name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct DropClusterReplicaV1 {
    pub cluster_id: String,
    pub cluster_name: String,
    // Events that predate v0.32.0 will not have this field set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replica_id: Option<String>,
    pub replica_name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct CreateClusterReplicaV1 {
    pub cluster_id: String,
    pub cluster_name: String,
    // Events that predate v0.32.0 will not have this field set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replica_id: Option<String>,
    pub replica_name: String,
    pub logical_size: String,
    pub disk: bool,
    pub billed_as: Option<String>,
    pub internal: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct CreateSourceSinkV1 {
    pub id: String,
    #[serde(flatten)]
    pub name: FullNameV1,
    pub size: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct CreateSourceSinkV2 {
    pub id: String,
    #[serde(flatten)]
    pub name: FullNameV1,
    pub size: Option<String>,
    #[serde(rename = "type")]
    pub external_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct CreateSourceSinkV3 {
    pub id: String,
    #[serde(flatten)]
    pub name: FullNameV1,
    #[serde(rename = "type")]
    pub external_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct AlterSourceSinkV1 {
    pub id: String,
    #[serde(flatten)]
    pub name: FullNameV1,
    pub old_size: Option<String>,
    pub new_size: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct AlterSetClusterV1 {
    pub id: String,
    #[serde(flatten)]
    pub name: FullNameV1,
    pub old_cluster: Option<String>,
    pub new_cluster: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct GrantRoleV1 {
    pub role_id: String,
    pub member_id: String,
    pub grantor_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct GrantRoleV2 {
    pub role_id: String,
    pub member_id: String,
    pub grantor_id: String,
    pub executed_by: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct RevokeRoleV1 {
    pub role_id: String,
    pub member_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct RevokeRoleV2 {
    pub role_id: String,
    pub member_id: String,
    pub grantor_id: String,
    pub executed_by: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct UpdatePrivilegeV1 {
    pub object_id: String,
    pub grantee_id: String,
    pub grantor_id: String,
    pub privileges: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct AlterDefaultPrivilegeV1 {
    pub role_id: String,
    pub database_id: Option<String>,
    pub schema_id: Option<String>,
    pub grantee_id: String,
    pub privileges: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct UpdateOwnerV1 {
    pub object_id: String,
    pub old_owner_id: String,
    pub new_owner_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct SchemaV1 {
    pub id: String,
    pub name: String,
    pub database_name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct SchemaV2 {
    pub id: String,
    pub name: String,
    pub database_name: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct RenameSchemaV1 {
    pub id: String,
    pub database_name: Option<String>,
    pub old_name: String,
    pub new_name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct AlterRetainHistoryV1 {
    pub id: String,
    pub old_history: Option<String>,
    pub new_history: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct UpdateItemV1 {
    pub id: String,
    #[serde(flatten)]
    pub name: FullNameV1,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct ToNewIdV1 {
    pub id: String,
    pub new_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct FromPreviousIdV1 {
    pub id: String,
    pub previous_id: String,
}

impl EventDetails {
    pub fn as_json(&self) -> serde_json::Value {
        match self {
            EventDetails::CreateClusterReplicaV1(v) => {
                serde_json::to_value(v).expect("must serialize")
            }
            EventDetails::DropClusterReplicaV1(v) => {
                serde_json::to_value(v).expect("must serialize")
            }
            EventDetails::IdFullNameV1(v) => serde_json::to_value(v).expect("must serialize"),
            EventDetails::RenameClusterV1(v) => serde_json::to_value(v).expect("must serialize"),
            EventDetails::RenameClusterReplicaV1(v) => {
                serde_json::to_value(v).expect("must serialize")
            }
            EventDetails::RenameItemV1(v) => serde_json::to_value(v).expect("must serialize"),
            EventDetails::IdNameV1(v) => serde_json::to_value(v).expect("must serialize"),
            EventDetails::SchemaV1(v) => serde_json::to_value(v).expect("must serialize"),
            EventDetails::SchemaV2(v) => serde_json::to_value(v).expect("must serialize"),
            EventDetails::RenameSchemaV1(v) => serde_json::to_value(v).expect("must serialize"),
            EventDetails::CreateSourceSinkV1(v) => serde_json::to_value(v).expect("must serialize"),
            EventDetails::CreateSourceSinkV2(v) => serde_json::to_value(v).expect("must serialize"),
            EventDetails::CreateSourceSinkV3(v) => serde_json::to_value(v).expect("must serialize"),
            EventDetails::AlterSourceSinkV1(v) => serde_json::to_value(v).expect("must serialize"),
            EventDetails::AlterSetClusterV1(v) => serde_json::to_value(v).expect("must serialize"),
            EventDetails::GrantRoleV1(v) => serde_json::to_value(v).expect("must serialize"),
            EventDetails::GrantRoleV2(v) => serde_json::to_value(v).expect("must serialize"),
            EventDetails::RevokeRoleV1(v) => serde_json::to_value(v).expect("must serialize"),
            EventDetails::RevokeRoleV2(v) => serde_json::to_value(v).expect("must serialize"),
            EventDetails::UpdatePrivilegeV1(v) => serde_json::to_value(v).expect("must serialize"),
            EventDetails::AlterDefaultPrivilegeV1(v) => {
                serde_json::to_value(v).expect("must serialize")
            }
            EventDetails::UpdateOwnerV1(v) => serde_json::to_value(v).expect("must serialize"),
            EventDetails::UpdateItemV1(v) => serde_json::to_value(v).expect("must serialize"),
            EventDetails::AlterRetainHistoryV1(v) => {
                serde_json::to_value(v).expect("must serialize")
            }
            EventDetails::ToNewIdV1(v) => serde_json::to_value(v).expect("must serialize"),
            EventDetails::FromPreviousIdV1(v) => serde_json::to_value(v).expect("must serialize"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct EventV1 {
    pub id: u64,
    pub event_type: EventType,
    pub object_type: ObjectType,
    pub details: EventDetails,
    pub user: Option<String>,
    pub occurred_at: EpochMillis,
}

impl EventV1 {
    fn new(
        id: u64,
        event_type: EventType,
        object_type: ObjectType,
        details: EventDetails,
        user: Option<String>,
        occurred_at: EpochMillis,
    ) -> EventV1 {
        EventV1 {
            id,
            event_type,
            object_type,
            details,
            user,
            occurred_at,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct StorageUsageV1 {
    pub id: u64,
    pub shard_id: Option<String>,
    pub size_bytes: u64,
    pub collection_timestamp: EpochMillis,
}

impl StorageUsageV1 {
    pub fn new(
        id: u64,
        shard_id: Option<String>,
        size_bytes: u64,
        collection_timestamp: EpochMillis,
    ) -> StorageUsageV1 {
        StorageUsageV1 {
            id,
            shard_id,
            size_bytes,
            collection_timestamp,
        }
    }
}

/// Describes the environment's storage usage at a point in time.
///
/// This type is persisted in the catalog across restarts, so any updates to the
/// schema will require a new version.
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub enum VersionedStorageUsage {
    V1(StorageUsageV1),
}

impl VersionedStorageUsage {
    /// Create a new metric snapshot.
    /// This function must always require and produce the most
    /// recent variant of VersionedStorageMetrics.
    pub fn new(
        id: u64,
        object_id: Option<String>,
        size_bytes: u64,
        collection_timestamp: EpochMillis,
    ) -> Self {
        Self::V1(StorageUsageV1::new(
            id,
            object_id,
            size_bytes,
            collection_timestamp,
        ))
    }

    // Implement deserialize and serialize so writers and readers don't have to
    // coordinate about which Serializer to use.
    pub fn deserialize(data: &[u8]) -> Result<Self, anyhow::Error> {
        Ok(serde_json::from_slice(data)?)
    }

    pub fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("must serialize")
    }

    pub fn timestamp(&self) -> EpochMillis {
        match self {
            VersionedStorageUsage::V1(StorageUsageV1 {
                collection_timestamp,
                ..
            }) => *collection_timestamp,
        }
    }

    /// Returns a globally sortable event order. All event versions must have this
    /// field.
    pub fn sortable_id(&self) -> u64 {
        match self {
            VersionedStorageUsage::V1(usage) => usage.id,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{EventDetails, EventType, EventV1, IdNameV1, ObjectType, VersionedEvent};

    // Test all versions of events. This test hard codes bytes so that
    // programmers are not able to change data structures here without this test
    // failing. Instead of changing data structures, add new variants.
    #[mz_ore::test]
    fn test_audit_log() -> Result<(), anyhow::Error> {
        let cases: Vec<(VersionedEvent, &'static str)> = vec![(
            VersionedEvent::V1(EventV1::new(
                2,
                EventType::Drop,
                ObjectType::ClusterReplica,
                EventDetails::IdNameV1(IdNameV1 {
                    id: "u1".to_string(),
                    name: "name".into(),
                }),
                None,
                2,
            )),
            r#"{"V1":{"id":2,"event_type":"drop","object_type":"cluster-replica","details":{"IdNameV1":{"id":"u1","name":"name"}},"user":null,"occurred_at":2}}"#,
        )];

        for (event, expected_bytes) in cases {
            let event_bytes = serde_json::to_vec(&event).unwrap();
            assert_eq!(
                event_bytes,
                expected_bytes.as_bytes(),
                "expected bytes {}, got {}",
                expected_bytes,
                std::str::from_utf8(&event_bytes).unwrap(),
            );
        }

        Ok(())
    }
}
