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

use serde::{Deserialize, Serialize};

use mz_ore::now::EpochMillis;

/// New version variants should be added if fields need to be added, changed, or removed.
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
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
        event_details: EventDetails,
        user: String,
        occurred_at: EpochMillis,
    ) -> Self {
        Self::V1(EventV1::new(
            id,
            event_type,
            object_type,
            event_details,
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum EventType {
    Create,
    Drop,
    Alter,
}

serde_plain::derive_display_from_serialize!(EventType);

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum ObjectType {
    Cluster,
    ClusterReplica,
    Index,
    Sink,
    Source,
    View,
    MaterializedView,
}

serde_plain::derive_display_from_serialize!(ObjectType);

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub enum EventDetails {
    CreateComputeInstanceReplicaV1(CreateComputeInstanceReplicaV1),
    DropComputeInstanceReplicaV1(DropComputeInstanceReplicaV1),
    FullNameV1(FullNameV1),
    NameV1(NameV1),
    RenameItemV1(RenameItemV1),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct FullNameV1 {
    pub database: String,
    pub schema: String,
    pub item: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct NameV1 {
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct RenameItemV1 {
    pub previous_name: FullNameV1,
    pub new_name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct DropComputeInstanceReplicaV1 {
    pub cluster_name: String,
    pub replica_name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct CreateComputeInstanceReplicaV1 {
    pub cluster_name: String,
    pub replica_name: String,
    pub logical_size: String,
}

impl EventDetails {
    pub fn as_json(&self) -> serde_json::Value {
        match self {
            EventDetails::CreateComputeInstanceReplicaV1(v) => {
                serde_json::to_value(v).expect("must serialize")
            }
            EventDetails::DropComputeInstanceReplicaV1(v) => {
                serde_json::to_value(v).expect("must serialize")
            }
            EventDetails::RenameItemV1(v) => serde_json::to_value(v).expect("must serialize"),
            EventDetails::NameV1(v) => serde_json::to_value(v).expect("must serialize"),
            EventDetails::FullNameV1(v) => serde_json::to_value(v).expect("must serialize"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct EventV1 {
    pub id: u64,
    pub event_type: EventType,
    pub object_type: ObjectType,
    pub event_details: EventDetails,
    pub user: String,
    pub occurred_at: EpochMillis,
}

impl EventV1 {
    fn new(
        id: u64,
        event_type: EventType,
        object_type: ObjectType,
        event_details: EventDetails,
        user: String,
        occurred_at: EpochMillis,
    ) -> EventV1 {
        EventV1 {
            id,
            event_type,
            object_type,
            event_details,
            user,
            occurred_at,
        }
    }
}

// Test all versions of events. This test hard codes bytes so that
// programmers are not able to change data structures here without this test
// failing. Instead of changing data structures, add new variants.
#[test]
fn test_audit_log() -> Result<(), anyhow::Error> {
    let cases: Vec<(VersionedEvent, &'static str)> = vec![(
        VersionedEvent::V1(EventV1::new(
            1,
            EventType::Create,
            ObjectType::View,
            EventDetails::NameV1(NameV1 {
                name: "name".into(),
            }),
            "user".into(),
            1,
        )),
        r#"{"V1":{"id":1,"event_type":"create","object_type":"view","event_details":{"NameV1":{"name":"name"}},"user":"user","occurred_at":1}}"#,
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

/// Describes the environment's storage usage at a point in time.
///
/// This type is persisted in the catalog across restarts, so any updates to the
/// schema will require a new version.
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub enum VersionedStorageUsage {
    V1(StorageUsageV1),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct StorageUsageV1 {
    pub id: u64,
    pub object_id: Option<String>,
    pub size_bytes: u64,
    pub collection_timestamp: EpochMillis,
}

impl StorageUsageV1 {
    pub fn new(
        id: u64,
        object_id: Option<String>,
        size_bytes: u64,
        collection_timestamp: EpochMillis,
    ) -> StorageUsageV1 {
        StorageUsageV1 {
            id,
            object_id,
            size_bytes,
            collection_timestamp,
        }
    }
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
}
