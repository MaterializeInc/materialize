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
use uuid::Uuid;

/// New version variants should be added if fields need to be added, changed, or removed.
#[derive(Serialize, Deserialize)]
pub enum VersionedEvent {
    V1(EventV1),
}

impl VersionedEvent {
    /// Create a new event, with a generated UUID. This function must always
    /// require and produce the most recent variant of VersionedEvent.
    pub fn new(
        event_type: EventType,
        object_type: ObjectType,
        event_details: EventDetails,
        user: String,
        occurred_at_unix_epoch_nanos: u64,
    ) -> Self {
        Self::new_uuid(
            Uuid::new_v4(),
            event_type,
            object_type,
            event_details,
            user,
            occurred_at_unix_epoch_nanos,
        )
    }

    /// Create a new event. This function must always require and produce the most
    /// recent variant of VersionedEvent.
    pub fn new_uuid(
        uuid: Uuid,
        event_type: EventType,
        object_type: ObjectType,
        event_details: EventDetails,
        user: String,
        occurred_at_unix_epoch_nanos: u64,
    ) -> Self {
        Self::V1(EventV1::new(
            uuid,
            event_type,
            object_type,
            event_details,
            user,
            occurred_at_unix_epoch_nanos,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EventType {
    Create,
    Drop,
    Alter,
    Rename,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ObjectType {
    Cluster,
    ClusterReplica,
    Index,
    Sink,
    Source,
    View,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EventDetails {
    CreateComputeInstanceReplicaV1(CreateComputeInstanceReplicaV1),
    DropComputeInstanceReplicaV1(DropComputeInstanceReplicaV1),
    FullNameV1(FullNameV1),
    NameV1(NameV1),
    RenameItemV1(RenameItemV1),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FullNameV1 {
    pub database: String,
    pub schema: String,
    pub item: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NameV1 {
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RenameItemV1 {
    pub previous_name: FullNameV1,
    pub new_name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DropComputeInstanceReplicaV1 {
    pub cluster_name: String,
    pub replica_name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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

#[derive(Serialize, Deserialize)]
pub struct EventV1 {
    pub uuid: Uuid,
    pub event_type: EventType,
    pub object_type: ObjectType,
    pub event_details: EventDetails,
    pub user: String,
    pub occurred_at: u64,
}

impl EventV1 {
    fn new(
        uuid: Uuid,
        event_type: EventType,
        object_type: ObjectType,
        event_details: EventDetails,
        user: String,
        occurred_at: u64,
    ) -> EventV1 {
        EventV1 {
            uuid,
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
            Uuid::from_u128(1),
            EventType::Create,
            ObjectType::View,
            EventDetails::NameV1(NameV1 {
                name: "name".into(),
            }),
            "user".into(),
            1,
        )),
        r#"{"V1":{"uuid":"00000000-0000-0000-0000-000000000001","event_type":"Create","object_type":"View","event_details":{"NameV1":{"name":"name"}},"user":"user","occurred_at":1}}"#,
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
