// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of deserialization of AWS [S3 Bucket notifications][n]
//!
//! The structs can deserialize the [event message structure version 2.2][structure].
//!
//! [n]: https://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html
//! [structure]: https://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html

use serde::{Deserialize, Serialize};

/// The test message that AWS sends on initial configuration of events
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct TestEvent {
    pub service: String,
    pub event: String,
    pub time: String,
    pub bucket: String,
    pub request_id: String,
    pub host_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Event {
    pub records: Vec<Record>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Record {
    pub event_version: String,
    pub event_source: String,
    pub aws_region: String,
    pub event_time: String,
    #[serde(rename = "eventName")]
    pub event_type: EventType,
    pub user_identity: UserIdentity,
    pub request_parameters: RequestParameters,
    pub response_elements: ResponseElements,
    pub s3: S3,
    pub glacier_event_data: Option<GlacierEventData>,
}

/// The Event type
///
/// <https://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html#supported-notification-event-types>
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EventType {
    #[serde(rename = "ObjectCreated:Put")]
    ObjectCreatedPut,
    #[serde(rename = "ObjectCreated:Post")]
    ObjectCreatedPost,
    #[serde(rename = "ObjectCreated:Copy")]
    ObjectCreatedCopy,
    #[serde(rename = "ObjectCreated:CompleteMultipartUpload")]
    ObjectCreatedCompleteMultipartUpload,
    #[serde(rename = "ObjectRemoved:Delete")]
    ObjectRemovedDelete,
    #[serde(rename = "ObjectRemoved:DeleteMarkerCreated")]
    ObjectRemovedDeleteMarkerCreated,
    #[serde(rename = "ObjectRestore:Post")]
    ObjectRestorePost,
    #[serde(rename = "ObjectRestore:Completed")]
    ObjectRestoreCompleted,
    #[serde(rename = "ReducedRedundancyLostObject")]
    ReducedRedundancyLostObject,
    #[serde(rename = "Replication:OperationFailedReplication")]
    ReplicationOperationFailedReplication,
    #[serde(rename = "Replication:OperationMissedThreshold")]
    ReplicationOperationMissedThreshold,
    #[serde(rename = "Replication:OperationReplicatedAfterThreshold")]
    ReplicationOperationReplicatedAfterThreshold,
    #[serde(rename = "Replication:OperationNotTracked")]
    ReplicationOperationNotTracked,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserIdentity {
    pub principal_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestParameters {
    #[serde(rename = "sourceIPAddress")]
    pub source_ip: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResponseElements {
    #[serde(rename = "x-amz-request-id")]
    pub request_id: String,
    #[serde(rename = "x-amz-id-2")]
    pub id2: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3 {
    pub s3_schema_version: String,
    pub configuration_id: String,
    pub bucket: Bucket,
    pub object: Object,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Bucket {
    pub name: String,
    pub owner_identity: OwnerIdentity,
    pub arn: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OwnerIdentity {
    pub principal_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Object {
    pub key: String,
    pub size: usize,
    pub e_tag: String,
    pub version_id: Option<String>,
    pub sequencer: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GlacierEventData {
    pub restore_event_data: RestoreEventData,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreEventData {
    pub lifecycle_restoration_expiry_time: String,
    pub lifecycle_restore_storage_class: String,
}
