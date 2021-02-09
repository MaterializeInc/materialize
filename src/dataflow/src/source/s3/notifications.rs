// Copyright Materialize, Inc. All rights reserved.
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
/// https://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html#supported-notification-event-types
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

#[test]
fn can_deserialize() {
    let _: Event = serde_json::from_str(r#"{
  "Records": [
    {
      "eventVersion": "2.1",
      "eventSource": "aws:s3",
      "awsRegion": "us-east-2",
      "eventTime": "2021-02-09T18:22:20.910Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "AWS:AROAV2KIV5LPU2NEHHVLJ:maister"
      },
      "requestParameters": {
        "sourceIPAddress": "74.101.33.144"
      },
      "responseElements": {
        "x-amz-request-id": "WNEGGBKBC2Z00TPT",
        "x-amz-id-2": "KSu/nrcf5Uci4VwoGmWVY2C9EApJzwKcAGw1QKRJDuKlduEELM/LCosTygEqxt9hZHQz6SZiGNdbY7b4SDG+e8wM4LDALNh7"
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "NGQxMjYzYTktZDliZC00YmVhLWEyN2MtMDBlNzIxOTc2N2Rj",
        "bucket": {
          "name": "materialize-ci-testdrive-1383595544",
          "ownerIdentity": {
            "principalId": "A3HLZ28Y3OEX06"
          },
          "arn": "arn:aws:s3:::materialize-ci-testdrive-1383595544"
        },
        "object": {
          "key": "short/z",
          "size": 2,
          "eTag": "a8a78d0ff555c931f045b6f448129846",
          "sequencer": "006022D2E196B763B8"
        }
      }
    }
  ]
}"#).unwrap();

    let _: Event = serde_json::from_str(
        r#"{
  "Records": [
    {
      "eventVersion": "2.1",
      "eventSource": "aws:s3",
      "awsRegion": "us-east-2",
      "eventTime": "2021-02-09T20:36:29.767Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "AWS:AROAV2KIV5LPU2NEHHVLJ:1612902961967847000"
      },
      "requestParameters": {
        "sourceIPAddress": "74.101.33.144"
      },
      "responseElements": {
        "x-amz-request-id": "HNRK42YE6BB9YG05",
        "x-amz-id-2": "4b+BVPc9ddP8PqPQ/bWEzFKx75GHDRTGgXj2kzmUJn5Yg9xrWzhrdVQyALvHhCfxZ3DOimAXMGnEiNlLtF5y0lTkLVIIV1TX"
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "MmM1MGQ1MGUtYjA4NC00YmZiLWI1M2EtNDRlOTIzZTI4NDMw",
        "bucket": {
          "name": "materialize-ci-testdrive-3874882343",
          "ownerIdentity": {
            "principalId": "A3HLZ28Y3OEX06"
          },
          "arn": "arn:aws:s3:::materialize-ci-testdrive-3874882343"
        },
        "object": {
          "key": "csv.csv",
          "size": 11,
          "eTag": "39753c6cef323ac27ad4da83b03a2e6f",
          "sequencer": "006022F2503A80ED06"
        }
      }
    }
  ]
}
"#,
    ).unwrap();
}
