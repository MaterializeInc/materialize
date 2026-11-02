// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to Kafka listener sources.
//!
//! A Kafka listener source does not read from an external system. Instead, the replica running
//! the source accepts Kafka producer traffic for the source's topic and ingests every record
//! produced to it. Records have the same shape as those read by a Kafka source, so the two share
//! formats, envelopes and metadata columns.

use std::str::FromStr;
use std::sync::LazyLock;

use mz_persist_types::ShardId;
use mz_repr::{CatalogItemId, GlobalId, RelationDesc, SqlScalarType};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::AlterCompatible;
use crate::controller::AlterError;
use crate::sources::SourceConnection;
use crate::sources::kafka::KafkaMetadataKind;

/// The progress collection of a Kafka listener source. Its single partition's offsets are the
/// source's timestamps.
pub static KAFKA_LISTENER_PROGRESS_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column("offset", SqlScalarType::UInt64.nullable(true))
        .finish()
});

/// Returns the shard that holds the log of the Kafka listener source whose remap collection is
/// stored in `remap_shard`.
///
/// The log is not a storage collection, so it has no entry in the catalog's shard mapping.
/// Deriving its ID from the remap shard, which is unique to the source and lives exactly as long
/// as it, lets the replica find the log and the controller finalize it when the source is
/// dropped.
pub fn kafka_listener_log_shard(remap_shard: ShardId) -> ShardId {
    let digest = Sha256::new()
        .chain_update(b"kafka-listener-log")
        .chain_update(remap_shard.to_string().as_bytes())
        .finalize();
    let bytes: [u8; 16] = digest[..16].try_into().expect("digest is 32 bytes");
    ShardId::from_str(&format!("s{}", uuid::Uuid::from_bytes(bytes))).expect("valid shard id")
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaListenerSourceConnection {
    /// The topics that producers write to. A topic is unique among the sources of a cluster.
    pub topics: KafkaListenerTopics,
    /// SASL/PLAIN credentials that producers must present. If `None`, producers need not
    /// authenticate.
    pub sasl: Option<KafkaListenerSaslCredentials>,
    /// The metadata columns of the primary source export.
    pub metadata_columns: Vec<(String, KafkaMetadataKind)>,
    /// Whether a produced batch is rejected unless each of its records decodes, and passes the
    /// envelope's checks, for the export of its topic.
    #[serde(default)]
    pub validate_on_produce: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KafkaListenerTopics {
    /// A single topic, whose records the source's own relation holds.
    Topic(String),
    /// The topics of the source's tables, which must start with this prefix. The source's own
    /// relation holds no records.
    Prefix(String),
}

/// The details of an export of a Kafka listener source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaListenerSourceExportDetails {
    /// The topic whose records the export holds.
    pub topic: String,
    pub metadata_columns: Vec<(String, KafkaMetadataKind)>,
}

impl AlterCompatible for KafkaListenerSourceExportDetails {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        if self == other {
            Ok(())
        } else {
            tracing::warn!(
                "KafkaListenerSourceExportDetails incompatible:\nself:\n{:#?}\n\nother\n{:#?}",
                self,
                other
            );
            Err(AlterError { id })
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaListenerSaslCredentials {
    pub username: String,
    /// The secret holding the password.
    pub password: CatalogItemId,
}

impl SourceConnection for KafkaListenerSourceConnection {
    fn name(&self) -> &'static str {
        "kafka-listener"
    }

    fn external_reference(&self) -> Option<&str> {
        match &self.topics {
            KafkaListenerTopics::Topic(topic) => Some(topic.as_str()),
            KafkaListenerTopics::Prefix(_) => None,
        }
    }

    fn default_key_desc(&self) -> RelationDesc {
        RelationDesc::builder()
            .with_column("key", SqlScalarType::Bytes.nullable(true))
            .finish()
    }

    fn default_value_desc(&self) -> RelationDesc {
        RelationDesc::builder()
            .with_column("value", SqlScalarType::Bytes.nullable(true))
            .finish()
    }

    fn timestamp_desc(&self) -> RelationDesc {
        KAFKA_LISTENER_PROGRESS_DESC.clone()
    }

    fn connection_id(&self) -> Option<CatalogItemId> {
        None
    }

    fn supports_read_only(&self) -> bool {
        // A read-only ingestion cannot make records durable, so it could never acknowledge the
        // records producers send to it.
        false
    }

    fn prefers_single_replica(&self) -> bool {
        // Producers connect to the replica that hosts the topic.
        true
    }
}

impl AlterCompatible for KafkaListenerSourceConnection {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        if self == other {
            Ok(())
        } else {
            tracing::warn!(
                "KafkaListenerSourceConnection incompatible:\nself:\n{:#?}\n\nother\n{:#?}",
                self,
                other
            );
            Err(AlterError { id })
        }
    }
}
