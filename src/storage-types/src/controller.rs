// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::fmt::{self, Debug};

use itertools::Itertools;
use mz_persist_client::{PersistLocation, ShardId};
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{GlobalId, RelationDesc};
use mz_stash_types::StashError;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::errors::DataflowError;
use crate::instances::StorageInstanceId;

include!(concat!(env!("OUT_DIR"), "/mz_storage_types.controller.rs"));

/// Metadata required by a storage instance to read a storage collection
#[derive(Arbitrary, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionMetadata {
    /// The persist location where the shards are located.
    pub persist_location: PersistLocation,
    /// The persist shard id of the remap collection used to reclock this collection.
    pub remap_shard: Option<ShardId>,
    /// The persist shard containing the contents of this storage collection.
    pub data_shard: ShardId,
    /// The persist shard containing the status updates for this storage collection.
    pub status_shard: Option<ShardId>,
    /// The `RelationDesc` that describes the contents of the `data_shard`.
    pub relation_desc: RelationDesc,
}

impl RustType<ProtoCollectionMetadata> for CollectionMetadata {
    fn into_proto(&self) -> ProtoCollectionMetadata {
        ProtoCollectionMetadata {
            blob_uri: self.persist_location.blob_uri.clone(),
            consensus_uri: self.persist_location.consensus_uri.clone(),
            data_shard: self.data_shard.to_string(),
            remap_shard: self.remap_shard.map(|s| s.to_string()),
            status_shard: self.status_shard.map(|s| s.to_string()),
            relation_desc: Some(self.relation_desc.into_proto()),
        }
    }

    fn from_proto(value: ProtoCollectionMetadata) -> Result<Self, TryFromProtoError> {
        Ok(CollectionMetadata {
            persist_location: PersistLocation {
                blob_uri: value.blob_uri,
                consensus_uri: value.consensus_uri,
            },
            remap_shard: value
                .remap_shard
                .map(|s| s.parse().map_err(TryFromProtoError::InvalidShardId))
                .transpose()?,
            data_shard: value
                .data_shard
                .parse()
                .map_err(TryFromProtoError::InvalidShardId)?,
            status_shard: value
                .status_shard
                .map(|s| s.parse().map_err(TryFromProtoError::InvalidShardId))
                .transpose()?,
            relation_desc: value
                .relation_desc
                .into_rust_if_some("ProtoCollectionMetadata::relation_desc")?,
        })
    }
}

/// The subset of [`CollectionMetadata`] that must be durable stored.
#[derive(Arbitrary, Clone, Debug, PartialEq, PartialOrd, Ord, Eq, Serialize, Deserialize)]
pub struct DurableCollectionMetadata {
    pub data_shard: ShardId,
}

impl RustType<ProtoDurableCollectionMetadata> for DurableCollectionMetadata {
    fn into_proto(&self) -> ProtoDurableCollectionMetadata {
        ProtoDurableCollectionMetadata {
            data_shard: self.data_shard.to_string(),
        }
    }

    fn from_proto(value: ProtoDurableCollectionMetadata) -> Result<Self, TryFromProtoError> {
        Ok(DurableCollectionMetadata {
            data_shard: value
                .data_shard
                .parse()
                .map_err(TryFromProtoError::InvalidShardId)?,
        })
    }
}

impl RustType<mz_stash_types::objects::proto::DurableCollectionMetadata>
    for DurableCollectionMetadata
{
    fn into_proto(&self) -> mz_stash_types::objects::proto::DurableCollectionMetadata {
        mz_stash_types::objects::proto::DurableCollectionMetadata {
            data_shard: self.data_shard.into_proto(),
        }
    }

    fn from_proto(
        proto: mz_stash_types::objects::proto::DurableCollectionMetadata,
    ) -> Result<Self, TryFromProtoError> {
        Ok(DurableCollectionMetadata {
            data_shard: proto.data_shard.into_rust()?,
        })
    }
}

#[derive(Debug)]
pub enum StorageError {
    /// The source identifier was re-created after having been dropped,
    /// or installed with a different description.
    SourceIdReused(GlobalId),
    /// The sink identifier was re-created after having been dropped, or
    /// installed with a different description.
    SinkIdReused(GlobalId),
    /// The source identifier is not present.
    IdentifierMissing(GlobalId),
    /// The provided identifier was invalid, maybe missing, wrong type, not registered, etc.
    IdentifierInvalid(GlobalId),
    /// The update contained in the appended batch was at a timestamp equal or beyond the batch's upper
    UpdateBeyondUpper(GlobalId),
    /// The read was at a timestamp before the collection's since
    ReadBeforeSince(GlobalId),
    /// The expected upper of one or more appends was different from the actual upper of the collection
    InvalidUppers(Vec<GlobalId>),
    /// An operation failed to read or write state
    IOError(StashError),
    /// The (client for) the requested cluster instance is missing.
    IngestionInstanceMissing {
        storage_instance_id: StorageInstanceId,
        ingestion_id: GlobalId,
    },
    /// The (client for) the requested cluster instance is missing.
    ExportInstanceMissing {
        storage_instance_id: StorageInstanceId,
        export_id: GlobalId,
    },
    /// Dataflow was not able to process a request
    DataflowError(DataflowError),
    /// Response to an invalid/unsupported `ALTER SOURCE` command.
    InvalidAlterSource { id: GlobalId },
    /// The controller API was used in some invalid way. This usually indicates
    /// a bug.
    InvalidUsage(String),
    /// The specified resource was exhausted, and is not currently accepting more requests.
    ResourceExhausted(&'static str),
    /// The specified component is shutting down.
    ShuttingDown(&'static str),
    /// Response if we try to change a sink's description to a state
    /// incompatible with its current state.
    IncompatibleSinkDescriptions { id: GlobalId },
    /// A generic error that happens during operations of the storage controller.
    // TODO(aljoscha): Get rid of this!
    Generic(anyhow::Error),
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SourceIdReused(_) => None,
            Self::SinkIdReused(_) => None,
            Self::IdentifierMissing(_) => None,
            Self::IdentifierInvalid(_) => None,
            Self::UpdateBeyondUpper(_) => None,
            Self::ReadBeforeSince(_) => None,
            Self::InvalidUppers(_) => None,
            Self::IngestionInstanceMissing { .. } => None,
            Self::ExportInstanceMissing { .. } => None,
            Self::IOError(err) => Some(err),
            Self::DataflowError(err) => Some(err),
            Self::InvalidAlterSource { .. } => None,
            Self::InvalidUsage(_) => None,
            Self::ResourceExhausted(_) => None,
            Self::ShuttingDown(_) => None,
            Self::IncompatibleSinkDescriptions { .. } => None,
            Self::Generic(err) => err.source(),
        }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("storage error: ")?;
        match self {
            Self::SourceIdReused(id) => write!(
                f,
                "source identifier was re-created after having been dropped: {id}"
            ),
            Self::SinkIdReused(id) => write!(
                f,
                "sink identifier was re-created after having been dropped: {id}"
            ),
            Self::IdentifierMissing(id) => write!(f, "collection identifier is not present: {id}"),
            Self::IdentifierInvalid(id) => write!(f, "collection identifier is invalid {id}"),
            Self::UpdateBeyondUpper(id) => {
                write!(
                    f,
                    "append batch for {id} contained update at or beyond its upper"
                )
            }
            Self::ReadBeforeSince(id) => {
                write!(f, "read for {id} was at a timestamp before its since")
            }
            Self::InvalidUppers(id) => {
                write!(
                    f,
                    "expected upper was different from the actual upper for: {}",
                    id.iter().map(|id| id.to_string()).join(", ")
                )
            }
            Self::IngestionInstanceMissing {
                storage_instance_id,
                ingestion_id,
            } => write!(
                f,
                "instance {} missing for ingestion {}",
                storage_instance_id, ingestion_id
            ),
            Self::ExportInstanceMissing {
                storage_instance_id,
                export_id,
            } => write!(
                f,
                "instance {} missing for export {}",
                storage_instance_id, export_id
            ),
            // N.B. For these errors, the underlying error is reported in `source()`, and it
            // is the responsibility of the caller to print the chain of errors, when desired.
            Self::IOError(_err) => write!(f, "failed to read or write state",),
            // N.B. For these errors, the underlying error is reported in `source()`, and it
            // is the responsibility of the caller to print the chain of errors, when desired.
            Self::DataflowError(_err) => write!(f, "dataflow failed to process request",),
            Self::InvalidAlterSource { id } => {
                write!(f, "{id} cannot be altered in the requested way")
            }
            Self::InvalidUsage(err) => write!(f, "invalid usage: {}", err),
            Self::ResourceExhausted(rsc) => write!(f, "{rsc} is exhausted"),
            Self::ShuttingDown(cmp) => write!(f, "{cmp} is shutting down"),
            Self::IncompatibleSinkDescriptions { id } => {
                // n.b. this error is only used in assertions currently, so
                // doesn't need to contain more detail until we support `ALTER
                // SINK`.
                write!(
                    f,
                    "{id} cannot be have its description changed in the requested way"
                )
            }
            Self::Generic(err) => std::fmt::Display::fmt(err, f),
        }
    }
}

impl From<StashError> for StorageError {
    fn from(error: StashError) -> Self {
        Self::IOError(error)
    }
}

impl From<DataflowError> for StorageError {
    fn from(error: DataflowError) -> Self {
        Self::DataflowError(error)
    }
}
