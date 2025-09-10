// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::fmt::{self, Debug, Display};

use itertools::Itertools;
use mz_ore::assert_none;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::schema::SchemaId;
use mz_persist_types::stats::PartStats;
use mz_persist_types::txn::{TxnsCodec, TxnsEntry};
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::{Datum, GlobalId, RelationDesc, Row, SqlScalarType};
use mz_sql_parser::ast::UnresolvedItemName;
use mz_timely_util::antichain::AntichainExt;
use serde::{Deserialize, Serialize};
use timely::progress::Antichain;
use tracing::error;

use crate::errors::DataflowError;
use crate::instances::StorageInstanceId;
use crate::sources::SourceData;

/// Metadata required by a storage instance to read a storage collection
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionMetadata {
    /// The persist location where the shards are located.
    pub persist_location: PersistLocation,
    /// The persist shard id of the remap collection used to reclock this collection.
    pub remap_shard: Option<ShardId>,
    /// The persist shard containing the contents of this storage collection.
    pub data_shard: ShardId,
    /// The `RelationDesc` that describes the contents of the `data_shard`.
    pub relation_desc: RelationDesc,
    /// The shard id of the txn-wal shard, if `self.data_shard` is managed
    /// by the txn-wal system, or None if it's not.
    pub txns_shard: Option<ShardId>,
}

impl crate::AlterCompatible for CollectionMetadata {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), self::AlterError> {
        if self == other {
            return Ok(());
        }

        let CollectionMetadata {
            // persist locations may change (though not as a result of ALTER);
            // we allow this because if this changes unexpectedly, we will
            // notice in other ways.
            persist_location: _,
            remap_shard,
            data_shard,
            relation_desc,
            txns_shard,
        } = self;

        let compatibility_checks = [
            (remap_shard == &other.remap_shard, "remap_shard"),
            (data_shard == &other.data_shard, "data_shard"),
            (relation_desc == &other.relation_desc, "relation_desc"),
            (txns_shard == &other.txns_shard, "txns_shard"),
        ];

        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "CollectionMetadata incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(AlterError { id });
            }
        }

        Ok(())
    }
}

/// The subset of [`CollectionMetadata`] that must be durable stored.
#[derive(Clone, Debug, PartialEq, PartialOrd, Ord, Eq, Serialize, Deserialize)]
pub struct DurableCollectionMetadata {
    pub data_shard: ShardId,
}

#[derive(Debug)]
pub enum StorageError<T> {
    /// The source identifier was re-created after having been dropped,
    /// or installed with a different description.
    CollectionIdReused(GlobalId),
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
    InvalidUppers(Vec<InvalidUpper<T>>),
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
    /// Response to an invalid/unsupported `ALTER..` command.
    InvalidAlter(AlterError),
    /// The controller API was used in some invalid way. This usually indicates
    /// a bug.
    InvalidUsage(String),
    /// The specified resource was exhausted, and is not currently accepting more requests.
    ResourceExhausted(&'static str),
    /// The specified component is shutting down.
    ShuttingDown(&'static str),
    /// Collection metadata already exists for ID.
    CollectionMetadataAlreadyExists(GlobalId),
    /// Some other collection is already writing to this persist shard.
    PersistShardAlreadyInUse(ShardId),
    /// Raced with some other process while trying to evolve the schema of a Persist shard.
    PersistSchemaEvolveRace {
        global_id: GlobalId,
        shard_id: ShardId,
        schema_id: SchemaId,
        relation_desc: RelationDesc,
    },
    /// We tried to evolve the schema of a Persist shard in an invalid way.
    PersistInvalidSchemaEvolve {
        global_id: GlobalId,
        shard_id: ShardId,
    },
    /// Txn WAL shard already exists.
    TxnWalShardAlreadyExists,
    /// The item that a subsource refers to is unexpectedly missing from the
    /// source.
    MissingSubsourceReference {
        ingestion_id: GlobalId,
        reference: UnresolvedItemName,
    },
    /// We failed to determine the real-time-recency timestamp.
    RtrTimeout(GlobalId),
    /// The collection was dropped before we could ingest its external frontier.
    RtrDropFailure(GlobalId),
    /// A generic error that happens during operations of the storage controller.
    // TODO(aljoscha): Get rid of this!
    Generic(anyhow::Error),
    /// We are in read-only mode and were asked to do a something that requires
    /// writing.
    ReadOnly,
}

impl<T: Debug + Display + 'static> Error for StorageError<T> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CollectionIdReused(_) => None,
            Self::SinkIdReused(_) => None,
            Self::IdentifierMissing(_) => None,
            Self::IdentifierInvalid(_) => None,
            Self::UpdateBeyondUpper(_) => None,
            Self::ReadBeforeSince(_) => None,
            Self::InvalidUppers(_) => None,
            Self::IngestionInstanceMissing { .. } => None,
            Self::ExportInstanceMissing { .. } => None,
            Self::DataflowError(err) => Some(err),
            Self::InvalidAlter { .. } => None,
            Self::InvalidUsage(_) => None,
            Self::ResourceExhausted(_) => None,
            Self::ShuttingDown(_) => None,
            Self::CollectionMetadataAlreadyExists(_) => None,
            Self::PersistShardAlreadyInUse(_) => None,
            Self::PersistSchemaEvolveRace { .. } => None,
            Self::PersistInvalidSchemaEvolve { .. } => None,
            Self::TxnWalShardAlreadyExists => None,
            Self::MissingSubsourceReference { .. } => None,
            Self::RtrTimeout(_) => None,
            Self::RtrDropFailure(_) => None,
            Self::Generic(err) => err.source(),
            Self::ReadOnly => None,
        }
    }
}

impl<T: fmt::Display + 'static> fmt::Display for StorageError<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("storage error: ")?;
        match self {
            Self::CollectionIdReused(id) => write!(
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
                    id.iter()
                        .map(|InvalidUpper { id, current_upper }| {
                            format!("(id: {}; actual upper: {})", id, current_upper.pretty())
                        })
                        .join(", ")
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
            Self::DataflowError(_err) => write!(f, "dataflow failed to process request",),
            Self::InvalidAlter(err) => std::fmt::Display::fmt(err, f),
            Self::InvalidUsage(err) => write!(f, "invalid usage: {}", err),
            Self::ResourceExhausted(rsc) => write!(f, "{rsc} is exhausted"),
            Self::ShuttingDown(cmp) => write!(f, "{cmp} is shutting down"),
            Self::CollectionMetadataAlreadyExists(key) => {
                write!(f, "storage metadata for '{key}' already exists")
            }
            Self::PersistShardAlreadyInUse(shard) => {
                write!(f, "persist shard already in use: {shard}")
            }
            Self::PersistSchemaEvolveRace {
                global_id,
                shard_id,
                ..
            } => {
                write!(
                    f,
                    "persist raced when trying to evolve the schema of a shard: {global_id}, {shard_id}"
                )
            }
            Self::PersistInvalidSchemaEvolve {
                global_id,
                shard_id,
            } => {
                write!(
                    f,
                    "persist shard evolved in an invalid way: {global_id}, {shard_id}"
                )
            }
            Self::TxnWalShardAlreadyExists => {
                write!(f, "txn WAL already exists")
            }
            Self::MissingSubsourceReference {
                ingestion_id,
                reference,
            } => write!(
                f,
                "ingestion {ingestion_id} unexpectedly missing reference to {}",
                reference
            ),
            Self::RtrTimeout(_) => {
                write!(
                    f,
                    "timed out before ingesting the source's visible frontier when real-time-recency query issued"
                )
            }
            Self::RtrDropFailure(_) => write!(
                f,
                "real-time source dropped before ingesting the upstream system's visible frontier"
            ),
            Self::Generic(err) => std::fmt::Display::fmt(err, f),
            Self::ReadOnly => write!(f, "cannot write in read-only mode"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct InvalidUpper<T> {
    pub id: GlobalId,
    pub current_upper: Antichain<T>,
}

#[derive(Debug)]
pub struct AlterError {
    pub id: GlobalId,
}

impl Error for AlterError {}

impl fmt::Display for AlterError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} cannot be altered in the requested way", self.id)
    }
}

impl<T> From<AlterError> for StorageError<T> {
    fn from(error: AlterError) -> Self {
        Self::InvalidAlter(error)
    }
}

impl<T> From<DataflowError> for StorageError<T> {
    fn from(error: DataflowError) -> Self {
        Self::DataflowError(error)
    }
}

#[derive(Debug)]
pub struct TxnsCodecRow;

impl TxnsCodecRow {
    pub fn desc() -> RelationDesc {
        RelationDesc::builder()
            .with_column("shard_id", SqlScalarType::String.nullable(false))
            .with_column("ts", SqlScalarType::UInt64.nullable(false))
            .with_column("batch", SqlScalarType::Bytes.nullable(true))
            .finish()
    }
}

impl TxnsCodec for TxnsCodecRow {
    type Key = SourceData;
    type Val = ();

    fn schemas() -> (
        <Self::Key as mz_persist_types::Codec>::Schema,
        <Self::Val as mz_persist_types::Codec>::Schema,
    ) {
        (Self::desc(), UnitSchema)
    }

    fn encode(e: TxnsEntry) -> (Self::Key, Self::Val) {
        let row = match &e {
            TxnsEntry::Register(data_id, ts) => Row::pack([
                Datum::from(data_id.to_string().as_str()),
                Datum::from(u64::from_le_bytes(*ts)),
                Datum::Null,
            ]),
            TxnsEntry::Append(data_id, ts, batch) => Row::pack([
                Datum::from(data_id.to_string().as_str()),
                Datum::from(u64::from_le_bytes(*ts)),
                Datum::from(batch.as_slice()),
            ]),
        };
        (SourceData(Ok(row)), ())
    }

    fn decode(row: SourceData, _: ()) -> TxnsEntry {
        let mut datums = row.0.as_ref().expect("valid entry").iter();
        let data_id = datums.next().expect("valid entry").unwrap_str();
        let data_id = data_id.parse::<ShardId>().expect("valid entry");
        let ts = datums.next().expect("valid entry");
        let ts = u64::to_le_bytes(ts.unwrap_uint64());
        let batch = datums.next().expect("valid entry");
        assert_none!(datums.next());
        if batch.is_null() {
            TxnsEntry::Register(data_id, ts)
        } else {
            TxnsEntry::Append(data_id, ts, batch.unwrap_bytes().to_vec())
        }
    }

    fn should_fetch_part(data_id: &ShardId, stats: &PartStats) -> Option<bool> {
        let stats = stats
            .key
            .col("key")?
            .try_as_optional_struct()
            .map_err(|err| error!("unexpected stats type for col 'key': {}", err))
            .ok()?;
        let stats = stats
            .some
            .col("shard_id")?
            .try_as_string()
            .map_err(|err| error!("unexpected stats type for col 'shard_id': {}", err))
            .ok()?;
        let data_id_str = data_id.to_string();
        Some(stats.lower <= data_id_str && stats.upper >= data_id_str)
    }
}
