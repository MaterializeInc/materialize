// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types for the txn-wal crate.

use std::fmt::Debug;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::stats::PartStats;
use crate::{Codec, Codec64, ShardId};

/// The in-mem representation of an update in the txns shard.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TxnsEntry {
    /// A data shard register operation.
    Register {
        /// The id of the data shard.
        data_id: ShardId,
        /// A Codec64 encoded timestamp.
        ts: [u8; 8],
        /// The key schema of the data shard, serialized.
        #[serde(default)]
        key_schema: Bytes,
        /// The val schema of the data shard, serialized.
        #[serde(default)]
        val_schema: Bytes,
    },
    /// A batch written to a data shard in a txn.
    Append {
        /// The id of the data shard.
        data_id: ShardId,
        /// A Codec64 encoded timestamp.
        ts: [u8; 8],
        /// The transmittable serialization of the batch.
        batch: Vec<u8>,
    },
}

impl TxnsEntry {
    /// Returns the ShardId of the data shard targeted by this entry.
    pub fn data_id(&self) -> &ShardId {
        match self {
            TxnsEntry::Register { data_id, .. } => data_id,
            TxnsEntry::Append { data_id, .. } => data_id,
        }
    }

    /// Returns the decoded timestamp of this entry.
    pub fn ts<T: Codec64>(&self) -> T {
        match self {
            TxnsEntry::Register { ts, .. } => T::decode(*ts),
            TxnsEntry::Append { ts, .. } => T::decode(*ts),
        }
    }
}

/// An abstraction over the encoding format of [TxnsEntry].
///
/// This enables users of this crate to control how data is written to the txns
/// shard (which will allow mz to present it as a normal introspection source).
pub trait TxnsCodec: Debug {
    /// The `K` type used in the txns shard.
    type Key: Debug + Codec + Default;
    /// The `V` type used in the txns shard.
    type Val: Debug + Codec + Default;

    /// Returns the Schemas to use with [Self::Key] and [Self::Val].
    fn schemas() -> (<Self::Key as Codec>::Schema, <Self::Val as Codec>::Schema);
    /// Encodes a [TxnsEntry] in the format persisted in the txns shard.
    fn encode(e: TxnsEntry) -> (Self::Key, Self::Val);
    /// Decodes a [TxnsEntry] from the format persisted in the txns shard.
    ///
    /// Implementations should panic if the values are invalid.
    ///
    /// If the previous format of TxnsEntry encoding is encountered (i.e. the
    /// one without schemas), this must return empty bytes for the schemas.
    fn decode(key: Self::Key, val: Self::Val) -> TxnsEntry;

    /// Returns if a part might include the given data shard based on pushdown
    /// stats.
    ///
    /// False positives are okay (needless fetches) but false negatives are not
    /// (incorrectness). Returns an Option to make `?` convenient, `None` is
    /// treated the same as `Some(true)`.
    fn should_fetch_part(data_id: &ShardId, stats: &PartStats) -> Option<bool>;
}

/// [crate::columnar::Schema] of a data shard used with txn-wal.
pub trait TxnsDataSchema: Debug + PartialEq {
    /// Encode this schema for permanent storage.
    ///
    /// This must perfectly round-trip Self through [TxnsDataSchema::decode]. If
    /// the encode function for this schema ever changes, decode must be able to
    /// handle bytes output by all previous versions of encode.
    fn encode(&self) -> Bytes;
    /// Decode a schema previous encoded with this schema's
    /// [TxnsDataSchema::encode].
    ///
    /// This must perfectly round-trip Self through [TxnsDataSchema::decode]. If
    /// the encode function for this schema ever changes, decode must be able to
    /// handle bytes output by all previous versions of encode.
    ///
    /// TODO: While migrating, an empty buf should be decoded to any placeholder
    /// schema (in practice we use RelationDesc::empty again).
    fn decode(buf: Bytes) -> Self;
}
