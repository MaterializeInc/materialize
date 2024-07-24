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

use serde::{Deserialize, Serialize};

use crate::stats::PartStats;
use crate::{Codec, Codec64, ShardId};

/// The in-mem representation of an update in the txns shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TxnsEntry {
    /// A data shard register operation.
    ///
    /// The `[u8; 8]` is a Codec64 encoded timestamp.
    Register(ShardId, [u8; 8]),
    /// A batch written to a data shard in a txn.
    ///
    /// The `[u8; 8]` is a Codec64 encoded timestamp.
    Append(ShardId, [u8; 8], Vec<u8>),
}

impl TxnsEntry {
    /// Returns the ShardId of the data shard targeted by this entry.
    pub fn data_id(&self) -> &ShardId {
        match self {
            TxnsEntry::Register(data_id, _) => data_id,
            TxnsEntry::Append(data_id, _, _) => data_id,
        }
    }

    /// Returns the decoded timestamp of this entry.
    pub fn ts<T: Codec64>(&self) -> T {
        match self {
            TxnsEntry::Register(_, ts) => T::decode(*ts),
            TxnsEntry::Append(_, ts, _) => T::decode(*ts),
        }
    }
}

/// An abstraction over the encoding format of [TxnsEntry].
///
/// This enables users of this crate to control how data is written to the txns
/// shard (which will allow mz to present it as a normal introspection source).
pub trait TxnsCodec: Debug {
    /// The `K` type used in the txns shard.
    type Key: Debug + Codec;
    /// The `V` type used in the txns shard.
    type Val: Debug + Codec;

    /// Returns the Schemas to use with [Self::Key] and [Self::Val].
    fn schemas() -> (<Self::Key as Codec>::Schema, <Self::Val as Codec>::Schema);
    /// Encodes a [TxnsEntry] in the format persisted in the txns shard.
    fn encode(e: TxnsEntry) -> (Self::Key, Self::Val);
    /// Decodes a [TxnsEntry] from the format persisted in the txns shard.
    ///
    /// Implementations should panic if the values are invalid.
    fn decode(key: Self::Key, val: Self::Val) -> TxnsEntry;

    /// Returns if a part might include the given data shard based on pushdown
    /// stats.
    ///
    /// False positives are okay (needless fetches) but false negatives are not
    /// (incorrectness). Returns an Option to make `?` convenient, `None` is
    /// treated the same as `Some(true)`.
    fn should_fetch_part(data_id: &ShardId, stats: &PartStats) -> Option<bool>;
}
