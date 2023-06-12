// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Aggregate statistics about data stored in persist.

use mz_persist::indexed::columnar::ColumnarRecords;
use mz_persist_types::columnar::{PartEncoder, Schema};
use mz_persist_types::part::{Part, PartBuilder};
use mz_persist_types::stats::StructStats;
use mz_persist_types::Codec;
use proptest_derive::Arbitrary;
use timely::progress::Antichain;

use crate::internal::encoding::Schemas;
use crate::ShardId;

/// Aggregate statistics about data contained in a [Part].
#[derive(Arbitrary, Debug)]
pub struct PartStats {
    /// Aggregate statistics about key data contained in a [Part].
    pub key: StructStats,
}

impl serde::Serialize for PartStats {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let PartStats { key } = self;
        key.serialize(s)
    }
}

impl PartStats {
    pub(crate) fn new(part: &Part) -> Result<Self, String> {
        let key = part.key_stats()?;
        Ok(PartStats { key })
    }

    pub(crate) fn legacy_part_format<K: Codec, V: Codec>(
        schemas: &Schemas<K, V>,
        part: &[ColumnarRecords],
    ) -> Result<Self, String> {
        // This is a laughably inefficient placeholder implementation of stats
        // on the old part format. We don't intend to make this fast, rather we
        // intend to compute stats on the new part format.
        let mut new_format = PartBuilder::new(schemas.key.as_ref(), schemas.val.as_ref());
        let mut builder = new_format.get_mut();
        let mut key = schemas.key.encoder(builder.key)?;
        let mut val = schemas.val.encoder(builder.val)?;
        for x in part {
            for ((k, v), t, d) in x.iter() {
                let k = K::decode(k)?;
                let v = V::decode(v)?;
                key.encode(&k);
                val.encode(&v);
                builder.ts.push(i64::from_le_bytes(t));
                builder.diff.push(i64::from_le_bytes(d));
            }
        }
        drop(key);
        drop(val);
        let new_format = new_format.finish()?;
        Self::new(&new_format)
    }
}

/// Statistics about the contents of a shard as_of some time.
///
/// TODO: Add more stats here as they become necessary.
#[derive(Debug)]
pub struct SnapshotStats<T> {
    /// The shard these statistics are for.
    pub shard_id: ShardId,
    /// The frontier at which these statistics are valid.
    pub as_of: Antichain<T>,
    /// An estimate of the count of updates in the shard.
    ///
    /// This is an upper bound on the number of updates that persist_source
    /// would emit if you snapshot the source at the given as_of. The real
    /// number of updates, after consolidation, might be lower. It includes both
    /// additions and retractions.
    ///
    /// NB: Because of internal persist compaction, the answer for a given as_of
    /// may change over time (as persist advances through Seqnos), but because
    /// compaction never results in more updates than the sum of the inputs, it
    /// can only go down.
    pub num_updates: usize,
}
