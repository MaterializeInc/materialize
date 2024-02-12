// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Aggregate statistics about data stored in persist.

use std::borrow::Cow;

use mz_dyncfg::{Config, ConfigSet};
use mz_persist::indexed::columnar::ColumnarRecords;
use mz_persist_types::columnar::{PartEncoder, Schema};
use mz_persist_types::part::{Part, PartBuilder};
use mz_persist_types::stats::StructStats;
use mz_persist_types::Codec;
use proptest_derive::Arbitrary;

use crate::batch::UntrimmableColumns;
use crate::internal::encoding::Schemas;
use crate::ShardId;

/// Percent of filtered data to opt in to correctness auditing.
pub(crate) const STATS_AUDIT_PERCENT: Config<usize> = Config::new(
    "persist_stats_audit_percent",
    0,
    "Percent of filtered data to opt in to correctness auditing (Materialize).",
);

/// Computes and stores statistics about each batch part.
///
/// These can be used at read time to entirely skip fetching a part based on its
/// statistics. See [STATS_FILTER_ENABLED].
pub(crate) const STATS_COLLECTION_ENABLED: Config<bool> = Config::new(
    "persist_stats_collection_enabled",
    true,
    "\
    Whether to calculate and record statistics about the data stored in \
    persist to be used at read time, see persist_stats_filter_enabled \
    (Materialize).",
);

/// Uses previously computed statistics about batch parts to entirely skip
/// fetching them at read time.
///
/// See `STATS_COLLECTION_ENABLED`.
pub const STATS_FILTER_ENABLED: Config<bool> = Config::new(
    "persist_stats_filter_enabled",
    true,
    "\
    Whether to use recorded statistics about the data stored in persist to \
    filter at read time, see persist_stats_collection_enabled (Materialize).",
);

/// The budget (in bytes) of how many stats to write down per batch part. When
/// the budget is exceeded, stats will be trimmed away according to a variety of
/// heuristics.
pub(crate) const STATS_BUDGET_BYTES: Config<usize> = Config::new(
    "persist_stats_budget_bytes",
    1024,
    "The budget (in bytes) of how many stats to maintain per batch part.",
);

pub(crate) const STATS_UNTRIMMABLE_COLUMNS_EQUALS: Config<String> = Config::new(
    "persist_stats_untrimmable_columns_equals",
    concat!(
        // If we trim the "err" column, then we can't ever use pushdown on a
        // part (because it could have >0 errors).
        "err,",
        "ts,",
        "receivedat,",
        "createdat,",
        // Fivetran created tables track deleted rows by setting this column.
        //
        // See <https://fivetran.com/docs/using-fivetran/features#capturedeletes>.
        "_fivetran_deleted,",
    ),
    "\
    Which columns to always retain during persist stats trimming. Any column \
    with a name exactly equal (case-insensitive) to one of these will be kept. \
    Comma separated list.",
);

pub(crate) const STATS_UNTRIMMABLE_COLUMNS_PREFIX: Config<String> = Config::new(
    "persist_stats_untrimmable_columns_prefix",
    concat!("last_,",),
    "\
    Which columns to always retain during persist stats trimming. Any column \
    with a name starting with (case-insensitive) one of these will be kept. \
    Comma separated list.",
);

pub(crate) const STATS_UNTRIMMABLE_COLUMNS_SUFFIX: Config<String> = Config::new(
    "persist_stats_untrimmable_columns_suffix",
    concat!("timestamp,", "time,", "_at,", "_tstamp,"),
    "\
    Which columns to always retain during persist stats trimming. Any column \
    with a name ending with (case-insensitive) one of these will be kept. \
    Comma separated list.",
);

pub(crate) fn untrimmable_columns(cfg: &ConfigSet) -> UntrimmableColumns {
    fn split(x: String) -> Vec<Cow<'static, str>> {
        x.split(',')
            .filter(|x| !x.is_empty())
            .map(|x| x.to_owned().into())
            .collect()
    }
    UntrimmableColumns {
        equals: split(STATS_UNTRIMMABLE_COLUMNS_EQUALS.get(cfg)),
        prefixes: split(STATS_UNTRIMMABLE_COLUMNS_PREFIX.get(cfg)),
        suffixes: split(STATS_UNTRIMMABLE_COLUMNS_SUFFIX.get(cfg)),
    }
}

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
pub struct SnapshotStats {
    /// The shard these statistics are for.
    pub shard_id: ShardId,
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
