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
use std::sync::Arc;

use arrow::array::ArrayRef;
use mz_dyncfg::{Config, ConfigSet};
use mz_persist::indexed::columnar::ColumnarRecordsStructuredExt;
use mz_persist::indexed::encoding::{BatchColumnarFormat, BlobTraceUpdates};
use mz_persist_types::part::{Part2, PartBuilder, PartBuilder2};
use mz_persist_types::stats::{ColumnStatKinds, PartStats};
use mz_persist_types::Codec;

use crate::batch::UntrimmableColumns;
use crate::internal::encoding::Schemas;
use crate::metrics::Metrics;
use crate::read::LazyPartStats;

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

pub(crate) const STATS_UNTRIMMABLE_COLUMNS_EQUALS: Config<fn() -> String> = Config::new(
    "persist_stats_untrimmable_columns_equals",
    || {
        [
            // If we trim the "err" column, then we can't ever use pushdown on a
            // part (because it could have >0 errors).
            "err",
            "ts",
            "receivedat",
            "createdat",
            // Fivetran created tables track deleted rows by setting this column.
            //
            // See <https://fivetran.com/docs/using-fivetran/features#capturedeletes>.
            "_fivetran_deleted",
        ]
        .join(",")
    },
    "\
    Which columns to always retain during persist stats trimming. Any column \
    with a name exactly equal (case-insensitive) to one of these will be kept. \
    Comma separated list.",
);

pub(crate) const STATS_UNTRIMMABLE_COLUMNS_PREFIX: Config<fn() -> String> = Config::new(
    "persist_stats_untrimmable_columns_prefix",
    || ["last_"].join(","),
    "\
    Which columns to always retain during persist stats trimming. Any column \
    with a name starting with (case-insensitive) one of these will be kept. \
    Comma separated list.",
);

pub(crate) const STATS_UNTRIMMABLE_COLUMNS_SUFFIX: Config<fn() -> String> = Config::new(
    "persist_stats_untrimmable_columns_suffix",
    || ["timestamp", "time", "_at", "_tstamp"].join(","),
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

/// Encodes a [`BlobTraceUpdates`] and calculates [`PartStats`].
/// We also return structured data iff [BatchColumnarFormat::is_structured] is enabled.
pub(crate) fn encode_updates<K, V>(
    schemas: &Schemas<K, V>,
    updates: &BlobTraceUpdates,
    format: &BatchColumnarFormat,
) -> Result<(Option<ColumnarRecordsStructuredExt>, PartStats), String>
where
    K: Codec,
    V: Codec,
{
    let updates = updates.records();

    if format.is_structured() {
        // Optionally we reuse instances of K and V and create instances of
        // K::Storage and V::Storage to reduce allocations.
        let mut k_reuse = K::default();
        let mut v_reuse = V::default();
        let mut k_storage = Some(K::Storage::default());
        let mut v_storage = Some(V::Storage::default());

        let mut builder = PartBuilder2::new(schemas.key.as_ref(), schemas.val.as_ref());
        for ((k, v), t, d) in updates.iter() {
            let t = i64::from_le_bytes(t);
            let d = i64::from_le_bytes(d);
            K::decode_from(&mut k_reuse, k, &mut k_storage)?;
            V::decode_from(&mut v_reuse, v, &mut v_storage)?;

            builder.push(&k_reuse, &v_reuse, t, d);
        }

        let Part2 {
            key,
            key_stats,
            val,
            ..
        } = builder.finish();
        let key_stats = match key_stats.into_non_null_values() {
            Some(ColumnStatKinds::Struct(stats)) => stats,
            key_stats => Err(format!(
                "found non-StructStats when encoding updates, {key_stats:?}"
            ))?,
        };

        Ok((
            Some(ColumnarRecordsStructuredExt { key, val }),
            PartStats { key: key_stats },
        ))
    } else {
        let mut builder = PartBuilder::new(schemas.key.as_ref(), schemas.val.as_ref())?;
        for ((k, v), t, d) in updates.iter() {
            let k = K::decode(k)?;
            let v = V::decode(v)?;
            let t = i64::from_le_bytes(t);
            let d = i64::from_le_bytes(d);

            builder.push(&k, &v, t, d);
        }

        let part = builder.finish();
        let stats = PartStats::new(&part)?;

        Ok((None, stats))
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

/// Statistics about the contents of the parts of a shard as_of some time.
#[derive(Debug)]
pub struct SnapshotPartsStats {
    /// Metrics for the persist backing shard, so the caller can report any
    /// necessary counters.
    pub metrics: Arc<Metrics>,
    /// The shard these statistics are for.
    pub shard_id: ShardId,
    /// Stats for individual parts.
    pub parts: Vec<SnapshotPartStats>,
}

/// Part-specific stats.
#[derive(Debug)]
pub struct SnapshotPartStats {
    /// The size of the encoded data in bytes.
    pub encoded_size_bytes: usize,
    /// The raw/encoded statistics for that part, if we have them.
    pub stats: Option<LazyPartStats>,
}
