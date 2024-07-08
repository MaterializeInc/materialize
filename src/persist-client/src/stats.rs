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

use mz_dyncfg::{Config, ConfigSet};
use mz_ore::soft_panic_or_log;
use mz_persist::indexed::encoding::BlobTraceUpdates;
use mz_persist_types::part::{Part, PartBuilder};
use mz_persist_types::stats::PartStats;
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

/// Encodes a [`BlobTraceUpdates`] into a [`Part`] and calculates [`PartStats`].
///
/// Note: The [`Part`] will contain the same data as [`BlobTraceUpdates`], but
/// the [`Part`] will have the data fully structured as opposed to an opaque
/// binary blob.
pub(crate) fn encode_updates<K, V>(
    schemas: &Schemas<K, V>,
    updates: &BlobTraceUpdates,
) -> Result<(Part, PartStats), String>
where
    K: Codec,
    V: Codec,
{
    let updates = match updates {
        BlobTraceUpdates::Row(updates) => itertools::Either::Left(updates.into_iter()),
        BlobTraceUpdates::Both(codec, _structured) => {
            // This is super unexpected, but not worthy of a panic.
            soft_panic_or_log!("re-encoding structured data?");
            itertools::Either::Right(std::iter::once(codec))
        }
    };

    let mut builder = PartBuilder::new(schemas.key.as_ref(), schemas.val.as_ref())?;
    for update in updates {
        for ((k, v), t, d) in update.iter() {
            let k = K::decode(k)?;
            let v = V::decode(v)?;
            let t = i64::from_le_bytes(t);
            let d = i64::from_le_bytes(d);

            builder.push(&k, &v, t, d);
        }
    }
    let part = builder.finish();
    let stats = PartStats::new(&part)?;

    Ok((part, stats))
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
