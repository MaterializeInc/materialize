// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Projection pushdown

use differential_dataflow::trace::Description;
use mz_dyncfg::{Config, ConfigSet};
use mz_ore::cast::CastFrom;
use mz_persist::indexed::columnar::ColumnarRecordsBuilder;
use mz_persist::indexed::encoding::BlobTraceUpdates;
use mz_persist_types::stats::PartStats;
use mz_persist_types::Codec64;
use mz_proto::RustType;
use timely::progress::{Antichain, Timestamp};
use tracing::debug;

use crate::internal::encoding::LazyInlineBatchPart;
use crate::internal::metrics::Metrics;
use crate::internal::state::{BatchPart, ProtoInlineBatchPart};

pub(crate) const OPTIMIZE_IGNORED_DATA_FETCH: Config<bool> = Config::new(
    "persist_optimize_ignored_data_fetch",
    true,
    "CYA to allow opt-out of a performance optimization to skip fetching ignored data",
);

pub(crate) const OPTIMIZE_IGNORED_DATA_DECODE: Config<bool> = Config::new(
    "persist_optimize_ignored_data_decode",
    true,
    "CYA to allow opt-out of a performance optimization to skip decoding ignored data",
);

/// Information about which columns of persist-stored data may not be needed.
///
/// TODO: This is mostly a placeholder for real projection pushdown, but in the
/// short-term it allows us a special case of projection pushdown: ignoring all
/// non-`Err` data. See [ProjectionPushdown::try_optimize_ignored_data_fetch].
#[derive(Debug, Clone)]
#[allow(rustdoc::private_intra_doc_links)]
pub enum ProjectionPushdown {
    /// Fetch all columns.
    FetchAll,
    /// For data with a top-level error column in the structured representation,
    /// ignore all columns except for data in parts that may contain an error.
    /// This may seem like a peculiar set of requirements, but it enables the
    /// aggressive [Self::try_optimize_ignored_data_fetch] optimization and it
    /// corresponds to a common query shape: `SELECT count(*)`.
    ///
    /// This error bit is certainly a bit of an abstraction breakage in the
    /// "persist is independent of mz" story, but it should go away when we
    /// implement full projection pushdown.
    IgnoreAllNonErr {
        /// The name of the top-level error column.
        err_col_name: &'static str,
        /// The `Codec` encoded key corresponding to ignored data.
        key_bytes: Vec<u8>,
        /// The `Codec` encoded val corresponding to ignored data.
        val_bytes: Vec<u8>,
    },
}

impl Default for ProjectionPushdown {
    fn default() -> Self {
        ProjectionPushdown::FetchAll
    }
}

impl ProjectionPushdown {
    /// If relevant, applies the [Self::IgnoreAllNonErr] projection to a part
    /// about to be fetched.
    ///
    /// If a part contains data from entirely before a snapshot `as_of`, and the
    /// pushed-down MFP projects to an empty list of columns, and we can prove
    /// that the part is error free, then we can use the diff sum from stats
    /// instead of loading the data. In this case, we return a `Some` with a
    /// replacement [BatchPart]. In all other cases, a None.
    ///
    /// - Summing the diffs in a part is equivalent to projecting the row in
    ///   each tuple to an empty row and then consolidating. (Which is pretty
    ///   much how `select count(*)` queries get compiled today.)
    /// - If the as-of timestamp falls in the middle of a part, we can just
    ///   fetch and process the part as normal. The optimization can still
    ///   provide a speedup for other parts. TODO: We could improve this by
    ///   keeping track in metadata of the largest timestamp(s) in a hollow
    ///   part.
    pub(crate) fn try_optimize_ignored_data_fetch<T: Timestamp + Codec64>(
        &self,
        cfg: &ConfigSet,
        metrics: &Metrics,
        as_of: &Antichain<T>,
        desc: &Description<T>,
        part: &BatchPart<T>,
    ) -> Option<BatchPart<T>> {
        if !OPTIMIZE_IGNORED_DATA_FETCH.get(cfg) {
            return None;
        }
        let (err_col_name, key_bytes, val_bytes) = match self {
            ProjectionPushdown::FetchAll => return None,
            ProjectionPushdown::IgnoreAllNonErr {
                err_col_name,
                key_bytes,
                val_bytes,
            } => (*err_col_name, key_bytes.as_slice(), val_bytes.as_slice()),
        };
        let (diffs_sum, stats) = match &part {
            BatchPart::Hollow(x) => (x.diffs_sum, x.stats.as_ref()),
            BatchPart::Inline { .. } => return None,
        };
        debug!(
            "try_optimize_ignored_data_fetch diffs_sum={:?} as_of={:?} lower={:?} upper={:?}",
            // This is only used for debugging, so hack to assume that D is i64.
            diffs_sum.map(i64::decode),
            as_of.elements(),
            desc.lower().elements(),
            desc.upper().elements()
        );
        let as_of = match &as_of.elements() {
            &[as_of] => as_of,
            _ => return None,
        };
        let eligible = desc.upper().less_equal(as_of) && desc.since().less_equal(as_of);
        if !eligible {
            return None;
        }
        let Some(diffs_sum) = diffs_sum else {
            return None;
        };
        let Some(true) = error_free(stats.map(|x| x.decode()), err_col_name) else {
            return None;
        };

        debug!(
            "try_optimize_ignored_data_fetch faked {:?} diffs at ts {:?} skipping fetch of {} bytes",
            // This is only used for debugging, so hack to assume that D is i64.
            i64::decode(diffs_sum),
            as_of,
            part.encoded_size_bytes(),
        );
        metrics.pushdown.parts_faked_count.inc();
        metrics
            .pushdown
            .parts_faked_bytes
            .inc_by(u64::cast_from(part.encoded_size_bytes()));

        let mut faked_data = ColumnarRecordsBuilder::default();
        assert!(faked_data.push(((key_bytes, val_bytes), T::encode(as_of), diffs_sum)));
        let updates = BlobTraceUpdates::Row(faked_data.finish(&metrics.columnar)).into_proto();
        let faked_data = LazyInlineBatchPart::from(&ProtoInlineBatchPart {
            desc: Some(desc.into_proto()),
            index: 0,
            updates: Some(updates),
        });
        Some(BatchPart::Inline {
            updates: faked_data,
            ts_rewrite: None,
            schema_id: None,
            deprecated_schema_id: None,
        })
    }
}

/// Returns whether the part is provably free of `SourceData(Err(_))`s.
///
/// Will return false if the part is known to contain errors or None if it's
/// unknown.
pub fn error_free(part_stats: Option<PartStats>, err_col_name: &str) -> Option<bool> {
    let part_stats = part_stats?;
    // Counter-intuitive: We can easily calculate the number of errors that
    // were None from the column stats, but not how many were Some. So, what
    // we do is count the number of Nones, which is the number of Oks, and
    // then subtract that from the total.
    let num_results = part_stats.key.len;
    // The number of OKs is the number of rows whose error is None.
    let num_oks = part_stats
        .key
        .col(err_col_name)?
        .try_as_optional_bytes()
        .expect("err column should be a Option<Vec<u8>>")
        .none;
    Some(num_results == num_oks)
}
