// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Aggregate statistics about data stored in persist.

use std::collections::BTreeMap;

use mz_persist::indexed::columnar::ColumnarRecords;
use mz_persist_types::columnar::{ColumnFormat, PartEncoder, Schema};
use mz_persist_types::part::{Part, PartBuilder};
use mz_persist_types::Codec;

use crate::internal::encoding::Schemas;

/// Logic for filtering parts in a read as uninteresting purely from stats about
/// the data held in them.
pub trait PartFilter<K, V>: 'static {
    /// True if the part should be fetched, false if it should be skipped.
    fn should_fetch(&mut self, stats: &PartStats) -> bool;
}

/// An implementation of [PartFilter] that always returns true.
#[derive(Debug)]
pub struct FetchAllPartFilter;

impl<K, V> PartFilter<K, V> for FetchAllPartFilter {
    fn should_fetch(&mut self, _stats: &PartStats) -> bool {
        true
    }
}

/// Aggregate statistics about data contained in a [Part].
///
/// TODO(mfp): PartialEq, Eq, PartialOrd, Ord are sus.
#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct PartStats {
    // TODO(mfp): Only contains Option<u64> columns in this initial skeleton.
    pub(crate) key_col_min_max_nulls: BTreeMap<String, (u64, u64, usize)>,
}

impl PartStats {
    pub(crate) fn new<K: Codec, V: Codec>(
        schemas: &Schemas<K, V>,
        part: &Part,
    ) -> Result<Self, String> {
        // TODO(mfp): This impl is all entirely just a placeholder. Zero chance
        // it works like this by the time it ships.
        let mut key_cols = part.key_ref();
        let mut key_col_min_max_nulls = BTreeMap::new();
        // TODO(mfp): We only need to plumb the schemas here if we end up
        // allowing them to specify arbitrary min/max logic for complex types.
        // If we don't end up doing that, consider pulling the schemas out of
        // here.
        for (name, typ) in schemas.key.columns() {
            let col = match (typ.optional, typ.format) {
                (true, ColumnFormat::U64) => key_cols.col::<Option<u64>>(name.as_str())?,
                // TODO(mfp): Support stats on other column types.
                _ => continue,
            };
            let (mut min, mut max, mut nulls) = (u64::MAX, u64::MIN, 0usize);
            for x in col.iter() {
                if let Some(x) = x {
                    min = std::cmp::min(*x, min);
                    max = std::cmp::max(*x, max);
                } else {
                    nulls += 1;
                }
            }
            // Skip adding nonsensical min=u64::MAX max=u64::MIN. This might
            // lose null count for an all null batch, but we keep sparse metrics
            // so fine for placeholder impl.
            if min <= max {
                key_col_min_max_nulls.insert(name, (min, max, nulls));
            }
        }
        drop(key_cols);
        Ok(PartStats {
            key_col_min_max_nulls,
        })
    }

    pub(crate) fn legacy_part_format<K: Codec, V: Codec>(
        schemas: &Schemas<K, V>,
        part: &[ColumnarRecords],
    ) -> Result<Self, String> {
        // This is a laughably inefficient placeholder implementation of stats
        // on the old part format. We don't intend to make this fast, rather we
        // intend to compute stats on the new part format.
        let mut new_format = PartBuilder::new(schemas.key.as_ref(), schemas.val.as_ref());
        let builder = new_format.get_mut();
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
        Self::new(schemas, &new_format)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self == &Self::default()
    }

    /// The min and max values as well as null count for an `Option<u64>` column
    /// in this part.
    ///
    /// Returns `None` if persist doesn't have stats about this column, or if
    /// the column is not `Option<u64>`.
    ///
    /// TODO(mfp): This is decidedly not the final public interface.
    pub fn opt_u64_key_col_min_max_nulls(&self, name: &str) -> Option<(u64, u64, usize)> {
        self.key_col_min_max_nulls.get(name).copied()
    }
}
