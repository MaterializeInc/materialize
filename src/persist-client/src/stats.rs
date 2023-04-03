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

use crate::internal::encoding::Schemas;

/// Aggregate statistics about data contained in a [Part].
#[derive(Debug)]
pub struct PartStats {
    /// Aggregate statistics about key data contained in a [Part].
    pub key: StructStats,
}

impl PartStats {
    pub(crate) fn new<K: Codec, V: Codec>(
        schemas: &Schemas<K, V>,
        part: &Part,
    ) -> Result<Self, String> {
        let mut key = StructStats {
            len: part.len(),
            cols: Default::default(),
        };
        // TODO(mfp): We only need to plumb the schemas here if we end up
        // allowing them to specify arbitrary min/max logic for complex types.
        // If we don't end up doing that, consider pulling the schemas out of
        // here.
        let mut key_cols = part.key_ref();
        for (name, _typ) in schemas.key.columns() {
            let col_stats = key_cols.stats(&name)?;
            key.cols.insert(name, col_stats);
        }
        key_cols.finish()?;
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
        let Self { key } = self;
        key.len == 0
    }
}
