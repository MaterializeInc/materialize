// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use columnar::FixedSizeCodec;
use parquet::bloom_filter::{Sbbf, hash};
use serde::{Serialize, Serializer};

use crate::columnar;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct BloomFilter {
    #[serde(serialize_with = "serialize_bloom_filter")]
    inner: Sbbf,
}

impl BloomFilter {
    fn new(inner: Sbbf) -> Self {
        Self { inner }
    }

    fn contains<T: Sized, K: FixedSizeCodec<T>>(&self, k: K) -> bool {
        self.inner.check_hash(hash(k.as_bytes()))
    }

    fn update(&mut self, filter: Sbbf) {
        self.inner = filter;
    }
}

fn serialize_bloom_filter<S: Serializer>(filter: &Sbbf, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_bytes(filter.get_bitset().as_slice())
}

pub fn bloom_filter_from_bytes(bytes: &[u8]) -> BloomFilter {
    let sbbf = Sbbf::new(bytes);
    BloomFilter::new(sbbf)
}
