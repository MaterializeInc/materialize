// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use columnar::FixedSizeCodec;
use parquet::bloom_filter::Sbbf;
use serde::{Serialize, Serializer};

use crate::columnar;

#[derive(Clone, Debug)]
pub struct BloomFilter {
    inner: Sbbf,
}

impl BloomFilter {
    fn new(inner: Sbbf) -> Self {
        Self { inner }
    }

    fn contains<T, K: FixedSizeCodec<T>>(&self, k: K) -> bool {
        self.inner.check(k.as_bytes())
    }

    fn update(&mut self, filter: Sbbf) {
        self.inner = filter;
    }
}

fn serialize_bloom_filter<S: Serializer>(filter: Sbbf, s: S) -> Result<S::Ok, S::Error> {
    filter.get_bitset()
}
