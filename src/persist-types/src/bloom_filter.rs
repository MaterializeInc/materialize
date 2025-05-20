// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use parquet::bloom_filter::Sbbf;
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct BloomFilter {}

impl BloomFilter {
    fn new(_inner: Sbbf) -> Self {
        Self {}
    }

    fn contains<K>(&self, k: K) -> bool {
        false
    }

    fn update(&mut self, filter: Sbbf) {}
}
