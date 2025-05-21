// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use parquet::bloom_filter::{Sbbf, hash};
use serde::{Serialize, Serializer};

/// TODO
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct BloomFilter {
    #[serde(serialize_with = "serialize_bloom_filter")]
    inner: Sbbf,
}

impl BloomFilter {
    /// create from an existing Sbbf.
    pub fn new(inner: Sbbf) -> Self {
        Self { inner }
    }

    pub fn from_bitset(bitset: Vec<Vec<u8>>) -> Self {
        Self {
            inner: Sbbf::new(bitset.concat().as_slice()),
        }
    }

    /// check if the key is in the key is in the bloom filter.
    pub fn contains<K: AsParquetBytes>(&self, k: K, buf: &mut Vec<u8>) -> bool {
        buf.clear();
        k.encode_into(buf);
        self.inner.check_hash(hash(&buf[..]))
    }

    /// update underlying bloom filter.
    pub fn update(&mut self, filter: Sbbf) {
        self.inner = filter;
    }

    /// get the underlying bloom filter as a bitset.
    pub fn into_bitset(&self) -> Vec<[u8; 32]> {
        self.inner.into_bitset()
    }
}

fn serialize_bloom_filter<S: Serializer>(filter: &Sbbf, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_bytes(filter.into_bitset().concat().as_slice())
}

pub fn bloom_filter_from_bytes(bytes: &[u8]) -> BloomFilter {
    let sbbf = Sbbf::new(bytes);
    BloomFilter::new(sbbf)
}

/// Encode a given type as Parquet would.
///
/// Allows you to check if a given type is present in a bloom filter.
pub trait AsParquetBytes {
    /// Encode self into the provided buffer as it would be encoded in Parquet.
    fn encode_into(&self, buf: &mut Vec<u8>);
}
