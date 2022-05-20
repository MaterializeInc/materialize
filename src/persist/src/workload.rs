// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A configurable data generator for benchmarking.

use std::cmp;
use std::env::{self, VarError};
use std::io::Write;
use std::mem::size_of;

use mz_ore::cast::CastFrom;

use crate::indexed::columnar::{ColumnarRecords, ColumnarRecordsBuilder};

/// A configurable data generator for benchmarking.
#[derive(Clone, Debug)]
pub struct DataGenerator {
    /// The total number of records to produce.
    pub record_count: usize,
    /// The number of "goodput" bytes to make each record.
    pub record_size_bytes: usize,
    /// The maximum number of records included in a generated batch of records.
    pub batch_max_count: usize,
    // TODO: unique: bool,
    key_buf: Vec<u8>,
    val_buf: Vec<u8>,
}

const RECORD_SIZE_BYTES_KEY: &'static str = "MZ_PERSIST_RECORD_SIZE_BYTES";
const RECORD_COUNT_KEY: &'static str = "MZ_PERSIST_RECORD_COUNT";
const BATCH_MAX_COUNT_KEY: &'static str = "MZ_PERSIST_BATCH_MAX_COUNT";

const RECORD_SIZE_BYTES_KEY_SMALL: &'static str = "MZ_PERSIST_RECORD_SIZE_BYTES_SMALL";
const RECORD_COUNT_KEY_SMALL: &'static str = "MZ_PERSIST_RECORD_COUNT_SMALL";
const BATCH_MAX_COUNT_KEY_SMALL: &'static str = "MZ_PERSIST_BATCH_MAX_COUNT_SMALL";

// Selected arbitrarily as representative of production record sizes.
const DEFAULT_RECORD_SIZE_BYTES: usize = 64;
// Selected arbitrarily to make ~8MiB batches.
const DEFAULT_BATCH_MAX_COUNT: usize = (8 * 1024 * 1024) / DEFAULT_RECORD_SIZE_BYTES;
// Manually tuned once to be the smallest value such that (1) we hit max
// throughput on the end_to_end benchmark and (2) goodput_pretty produces a
// round-ish number.
const DEFAULT_RECORD_COUNT: usize = 819_200;

// Selected arbitrarily as representative of production record sizes.
const DEFAULT_RECORD_SIZE_BYTES_SMALL: usize = 1024;
// Selected to produce a small number of batches with default settings.
const DEFAULT_BATCH_MAX_COUNT_SMALL: usize = DEFAULT_RECORD_COUNT_SMALL / 8;
// Selected arbitrarily to make ~64KiB of data.
const DEFAULT_RECORD_COUNT_SMALL: usize = (64 * 1024) / DEFAULT_RECORD_SIZE_BYTES_SMALL;

const TS_DIFF_GOODPUT_SIZE: usize = size_of::<u64>() + size_of::<i64>();

fn read_env_usize(key: &str, default: usize) -> usize {
    match env::var(key) {
        Ok(x) => x
            .parse()
            .map_err(|err| format!("invalid value for {}: {}", key, err))
            .unwrap(),
        Err(VarError::NotPresent) => default,
        Err(err) => panic!("invalid value for {}: {}", key, err),
    }
}

impl Default for DataGenerator {
    fn default() -> Self {
        let record_count = read_env_usize(RECORD_COUNT_KEY, DEFAULT_RECORD_COUNT);
        let record_size_bytes = read_env_usize(RECORD_SIZE_BYTES_KEY, DEFAULT_RECORD_SIZE_BYTES);
        let batch_max_count = read_env_usize(BATCH_MAX_COUNT_KEY, DEFAULT_BATCH_MAX_COUNT);

        eprintln!(
            "{}={} {}={} {}={}",
            RECORD_COUNT_KEY,
            record_count,
            RECORD_SIZE_BYTES_KEY,
            record_size_bytes,
            BATCH_MAX_COUNT_KEY,
            batch_max_count,
        );
        DataGenerator::new(record_count, record_size_bytes, batch_max_count)
    }
}

impl DataGenerator {
    /// Returns a new [DataGenerator].
    pub fn new(record_count: usize, record_size_bytes: usize, batch_max_count: usize) -> Self {
        // NB: Strict greater so we have at least one byte for key.
        assert!(record_size_bytes > TS_DIFF_GOODPUT_SIZE);
        assert!(batch_max_count > 0);
        DataGenerator {
            record_count,
            record_size_bytes,
            batch_max_count,
            key_buf: Vec::new(),
            val_buf: Vec::new(),
        }
    }

    /// Returns a new [DataGenerator] specifically for testing small data volumes.
    pub fn small() -> Self {
        let record_count_small = read_env_usize(RECORD_COUNT_KEY_SMALL, DEFAULT_RECORD_COUNT_SMALL);
        let record_size_bytes_small =
            read_env_usize(RECORD_SIZE_BYTES_KEY_SMALL, DEFAULT_RECORD_SIZE_BYTES_SMALL);
        let batch_max_count_small =
            read_env_usize(BATCH_MAX_COUNT_KEY_SMALL, DEFAULT_BATCH_MAX_COUNT_SMALL);

        eprintln!(
            "{}={} {}={} {}={}",
            RECORD_COUNT_KEY_SMALL,
            record_count_small,
            RECORD_SIZE_BYTES_KEY_SMALL,
            record_size_bytes_small,
            BATCH_MAX_COUNT_KEY_SMALL,
            batch_max_count_small,
        );
        DataGenerator::new(
            record_count_small,
            record_size_bytes_small,
            batch_max_count_small,
        )
    }

    /// Returns the number of "goodput" bytes represented by the entire dataset
    /// produced by this generator.
    pub fn goodput_bytes(&self) -> u64 {
        u64::cast_from(self.record_count * self.record_size_bytes)
    }

    /// Returns a more easily human readable version of [Self::goodput_bytes].
    pub fn goodput_pretty(&self) -> String {
        let goodput_bytes = self.goodput_bytes();
        const KIB: u64 = 1024;
        const MIB: u64 = 1024 * KIB;
        const GIB: u64 = 1024 * MIB;
        if goodput_bytes >= 10 * GIB {
            format!("{}GiB", goodput_bytes / GIB)
        } else if goodput_bytes >= 10 * MIB {
            format!("{}MiB", goodput_bytes / MIB)
        } else if goodput_bytes >= 10 * KIB {
            format!("{}KiB", goodput_bytes / KIB)
        } else {
            format!("{}B", goodput_bytes)
        }
    }

    /// Generates the requested batch of records.
    pub fn gen_batch(&mut self, batch_idx: usize) -> Option<ColumnarRecords> {
        let batch_start = self.batch_max_count * batch_idx;
        let batch_end = cmp::min(batch_start + self.batch_max_count, self.record_count);
        if batch_start >= batch_end {
            return None;
        }
        let mut batch = ColumnarRecordsBuilder::default();
        batch.reserve(
            batch_end - batch_start,
            self.record_size_bytes - TS_DIFF_GOODPUT_SIZE,
            0,
        );
        for record_idx in batch_start..batch_end {
            assert!(
                batch.push(self.gen_record(record_idx)),
                "generator exceeded batch size; smaller batches needed"
            );
        }
        Some(batch.finish())
    }

    fn gen_record(&mut self, record_idx: usize) -> ((&[u8], &[u8]), u64, i64) {
        assert!(record_idx < self.record_count);
        assert!(self.record_size_bytes > TS_DIFF_GOODPUT_SIZE);

        self.key_buf.clear();
        let key_len = self.record_size_bytes - TS_DIFF_GOODPUT_SIZE;
        if self.key_buf.capacity() < key_len {
            self.key_buf.reserve(key_len);
        }
        // This format `record_idx` as an integer and, if necessary, left-pads
        // it with 0s to be `key_len` chars long.
        write!(&mut self.key_buf, "{:01$}", record_idx, key_len)
            .expect("write to Vec is infallible");
        self.key_buf.truncate(key_len);
        assert_eq!(self.key_buf.len(), key_len);

        self.val_buf.clear();

        let ts = u64::cast_from(record_idx);
        let diff = 1;
        ((&self.key_buf, &self.val_buf), ts, diff)
    }

    /// Returns an [Iterator] of all records in batches.
    pub fn batches(&self) -> DataGeneratorBatchIter {
        DataGeneratorBatchIter {
            config: self.clone(),
            batch_idx: 0,
        }
    }

    /// Returns an [Iterator] of all records.
    pub fn records(&self) -> impl Iterator<Item = ((Vec<u8>, Vec<u8>), u64, i64)> {
        let mut config = self.clone();
        (0..self.record_count).map(move |record_idx| {
            let ((k, v), t, d) = config.gen_record(record_idx);
            ((k.to_vec(), v.to_vec()), t, d)
        })
    }
}

/// An implementation of [Iterator] for [DataGenerator].
#[derive(Debug)]
pub struct DataGeneratorBatchIter {
    config: DataGenerator,
    batch_idx: usize,
}

impl Iterator for DataGeneratorBatchIter {
    type Item = ColumnarRecords;

    fn next(&mut self) -> Option<Self::Item> {
        let batch_idx = self.batch_idx;
        self.batch_idx += 1;
        self.config.gen_batch(batch_idx)
    }
}

/// Encodes the given data into a flat buffer that is exactly
/// `data.goodput_bytes()` long.
pub fn flat_blob(data: &DataGenerator) -> Vec<u8> {
    let mut buf = Vec::with_capacity(usize::cast_from(data.goodput_bytes()));
    for batch in data.batches() {
        for ((k, v), t, d) in batch.iter() {
            buf.extend_from_slice(k);
            buf.extend_from_slice(v);
            buf.extend_from_slice(&t.to_le_bytes());
            buf.extend_from_slice(&d.to_le_bytes());
        }
    }
    assert_eq!(buf.len(), usize::cast_from(data.goodput_bytes()));
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_invariants() {
        fn testcase(c: DataGenerator) {
            let (mut actual_len, mut actual_goodput_bytes) = (0, 0);
            for batch in c.batches() {
                for ((k, v), _, _) in batch.iter() {
                    actual_len += 1;
                    actual_goodput_bytes += k.len() + v.len() + TS_DIFF_GOODPUT_SIZE;
                }
            }
            assert_eq!(actual_len, c.record_count);
            assert_eq!(actual_goodput_bytes, usize::cast_from(c.goodput_bytes()));
        }

        testcase(DataGenerator::new(1, 32, 1));
        testcase(DataGenerator::new(1, 32, 100));
        testcase(DataGenerator::new(100, 32, 7));
        testcase(DataGenerator::new(1000, 32, 100));
    }

    #[test]
    fn goodput_pretty() {
        fn testcase(bytes: usize) -> String {
            DataGenerator::new(1, bytes, 1).goodput_pretty()
        }

        assert_eq!(testcase(33), "33B");
        assert_eq!(testcase(10 * 1024 - 1), "10239B");
        assert_eq!(testcase(10 * 1024), "10KiB");
        assert_eq!(testcase(10 * 1024 * 1024 - 1), "10239KiB");
        assert_eq!(testcase(10 * 1024 * 1024), "10MiB");
        assert_eq!(testcase(10 * 1024 * 1024 * 1024 - 1), "10239MiB");
        assert_eq!(testcase(10 * 1024 * 1024 * 1024), "10GiB");
        assert_eq!(testcase(10 * 1024 * 1024 * 1024 * 1024 - 1), "10239GiB");
        assert_eq!(testcase(10 * 1024 * 1024 * 1024 * 1024), "10240GiB"); // No TiB
    }
}
