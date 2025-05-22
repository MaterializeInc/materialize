use std::collections::BTreeSet;

use differential_dataflow::lattice::Lattice;
use mz_persist_client::lru::Lru;
use mz_persist_client::read::ReadHandle;
use mz_persist_types::Codec64;
use mz_persist_types::bloom_filter::BloomFilter;
use mz_repr::{DatumVec, Row, TimestampManipulation};
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::SourceData;
use timely::progress::{Antichain, Timestamp};

/// TODO(upsert-in-persist), make this configurable.
///
/// 20 MiB
const LRU_CACHE_SIZE: usize = 20 * 1024 * 1024;

pub struct KeyValueReadHandle<T> {
    handle: ReadHandle<SourceData, (), T, StorageDiff>,
    cache: Lru<Row, Row>,
    cache_upper: T,
}

impl<T> KeyValueReadHandle<T>
where
    T: Timestamp + Lattice + Codec64 + Sync + TimestampManipulation,
{
    pub fn new(handle: ReadHandle<SourceData, (), T, StorageDiff>) -> Self {
        Self {
            handle,
            cache: Lru::new(LRU_CACHE_SIZE, move |_, _, _| {}),
            cache_upper: T::minimum(),
        }
    }

    pub async fn get_multi(
        &mut self,
        key_columns: &[usize],
        keys: Vec<Row>,
        ts: T,
    ) -> Vec<(Row, Row)> {
        // Check the cache first.
        // let mut cached_values = Vec::new();
        // let mut obtained_keys = BTreeSet::new();
        // for key in &keys {
        //     if let Some((_key, cached_val)) = self.cache.get(key) {
        //         tracing::info!(?key, ?ts, ?self.cache_upper, "found in cache");
        //         // if self.cache_upper.step_back().unwrap_or(T::minimum()) == ts {
        //         if self.cache_upper == ts {
        //             tracing::info!(?key, ?ts, "returning cached value");
        //             cached_values.push((key.clone(), cached_val.clone()));
        //             obtained_keys.insert(key.clone());
        //         }
        //     }
        // }

        // // Skip querying for keys that have already been obtained from the cache.
        // let keys: Vec<_> = keys
        //     .into_iter()
        //     .filter(|key| !obtained_keys.contains(key))
        //     .collect();

        // // If there isn't anything else to obtain then return early!
        // if keys.is_empty() {
        //     // return Vec::new();
        //     return cached_values;
        // }

        let as_of = Antichain::from_elem(ts);
        let batch_parts = self.handle.snapshot(as_of).await.expect("OH NO");
        assert_eq!(key_columns.len(), 1, "support composite keys");
        let key_col = key_columns[0];

        // We should fetch a RowGroup in a Part if it contains any of our keys.
        let mut datum_vec_a = DatumVec::new();
        let mut encode_buffer = Vec::new();
        let mut should_fetch = |bloom_filter: &BloomFilter| {
            keys.iter().any(|row| {
                let datums = datum_vec_a.borrow_with(row);
                assert_eq!(datums.len(), 1, "composite keys");
                let key = datums[0];
                let contains = bloom_filter.contains(key, &mut encode_buffer);
                // let contains = true;
                if contains {
                    tracing::info!("matched bloom filter for key {key}");
                } else {
                    tracing::info!("did not match bloom filter for key {key}");
                }
                contains
            })
        };

        let mut filtered_values = Vec::new();
        let mut datum_vec_b = DatumVec::new();
        let mut datum_vec_c = DatumVec::new();

        // TODO(upsert-in-persist)
        //
        // There are two more things we can do to make this faster:
        // 1. Check the statistics for the primary key column on each `Part`
        //    before even looking at the bloom filters.
        // 2. Sort the list of `Part`s by their upper in a descending order.
        //    If a key exists in an upsert source it is guaranteed to have a
        //    diff of 1 or -1, meaning the latest value for a key is the
        //    correct value. There is no need to scan for all previous
        //    instances of the key and then consolidate.

        for part in batch_parts {
            // Check if this part could possibly match any of the bloom filters.
            // let could_match = if let Some(mut bloom_filters) = part.pkey_bloom_filters() {
            //     bloom_filters.any(|filter| should_fetch(filter))
            // } else {
            //     // If there are no bloom filters then we could always match.
            //     true
            // };

            // If we can't possibly match this part then don't fetch it.
            // if !could_match {
            //     continue;
            // }

            let values = self.handle.fetch_values(&part, &mut should_fetch).await;
            for ((source_data, _unit_type), _ts, diff) in values {
                let source_data = source_data.expect("HACK WEEK");
                let candidate_row = source_data.0.expect("HACK WEEK");

                let maybe_matching_key = {
                    let candidate_datums = datum_vec_b.borrow_with(&candidate_row);
                    keys.iter().find(|wanted_row| {
                        let wanted_datums = datum_vec_c.borrow_with(wanted_row);
                        assert_eq!(wanted_datums.len(), 1, "composite keys");
                        wanted_datums[0] == candidate_datums[key_col]
                    })
                };
                if let Some(matching_key) = maybe_matching_key {
                    tracing::info!(?matching_key, ?candidate_row, ?diff, "found matching key");
                    filtered_values.push(((matching_key.clone(), candidate_row), diff));
                }
            }
        }

        differential_dataflow::consolidation::consolidate(&mut filtered_values);
        if !filtered_values.iter().all(|(_x, diff)| *diff == 1) {
            tracing::info!("filtered values: {filtered_values:?}");
        }
        // filtered_values.iter().all(|(_x, diff)| *diff == 1);

        filtered_values
            .into_iter()
            .map(|(payload, _diff)| payload)
            // .chain(cached_values)
            .collect()
    }

    pub fn apply_changes(&mut self, mut changes: Vec<((Row, Row), T, StorageDiff)>, upper: T) {
        changes.sort_by(|a, b| (a.1.clone(), a.2).cmp(&(b.1.clone(), b.2)));
        self.cache_upper = upper;

        for ((key, val), _ts, diff) in changes {
            if diff == 1 {
                let weight = val.byte_len();
                self.cache.insert(key, val, weight);
            } else if diff == -1 {
                self.cache.remove(&key);
            } else {
                panic!("unexpected diff value {diff}");
            }
        }
    }
}
