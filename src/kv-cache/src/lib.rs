use differential_dataflow::lattice::Lattice;
use mz_persist_client::read::ReadHandle;
use mz_persist_types::Codec64;
use mz_persist_types::bloom_filter::BloomFilter;
use mz_repr::{DatumVec, Row};
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::SourceData;
use timely::progress::{Antichain, Timestamp};

pub struct KeyValueReadHandle<T> {
    handle: ReadHandle<SourceData, (), T, StorageDiff>,
}

impl<T> KeyValueReadHandle<T>
where
    T: Timestamp + Lattice + Codec64 + Sync,
{
    pub fn new(handle: ReadHandle<SourceData, (), T, StorageDiff>) -> Self {
        todo!()
    }

    pub async fn get_multi(
        &mut self,
        key_columns: &[usize],
        keys: Vec<Row>,
        ts: T,
    ) -> Vec<(Row, Row)> {
        let as_of = Antichain::from_elem(ts);
        let batch_parts = self.handle.snapshot(as_of).await.expect("OH NO");
        assert_eq!(key_columns.len(), 1, "support composite keys");
        let key_col = key_columns[0];

        // We should fetch a RowGroup in a Part if it contains any of our keys.
        let mut datum_vec_a = DatumVec::new();
        let mut should_fetch = |bloom_filter: &BloomFilter| {
            keys.iter().any(|row| {
                let datums = datum_vec_a.borrow_with(row);
                assert_eq!(datums.len(), 1, "composite keys");
                let key = datums[0];
                bloom_filter.contains(key)
            })
        };

        let mut filtered_values = Vec::new();
        let mut datum_vec_b = DatumVec::new();
        let mut datum_vec_c = DatumVec::new();

        for part in batch_parts {
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
                    filtered_values.push(((matching_key.clone(), candidate_row), diff));
                }
            }
        }

        differential_dataflow::consolidation::consolidate(&mut filtered_values);
        assert!(filtered_values.iter().all(|(_x, diff)| *diff == 1));

        filtered_values
            .into_iter()
            .map(|(payload, _diff)| payload)
            .collect()
    }

    pub fn apply_changes(&mut self, changes: Vec<(SourceData, T, StorageDiff)>) {}
}
