use std::collections::BTreeMap;
use std::fmt::Debug;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_persist_client::read::ReadHandle;
use mz_persist_types::Codec64;
use mz_repr::{DatumVec, Row};
use mz_storage_types::sources::SourceData;
use timely::progress::{Antichain, Timestamp};

pub struct KeyValueReadHandle<T, D> {
    handle: ReadHandle<SourceData, (), T, D>,
}

impl<T, D> KeyValueReadHandle<T, D>
where
    T: Timestamp + Lattice + Codec64 + Sync,
    D: Semigroup + Codec64 + Send + Sync,
{
    pub fn new(handle: ReadHandle<SourceData, (), T, D>) -> Self {
        todo!()
    }

    pub async fn get_multi(
        &mut self,
        key_columns: &[usize],
        keys: DatumVec,
        ts: T,
    ) -> Vec<(DatumVec, Row)> {
        let as_of = Antichain::from_elem(ts);
        let batch_parts = self.handle.snapshot(as_of).await.expect("OH NO");
        for part in batch_parts {}

        todo!()
    }

    pub fn apply_changes(&mut self, changes: Vec<(SourceData, T, D)>) {}
}
