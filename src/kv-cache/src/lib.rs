use mz_persist_client::read::ReadHandle;
use mz_repr::{DatumVec, Row};
use mz_storage_types::sources::SourceData;

pub struct KeyValueReadHandle<T, D> {
    handle: ReadHandle<SourceData, (), T, D>,
}

impl<T, D> KeyValueReadHandle<T, D> {
    pub fn new(handle: ReadHandle<SourceData, (), T, D>) -> Self {
        todo!()
    }

    pub fn get_multi(&self, key_columns: &[usize], keys: DatumVec, ts: T) -> Vec<(DatumVec, Row)> {
        todo!()
    }

    pub fn apply_changes(&mut self, changes: Vec<(SourceData, T, D)>) {}
}
