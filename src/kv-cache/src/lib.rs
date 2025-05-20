use mz_persist_client::read::ReadHandle;
use mz_persist_types::Codec;

pub struct KeyValueReadHandle<K: Codec, V: Codec, T, D> {
    handle: ReadHandle<K, V, T, D>,
}

impl<K: Codec, V: Codec, T, D> KeyValueReadHandle<K, V, T, D> {
    pub fn new(handle: ReadHandle<K, V, T, D>) -> Self {
        todo!()
    }

    pub fn get_multi(&self, keys: Vec<K>, ts: T) -> Vec<(K, V)> {
        todo!()
    }

    pub fn apply_changes(&mut self, changes: Vec<(K, V, D)>) {

    }
}
