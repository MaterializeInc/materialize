// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs, dead_code)] // WIP

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

use mz_persist::indexed::encoding::SchemaId;
use mz_persist_types::Codec;

#[derive(Debug)]
pub struct SchemaVersions<K: Codec, V: Codec> {
    pub(crate) key_schemas: BTreeMap<SchemaId, Arc<K::Schema>>,
    pub(crate) val_schemas: BTreeMap<SchemaId, Arc<V::Schema>>,
}

impl<K: Codec, V: Codec> Default for SchemaVersions<K, V> {
    fn default() -> Self {
        Self {
            key_schemas: Default::default(),
            val_schemas: Default::default(),
        }
    }
}

impl<K: Codec, V: Codec> Clone for SchemaVersions<K, V> {
    fn clone(&self) -> Self {
        Self {
            key_schemas: self.key_schemas.clone(),
            val_schemas: self.val_schemas.clone(),
        }
    }
}

impl<K: Codec + std::fmt::Debug, V: Codec> SchemaVersions<K, V> {
    pub(crate) fn migrate_key(&self, from: SchemaId, to: SchemaId, k: &mut K) {
        eprintln!("migrate_key {}->{} BEFORE: {:?}", from, to, k);
        match from.cmp(&to) {
            // Nothign to do!
            Ordering::Equal => {}
            Ordering::Less => {
                let next = from.next();
                mz_persist_types::columnar::migrate_one(
                    self.key_schemas.get(&from).unwrap().as_ref(),
                    self.key_schemas.get(&next).unwrap().as_ref(),
                    k,
                );
                // WIP don't implement with recursion
                self.migrate_key(next, to, k);
            }
            Ordering::Greater => {
                let next = to.next();
                // WIP don't implement with recursion
                self.migrate_key(from, next, k);
                mz_persist_types::columnar::migrate_one(
                    self.key_schemas.get(&next).unwrap().as_ref(),
                    self.key_schemas.get(&to).unwrap().as_ref(),
                    k,
                );
            }
        }
        eprintln!("migrate_key {}->{} AFTER : {:?}", from, to, k);
    }
}

#[cfg(test)]
mod tests {
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_persist_types::columnar::{
        ColumnCfg, ColumnGet, Data, PartDecoder, PartEncoder, Schema,
    };
    use mz_persist_types::dyn_struct::{ColumnsMut, ColumnsRef, DynStructCfg};
    use mz_persist_types::stats::StatsFn;
    use timely::progress::Antichain;

    use crate::fetch::fetch_leased_part;
    use crate::read::ReadHandle;
    use crate::tests::new_test_client;
    use crate::{Diagnostics, ShardId};

    pub use super::*;

    #[derive(Debug, PartialOrd, Ord, PartialEq, Eq)]
    struct U64s(Vec<Option<u64>>);
    impl Codec for U64s {
        type Schema = U64sSchema;
        fn codec_name() -> String {
            "u64s".into()
        }
        fn encode<B: bytes::BufMut>(&self, buf: &mut B) {
            for x in self.0.iter() {
                // WIP hacks
                let x = x.unwrap_or(u64::MAX);
                buf.put_u64_le(x);
            }
        }
        fn decode<'a>(mut buf: &'a [u8]) -> Result<Self, String> {
            let mut ret = U64s(Vec::new());
            while !buf.is_empty() {
                let x = u64::from_le_bytes(buf[..8].try_into().unwrap());
                buf = &buf[8..];
                let x = Some(x).filter(|x| x != &u64::MAX);
                ret.0.push(x)
            }
            Ok(ret)
        }
    }

    #[derive(Debug)]
    struct U64sSchema(u32);
    struct U64sEncoder<'a>(&'a mut usize, Vec<&'a mut <Option<u64> as Data>::Mut>);
    struct U64sDecoder<'a>(Vec<&'a <Option<u64> as Data>::Col>);
    impl U64sSchema {
        fn col_names(&self) -> impl Iterator<Item = String> {
            (0u32..self.0).map(|idx| char::from_u32(u32::from('a') + idx).unwrap().into())
        }
    }
    impl Schema<U64s> for U64sSchema {
        type Encoder<'a> = U64sEncoder<'a>;
        type Decoder<'a> = U64sDecoder<'a>;
        fn columns(&self) -> DynStructCfg {
            let typ = <() as ColumnCfg<Option<u64>>>::as_type(&());
            let cols = self
                .col_names()
                .map(|name| (name, typ.clone(), StatsFn::Default))
                .collect::<Vec<_>>();
            DynStructCfg::from(cols)
        }
        fn decoder<'a>(&self, mut cols: ColumnsRef<'a>) -> Result<Self::Decoder<'a>, String> {
            let mut ret = U64sDecoder(Vec::new());
            for name in self.col_names() {
                ret.0.push(cols.col::<Option<u64>>(&name)?);
            }
            let () = cols.finish()?;
            Ok(ret)
        }
        fn encoder<'a>(&self, mut cols: ColumnsMut<'a>) -> Result<Self::Encoder<'a>, String> {
            let mut ret = Vec::new();
            for name in self.col_names() {
                ret.push(cols.col::<Option<u64>>(&name)?);
            }
            let (len, ()) = cols.finish()?;
            Ok(U64sEncoder(len, ret))
        }
    }
    impl<'a> PartEncoder<'a, U64s> for U64sEncoder<'a> {
        fn encode(&mut self, val: &U64s) {
            assert_eq!(val.0.len(), self.1.len());
            for (val, col) in val.0.iter().zip(self.1.iter_mut()) {
                col.push(*val);
            }
            *self.0 += 1;
        }
    }
    impl<'a> PartDecoder<'a, U64s> for U64sDecoder<'a> {
        fn decode(&self, idx: usize, val: &mut U64s) {
            val.0.clear();
            for col in self.0.iter() {
                val.0.push(col.get(idx));
            }
        }
    }

    impl ReadHandle<U64s, (), u64, i64> {
        async fn expect_snapshot_schema(
            &mut self,
            as_of: u64,
            target_schema: SchemaId,
            schemas: &Arc<SchemaVersions<U64s, ()>>,
        ) -> Vec<String> {
            let parts = self.snapshot(Antichain::from_elem(as_of)).await.unwrap();
            let mut updates = Vec::new();
            for part in parts {
                let mut fetched_part = fetch_leased_part::<_, _, _, i64>(
                    &part,
                    self.blob.as_ref(),
                    Arc::clone(&self.metrics),
                    &self.metrics.read.snapshot,
                    &self.machine.applier.shard_metrics,
                    Some(&self.reader_id),
                    self.schemas.clone(),
                    target_schema,
                )
                .await;
                self.process_returned_leased_part(part);
                // WIP instead, derive this from the registered schemas in
                // state, the actual part schema and the target schema
                fetched_part.schemas = Arc::clone(&schemas);
                updates.extend(fetched_part);
            }
            updates.sort_by(|(_, at, _), (_, bt, _)| at.cmp(bt));
            updates
                .into_iter()
                .map(|((k, v), _t, d)| {
                    let (k, ()) = (k.unwrap(), v.unwrap());
                    assert_eq!(d, 1);
                    k.0.iter()
                        .map(|x| match x {
                            Some(x) => x.to_string(),
                            None => "x".to_string(),
                        })
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .collect()
        }
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn schema_change() {
        let mut client = new_test_client().await;
        // WIP Who knows what compaction would do to these batches
        client.cfg.compaction_enabled = false;
        let shard_id = ShardId::new();
        let d = Diagnostics::for_tests();
        let mut schemas = SchemaVersions::<U64s, ()>::default();
        let val_schema = Arc::new(UnitSchema);

        // Start with one column.
        let s0 = SchemaId::default();
        let s0_key = Arc::new(U64sSchema(1));
        schemas.key_schemas.insert(s0, Arc::clone(&s0_key));
        schemas.val_schemas.insert(s0, Arc::clone(&val_schema));
        let (mut s0_write, mut s0_read) = client
            .open::<U64s, (), u64, i64>(shard_id, s0_key, Arc::clone(&val_schema), d.clone())
            .await
            .unwrap();

        // Write a value at s0 and read it back.
        s0_write
            .expect_compare_and_append(&[((U64s(vec![Some(0)]), ()), 0, 1)], 0, 1)
            .await;
        eprintln!("\n\n reading with s0");
        let actual = s0_read
            .expect_snapshot_schema(0, s0, &Arc::new(schemas.clone()))
            .await;
        assert_eq!(actual, vec!["0"]);

        // Add a second (optional) column.
        let s1 = s0.next();
        let s1_key = Arc::new(U64sSchema(2));
        schemas.key_schemas.insert(s1, Arc::clone(&s1_key));
        schemas.val_schemas.insert(s1, Arc::clone(&val_schema));
        let (mut s1_write, mut s1_read) = client
            .open::<U64s, (), u64, i64>(shard_id, s1_key, Arc::clone(&val_schema), d.clone())
            .await
            .unwrap();
        // WIP instead get this via the open call somehow
        s1_write.schemas.id = s1;

        // Write a value at s1 and read everything back
        s1_write
            .expect_compare_and_append(&[((U64s(vec![Some(1), Some(1)]), ()), 1, 1)], 1, 2)
            .await;
        eprintln!("\n\n reading with s1");
        let actual = s1_read
            .expect_snapshot_schema(1, s1, &Arc::new(schemas.clone()))
            .await;
        assert_eq!(actual, vec!["0 x", "1 1"]);

        // Now read everything with s0, including the data written with s1.
        eprintln!("\n\n reading with s1");
        let actual = s0_read
            .expect_snapshot_schema(1, s0, &Arc::new(schemas.clone()))
            .await;
        assert_eq!(actual, vec!["0", "1"]);
    }
}
