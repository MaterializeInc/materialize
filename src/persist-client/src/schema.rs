// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persist shard schema information.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use differential_dataflow::difference::Monoid;
use differential_dataflow::lattice::Lattice;
use mz_ore::cast::CastFrom;
use mz_persist_types::columnar::data_type;
use mz_persist_types::schema::{Migration, SchemaId, backward_compatible};
use mz_persist_types::{Codec, Codec64};
use timely::progress::Timestamp;

use crate::internal::apply::Applier;
use crate::internal::encoding::Schemas;
use crate::internal::metrics::{SchemaCacheMetrics, SchemaMetrics};
use crate::internal::state::{BatchPart, EncodedSchemas};

/// The result returned by [crate::PersistClient::compare_and_evolve_schema].
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum CaESchema<K: Codec, V: Codec> {
    /// The schema was successfully evolved and registered with the included id.
    Ok(SchemaId),
    /// The schema was not compatible with previously registered schemas.
    Incompatible,
    /// The `expected` SchemaId did not match reality. The current one is
    /// included for easy of retry.
    ExpectedMismatch {
        /// The current schema id.
        schema_id: SchemaId,
        /// The key schema at this id.
        key: K::Schema,
        /// The val schema at this id.
        val: V::Schema,
    },
}

/// A cache of decoded schemas and schema migrations.
///
/// The decoded schemas are a cache of the registry in state, and so are shared
/// process-wide.
///
/// On the other hand, the migrations have an N^2 problem and so are per-handle.
/// This also seems reasonable because for any given write handle, the write
/// schema will be the same for all migration entries, and ditto for read handle
/// and read schema.
#[derive(Debug)]
pub(crate) struct SchemaCache<K: Codec, V: Codec, T, D> {
    maps: Arc<SchemaCacheMaps<K, V>>,
    applier: Applier<K, V, T, D>,
    key_migration_by_ids: MigrationCacheMap,
    val_migration_by_ids: MigrationCacheMap,
}

impl<K: Codec, V: Codec, T: Clone, D> Clone for SchemaCache<K, V, T, D> {
    fn clone(&self) -> Self {
        Self {
            maps: Arc::clone(&self.maps),
            applier: self.applier.clone(),
            key_migration_by_ids: self.key_migration_by_ids.clone(),
            val_migration_by_ids: self.val_migration_by_ids.clone(),
        }
    }
}

impl<K: Codec, V: Codec, T, D> Drop for SchemaCache<K, V, T, D> {
    fn drop(&mut self) {
        let dropped = u64::cast_from(
            self.key_migration_by_ids.by_ids.len() + self.val_migration_by_ids.by_ids.len(),
        );
        self.applier
            .metrics
            .schema
            .cache_migration
            .dropped_count
            .inc_by(dropped);
    }
}

impl<K, V, T, D> SchemaCache<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64 + Sync,
    D: Monoid + Codec64,
{
    pub fn new(maps: Arc<SchemaCacheMaps<K, V>>, applier: Applier<K, V, T, D>) -> Self {
        let key_migration_by_ids = MigrationCacheMap {
            metrics: applier.metrics.schema.cache_migration.clone(),
            by_ids: BTreeMap::new(),
        };
        let val_migration_by_ids = MigrationCacheMap {
            metrics: applier.metrics.schema.cache_migration.clone(),
            by_ids: BTreeMap::new(),
        };
        SchemaCache {
            maps,
            applier,
            key_migration_by_ids,
            val_migration_by_ids,
        }
    }

    async fn schemas(&self, id: &SchemaId) -> Option<Schemas<K, V>> {
        let key = self
            .get_or_try_init(&self.maps.key_by_id, id, |schemas| {
                self.maps.key_by_id.metrics.computed_count.inc();
                schemas.get(id).map(|x| K::decode_schema(&x.key))
            })
            .await?;
        let val = self
            .get_or_try_init(&self.maps.val_by_id, id, |schemas| {
                self.maps.val_by_id.metrics.computed_count.inc();
                schemas.get(id).map(|x| V::decode_schema(&x.val))
            })
            .await?;
        Some(Schemas {
            id: Some(*id),
            key,
            val,
        })
    }

    fn key_migration(
        &mut self,
        write: &Schemas<K, V>,
        read: &Schemas<K, V>,
    ) -> Option<Arc<Migration>> {
        let migration_fn = || Self::migration::<K>(&write.key, &read.key);
        let (Some(write_id), Some(read_id)) = (write.id, read.id) else {
            // TODO: Annoying to cache this because we're missing an id. This
            // will probably require some sort of refactor to fix so punting for
            // now.
            self.key_migration_by_ids.metrics.computed_count.inc();
            return migration_fn().map(Arc::new);
        };
        self.key_migration_by_ids
            .get_or_try_insert(write_id, read_id, migration_fn)
    }

    fn val_migration(
        &mut self,
        write: &Schemas<K, V>,
        read: &Schemas<K, V>,
    ) -> Option<Arc<Migration>> {
        let migration_fn = || Self::migration::<V>(&write.val, &read.val);
        let (Some(write_id), Some(read_id)) = (write.id, read.id) else {
            // TODO: Annoying to cache this because we're missing an id. This
            // will probably require some sort of refactor to fix so punting for
            // now.
            self.val_migration_by_ids.metrics.computed_count.inc();
            return migration_fn().map(Arc::new);
        };
        self.val_migration_by_ids
            .get_or_try_insert(write_id, read_id, migration_fn)
    }

    fn migration<C: Codec>(write: &C::Schema, read: &C::Schema) -> Option<Migration> {
        let write_dt = data_type::<C>(write).expect("valid schema");
        let read_dt = data_type::<C>(read).expect("valid schema");
        backward_compatible(&write_dt, &read_dt)
    }

    async fn get_or_try_init<MK: Clone + Ord, MV: PartialEq + Debug>(
        &self,
        map: &SchemaCacheMap<MK, MV>,
        key: &MK,
        f: impl Fn(&BTreeMap<SchemaId, EncodedSchemas>) -> Option<MV>,
    ) -> Option<Arc<MV>> {
        let ret = map.get_or_try_init(key, || {
            self.applier
                .schemas(|seqno, schemas| f(schemas).ok_or(seqno))
        });
        let seqno = match ret {
            Ok(ret) => return Some(ret),
            Err(seqno) => seqno,
        };
        self.applier.metrics.schema.cache_fetch_state_count.inc();
        self.applier.fetch_and_update_state(Some(seqno)).await;
        map.get_or_try_init(key, || {
            self.applier
                .schemas(|seqno, schemas| f(schemas).ok_or(seqno))
        })
        .ok()
    }
}

#[derive(Debug)]
pub(crate) struct SchemaCacheMaps<K: Codec, V: Codec> {
    key_by_id: SchemaCacheMap<SchemaId, K::Schema>,
    val_by_id: SchemaCacheMap<SchemaId, V::Schema>,
}

impl<K: Codec, V: Codec> SchemaCacheMaps<K, V> {
    pub(crate) fn new(metrics: &SchemaMetrics) -> Self {
        Self {
            key_by_id: SchemaCacheMap {
                metrics: metrics.cache_schema.clone(),
                map: RwLock::new(BTreeMap::new()),
            },
            val_by_id: SchemaCacheMap {
                metrics: metrics.cache_schema.clone(),
                map: RwLock::new(BTreeMap::new()),
            },
        }
    }
}

#[derive(Debug)]
struct SchemaCacheMap<I, S> {
    metrics: SchemaCacheMetrics,
    map: RwLock<BTreeMap<I, Arc<S>>>,
}

impl<I: Clone + Ord, S: PartialEq + Debug> SchemaCacheMap<I, S> {
    fn get_or_try_init<E>(
        &self,
        id: &I,
        state_fn: impl FnOnce() -> Result<S, E>,
    ) -> Result<Arc<S>, E> {
        // First see if we have the value cached.
        {
            let map = self.map.read().expect("lock");
            if let Some(ret) = map.get(id).map(Arc::clone) {
                self.metrics.cached_count.inc();
                return Ok(ret);
            }
        }
        // If not, see if we can get the value from current state.
        let ret = state_fn().map(Arc::new);
        if let Ok(val) = ret.as_ref() {
            let mut map = self.map.write().expect("lock");
            // If any answers got written in the meantime, they should be the
            // same, so just overwrite
            let prev = map.insert(id.clone(), Arc::clone(val));
            match prev {
                Some(prev) => debug_assert_eq!(*val, prev),
                None => self.metrics.added_count.inc(),
            }
        } else {
            self.metrics.unavailable_count.inc();
        }
        ret
    }
}

impl<I, K> Drop for SchemaCacheMap<I, K> {
    fn drop(&mut self) {
        let map = self.map.read().expect("lock");
        self.metrics.dropped_count.inc_by(u64::cast_from(map.len()));
    }
}

#[derive(Debug, Clone)]
struct MigrationCacheMap {
    metrics: SchemaCacheMetrics,
    by_ids: BTreeMap<(SchemaId, SchemaId), Arc<Migration>>,
}

impl MigrationCacheMap {
    fn get_or_try_insert(
        &mut self,
        write_id: SchemaId,
        read_id: SchemaId,
        migration_fn: impl FnOnce() -> Option<Migration>,
    ) -> Option<Arc<Migration>> {
        if let Some(migration) = self.by_ids.get(&(write_id, read_id)) {
            self.metrics.cached_count.inc();
            return Some(Arc::clone(migration));
        };
        self.metrics.computed_count.inc();
        let migration = migration_fn().map(Arc::new);
        if let Some(migration) = migration.as_ref() {
            self.metrics.added_count.inc();
            // We just looked this up above and we've got mutable access, so no
            // race issues.
            self.by_ids
                .insert((write_id, read_id), Arc::clone(migration));
        } else {
            self.metrics.unavailable_count.inc();
        }
        migration
    }
}

#[derive(Debug)]
pub(crate) enum PartMigration<K: Codec, V: Codec> {
    /// No-op!
    SameSchema { both: Schemas<K, V> },
    /// We don't have a schema id for write schema.
    Schemaless { read: Schemas<K, V> },
    /// We have both write and read schemas, and they don't match.
    Either {
        write: Schemas<K, V>,
        read: Schemas<K, V>,
        key_migration: Arc<Migration>,
        val_migration: Arc<Migration>,
    },
}

impl<K: Codec, V: Codec> Clone for PartMigration<K, V> {
    fn clone(&self) -> Self {
        match self {
            Self::SameSchema { both } => Self::SameSchema { both: both.clone() },
            Self::Schemaless { read } => Self::Schemaless { read: read.clone() },
            Self::Either {
                write,
                read,
                key_migration,
                val_migration,
            } => Self::Either {
                write: write.clone(),
                read: read.clone(),
                key_migration: Arc::clone(key_migration),
                val_migration: Arc::clone(val_migration),
            },
        }
    }
}

impl<K, V> PartMigration<K, V>
where
    K: Debug + Codec,
    V: Debug + Codec,
{
    pub(crate) async fn new<T, D>(
        part: &BatchPart<T>,
        read: Schemas<K, V>,
        schema_cache: &mut SchemaCache<K, V, T, D>,
    ) -> Result<Self, Schemas<K, V>>
    where
        T: Timestamp + Lattice + Codec64 + Sync,
        D: Monoid + Codec64,
    {
        // At one point in time during our structured data migration, we deprecated the
        // already written schema IDs because we made all columns at the Arrow/Parquet
        // level nullable, thus changing the schema parts were written with.
        //
        // _After_ this deprecation, we've observed at least one instance where a
        // structured only Part was written with the schema ID in the _old_ deprecated
        // field. While unexpected, given the ordering of our releases it is safe to
        // use the deprecated schema ID if we have a structured only part.
        let write = match (part.schema_id(), part.deprecated_schema_id()) {
            (Some(write_id), _) => Some(write_id),
            (None, Some(deprecated_id))
                if part.is_structured_only(&schema_cache.applier.metrics.columnar) =>
            {
                tracing::warn!(?deprecated_id, "falling back to deprecated schema ID");
                Some(deprecated_id)
            }
            (None, _) => None,
        };

        match (write, read.id) {
            (None, _) => Ok(PartMigration::Schemaless { read }),
            (Some(w), Some(r)) if w == r => Ok(PartMigration::SameSchema { both: read }),
            (Some(w), _) => {
                let write = schema_cache
                    .schemas(&w)
                    .await
                    .expect("appended part should reference registered schema");
                // Even if we missing a schema id, if the schemas are equal, use
                // `SameSchema`. This isn't a correctness issue, we'd just
                // generate NoOp migrations, but it'll make the metrics more
                // intuitive.
                if write.key == read.key && write.val == read.val {
                    return Ok(PartMigration::SameSchema { both: read });
                }

                let start = Instant::now();
                let key_migration = schema_cache
                    .key_migration(&write, &read)
                    .ok_or_else(|| read.clone())?;
                let val_migration = schema_cache
                    .val_migration(&write, &read)
                    .ok_or_else(|| read.clone())?;
                schema_cache
                    .applier
                    .metrics
                    .schema
                    .migration_new_count
                    .inc();
                schema_cache
                    .applier
                    .metrics
                    .schema
                    .migration_new_seconds
                    .inc_by(start.elapsed().as_secs_f64());

                Ok(PartMigration::Either {
                    write,
                    read,
                    key_migration,
                    val_migration,
                })
            }
        }
    }
}

impl<K: Codec, V: Codec> PartMigration<K, V> {
    pub(crate) fn codec_read(&self) -> &Schemas<K, V> {
        match self {
            PartMigration::SameSchema { both } => both,
            PartMigration::Schemaless { read } => read,
            PartMigration::Either { read, .. } => read,
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        Array, ArrayBuilder, StringArray, StringBuilder, StructArray, as_string_array,
    };
    use arrow::datatypes::{DataType, Field};
    use bytes::BufMut;
    use futures::StreamExt;
    use mz_dyncfg::ConfigUpdates;
    use mz_persist_types::ShardId;
    use mz_persist_types::arrow::ArrayOrd;
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_persist_types::columnar::{ColumnDecoder, ColumnEncoder, Schema};
    use mz_persist_types::stats::{NoneStats, StructStats};
    use timely::progress::Antichain;

    use crate::Diagnostics;
    use crate::cli::admin::info_log_non_zero_metrics;
    use crate::read::ReadHandle;
    use crate::tests::new_test_client;

    use super::*;

    #[mz_ore::test]
    fn schema_id() {
        assert_eq!(SchemaId(1).to_string(), "h1");
        assert_eq!(SchemaId::try_from("h1".to_owned()), Ok(SchemaId(1)));
        assert!(SchemaId::try_from("nope".to_owned()).is_err());
    }

    #[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
    struct Strings(Vec<String>);

    impl Codec for Strings {
        type Schema = StringsSchema;
        type Storage = ();

        fn codec_name() -> String {
            "Strings".into()
        }

        fn encode<B: BufMut>(&self, buf: &mut B) {
            buf.put_slice(self.0.join(",").as_bytes());
        }
        fn decode<'a>(buf: &'a [u8], schema: &Self::Schema) -> Result<Self, String> {
            let buf = std::str::from_utf8(buf).map_err(|err| err.to_string())?;
            let mut ret = buf.split(",").map(|x| x.to_owned()).collect::<Vec<_>>();
            // Fill in nulls or drop columns to match the requested schema.
            while schema.0.len() > ret.len() {
                ret.push("".into());
            }
            while schema.0.len() < ret.len() {
                ret.pop();
            }
            Ok(Strings(ret))
        }

        fn encode_schema(schema: &Self::Schema) -> bytes::Bytes {
            schema
                .0
                .iter()
                .map(|x| x.then_some('n').unwrap_or(' '))
                .collect::<String>()
                .into_bytes()
                .into()
        }
        fn decode_schema(buf: &bytes::Bytes) -> Self::Schema {
            let buf = std::str::from_utf8(buf).expect("valid schema");
            StringsSchema(
                buf.chars()
                    .map(|x| match x {
                        'n' => true,
                        ' ' => false,
                        _ => unreachable!(),
                    })
                    .collect(),
            )
        }
    }

    #[derive(Debug, Clone, Default, PartialEq)]
    struct StringsSchema(Vec<bool>);

    impl Schema<Strings> for StringsSchema {
        type ArrowColumn = StructArray;
        type Statistics = NoneStats;
        type Decoder = StringsDecoder;
        type Encoder = StringsEncoder;

        fn decoder(&self, col: Self::ArrowColumn) -> Result<Self::Decoder, anyhow::Error> {
            let mut cols = Vec::new();
            for (idx, _) in self.0.iter().enumerate() {
                cols.push(as_string_array(col.column_by_name(&idx.to_string()).unwrap()).clone());
            }
            Ok(StringsDecoder(cols))
        }
        fn encoder(&self) -> Result<Self::Encoder, anyhow::Error> {
            let mut fields = Vec::new();
            let mut arrays = Vec::new();
            for (idx, nullable) in self.0.iter().enumerate() {
                fields.push(Field::new(idx.to_string(), DataType::Utf8, *nullable));
                arrays.push(StringBuilder::new());
            }
            Ok(StringsEncoder { fields, arrays })
        }
    }

    #[derive(Debug)]
    struct StringsDecoder(Vec<StringArray>);
    impl ColumnDecoder<Strings> for StringsDecoder {
        fn decode(&self, idx: usize, val: &mut Strings) {
            val.0.clear();
            for col in self.0.iter() {
                if col.is_valid(idx) {
                    val.0.push(col.value(idx).into());
                } else {
                    val.0.push("".into());
                }
            }
        }
        fn is_null(&self, _: usize) -> bool {
            false
        }
        fn goodbytes(&self) -> usize {
            self.0
                .iter()
                .map(|val| ArrayOrd::String(val.clone()).goodbytes())
                .sum()
        }
        fn stats(&self) -> StructStats {
            StructStats {
                len: self.0[0].len(),
                cols: Default::default(),
            }
        }
    }

    #[derive(Debug)]
    struct StringsEncoder {
        fields: Vec<Field>,
        arrays: Vec<StringBuilder>,
    }
    impl ColumnEncoder<Strings> for StringsEncoder {
        type FinishedColumn = StructArray;

        fn goodbytes(&self) -> usize {
            self.arrays.iter().map(|a| a.values_slice().len()).sum()
        }

        fn append(&mut self, val: &Strings) {
            for (idx, val) in val.0.iter().enumerate() {
                if val.is_empty() {
                    self.arrays[idx].append_null();
                } else {
                    self.arrays[idx].append_value(val);
                }
            }
        }
        fn append_null(&mut self) {
            unreachable!()
        }
        fn finish(self) -> Self::FinishedColumn {
            assert_eq!(self.fields.len(), self.arrays.len(), "invalid schema");
            if self.fields.is_empty() {
                StructArray::new_empty_fields(0, None)
            } else {
                let arrays = self
                    .arrays
                    .into_iter()
                    .map(|mut x| ArrayBuilder::finish(&mut x))
                    .collect();
                StructArray::new(self.fields.into(), arrays, None)
            }
        }
    }

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn compare_and_evolve_schema(dyncfgs: ConfigUpdates) {
        let client = new_test_client(&dyncfgs).await;
        let d = Diagnostics::for_tests();
        let shard_id = ShardId::new();
        let schema0 = StringsSchema(vec![false]);
        let schema1 = StringsSchema(vec![false, true]);

        let mut write0 = client
            .open_writer::<Strings, (), u64, i64>(
                shard_id,
                Arc::new(schema0.clone()),
                Arc::new(UnitSchema),
                d.clone(),
            )
            .await
            .unwrap();

        write0.try_register_schema().await;
        assert_eq!(write0.write_schemas.id.unwrap(), SchemaId(0));

        // Not backward compatible (yet... we don't support dropping a column at
        // the moment).
        let res = client
            .compare_and_evolve_schema::<Strings, (), u64, i64>(
                shard_id,
                SchemaId(0),
                &StringsSchema(vec![]),
                &UnitSchema,
                d.clone(),
            )
            .await
            .unwrap();
        assert_eq!(res, CaESchema::Incompatible);

        // Incorrect expectation
        let res = client
            .compare_and_evolve_schema::<Strings, (), u64, i64>(
                shard_id,
                SchemaId(1),
                &schema1,
                &UnitSchema,
                d.clone(),
            )
            .await
            .unwrap();
        assert_eq!(
            res,
            CaESchema::ExpectedMismatch {
                schema_id: SchemaId(0),
                key: schema0,
                val: UnitSchema
            }
        );

        // Successful evolution
        let res = client
            .compare_and_evolve_schema::<Strings, (), u64, i64>(
                shard_id,
                SchemaId(0),
                &schema1,
                &UnitSchema,
                d.clone(),
            )
            .await
            .unwrap();
        assert_eq!(res, CaESchema::Ok(SchemaId(1)));

        // Create a write handle with the new schema and validate that it picks
        // up the correct schema id.
        let write1 = client
            .open_writer::<Strings, (), u64, i64>(
                shard_id,
                Arc::new(schema1),
                Arc::new(UnitSchema),
                d.clone(),
            )
            .await
            .unwrap();
        assert_eq!(write1.write_schemas.id.unwrap(), SchemaId(1));
    }

    fn strings(xs: &[((Result<Strings, String>, Result<(), String>), u64, i64)]) -> Vec<Vec<&str>> {
        xs.iter()
            .map(|((k, _), _, _)| k.as_ref().unwrap().0.iter().map(|x| x.as_str()).collect())
            .collect()
    }

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn schema_evolution(dyncfgs: ConfigUpdates) {
        async fn snap_streaming(
            as_of: u64,
            read: &mut ReadHandle<Strings, (), u64, i64>,
        ) -> Vec<((Result<Strings, String>, Result<(), String>), u64, i64)> {
            // NB: We test with both snapshot_and_fetch and snapshot_and_stream
            // because one uses the consolidating iter and one doesn't.
            let mut ret = read
                .snapshot_and_stream(Antichain::from_elem(as_of))
                .await
                .unwrap()
                .collect::<Vec<_>>()
                .await;
            ret.sort();
            ret
        }

        let client = new_test_client(&dyncfgs).await;
        let d = Diagnostics::for_tests();
        let shard_id = ShardId::new();
        let schema0 = StringsSchema(vec![false]);
        let schema1 = StringsSchema(vec![false, true]);

        // Write some data at the original schema.
        let (mut write0, mut read0) = client
            .open::<Strings, (), u64, i64>(
                shard_id,
                Arc::new(schema0.clone()),
                Arc::new(UnitSchema),
                d.clone(),
                true,
            )
            .await
            .unwrap();
        write0
            .expect_compare_and_append(&[((Strings(vec!["0 before".into()]), ()), 0, 1)], 0, 1)
            .await;
        let expected = vec![vec!["0 before"]];
        assert_eq!(strings(&snap_streaming(0, &mut read0).await), expected);
        assert_eq!(strings(&read0.expect_snapshot_and_fetch(0).await), expected);

        // Register and write some data at the new schema.
        let res = client
            .compare_and_evolve_schema::<Strings, (), u64, i64>(
                shard_id,
                SchemaId(0),
                &schema1,
                &UnitSchema,
                d.clone(),
            )
            .await
            .unwrap();
        assert_eq!(res, CaESchema::Ok(SchemaId(1)));
        let (mut write1, mut read1) = client
            .open::<Strings, (), u64, i64>(
                shard_id,
                Arc::new(schema1.clone()),
                Arc::new(UnitSchema),
                d.clone(),
                true,
            )
            .await
            .unwrap();
        write1
            .expect_compare_and_append(
                &[
                    ((Strings(vec!["1 null".into(), "".into()]), ()), 1, 1),
                    ((Strings(vec!["1 not".into(), "x".into()]), ()), 1, 1),
                ],
                1,
                2,
            )
            .await;

        // Continue to write data with the original schema.
        write0
            .expect_compare_and_append(&[((Strings(vec!["0 after".into()]), ()), 2, 1)], 2, 3)
            .await;

        // Original schema drops the new column in data written by new schema.
        let expected = vec![
            vec!["0 after"],
            vec!["0 before"],
            vec!["1 not"],
            vec!["1 null"],
        ];
        assert_eq!(strings(&snap_streaming(2, &mut read0).await), expected);
        assert_eq!(strings(&read0.expect_snapshot_and_fetch(2).await), expected);

        // New schema adds nulls (represented by empty string in Strings) in
        // data written by old schema.
        let expected = vec![
            vec!["0 after", ""],
            vec!["0 before", ""],
            vec!["1 not", "x"],
            vec!["1 null", ""],
        ];
        assert_eq!(strings(&snap_streaming(2, &mut read1).await), expected);
        assert_eq!(strings(&read1.expect_snapshot_and_fetch(2).await), expected);

        // Probably too spammy to leave in the logs, but it was useful to have
        // hooked up while iterating.
        if false {
            info_log_non_zero_metrics(&client.metrics.registry.gather());
        }
    }
}
