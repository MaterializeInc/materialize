// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Parquet [`OneshotFormat`].

use std::fmt;
use std::sync::Arc;

use arrow::array::{make_array, Array, RecordBatch, StructArray};
use bytes::{Bytes, BytesMut};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use mz_persist_types::arrow::ProtoArrayData;
use mz_proto::{ProtoType, RustType};
use mz_repr::RelationDesc;
use parquet::arrow::async_reader::{AsyncFileReader, MetadataFetch};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use prost::Message;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use smallvec::{smallvec, SmallVec};

use crate::oneshot_source::{
    OneshotFormat, OneshotObject, OneshotSource, StorageErrorX, StorageErrorXContext,
    StorageErrorXKind,
};

#[derive(Debug, Clone)]
pub struct ParquetFormat {
    desc: RelationDesc,
}

impl ParquetFormat {
    pub fn new(desc: RelationDesc) -> Self {
        ParquetFormat { desc }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParquetWorkRequest<O, C> {
    object: O,
    checksum: C,
    row_groups: SmallVec<[usize; 1]>,
}

#[derive(Clone, Debug)]
pub struct ParquetRowGroup {
    record_batch: RecordBatch,
}

impl Serialize for ParquetRowGroup {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        todo!()
    }
}

impl<'de> Deserialize<'de> for ParquetRowGroup {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        todo!()
    }
}

impl OneshotFormat for ParquetFormat {
    type WorkRequest<S>
        = ParquetWorkRequest<S::Object, S::Checksum>
    where
        S: OneshotSource;
    type RecordChunk = ParquetRowGroup;

    async fn split_work<S: OneshotSource + Send>(
        &self,
        source: S,
        object: S::Object,
        checksum: S::Checksum,
    ) -> Result<Vec<Self::WorkRequest<S>>, StorageErrorX> {
        let mut adapter = ParquetReaderAdapter::new(source, object.clone(), checksum.clone());
        let parquet_metadata = adapter.get_metadata().await?;

        tracing::info!(
            object = object.name(),
            row_groups = parquet_metadata.num_row_groups(),
            "splitting Parquet object"
        );

        // Split up the file by the number of RowGroups.
        //
        // TODO(cf3): Support splitting up large RowGroups.
        let work = (0..parquet_metadata.num_row_groups())
            .map(|row_group| ParquetWorkRequest {
                object: object.clone(),
                checksum: checksum.clone(),
                row_groups: smallvec![row_group],
            })
            .collect();

        Ok(work)
    }

    fn fetch_work<'a, S: OneshotSource + Sync + 'static>(
        &'a self,
        source: &'a S,
        request: Self::WorkRequest<S>,
    ) -> BoxStream<'a, Result<Self::RecordChunk, StorageErrorX>> {
        let ParquetWorkRequest {
            object,
            checksum,
            row_groups,
        } = request;

        let adapter = ParquetReaderAdapter::new(source.clone(), object.clone(), checksum.clone());

        let initial_work = async move {
            ParquetRecordBatchStreamBuilder::new(adapter)
                .await?
                .with_row_groups(row_groups.to_vec())
                .build()
        };

        futures::stream::once(initial_work)
            .try_flatten()
            .map_ok(|record_batch| ParquetRowGroup { record_batch })
            .err_into()
            .boxed()
    }

    fn decode_chunk(
        &self,
        chunk: Self::RecordChunk,
        rows: &mut Vec<mz_repr::Row>,
    ) -> Result<usize, StorageErrorX> {
        let ParquetRowGroup { record_batch } = chunk;

        let struct_array = StructArray::from(record_batch);
        let reader = mz_arrow_util::reader::ArrowReader::new(&self.desc, struct_array)
            .map_err(|err| StorageErrorXKind::ParquetError(err.to_string().into()))
            .context("reader")?;
        let rows_read = reader
            .read_all(rows)
            .map_err(|err| StorageErrorXKind::ParquetError(err.to_string().into()))
            .context("read_all")?;

        Ok(rows_read)
    }
}

/// A newtype wrapper around a [`OneshotSource`] that allows us to implement
/// [`AsyncFileReader`] and [`MetadataFetch`] for all types that implement
/// [`OneshotSource`].
#[derive(Clone)]
struct ParquetReaderAdapter<S: OneshotSource> {
    source: S,
    object: S::Object,
    checksum: S::Checksum,
}

impl<S: OneshotSource> fmt::Debug for ParquetReaderAdapter<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectStoreAdapter").finish()
    }
}

impl<S: OneshotSource> ParquetReaderAdapter<S> {
    fn new(source: S, object: S::Object, checksum: S::Checksum) -> Self {
        ParquetReaderAdapter {
            source,
            object,
            checksum,
        }
    }
}

impl<S: OneshotSource> MetadataFetch for ParquetReaderAdapter<S> {
    fn fetch(
        &mut self,
        range: std::ops::Range<usize>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let inclusive_end = std::cmp::max(range.start, range.end.saturating_sub(1));

        Box::pin(async move {
            // Fetch the specified range.
            let result: Result<Vec<_>, _> = self
                .source
                .get(
                    self.object.clone(),
                    self.checksum.clone(),
                    Some(range.start..=inclusive_end),
                )
                .try_collect()
                .await;
            let bytes = match result {
                Err(e) => return Err(ParquetError::General(e.to_string())),
                Ok(bytes) => bytes,
            };

            // Join the stream into a single chunk.
            let total_length = inclusive_end.saturating_sub(range.start);
            let mut joined_bytes = BytesMut::with_capacity(total_length);
            joined_bytes.extend(bytes);

            Ok(joined_bytes.freeze())
        })
    }
}

impl<S: OneshotSource> AsyncFileReader for ParquetReaderAdapter<S> {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<usize>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        MetadataFetch::fetch(self, range)
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let mut reader = ParquetMetaDataReader::new();
            let object_size = self.object.size();
            reader.try_load(self, object_size).await?;
            reader.finish().map(|metadata| Arc::new(metadata))
        })
    }
}
