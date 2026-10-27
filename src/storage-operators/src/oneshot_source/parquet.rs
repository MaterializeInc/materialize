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

use arrow::array::{RecordBatch, StructArray};
use arrow_ipc::writer::StreamWriter;
use bytes::{Bytes, BytesMut};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_repr::RelationDesc;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::{AsyncFileReader, MetadataFetch};
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use serde::{Deserialize, Serialize};
use smallvec::{SmallVec, smallvec};

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
        let parquet_metadata = adapter.get_metadata(None).await?;

        tracing::info!(
            object = object.name(),
            schema = ?parquet_metadata.file_metadata().schema_descr(),
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
            .inspect_err(|err| tracing::warn!("error: {err:#?}"))
            .map_err(|err| StorageErrorXKind::ParquetError(err.to_string().into()))
            .context("reader")?;
        let rows_read = reader
            .read_all(rows)
            .inspect_err(|err| tracing::warn!("error: {err:#?}"))
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
        range: std::ops::Range<u64>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let inclusive_end = std::cmp::max(range.start, range.end.saturating_sub(1));

        Box::pin(async move {
            let range_start = usize::cast_from(range.start);
            let inclusive_end = usize::cast_from(inclusive_end);
            // Fetch the specified range.
            let result: Result<Vec<_>, _> = self
                .source
                .get(
                    self.object.clone(),
                    self.checksum.clone(),
                    Some(range_start..=inclusive_end),
                )
                .try_collect()
                .await;
            let bytes = match result {
                Err(e) => return Err(ParquetError::General(e.to_string())),
                Ok(bytes) => bytes,
            };

            // Join the stream into a single chunk.
            let total_length = inclusive_end.saturating_sub(range_start);
            let mut joined_bytes = BytesMut::with_capacity(total_length);
            joined_bytes.extend(bytes);

            Ok(joined_bytes.freeze())
        })
    }
}

impl<S: OneshotSource> AsyncFileReader for ParquetReaderAdapter<S> {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        MetadataFetch::fetch(self, range)
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a parquet::arrow::arrow_reader::ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let mut reader = ParquetMetaDataReader::new();
            let object_size = u64::cast_from(self.object.size());
            reader.try_load(self, object_size).await?;
            reader.finish().map(Arc::new)
        })
    }
}

// Note(parkmycar): Instead of a manual implementation of Serialize and Deserialize we could
// change `ParquetRowGroup` to have a type which we can derive the impl for. But no types from the
// `arrow` crate do, and we'd prefer not to use a Vec<u8> since serialization is only required when
// Timely workers span multiple processes.
impl Serialize for ParquetRowGroup {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Note: This implementation isn't very efficient, but it should only be rarely used so
        // it's not too much of a concern.
        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, &self.record_batch.schema())
            .map_err(serde::ser::Error::custom)?;
        writer
            .write(&self.record_batch)
            .map_err(serde::ser::Error::custom)?;
        writer.finish().map_err(serde::ser::Error::custom)?;
        buf.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ParquetRowGroup {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        let reader = arrow_ipc::reader::StreamReader::try_new(bytes.as_slice(), None)
            .map_err(serde::de::Error::custom)?;

        let record_batch = reader
            .expect_element(|| "expected exactly one record batch in IPC stream")
            .map_err(serde::de::Error::custom)?;

        Ok(ParquetRowGroup { record_batch })
    }
}
