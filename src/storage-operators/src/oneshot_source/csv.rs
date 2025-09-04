// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! CSV to Row Decoder.

use std::fmt::Debug;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use futures::TryStreamExt;
use futures::stream::{BoxStream, StreamExt};
use itertools::Itertools;
use mz_pgcopy::CopyCsvFormatParams;
use mz_repr::{Datum, RelationDesc, Row, RowArena};
use serde::{Deserialize, Serialize};
use smallvec::{SmallVec, smallvec};
use tokio_util::io::StreamReader;

use crate::oneshot_source::{
    Encoding, OneshotFormat, OneshotObject, OneshotSource, StorageErrorX, StorageErrorXKind,
};

#[derive(Debug, Clone)]
pub struct CsvDecoder {
    /// Properties of the CSV Reader.
    params: CopyCsvFormatParams<'static>,
    /// Types of the table we're copying into.
    column_types: Arc<[mz_pgrepr::Type]>,
}

impl CsvDecoder {
    pub fn new(params: CopyCsvFormatParams<'static>, desc: &RelationDesc) -> Self {
        let column_types = desc
            .iter_types()
            .map(|x| &x.scalar_type)
            .map(mz_pgrepr::Type::from)
            .collect();
        CsvDecoder {
            params,
            column_types,
        }
    }
}

/// Instructions on how to parse a single CSV file.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CsvWorkRequest<O, C> {
    object: O,
    checksum: C,
    encodings: SmallVec<[Encoding; 1]>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CsvRecord {
    bytes: Vec<u8>,
    ranges: Vec<std::ops::Range<usize>>,
}

impl OneshotFormat for CsvDecoder {
    type WorkRequest<S>
        = CsvWorkRequest<S::Object, S::Checksum>
    where
        S: OneshotSource;
    type RecordChunk = CsvRecord;

    async fn split_work<S: OneshotSource + Send>(
        &self,
        _source: S,
        object: S::Object,
        checksum: S::Checksum,
    ) -> Result<Vec<Self::WorkRequest<S>>, StorageErrorX> {
        // Decoding a CSV in parallel is hard.
        //
        // TODO(cf3): If necessary, we can get a 2x speedup by parsing a CSV
        // from the start and end in parallel, and meeting in the middle.
        //
        // See <https://badrish.net/papers/dp-sigmod19.pdf> for general parallelization strategies.

        // TODO(cf1): Check the encodings from the object to determine
        // what decompression to apply. Also support the user manually
        // specifying certain encodings.

        let encodings = if object.name().ends_with(".gz") {
            smallvec![Encoding::Gzip]
        } else if object.name().ends_with(".bz2") {
            smallvec![Encoding::Bzip2]
        } else if object.name().ends_with(".xz") {
            smallvec![Encoding::Xz]
        } else if object.name().ends_with(".zst") {
            smallvec![Encoding::Zstd]
        } else {
            smallvec![]
        };

        let request = CsvWorkRequest {
            object,
            checksum,
            encodings,
        };
        Ok(vec![request])
    }

    fn fetch_work<'a, S: OneshotSource + Sync + 'static>(
        &'a self,
        source: &'a S,
        request: Self::WorkRequest<S>,
    ) -> BoxStream<'a, Result<Self::RecordChunk, StorageErrorX>> {
        let CsvWorkRequest {
            object,
            checksum,
            encodings,
        } = request;

        // Wrap our `Stream<Bytes>` into a type that implements `tokio::io::AsyncRead`.
        let raw_byte_stream = source
            .get(object, checksum, None)
            .map_err(|e| io::Error::new(io::ErrorKind::Interrupted, format!("{e:?}")));
        let stream_reader = StreamReader::new(raw_byte_stream);

        // TODO(cf3): Support multiple encodings.
        assert!(encodings.len() <= 1, "TODO support multiple encodings");

        // Decompress the byte stream, if necessary.
        let reader: Pin<Box<dyn tokio::io::AsyncRead + Send>> = if let Some(encoding) =
            encodings.into_iter().next()
        {
            tracing::info!(?encoding, "decompressing byte stream");
            match encoding {
                Encoding::Bzip2 => {
                    let decoder = async_compression::tokio::bufread::BzDecoder::new(stream_reader);
                    Box::pin(decoder)
                }
                Encoding::Gzip => {
                    let decoder =
                        async_compression::tokio::bufread::GzipDecoder::new(stream_reader);
                    Box::pin(decoder)
                }
                Encoding::Xz => {
                    let decoder = async_compression::tokio::bufread::XzDecoder::new(stream_reader);
                    Box::pin(decoder)
                }
                Encoding::Zstd => {
                    let decoder =
                        async_compression::tokio::bufread::ZstdDecoder::new(stream_reader);
                    Box::pin(decoder)
                }
            }
        } else {
            Box::pin(stream_reader)
        };

        let (double_quote, escape) = if self.params.quote == self.params.escape {
            (true, None)
        } else {
            (false, Some(self.params.escape))
        };

        // Configure our CSV reader.
        let reader = csv_async::AsyncReaderBuilder::new()
            .delimiter(self.params.delimiter)
            .quote(self.params.quote)
            .has_headers(self.params.header)
            .double_quote(double_quote)
            .escape(escape)
            // Be maximally permissive. If there is a a record with the wrong
            // number of columns Row decoding will error, if we care.
            .flexible(true)
            .create_reader(reader);

        // Return a stream of records.
        reader
            .into_byte_records()
            .map_ok(|record| {
                let bytes = record.as_slice().to_vec();
                let ranges = (0..record.len())
                    .map(|idx| record.range(idx).expect("known to exist"))
                    .collect();
                CsvRecord { bytes, ranges }
            })
            .map_err(|err| StorageErrorXKind::from(err).with_context("csv decoding"))
            .boxed()
    }

    fn decode_chunk(
        &self,
        chunk: Self::RecordChunk,
        rows: &mut Vec<Row>,
    ) -> Result<usize, StorageErrorX> {
        let CsvRecord { bytes, ranges } = chunk;

        // Make sure the CSV record has the correct number of columns.
        if self.column_types.len() != ranges.len() {
            let msg = format!(
                "wrong number of columns, desc: {} record: {}",
                self.column_types.len(),
                ranges.len()
            );
            return Err(StorageErrorXKind::invalid_record_batch(msg).into());
        }

        let str_slices = ranges.into_iter().map(|range| {
            bytes
                .get(range)
                .ok_or_else(|| StorageErrorXKind::programming_error("invalid byte range"))
        });

        // Decode a Row from the CSV record.
        let mut row = Row::default();
        let mut packer = row.packer();
        let arena = RowArena::new();

        for (typ, maybe_raw_value) in self.column_types.iter().zip_eq(str_slices) {
            let raw_value = maybe_raw_value?;

            if raw_value == self.params.null.as_bytes() {
                packer.push(Datum::Null);
            } else {
                let value = mz_pgrepr::Value::decode_text(typ, raw_value).map_err(|err| {
                    StorageErrorXKind::invalid_record_batch(err.to_string())
                        .with_context("decode_text")
                })?;
                packer.push(value.into_datum(&arena, typ));
            }
        }

        rows.push(row);

        Ok(1)
    }
}
