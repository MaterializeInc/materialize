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

use futures::stream::{BoxStream, StreamExt};
use futures::TryStreamExt;
use mz_repr::fixed_length::FromDatumIter;
use mz_repr::{Datum, Row};
use serde::{Deserialize, Serialize};
use smallvec::{smallvec, SmallVec};
use tokio_util::io::StreamReader;

use crate::oneshot_source::{
    Encoding, OneshotFormat, OneshotObject, OneshotSource, StorageErrorX, StorageErrorXKind,
};

#[derive(Default, Debug, Clone)]
pub struct CsvDecoder;

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
        // TODO(parkmycar): Document the strategy here.

        // TODO(parkmycar): Check the encodings from the object to determine
        // what decompression to apply. Also support the user manually
        // specifying certain encodings.

        let encodings = if object.name().ends_with(".gz") {
            smallvec![Encoding::Gzip]
        } else if object.name().ends_with(".bz2") {
            smallvec![Encoding::Bzip2]
        } else if object.name().ends_with(".br") {
            smallvec![Encoding::Brotli]
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

    fn process_work<'a, S: OneshotSource + Sync + 'static>(
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
                Encoding::Brotli => {
                    let decoder =
                        async_compression::tokio::bufread::BrotliDecoder::new(stream_reader);
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

        // Finally, decode as CSV.
        csv_async::AsyncReader::from_reader(reader)
            .into_byte_records()
            .map_ok(|record| {
                let bytes = record.as_slice().to_vec();
                let ranges = (0..record.len())
                    .map(|idx| record.range(idx).expect("known to exist"))
                    .collect();

                CsvRecord { bytes, ranges }
            })
            .map_err(|err| StorageErrorXKind::CsvDecoding(err).with_context("csv decoding"))
            .boxed()
    }

    fn decode_chunk(
        &self,
        chunk: Self::RecordChunk,
        rows: &mut Vec<Row>,
    ) -> Result<usize, StorageErrorX> {
        let CsvRecord { bytes, ranges } = chunk;

        let datums = ranges.into_iter().map(|range| {
            let slice = bytes.get(range).expect("known to exist");
            let s = std::str::from_utf8(slice).expect("valid UTF-8");
            Datum::String(s)
        });

        let mut row = Row::default();
        row.from_datum_iter(datums);
        rows.push(row);

        Ok(1)
    }
}
