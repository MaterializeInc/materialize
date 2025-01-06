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

use futures::stream::{BoxStream, StreamExt};
use futures::TryStreamExt;
use mz_repr::fixed_length::FromDatumIter;
use mz_repr::{Datum, Row};
use serde::{Deserialize, Serialize};
use tokio_util::io::StreamReader;

use crate::oneshot_source::{OneshotFormat, OneshotSource, StorageErrorX, StorageErrorXKind};

#[derive(Default, Clone)]
pub struct CsvDecoder;

/// Instructions on how to parse a single CSV file.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CsvWorkRequest<O, C> {
    object: O,
    checksum: C,
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
        let request = CsvWorkRequest { object, checksum };
        Ok(vec![request])
    }

    fn process_work<'a, S: OneshotSource + Sync>(
        &'a self,
        source: &'a S,
        request: Self::WorkRequest<S>,
    ) -> BoxStream<'a, Result<Self::RecordChunk, StorageErrorX>> {
        let CsvWorkRequest { object, checksum } = request;

        let raw_byte_stream = source
            .get(object, checksum, None)
            .map_err(|e| io::Error::new(io::ErrorKind::Interrupted, format!("{e:?}")));
        let stream_reader = StreamReader::new(raw_byte_stream);

        csv_async::AsyncReader::from_reader(stream_reader)
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

        // TODO(cf1): Get a RelationDesc here to parse the proper types.
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
