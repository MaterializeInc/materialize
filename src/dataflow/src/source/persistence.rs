// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to source persistence.
//
// TODO: currently everything is fairly Kafka-centric and we should probably
// not directly usable for some other source types.

use std::path::Path;
use std::pin::Pin;

use anyhow::{bail, Error};
use byteorder::{ByteOrder, NetworkEndian, WriteBytesExt};

use dataflow_types::Timestamp;
use expr::GlobalId;
use futures::sink::Sink;
use log::error;
use repr::{Datum, Row};
use serde::{Deserialize, Serialize};

use crate::server::PersistenceMessage;

static RECORD_FILE_PREFIX: &str = "materialize";

/// Type alias for object that sends data to the persister.
pub type PersistenceSender = Pin<Box<dyn Sink<PersistenceMessage, Error = comm::Error> + Send>>;

/// A single record from a source and partition that can be written to disk by
/// the persister thread, and read back in and sent to the ingest pipeline later.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Record {
    /// Offset of record in a partition.
    pub offset: i64,
    /// Timestamp of record.
    pub timestamp: Timestamp,
    /// The offset of the record that comes before this one in the topic.
    pub predecessor: Option<i64>,
    /// Record key.
    pub key: Vec<u8>,
    /// Record value.
    pub value: Vec<u8>,
}

impl Record {
    /// Encode the record as a length-prefixed Row, and then append that Row to the buffer.
    /// This function will throw an error if the row is larger than 4 GB.
    /// TODO: could this be made more efficient with a RowArena?
    pub fn write_record(&self, buf: &mut Vec<u8>) -> Result<(), anyhow::Error> {
        let row = Row::pack(&[
            Datum::Int64(self.offset),
            Datum::Int64(self.timestamp as i64),
            Datum::Bytes(&self.key),
            Datum::Bytes(&self.value),
        ]);

        encode_row(&row, buf)
    }

    /// Read a encoded length-prefixed Row from a buffer at an offset, and try
    /// to convert it back to a record. Returns the record and the next offset
    /// to read from, if possible.
    fn read_record(buf: &[u8], offset: usize) -> Option<(Self, usize)> {
        if offset >= buf.len() {
            return None;
        }

        // Let's start by only looking at the buffer at the offset.
        let (_, data) = buf.split_at(offset);

        // Read in the length of the encoded row.
        let len = NetworkEndian::read_u32(&data) as usize;
        assert!(
            len >= 16,
            format!(
                "expected to see at least 16 bytes in record, but saw {}",
                len
            )
        );

        // Grab the next len bytes after the 4 byte length header, and turn
        // it into a vector so that we can extract things from it as a Row.
        // TODO: could we avoid the extra allocation here?
        let (_, rest) = data.split_at(4);
        let row = rest[..len].to_vec();

        let rec = Row::new(row);
        let row = rec.unpack();

        let source_offset = row[0].unwrap_int64();
        let timestamp = row[1].unwrap_int64() as Timestamp;
        let key = row[2].unwrap_bytes();
        let value = row[3].unwrap_bytes();

        Some((
            Record {
                predecessor: None,
                offset: source_offset,
                timestamp,
                key: key.into(),
                value: value.into(),
            },
            offset + len + 4,
        ))
    }
}

/// Iterator through a persisted set of records.
pub struct RecordIter {
    /// Underlying data from which we read the records.
    pub data: Vec<u8>,
    /// Offset into the data.
    pub offset: usize,
}

impl Iterator for RecordIter {
    type Item = Record;

    fn next(&mut self) -> Option<Record> {
        if let Some((record, next_offset)) = Record::read_record(&self.data, self.offset) {
            self.offset = next_offset;
            Some(record)
        } else {
            None
        }
    }
}

/// Describes what is provided from a persisted file.
#[derive(Debug)]
pub struct RecordFileMetadata {
    /// The source global ID this file represents.
    pub source_id: GlobalId,
    /// The partition ID this file represents.
    pub partition_id: i32,
    /// The inclusive lower bound of offsets provided by this file.
    pub start_offset: i64,
    /// The exclusive upper bound of offsets provided by this file.
    pub end_offset: i64,
}

impl RecordFileMetadata {
    /// Parse a file's metadata from its path.
    pub fn from_path(path: &Path) -> Result<Option<Self>, Error> {
        let file_name = path.file_name();

        if file_name.is_none() {
            // Path ends in .. . This should never happen but let's
            // just ignore for now.
            return Ok(None);
        }

        let file_name = file_name.expect("known to have a file name").to_str();

        if file_name.is_none() {
            // Path cannot be converted to a UTF-8 string. This
            // should not be the case for persistence files as we
            // control every aspect of the name.
            // TODO(rkhaitan): Make sure this assumption is valid.
            return Ok(None);
        }

        let file_name = file_name.expect("known to be a valid UTF-8 file name");

        if !file_name.starts_with(RECORD_FILE_PREFIX) {
            // File name doesn't match the prefix we use to write
            // down persistence data.
            return Ok(None);
        }

        let parts: Vec<_> = file_name.split('-').collect();

        if parts.len() != 5 {
            // File is either partially written, or entirely irrelevant.
            error!(
                "Found invalid persistence file name: {}. Ignoring",
                file_name
            );
            return Ok(None);
        }
        Ok(Some(Self {
            source_id: parts[1].parse()?,
            partition_id: parts[2].parse()?,
            start_offset: parts[3].parse()?,
            end_offset: parts[4].parse()?,
        }))
    }

    /// Generate a file name that can later be parsed into metadata.
    pub fn generate_file_name(
        source_id: GlobalId,
        partition_id: i32,
        start_offset: i64,
        end_offset: i64,
    ) -> String {
        format!(
            "{}-{}-{}-{}-{}",
            RECORD_FILE_PREFIX, source_id, partition_id, start_offset, end_offset
        )
    }
}

/// Source data that gets sent to the persistence thread to place in persistent storage.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkerPersistenceData {
    /// Global Id of the Source whose data is being persisted.
    pub source_id: GlobalId,
    /// Partition the record belongs to.
    pub partition_id: i32,
    /// The record itself.
    pub record: Record,
}

/// Write a length-prefixed Row to a buffer
fn encode_row(row: &Row, buf: &mut Vec<u8>) -> Result<(), anyhow::Error> {
    let data = row.data();

    if data.len() >= u32::MAX as usize {
        bail!("failed to encode row: row too large");
    }

    buf.write_u32::<NetworkEndian>(data.len() as u32)
        .expect("writes to vec cannot fail");
    buf.extend_from_slice(data);
    Ok(())
}
