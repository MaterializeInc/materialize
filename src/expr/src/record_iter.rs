// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::bail;
use byteorder::{ByteOrder, NetworkEndian, WriteBytesExt};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::id::GlobalId;
use repr::{Datum, Row, Timestamp};

/// A single record from a source and partition that can be written to disk by
/// the persister thread, and read back in and sent to the ingest pipeline later.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct PersistedRecord {
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

impl PersistedRecord {
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
            PersistedRecord {
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
#[derive(Debug)]
pub struct RecordIter {
    /// Underlying data from which we read the records.
    pub data: Vec<u8>,
    /// Offset into the data.
    pub offset: usize,
}

impl Iterator for RecordIter {
    type Item = PersistedRecord;

    fn next(&mut self) -> Option<PersistedRecord> {
        if let Some((record, next_offset)) = PersistedRecord::read_record(&self.data, self.offset) {
            self.offset = next_offset;
            Some(record)
        } else {
            None
        }
    }
}

impl RecordIter {
    pub fn new(data: Vec<u8>) -> Self {
        RecordIter { data, offset: 0 }
    }

    // TODO(justin): this should return an error.
    pub fn files_for_source(id: GlobalId) -> Vec<PathBuf> {
        // TODO(justin): plumb through the configured persistence directory, for now this is only
        // suitable for testing.
        let persistence_dir = PathBuf::from("mzdata/persistence");
        let source_path = persistence_dir.join(id.to_string());
        match std::fs::read_dir(&source_path) {
            Ok(entries) => entries.map(|e| e.unwrap().path()).collect(),
            Err(_) => {
                // TODO(justin): it would be better to have a real error here, but saying
                // "there are no records" is also acceptable for now.
                return vec![];
            }
        }
    }
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
