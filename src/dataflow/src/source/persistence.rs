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

use anyhow::{anyhow, bail, Error};
use byteorder::{ByteOrder, NetworkEndian, WriteBytesExt};

use dataflow_types::Timestamp;
use expr::GlobalId;
use repr::{Datum, Row};
use serde::{Deserialize, Serialize};

/// A single record from a source and partition that can be written to disk by
/// the persister thread, and read back in and sent to the ingest pipeline later.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Record {
    /// Offset of record in a partition.
    pub offset: i64,
    /// Timestamp of record.
    pub timestamp: Timestamp,
    /// Record key.
    pub key: Vec<u8>,
    /// Record value.
    pub value: Vec<u8>,
}

impl Record {
    /// Encode the record as a length-prefixed Row, and then append that Row to the buffer.
    /// This function will throw an error if the row is larger than 4 GB.
    pub fn write_record(&self, buf: &mut Vec<u8>) -> Result<(), anyhow::Error> {
        let row = Row::pack(&[
            Datum::Int64(self.offset),
            Datum::Int64(self.timestamp as i64),
            Datum::Bytes(&self.key),
            Datum::Bytes(&self.value),
        ]);

        encode_row(&row, buf)
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
        if self.offset >= self.data.len() {
            return None;
        }

        let (_, data) = self.data.split_at_mut(self.offset);
        let len = NetworkEndian::read_u32(&data) as usize;
        assert!(
            len >= 16,
            format!(
                "expected to see at least 16 bytes in record, but saw {}",
                len
            )
        );
        let (_, rest) = data.split_at_mut(4);
        let row = rest[..len].to_vec();
        self.offset += 4 + len;

        let rec = Row::new(row);
        let row = rec.unpack();

        let offset = row[0].unwrap_int64();
        let timestamp = row[1].unwrap_int64() as Timestamp;
        let key = row[2].unwrap_bytes();
        let value = row[3].unwrap_bytes();
        Some(Record {
            offset,
            timestamp,
            key: key.into(),
            value: value.into(),
        })
    }
}

/// Describes what is provided from a persisted file.
#[derive(Debug)]
pub struct PersistedFileMetadata {
    /// The source global ID this file represents.
    pub id: GlobalId,
    /// The partition ID this file represents.
    pub partition_id: i32,
    /// The inclusive lower bound of offsets provided by this file.
    pub start_offset: i64,
    /// The exclusive upper bound of offsets provided by this file.
    pub end_offset: i64,
    /// Whether or not this file was completely written.
    pub is_complete: bool,
}

impl PersistedFileMetadata {
    /// Parse a file's metadata from its filename.
    pub fn from_fname(s: &str) -> Result<Self, Error> {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() < 5 || parts.len() > 6 {
            return Err(anyhow!(
                "expected filename to have 5 (or 6) parts, but it was {}",
                s
            ));
        }
        let is_complete = parts.len() < 6 || parts[5] != "tmp";
        Ok(PersistedFileMetadata {
            id: parts[1].parse()?,
            partition_id: parts[2].parse()?,
            start_offset: parts[3].parse()?,
            end_offset: parts[4].parse()?,
            is_complete,
        })
    }
}

/// Source data that gets sent to the persistence thread to place in persistent storage.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkerPersistenceData {
    /// Global Id of the Source whose data is being persisted.
    pub source_id: GlobalId,
    /// Partition the record belongs to.
    pub partition: i32,
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
