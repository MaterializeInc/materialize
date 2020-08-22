// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to source persistence.

use anyhow::{anyhow, Error};
use byteorder::{ByteOrder, NetworkEndian};

use dataflow_types::Timestamp;
use expr::GlobalId;
use repr::Row;
use serde::{Deserialize, Serialize};

/// A single record from a persisted file.
#[derive(Debug, Clone)]
pub struct Record {
    /// Offset of record in a partition.
    pub offset: i64,
    /// Timestamp of record.
    pub time: i64,

    /// Record key.
    pub key: Vec<u8>,
    /// Record value
    pub data: Vec<u8>,
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
        let time = row[1].unwrap_int64();
        let key = row[2].unwrap_bytes();
        let data = row[3].unwrap_bytes();
        Some(Record {
            offset,
            time,
            key: key.into(),
            data: data.into(),
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
/// TODO currently fairly Kafka input-centric.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkerPersistenceData {
    /// Global Id of the Source whose data is being persisted.
    pub source_id: GlobalId,
    /// Partition the record belongs to.
    pub partition: i32,
    /// Offset where we found the message.
    pub offset: i64,
    /// Timestamp assigned to the message.
    pub timestamp: Timestamp,
    /// The key of the message.
    pub key: Vec<u8>,
    /// The data of the message.
    pub payload: Vec<u8>,
}

/// TODO remove this
#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PersistedRecord {
    /// ...
    pub offset: i64,
    /// ...
    pub timestamp: Timestamp,
    /// ...
    pub key: Vec<u8>,
    /// ...
    pub payload: Vec<u8>,
}
