// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A Write-ahead log (WAL).
//!
//! `WriteAheadLogs` is a map from relation ids, to the on-disk `WriteAheadLog` where
//! we write down all of the updates in that relation as they occur.
//!
//! Each `WriteAheadLog` lives in its own directory and consists of one or more files, or log
//! segments. Only one log segment is open for writing, and we append new data updates of the form
//! `(Row, timestamp, diff)` and timestamp progress messages (basically a message that indicates
//! that no messages at t < closet_timestamp will be added to the log) to the end of the file.
//! Each log segment has to start and end with a timestamp progress message so that we can
//! derive lower and upper bounds for the timestamps contained in that log segment.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use log::error;
use timely::progress::timestamp::Timestamp as TimelyTimestamp;

use dataflow_types::Update;
use expr::GlobalId;
use repr::{Row, Timestamp};

// Data stored in Batches and log segment files.
#[derive(Debug, PartialEq)]
pub enum Message {
    // (Row, time, diff) tuples
    Data(Update),
    // Statements about which timestamps we might still receive data at.
    Progress(Timestamp),
}

/// Limit at which we will make a new segment.
///
/// Note that this doesn't provide a true upper bound to the size of a log
/// segment because a single transaction / single timestamp's updates will always
/// be placed in the same segment. However, this limit is the threshold at which
/// the WAL writer will stop writing to the current log segment and open a new
/// one.
// TODO: make this configurable..
static LOG_SEGMENT_SIZE_LIMIT: usize = 1usize << 24;

/// Segmented write ahead log to persist (data, time, records) from a relation
/// to disk.
///
/// Each log segment is named `log-{sequence-number}` where sequence numbers are
/// strictly increasing. Every log segment consists of binary representations of
/// (data, time, diff) messages and progress messages that indicate that no further
/// records with time < t will be present in the log. Each log segment file must
/// begin and end with a progress message that indicates a [lower, upper) bound
/// for the data in that log segment.
///
/// TODO: note that we currently keep an open file descriptor for each write ahead log.
/// In the future this could lead to resource exhaustion problems.
/// TODO: We currently write progress updates even for times where we know the times
/// contain no records. We could potentially omit those progress messages and save
/// space in the WAL.
pub struct WriteAheadLog {
    /// Id of the relation.
    id: GlobalId,
    /// Directory where we store this relation's log files.
    base_path: PathBuf,
    /// Log file we are currently writing to.
    current_file: File,
    /// Sequence number of the current log segment.
    current_sequence_number: usize,
    /// Number of bytes written to the current log segment.
    current_bytes_written: usize,
    /// Total number of bytes written to this write ahead log.
    /// TODO: this number is not accurate across restarts.
    total_bytes_written: usize,
    /// Last timestamp closed in this write-ahead log.
    last_closed_timestamp: Timestamp,
}

impl WriteAheadLog {
    /// Create a new write ahead log for relation `id` at directory `base_path`.
    fn create(id: GlobalId, base_path: PathBuf) -> Result<Self, anyhow::Error> {
        // First lets create the directory where these log segments will live.
        // We expect that this directory should not exist and will error if it
        // does.
        fs::create_dir(&base_path).with_context(|| {
            format!(
                "trying to create wal directory for relation: {} path: {}",
                id,
                base_path.display(),
            )
        })?;

        // TODO: clean this up
        let file_path = base_path.join("log-0");
        let current_file = open_log_segment(&file_path, true)?;

        let mut ret = Self {
            id,
            base_path,
            current_file,
            current_sequence_number: 0,
            current_bytes_written: 0,
            total_bytes_written: 0,
            last_closed_timestamp: TimelyTimestamp::minimum(),
        };

        // Write a timestamp message here so we can establish `lower` for the first log
        // segment.
        ret.write_progress_inner(0)?;
        Ok(ret)
    }

    /// Append `buf` to the end of the current log segment
    ///
    /// TODO: We don't currently sync after every write so there is still the
    /// potential for data loss here. Calling `fs::sync_all` after every write
    /// results in a ~20x slowdown. Explore opening the file in `O_SYNC` or `O_DSYNC`
    /// and also `O_DIRECT` + aligned block size writes.
    /// TODO: Also note that Postgres exposes a few different WAL writing modes,
    /// some faster and some safer and we could do a similar thing.
    fn write_inner(&mut self, buf: &[u8]) -> Result<(), anyhow::Error> {
        let len = buf.len();
        self.current_file.write_all(&buf).with_context(|| {
            format!(
                "failed to write to relation: {} wal segment {}",
                self.id, self.current_sequence_number
            )
        })?;
        self.current_file.flush().with_context(|| {
            format!(
                "failed to flush write to relation: {} wal segment {}",
                self.id, self.current_sequence_number
            )
        })?;
        self.current_bytes_written += len;
        self.total_bytes_written += len;

        Ok(())
    }

    /// Write a set of (Row, Timestamp, Diff) updates to the write-ahead log.
    fn write(&mut self, updates: &[Update]) -> Result<(), anyhow::Error> {
        // Check that all updates have a timestamp valid timestamp that is >=
        // the last closed timestamp.
        for update in updates {
            if update.timestamp < self.last_closed_timestamp {
                bail!(
                    "Invalid update timestamp {} last closed timestamp {}",
                    update.timestamp,
                    self.last_closed_timestamp,
                );
            }
        }
        // TODO: The allocation discipline for writes could probably be a lot
        // better. Each WAL writer could keep a buffer that it writes into
        // for everything and fall back to allocating stuff if we run out of
        // space.
        let mut buf = Vec::new();
        for update in updates {
            encode_update(&update.row, update.timestamp, update.diff, &mut buf)?;
        }

        self.write_inner(&buf)
    }

    fn write_progress_inner(&mut self, timestamp: Timestamp) -> Result<(), anyhow::Error> {
        let mut buf = Vec::new();
        encode_progress(timestamp, &mut buf)?;
        self.write_inner(&buf)
    }

    /// Write a progress (alternatively, timestamp closure) message to the write-ahead
    /// log.
    ///
    /// Optionally also switch to a new log segment file if the current log segment file
    /// grows too large.
    fn write_progress(&mut self, timestamp: Timestamp) -> Result<(), anyhow::Error> {
        // This timestamp needs to be greater than the last closed timestamp.
        if timestamp <= self.last_closed_timestamp {
            bail!(
                "Invalid attempt to close timestamp {} last closed {}",
                timestamp,
                self.last_closed_timestamp
            );
        }

        self.write_progress_inner(timestamp)?;

        // We only want to rotate the log segments at progress messages
        // so that we can extract batches.
        if self.current_bytes_written > LOG_SEGMENT_SIZE_LIMIT {
            self.rotate_log_segment()?;
            // Write the progress update to the new file to make sure we
            // know the `lower` for the new file
            self.write_progress_inner(timestamp)?;
        }

        Ok(())
    }

    /// Switch to writing a new log segment file.
    ///
    /// Mark the previous one as finished by prepending `-final` to its name.
    fn rotate_log_segment(&mut self) -> Result<(), anyhow::Error> {
        let old_file_path = self
            .base_path
            .join(format!("log-{}", self.current_sequence_number));
        let old_file_rename = self
            .base_path
            .join(format!("log-{}-final", self.current_sequence_number));
        let new_file_path = self
            .base_path
            .join(format!("log-{}", self.current_sequence_number + 1));

        // First lets open the new file
        let new_file = open_log_segment(&new_file_path, true)?;
        let old_file = std::mem::replace(&mut self.current_file, new_file);

        // Let's close the file descriptor from the old file and mark it as final.
        old_file.sync_all().with_context(|| {
            format!(
                "failed to sync state for finished log segment: {}",
                old_file_path.display()
            )
        })?;

        // TODO: Need to sync the parent directory here to be sure that
        // we durably persist the rename. I think perhaps a clearer protocol
        // then is to:
        // * write a termination 4 byte sequence
        // * fsync() the file
        // * rename the file to mark completion
        // * fsync() the parent directory to durably persist the rename.
        // * open a new file
        // * fsync() the parent directory to durably persist the file creation.
        // Luckily I think the existing behavior is ok on most filesystems.
        fs::rename(&old_file_path, &old_file_rename).with_context(|| {
            format!(
                "failed to rename finished log segment from: {} to: {}",
                old_file_path.display(),
                old_file_rename.display()
            )
        })?;

        // Update our own local state
        self.current_sequence_number += 1;
        self.current_bytes_written = 0;
        Ok(())
    }

    /// Continue writing to the write-ahead log after a restart.
    ///
    /// TODO: the important invariants after restart for the WAL are:
    /// * potentially many finished segments
    /// * the finished segments form a prefix of sequence numbers [low_seq_num, high_seq_num)
    /// * ie no duplicate or missing sequence numbers + low_seq_num >= 0
    /// * the unfinished wal segment is at `high_seq_number`
    fn resume(id: GlobalId, base_path: PathBuf) -> Result<WriteAheadLog, anyhow::Error> {
        let mut unfinished_file: Option<(PathBuf, usize)> = None;
        // List out all of the files in the write-ahead log directory. There should
        // be exactly one unfinished file, and potentially more than one finished
        // file. If that's not the case, we need to error out.
        let entries = std::fs::read_dir(&base_path)
            .with_context(|| format!("failed to read wal directory {}", base_path.display()))?;
        for entry in entries {
            if let Ok(file) = entry {
                let path = file.path();
                let file_name = path.file_name();
                if file_name.is_none() {
                    continue;
                }

                let file_name = file_name.unwrap().to_str();

                if file_name.is_none() {
                    continue;
                }

                let file_name = file_name.unwrap();

                if !file_name.starts_with("log-") {
                    bail!(
                        "found unexpected file when trying to resume wal for relation {}: {}",
                        id,
                        file_name,
                    )
                }

                if !file_name.ends_with("final") {
                    if unfinished_file.is_some() {
                        bail!("found more than one in-progress log segments when trying to resume wal for relation {}: {} and {}",
                            id,
                            file_name,
                            unfinished_file.unwrap().0.display())
                    }
                    let parts: Vec<_> = file_name.split('-').collect();
                    let sequence_number = parts[1].parse::<usize>()?;
                    unfinished_file = Some((path.to_path_buf(), sequence_number));
                }
            }
        }

        if let Some((unfinished_file, sequence_number)) = unfinished_file {
            // Lets start writing to the previously created file.
            let messages = read_segment(&unfinished_file)?;
            let mut last_closed_timestamp = None;

            // Try to find the last closed timestamp in this WAL segment.
            for message in messages {
                match message {
                    Message::Data(_) => continue,
                    Message::Progress(timestamp) => {
                        last_closed_timestamp = Some(timestamp);
                    }
                }
            }

            if last_closed_timestamp.is_none() {
                bail!(
                    "unable to determine last closed timestamp from unfinished log segment {}",
                    unfinished_file.display()
                );
            }

            let file = open_log_segment(&unfinished_file, false)?;

            let ret = Self {
                id,
                base_path,
                current_file: file,
                current_sequence_number: sequence_number,
                current_bytes_written: 0,
                total_bytes_written: 0,
                last_closed_timestamp: last_closed_timestamp.expect("known to exist"),
            };

            Ok(ret)
        } else {
            bail!("couldn't find any in-progress log segments when trying to resume wal")
        }
    }
}

/// The set of write ahead logs for all persisted relations.
pub struct WriteAheadLogs {
    path: PathBuf,
    wals: HashMap<GlobalId, WriteAheadLog>,
}

impl WriteAheadLogs {
    pub fn new(path: PathBuf) -> Result<Self, anyhow::Error> {
        Ok(Self {
            path,
            wals: HashMap::new(),
        })
    }

    // TODO make this path use cluster_id
    fn get_path(&self, id: GlobalId) -> PathBuf {
        self.path.join(id.to_string())
    }

    pub fn create(&mut self, id: GlobalId) -> Result<(), anyhow::Error> {
        if self.wals.contains_key(&id) {
            bail!(
                "asked to create wal for relation: {:?} that was already created",
                id
            )
        }

        let wal_base_path = self.get_path(id);
        let wal = WriteAheadLog::create(id, wal_base_path)?;
        self.wals.insert(id, wal);
        Ok(())
    }

    pub fn destroy(&mut self, id: GlobalId) -> Result<(), anyhow::Error> {
        if !self.wals.contains_key(&id) {
            bail!(
                "asked to delete wal for relation: {:?} that doesn't exist",
                id
            )
        }

        self.wals.remove(&id);
        Ok(())
    }

    pub fn write(&mut self, id: GlobalId, updates: &[Update]) -> Result<(), anyhow::Error> {
        if !self.wals.contains_key(&id) {
            bail!("asked to write to unknown relation: {}", id)
        }

        let wal = self.wals.get_mut(&id).expect("wal known to exist");
        wal.write(updates)?;
        Ok(())
    }

    pub fn resume(&mut self, id: GlobalId) -> Result<(), anyhow::Error> {
        if self.wals.contains_key(&id) {
            bail!(
                "asked to resume wal for relation: {:?} that already exists.",
                id
            )
        }
        let wal_path = self.get_path(id);
        let wal = WriteAheadLog::resume(id, wal_path)?;
        self.wals.insert(id, wal);

        Ok(())
    }

    pub fn write_progress(&mut self, timestamp: Timestamp) -> Result<(), anyhow::Error> {
        // For all logs, indicate that <timestamp> is now the min timestamp going
        // forward
        for (_, wal) in self.wals.iter_mut() {
            wal.write_progress(timestamp)?;
        }

        Ok(())
    }
}

// Write a 20 byte header + length-prefixed Row to a buffer
// TODO: change the on-disk representation to be more compact.
pub fn encode_update(
    row: &Row,
    timestamp: Timestamp,
    diff: isize,
    buf: &mut Vec<u8>,
) -> Result<(), anyhow::Error> {
    assert!(diff != 0);
    assert!(timestamp > 0);
    let data = row.data();

    if data.len() >= u32::MAX as usize {
        bail!("failed to encode row: row too large");
    }

    // Write out the header
    // Not a progress message so - 0
    buf.write_u32::<NetworkEndian>(0)
        .expect("writes to vec cannot fail");
    buf.write_u64::<NetworkEndian>(timestamp)
        .expect("writes to vec cannot fail");
    buf.write_i64::<NetworkEndian>(diff as i64)
        .expect("writes to vec cannot fail");

    // Now write out the data
    buf.write_u32::<NetworkEndian>(data.len() as u32)
        .expect("writes to vec cannot fail");
    buf.extend_from_slice(data);
    Ok(())
}

pub fn encode_progress(timestamp: Timestamp, buf: &mut Vec<u8>) -> Result<(), anyhow::Error> {
    // This is a progress update so - 1
    buf.write_u32::<NetworkEndian>(1).unwrap();
    buf.write_u64::<NetworkEndian>(timestamp).unwrap();

    Ok(())
}

// Open a new log segment file to write into.
//
// Importantly, we need to open this file with O_APPEND and we should fail if the
// log segment file already exists.
// TODO: We also want to open this file with `O_SYNC` or otherwise guarantee that
// every write to the WAL is followed by a `fsync()` to make sure we durably
// flush updates to disk.
// TODO: we need to also fsync the parent directory to durably persist the file
// entry.
fn open_log_segment(path: &Path, create_new: bool) -> Result<File, anyhow::Error> {
    let mut options = fs::OpenOptions::new();
    options.append(true);

    // TODO: perhaps setting this option to false when not previously set is a no-op.
    // Was not obvious to me from the documentation.
    if create_new {
        options.create_new(true);
    }
    options.open(path).with_context(|| {
        format!(
            "trying to open wal segment file at path: {}",
            path.display()
        )
    })
}

/// Try to decode the next message from `buf` starting at `offset`.
///
/// Returns a Message and the next offset to read from if successful, or None.
/// TODO: make this API cleaner.
fn read_message(buf: &[u8], mut offset: usize) -> Option<(Message, usize)> {
    if offset >= buf.len() {
        return None;
    }

    // Let's start by only looking at the buffer at the offset.
    let (_, data) = buf.split_at(offset);

    if data.len() < 12 {
        error!(
            "invalid offset while reading file: {}. Expected at least 12 more bytes have {}",
            offset,
            data.len()
        );
        return None;
    }

    // Let's read the header first
    let mut cursor = Cursor::new(&data);

    let is_progress = cursor.read_u32::<NetworkEndian>().unwrap();

    if is_progress != 0 {
        // If this is a progress message let's seal a new
        // set of updates.

        // Lets figure out the time bound.
        let timestamp = cursor.read_u64::<NetworkEndian>().unwrap();
        // Advance the offset past what we've read.
        offset += 12;

        Some((Message::Progress(timestamp), offset))
    } else {
        // Let's make sure we have an appropriate number of bytes in the buffer.
        if data.len() < 24 {
            error!(
                "invalid offset while reading file: {}. Expected at least 24 more bytes have {}",
                offset,
                data.len()
            );
            return None;
        }
        // Otherwise lets read the data.
        let timestamp = cursor.read_u64::<NetworkEndian>().unwrap();
        let diff = cursor.read_i64::<NetworkEndian>().unwrap() as isize;
        let len = cursor.read_u32::<NetworkEndian>().unwrap() as usize;

        assert!(diff != 0);

        // Grab the next len bytes after the 24 byte length header, and turn
        // it into a vector so that we can extract things from it as a Row.
        // TODO: could we avoid the extra allocation here?
        let (_, rest) = data.split_at(24);

        if rest.len() < len {
            error!(
                "invalid row length: expected {} bytes but only have {} remaining",
                len,
                rest.len()
            );
            return None;
        }
        let row = rest[..len].to_vec();

        let row = unsafe { Row::from_bytes_unchecked(row) };
        // Update the offset to account for the data we just read
        offset = offset + 24 + len;
        Some((
            Message::Data(Update {
                row,
                timestamp,
                diff,
            }),
            offset,
        ))
    }
}

/// Iterator through a set of persisted messages.
#[derive(Debug)]
pub struct LogSegmentIter {
    /// Underlying data from which we read the records.
    pub data: Vec<u8>,
    /// Offset into the data.
    pub offset: usize,
}

impl Iterator for LogSegmentIter {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((message, next_offset)) = read_message(&self.data, self.offset) {
            self.offset = next_offset;
            Some(message)
        } else {
            None
        }
    }
}

impl LogSegmentIter {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data, offset: 0 }
    }
}

pub fn read_segment(path: &Path) -> Result<Vec<Message>, anyhow::Error> {
    let data = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    Ok(LogSegmentIter::new(data).collect())
}
