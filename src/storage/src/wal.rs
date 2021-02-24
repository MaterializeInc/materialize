// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context};
use byteorder::{NetworkEndian, WriteBytesExt};

use dataflow_types::Update;
use expr::GlobalId;
use repr::{Row, Timestamp};

// Limit at which we will make a new log segment
// TODO: make this configurable and bump default to be larger.
static MAX_LOG_SEGMENT_SIZE: usize = 50;

/// Segmented write ahead log to persist (data, time, records) from relation `id`
/// to disk.
///
/// Each log segment is named `log-{sequence-number}` where sequence numbers are
/// strictly increasing. Every log segment consists of binary representations of
/// (data, time, diff) messages and progress messages that indicate that no further
/// records with time < t will be present in the log. Each log segment file must
/// begin and end with a progress message that indicates a [lower, upper) bound
/// for the data in that log segment.
pub struct WriteAheadLog {
    base_path: PathBuf,
    current_file: File,
    current_sequence_number: usize,
    current_bytes_written: usize,
    total_bytes_written: usize,
}

impl WriteAheadLog {
    fn create(id: GlobalId, base_path: PathBuf) -> Result<Self, anyhow::Error> {
        // First lets create the directory where these log segments will live.
        // We expect that this directory should not exist and will error if it
        // does.
        fs::create_dir(&base_path).with_context(|| {
            anyhow!(
                "trying to create directory for relation: {} path: {:#?}",
                id,
                base_path
            )
        })?;

        // TODO: clean this up
        let file_path = base_path.join("log-0");
        let current_file = create_log_segment(&file_path)?;

        let mut ret = Self {
            base_path,
            current_file,
            current_sequence_number: 0,
            current_bytes_written: 0,
            total_bytes_written: 0,
        };

        // Write a timestamp message here so we can establish `lower` for the first log
        // segment.
        ret.write_progress(0)?;
        Ok(ret)
    }

    fn write_inner(&mut self, buf: &[u8]) -> Result<(), anyhow::Error> {
        let len = buf.len();
        self.current_file.write_all(&buf)?;
        self.current_file.flush()?;
        self.current_bytes_written += len;
        self.total_bytes_written += len;

        Ok(())
    }

    fn write(&mut self, updates: &[Update]) -> Result<(), anyhow::Error> {
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

    fn write_progress(&mut self, timestamp: Timestamp) -> Result<(), anyhow::Error> {
        self.write_progress_inner(timestamp)?;

        // We only want to rotate the log segments at progress messages
        // so that we can extract batches.
        if self.current_bytes_written > MAX_LOG_SEGMENT_SIZE {
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
        let new_file = create_log_segment(&new_file_path)?;
        let old_file = std::mem::replace(&mut self.current_file, new_file);

        // Let's close the file descriptor from the old file and mark it as final.
        old_file.sync_all()?;
        drop(old_file);
        fs::rename(&old_file_path, &old_file_rename)?;

        // Update our own local state
        self.current_sequence_number += 1;
        self.current_bytes_written = 0;
        Ok(())
    }

    fn resume(id: GlobalId, base_path: PathBuf) -> Result<WriteAheadLog, anyhow::Error> {
        let mut unfinished_file: Option<(PathBuf, usize)> = None;
        // If its not empty, list out all of the files. There should be
        // exactly one unfinished file, and potentially more than one
        // finished file. If that's not the case, let's fail to resume
        // the WAL.
        let entries = std::fs::read_dir(&base_path)?;
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
            // lets start writing to the previously created file.
            let file = fs::OpenOptions::new()
                .append(true)
                .open(&unfinished_file)
                .unwrap();

            let ret = Self {
                base_path,
                current_file: file,
                current_sequence_number: sequence_number,
                current_bytes_written: 0,
                total_bytes_written: 0,
            };

            Ok(ret)
        } else {
            bail!("couldn't find any in-progress log segments when trying to resume wal")
        }
    }
}

pub struct WriteAheadLogs {
    path: PathBuf,
    wals: HashMap<GlobalId, WriteAheadLog>,
}

impl WriteAheadLogs {
    pub fn new(path: PathBuf) -> Result<Self, anyhow::Error> {
        fs::create_dir_all(&path)
            .with_context(|| anyhow!("trying to create wal directory: {:#?}", path))?;
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
            // TODO get rid of this later
            panic!("asked to write to unknown table: {}", id)
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
    buf.write_u32::<NetworkEndian>(0).unwrap();
    buf.write_u64::<NetworkEndian>(timestamp).unwrap();
    buf.write_i64::<NetworkEndian>(diff as i64).unwrap();

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

fn create_log_segment(path: &Path) -> Result<File, anyhow::Error> {
    fs::OpenOptions::new()
        .append(true)
        .create_new(true)
        .open(path)
        .with_context(|| anyhow!("trying to create wal segment file at path: {:#?}", path))
}
