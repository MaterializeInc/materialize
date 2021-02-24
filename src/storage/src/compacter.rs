// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use lazy_static::lazy_static;
use log::{error, info, trace};
use regex::Regex;
use tokio::select;
use tokio::sync::mpsc;

use dataflow_types::Update;
use expr::GlobalId;
use repr::{Row, Timestamp};

use crate::wal::{encode_progress, encode_update};

static COMPACTER_INTERVAL: Duration = Duration::from_secs(600);

pub enum CompacterMessage {
    Add(GlobalId),
    Drop(GlobalId),
    Resume(GlobalId, Trace),
    AllowCompaction(Timestamp),
}

struct Batch {
    upper: Timestamp,
    lower: Timestamp,
    path: PathBuf,
}

impl Batch {
    fn create(log_segment_path: &Path, trace_path: &Path) -> Result<Self, anyhow::Error> {
        // First, lets re-read that finished segment
        let data = fs::read(log_segment_path)?;
        let messages: Vec<_> = LogSegmentIter::new(data).collect();

        let mut upper: Option<Timestamp> = None;
        let mut lower: Option<Timestamp> = None;
        let mut time_data = BTreeMap::new();
        for message in messages.iter() {
            match message {
                Message::Progress(time) => match (lower, upper) {
                    (None, None) => {
                        lower = Some(*time);
                    }
                    (Some(l), None) => {
                        assert!(*time >= l);
                        upper = Some(*time);
                    }
                    (Some(_), Some(u)) => {
                        assert!(*time >= u);
                        upper = Some(*time);
                    }
                    (None, Some(_)) => unreachable!(),
                },
                Message::Data(Update {
                    row,
                    timestamp,
                    diff,
                }) => {
                    let entry = time_data.entry((timestamp, row)).or_insert(0);
                    *entry += diff;
                }
            }
        }

        // Now let's prepare the output
        let mut buf = Vec::new();
        assert!(lower.is_some());
        assert!(upper.is_some());

        let lower = lower.unwrap();
        let upper = upper.unwrap();

        encode_progress(lower, &mut buf)?;
        for ((timestamp, row), diff) in time_data.into_iter() {
            if diff == 0 {
                continue;
            }

            encode_update(row, *timestamp, diff, &mut buf);
        }

        encode_progress(upper, &mut buf);

        let batch_name = format!("batch-{}-{}", lower, upper);
        let batch_path = trace_path.join(&batch_name);
        let batch_tmp_path = trace_path.join(format!("{}-tmp", batch_name));
        fs::write(&batch_tmp_path, buf)?;
        fs::rename(&batch_tmp_path, &batch_path)?;
        fs::remove_file(log_segment_path)?;

        Ok(Batch {
            upper,
            lower,
            path: batch_path,
        })
    }
}

pub struct Trace {
    id: GlobalId,
    trace_path: PathBuf,
    wal_path: PathBuf,
    batches: Vec<Batch>,
    compaction: Option<Timestamp>,
}

impl Trace {
    fn create(id: GlobalId, trace_path: PathBuf, wal_path: PathBuf) -> Result<Self, anyhow::Error> {
        let _ = fs::read_dir(&wal_path).with_context(|| {
            anyhow!(
                "trying to ensure wal directory {} exists for trace of relation {}",
                id,
                wal_path.display()
            )
        })?;

        // Create a new directory to store the trace
        fs::create_dir(&trace_path).with_context(|| {
            anyhow!("trying to create trace directory: {}", trace_path.display())
        })?;

        Ok(Self {
            id,
            trace_path,
            wal_path,
            batches: Vec::new(),
            compaction: None,
        })
    }

    // Let's delete the trace directory
    fn destroy(self) -> Result<(), anyhow::Error> {
        fs::remove_dir_all(self.trace_path)?;

        Ok(())
    }

    // Try to consume more completed log segments from the wal directory
    // return false if the wal directory no longer exists, or if any
    // files stop existing while we are trying to consume them.
    fn consume_wal(&mut self) -> Result<bool, anyhow::Error> {
        // TODO: handle ENOENT
        let finished_segments = self.get_finished_wal_segments()?;

        for segment in finished_segments {
            // TODO: handle ENOENT
            let batch = Batch::create(&segment, &self.trace_path)?;
            self.batches.push(batch);
        }

        if self.batches.len() > 10 {
            self.compact()?;
        }

        Ok(true)
    }

    fn get_finished_wal_segments(&self) -> Result<Vec<PathBuf>, anyhow::Error> {
        lazy_static! {
            static ref WAL_SEGMENT_REGEX: Regex = Regex::new("^log-[0-9]+-final$").unwrap();
        }

        let mut segments = read_dir_regex(&self.wal_path, &WAL_SEGMENT_REGEX)?;
        segments.sort();

        Ok(segments)
    }

    // Try to compact all of the batches we know about into a single batch from
    // [compaction_frontier, upper)
    fn compact(&mut self) -> Result<(), anyhow::Error> {
        unimplemented!()
    }

    pub fn resume(
        id: GlobalId,
        traces_path: PathBuf,
        wals_path: PathBuf,
    ) -> Result<Self, anyhow::Error> {
        // Need to instantiate a new trace and figure out what batches
        // we have access to.
        unimplemented!()
    }
}

pub struct Compacter {
    rx: mpsc::UnboundedReceiver<CompacterMessage>,
    traces: HashMap<GlobalId, Trace>,
    wals_path: PathBuf,
    traces_path: PathBuf,
}

impl Compacter {
    pub fn new(
        rx: mpsc::UnboundedReceiver<CompacterMessage>,
        wals_path: PathBuf,
        traces_path: PathBuf,
    ) -> Self {
        Self {
            rx,
            traces: HashMap::new(),
            wals_path,
            traces_path,
        }
    }

    async fn compact(&mut self) -> Result<(), anyhow::Error> {
        let mut interval = tokio::time::interval(COMPACTER_INTERVAL);
        loop {
            select! {
                data = self.rx.recv() => {
                    if let Some(data) = data {
                        self.handle_message(data)?
                    } else {
                        break;
                    }
                }
                _ = interval.tick() => {
                    for (_, s) in self.traces.iter_mut() {
                        // Check to see if the WAL still exists
                        // if so, check to see if there are any pending log segments to ingest
                        // finally, check to see if we can compact the data.
                        unimplemented!()
                    }

                }
            }
        }

        Ok(())
    }

    fn handle_message(&mut self, message: CompacterMessage) -> Result<(), anyhow::Error> {
        match message {
            CompacterMessage::Add(_) => unimplemented!(),
            CompacterMessage::Drop(_) => unimplemented!(),
            CompacterMessage::Resume(_, _) => unimplemented!(),
            CompacterMessage::AllowCompaction(_) => unimplemented!(),
        }
        Ok(())
    }

    pub async fn run(&mut self) {
        let ret = self.compact().await;

        match ret {
            Ok(_) => (),
            Err(e) => {
                error!("Compacter thread encountered an error: {:#}", e);
            }
        }
    }
}

fn read_dir_regex(path: &Path, regex: &Regex) -> Result<Vec<PathBuf>, anyhow::Error> {
    let entries = std::fs::read_dir(path)?;
    let mut results = vec![];
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
            if regex.is_match(&file_name) {
                results.push(path.to_path_buf());
            }
        }
    }

    Ok(results)
}

pub enum Message {
    Data(Update),
    Progress(Timestamp),
}

fn read_message(buf: &[u8], mut offset: usize) -> Option<(Message, usize)> {
    if offset >= buf.len() {
        return None;
    }

    // Let's start by only looking at the buffer at the offset.
    let (_, data) = buf.split_at(offset);

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
        // Otherwise lets read the data.
        let timestamp = cursor.read_u64::<NetworkEndian>().unwrap();
        let diff = cursor.read_i64::<NetworkEndian>().unwrap() as isize;
        let len = cursor.read_u32::<NetworkEndian>().unwrap() as usize;

        assert!(diff != 0);

        // Grab the next len bytes after the 24 byte length header, and turn
        // it into a vector so that we can extract things from it as a Row.
        // TODO: could we avoid the extra allocation here?
        let (_, rest) = data.split_at(24);
        let row = rest[..len].to_vec();

        let row = unsafe { Row::new(row) };
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
