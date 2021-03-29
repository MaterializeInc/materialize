// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::{self, BufRead, Read};
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;

use anyhow::{Context, Error};
use flate2::read::MultiGzDecoder;
#[cfg(target_os = "linux")]
use inotify::{Inotify, WatchMask};
use log::error;
use timely::scheduling::SyncActivator;

use dataflow_types::{
    AvroOcfEncoding, Compression, DataEncoding, ExternalSourceConnector, MzOffset,
};
use expr::{PartitionId, SourceInstanceId};
use mz_avro::types::Value;
use mz_avro::{AvroRead, Schema, Skip};

use crate::logging::materialized::Logger;
use crate::source::{NextMessage, SourceMessage, SourceReader};

/// Contains all information necessary to ingest data from file sources (either
/// regular sources, or Avro OCF sources)
pub struct FileSourceReader<Out> {
    /// Unique source ID
    id: SourceInstanceId,
    /// Receiver channel that ingests records
    receiver_stream: Receiver<Result<Out, Error>>,
    /// Current File Offset. This corresponds to the offset of last processed message
    /// (initially 0 if no records have been processed)
    current_file_offset: FileOffset,
}

#[derive(Copy, Clone)]
/// Represents an index into a file. Files are 1-indexed by Unix convention
pub struct FileOffset {
    pub offset: i64,
}

/// Convert from FileOffset to MzOffset (1-indexed)
impl From<FileOffset> for MzOffset {
    fn from(file_offset: FileOffset) -> Self {
        MzOffset {
            offset: file_offset.offset,
        }
    }
}

impl SourceReader<Value> for FileSourceReader<Value> {
    fn new(
        _name: String,
        source_id: SourceInstanceId,
        _: usize,
        consumer_activator: SyncActivator,
        connector: ExternalSourceConnector,
        encoding: DataEncoding,
        _: Option<Logger>,
    ) -> Result<(FileSourceReader<Value>, Option<PartitionId>), anyhow::Error> {
        let receiver = match connector {
            ExternalSourceConnector::AvroOcf(oc) => {
                let reader_schema = match &encoding {
                    DataEncoding::AvroOcf(AvroOcfEncoding { reader_schema }) => reader_schema,
                    _ => unreachable!(
                        "Internal error: \
                                         Avro OCF schema should have already been resolved.\n\
                                        Encoding is: {:?}",
                        encoding
                    ),
                };
                let reader_schema: Schema = reader_schema.parse().unwrap();
                let ctor = { move |file| mz_avro::Reader::with_schema(&reader_schema, file) };
                let tail = if oc.tail {
                    FileReadStyle::TailFollowFd
                } else {
                    FileReadStyle::ReadOnce
                };
                let (tx, rx) = std::sync::mpsc::sync_channel(10000_usize);
                std::thread::spawn(move || {
                    read_file_task(
                        oc.path,
                        tx,
                        Some(consumer_activator),
                        tail,
                        oc.compression,
                        ctor,
                    );
                });
                rx
            }
            _ => panic!("Only OCF sources are supported with Avro encoding of values."),
        };

        Ok((
            FileSourceReader {
                id: source_id,
                receiver_stream: receiver,
                current_file_offset: FileOffset { offset: 0 },
            },
            Some(PartitionId::File),
        ))
    }

    fn get_next_message(&mut self) -> Result<NextMessage<Value>, anyhow::Error> {
        match self.receiver_stream.try_recv() {
            Ok(Ok(record)) => {
                self.current_file_offset.offset += 1;
                let message = SourceMessage {
                    partition: PartitionId::File,
                    offset: self.current_file_offset.into(),
                    upstream_time_millis: None,
                    key: None,
                    payload: Some(record),
                };
                Ok(NextMessage::Ready(message))
            }
            Ok(Err(e)) => {
                error!("Failed to read file for {}. Error: {}.", self.id, e);
                Err(e)
            }
            Err(TryRecvError::Empty) => Ok(NextMessage::Pending),
            Err(TryRecvError::Disconnected) => Ok(NextMessage::Finished),
        }
    }
}

impl SourceReader<Vec<u8>> for FileSourceReader<Vec<u8>> {
    fn new(
        _name: String,
        source_id: SourceInstanceId,
        worker_id: usize,
        consumer_activator: SyncActivator,
        connector: ExternalSourceConnector,
        _: DataEncoding,
        _: Option<Logger>,
    ) -> Result<(FileSourceReader<Vec<u8>>, Option<PartitionId>), anyhow::Error> {
        let receiver = match connector {
            ExternalSourceConnector::File(fc) => {
                log::debug!("creating FileSourceReader worker_id={}", worker_id);
                let ctor = |fi| Ok(std::io::BufReader::new(fi).split(b'\n'));
                let (tx, rx) = std::sync::mpsc::sync_channel(10000);
                let tail = if fc.tail {
                    FileReadStyle::TailFollowFd
                } else {
                    FileReadStyle::ReadOnce
                };
                std::thread::spawn(move || {
                    read_file_task(
                        fc.path,
                        tx,
                        Some(consumer_activator),
                        tail,
                        fc.compression,
                        ctor,
                    );
                });
                rx
            }
            _ => panic!(
                "Only File sources are supported with File/OCF connectors and Vec<u8> encodings"
            ),
        };

        Ok((
            FileSourceReader {
                id: source_id,
                receiver_stream: receiver,
                current_file_offset: FileOffset { offset: 0 },
            },
            Some(PartitionId::File),
        ))
    }

    fn get_next_message(&mut self) -> Result<NextMessage<Vec<u8>>, anyhow::Error> {
        match self.receiver_stream.try_recv() {
            Ok(Ok(record)) => {
                self.current_file_offset.offset += 1;
                let message = SourceMessage {
                    partition: PartitionId::File,
                    offset: self.current_file_offset.into(),
                    upstream_time_millis: None,
                    key: None,
                    payload: Some(record),
                };
                Ok(NextMessage::Ready(message))
            }
            Ok(Err(e)) => {
                error!("Failed to read file for {}. Error: {}.", self.id, e);
                Err(e)
            }
            Err(TryRecvError::Empty) => Ok(NextMessage::Pending),
            Err(TryRecvError::Disconnected) => Ok(NextMessage::Finished),
        }
    }
}

/// Blocking logic to read from a file, intended for its own thread.
pub fn read_file_task<Ctor, I, Out, Err>(
    path: PathBuf,
    tx: std::sync::mpsc::SyncSender<Result<Out, anyhow::Error>>,
    activator: Option<SyncActivator>,
    read_style: FileReadStyle,
    compression: Compression,
    iter_ctor: Ctor,
) where
    I: IntoIterator<Item = Result<Out, Err>> + Send + 'static,
    Ctor: FnOnce(Box<dyn AvroRead + Send>) -> Result<I, Err>,
    Err: Into<anyhow::Error>,
{
    log::trace!("reading file {}", path.display());
    let file = match std::fs::File::open(&path).with_context(|| {
        format!(
            "file source: unable to open file at path {}",
            path.to_string_lossy(),
        )
    }) {
        Ok(file) => file,
        Err(err) => {
            // If we fail to send an error, it's likely due to a race condition
            // with the source being closed.
            let _ = tx.send(Err(err));
            return;
        }
    };

    let file_stream = match open_file_stream(path.clone(), file, read_style) {
        Ok(f) => f,
        Err(err) => {
            // If we fail to send an error, it's likely due to a race condition
            // with the source being closed.
            let _ = tx.send(Err(err));
            return;
        }
    };

    let file_stream: Box<dyn AvroRead + Send> = match compression {
        Compression::Gzip => Box::new(MultiGzDecoder::new(file_stream)),
        Compression::None => Box::new(file_stream),
    };

    let iter = iter_ctor(file_stream);

    match iter.map_err(Into::into).with_context(|| {
        format!(
            "Failed to obtain records from file at path {}",
            path.to_string_lossy(),
        )
    }) {
        Ok(i) => send_records(i, tx, activator),
        Err(e) => {
            // If we fail to send an error, it's likely due to a race condition
            // with the source being closed.
            let _ = tx.send(Err(e));
        }
    };
}

fn open_file_stream(
    _path: PathBuf,
    file: std::fs::File,
    read_style: FileReadStyle,
) -> Result<Box<dyn AvroRead + Send>, anyhow::Error> {
    match read_style {
        FileReadStyle::ReadOnce => Ok(Box::new(file)),
        FileReadStyle::TailFollowFd => {
            let (notice_tx, notice_rx) = mpsc::channel();

            // FSEvents doesn't raise events until you close the file, making it
            // useless for tailing log files that are kept open by the daemon
            // writing to them.
            //
            // Avoid this issue by just waking up and polling the file on macOS
            // every 100ms. We don't want to use notify::PollWatcher, since that
            // occasionally misses updates if the file is changed twice within
            // one second (it uses an mtime granularity of 1s). Plus it's not
            // actually more efficient; our call to poll_read will be as fast as
            // the PollWatcher's call to stat, and it actually saves a syscall
            // if the file has data available.
            //
            // https://github.com/notify-rs/notify/issues/240
            #[cfg(not(target_os = "linux"))]
            thread::spawn(move || {
                while let Ok(()) = notice_tx.send(()) {
                    thread::sleep(std::time::Duration::from_millis(100));
                }
            });

            #[cfg(target_os = "linux")]
            {
                let mut inotify = Inotify::init()
                    .with_context(|| format!("file source: failed to initialize inotify"))?;
                inotify
                    .add_watch(&_path, WatchMask::ALL_EVENTS)
                    .with_context(|| format!("failed to add watch for file {}", _path.display()))?;
                thread::spawn(move || {
                    // This buffer must be at least `sizeof(struct inotify_event) + NAME_MAX + 1`.
                    // The `inotify` crate documentation uses 1KB, so that's =
                    // what we do too.
                    let mut buf = [0; 1024];
                    loop {
                        if let Err(err) = inotify.read_events_blocking(&mut buf) {
                            if notice_tx
                                .send(Err(format!(
                                    "file source: failed to get events for file: {:#} (path: {})",
                                    err,
                                    _path.display()
                                )))
                                .is_err()
                            {
                                // If the notice_tx returns an error, it's because
                                // the source has been dropped. Just exit the
                                // thread.
                                return;
                            }
                            // We have no method for recovering from this error
                            // Close this thread and log an error message (which duplicates the err above)
                            error!(
                                "file source: closing stream due to read errors (path: {})",
                                _path.display()
                            );
                            return;
                        };
                        if notice_tx.send(Ok(())).is_err() {
                            // If the notice_tx returns an error, it's because
                            // the source has been dropped. Just exit the
                            // thread.
                            return;
                        }
                    }
                });
            };

            Ok(Box::new(ForeverTailedFile {
                rx: notice_rx,
                inner: file,
            }))
        }
    }
}

/// Strategies for streaming content from a file.
#[derive(PartialEq, Eq)]
pub enum FileReadStyle {
    /// File is read once and marked complete once the last line is read.
    ReadOnce,
    /// File is read and continually checked for new content, indefinitely.
    TailFollowFd,
    // TODO: TailFollowName,
}

/// Wraps a file, producing a stream that is tailed forever.
///
/// This involves silently swallowing EOFs,
/// and waiting on a Notify handle for more data to be written.
struct ForeverTailedFile<Ev> {
    rx: std::sync::mpsc::Receiver<Ev>,
    inner: std::fs::File,
}

impl<Ev> Skip for ForeverTailedFile<Ev> {
    fn skip(&mut self, len: usize) -> Result<(), io::Error> {
        self.inner.skip(len)
    }
}

impl<Ev> Read for ForeverTailedFile<Ev> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            // First drain the buffer of pending events from notify.
            for _ in self.rx.try_iter() {}
            match self.inner.read(buf)? {
                0 => {
                    if self.rx.recv().is_ok() {
                        // Notify thinks there might be new data. Go around
                        // the loop again to check.
                    } else {
                        error!("notify hung up while tailing file");
                        return Ok(0);
                    }
                }
                n => {
                    return Ok(n);
                }
            }
        }
    }
}

/// Sends a sequence of records and activates a timely operator for each.
fn send_records<I, Out, Err>(
    iter: I,
    tx: std::sync::mpsc::SyncSender<Result<Out, anyhow::Error>>,
    activator: Option<SyncActivator>,
) where
    I: IntoIterator<Item = Result<Out, Err>>,
    Err: Into<anyhow::Error>,
{
    let mut records = 0;
    for record in iter {
        records += 1;
        let record = record.map_err(Into::into);
        // TODO: each call to `send` allocates and performs some
        // atomic work; we could aim to batch up transmissions.
        if tx.send(record).is_err() {
            // The receiver went away, probably due to `DROP SOURCE`
            break;
        }
        // TODO: this is very spammy for the timely activator; it
        // appends an address to a list for each activation which
        // looks like it will be per-record in this case.
        if let Some(activator) = &activator {
            activator.activate().expect("activation failed");
        }
    }
    log::trace!("sent {} records to reader", records);
}
