// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fmt::Display;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(not(target_os = "macos"))]
use notify::{RecursiveMode, Watcher};

use dataflow_types::{ExternalSourceConnector, FileSourceConnector, Timestamp};
use expr::{PartitionId, SourceInstanceId};
use log::error;
use timely::dataflow::operators::Capability;
use timely::dataflow::Scope;
use timely::scheduling::SyncActivator;

use crate::server::TimestampHistories;
use crate::source::util::source;
use crate::source::{SourceConfig, SourceStatus, SourceToken};
use std::io::Read;
use std::sync::mpsc::TryRecvError;

#[derive(PartialEq, Eq)]
pub enum FileReadStyle {
    None,
    ReadOnce,
    TailFollowFd,
    // TODO: TailFollowName,
}

/// Wraps a file, producing a stream that is tailed forever.
///
/// This involves silently swallowing EOFs,
/// and waiting on a Notify handle for more data to be written.
struct ForeverTailedFile<Ev, Handle> {
    rx: std::sync::mpsc::Receiver<Ev>,
    inner: std::fs::File,
    // This field only exists to keep the file watcher or timer
    // alive
    _h: Handle,
}

impl<Ev, H> Read for ForeverTailedFile<Ev, H> {
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

fn send_records<I, Out, Err>(
    iter: I,
    tx: std::sync::mpsc::SyncSender<Out>,
    activator: Option<Arc<Mutex<SyncActivator>>>,
) where
    I: IntoIterator<Item = Result<Out, Err>>,
    Err: Display,
{
    for record in iter {
        let record = match record {
            Ok(record) => record,
            Err(err) => {
                error!("file source: error while reading file: {}", err);
                return;
            }
        };
        if tx.send(record).is_err() {
            // The receiver went away, probably due to `DROP SOURCE`
            break;
        }
        if let Some(activator) = &activator {
            activator
                .lock()
                .expect("activator lock poisoned")
                .activate()
                .expect("activation failed");
        }
    }
}

pub fn read_file_task<Ctor, I, Out, Err>(
    path: PathBuf,
    tx: std::sync::mpsc::SyncSender<Out>,
    activator: Option<Arc<Mutex<SyncActivator>>>,
    read_style: FileReadStyle,
    iter_ctor: Ctor,
) where
    I: IntoIterator<Item = Result<Out, Err>> + Send + 'static,
    Ctor: FnOnce(Box<dyn Read + Send>) -> Result<I, Err>,
    Err: Display,
{
    let file = match std::fs::File::open(&path) {
        Ok(file) => file,
        Err(err) => {
            error!(
                "file source: unable to open file at path {}. Error: {}",
                path.to_string_lossy(),
                err
            );
            return;
        }
    };

    let iter = match read_style {
        FileReadStyle::None => unreachable!(),
        FileReadStyle::ReadOnce => iter_ctor(Box::new(file)),
        FileReadStyle::TailFollowFd => {
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
            #[cfg(target_os = "macos")]
            let (file_events_stream, handle) = {
                let (timer_tx, timer_rx) = std::sync::mpsc::channel();
                std::thread::spawn(move || {
                    while let Ok(()) = timer_tx.send(()) {
                        std::thread::sleep(Duration::from_millis(100));
                    }
                });
                (timer_rx, ())
            };

            #[cfg(not(target_os = "macos"))]
            let (file_events_stream, handle) = {
                let (notice_tx, notice_rx) = std::sync::mpsc::channel();
                let mut w = match notify::RecommendedWatcher::new_raw(notice_tx) {
                    Ok(w) => w,
                    Err(err) => {
                        error!("file source: failed to create notify watcher: {}", err);
                        return;
                    }
                };
                if let Err(err) = w.watch(&path, RecursiveMode::NonRecursive) {
                    error!("file source: failed to add watch: {}", err);
                    return;
                }
                (notice_rx, w)
            };

            let file = ForeverTailedFile {
                rx: file_events_stream,
                inner: file,
                _h: handle,
            };

            iter_ctor(Box::new(file))
        }
    };

    match iter {
        Ok(i) => send_records(i, tx, activator),
        Err(e) => error!("Failed to obtain records from file. Error {}", e),
    };
}

/// Timestamp history map is of format [pid1: (ts1, offset1), (ts2, offset2), pid2: (ts1, offset)...].
/// For a given partition pid, messages in interval [0,offset1] get assigned ts1, all messages in interval [offset1+1,offset2]
/// get assigned ts2, etc.
/// When receive message with offset1, it is safe to downgrade the capability to the next
/// timestamp, which is either
/// 1) the timestamp associated with the next highest offset if it exists
/// 2) max(timestamp, offset1) + 1. The timestamp_history map can contain multiple timestamps for
/// the same offset. We pick the greatest one + 1
/// (the next message we generate will necessarily have timestamp timestamp + 1)
///
/// This method assumes that timestamps are inserted in increasing order in the hashmap
/// (even across partitions). This means that once we see a timestamp with ts x, no entry with
/// ts (x-1) will ever be inserted. Entries with timestamp x might still be inserted in different
/// partitions
fn downgrade_capability(
    id: &SourceInstanceId,
    cap: &mut Capability<Timestamp>,
    last_processed_offset: &mut i64,
    last_closed_ts: &mut u64,
    timestamp_histories: &TimestampHistories,
) {
    let mut changed = false;
    match timestamp_histories.borrow_mut().get_mut(id) {
        None => {}
        Some(entries) => {
            // Files do not have partitions. There should never be more than
            // one entry here
            for entries in entries.values_mut() {
                // Check whether timestamps can be closed on this partition
                while let Some((_, ts, offset)) = entries.first() {
                    if *last_processed_offset == *offset {
                        // We have now seen all messages corresponding to this timestamp.  We
                        // can close the timestamp and remove the associated metadata.
                        *last_closed_ts = *ts;
                        entries.remove(0);
                        changed = true;
                    } else {
                        // Offset isn't at a timestamp boundary, we take no action
                        break;
                    }
                }
            }
        }
    }
    // Downgrade capability to new minimum open timestamp (which corresponds to last_closed_ts + 1).
    if changed && (*last_closed_ts > 0) {
        cap.downgrade(&(*last_closed_ts + 1));
    }
}

/// For a given offset, returns an option type returning the matching timestamp or None
/// if no timestamp can be assigned. The timestamp history contains a sequence of
/// (timestamp, offset) tuples. A message with offset x will be assigned the first timestamp
/// for which offset>=x.
fn find_matching_timestamp(
    id: &SourceInstanceId,
    offset: i64,
    timestamp_histories: &TimestampHistories,
) -> Option<Timestamp> {
    match timestamp_histories.borrow().get(id) {
        None => None,
        Some(entries) => match entries.get(&PartitionId::File) {
            Some(entries) => {
                for (_, ts, max_offset) in entries {
                    if offset <= *max_offset {
                        return Some(ts.clone());
                    }
                }
                None
            }
            None => None,
        },
    }
}

pub fn file<G, Ctor, I, Out, Err>(
    source_config: SourceConfig<G>,
    name: String,
    path: PathBuf,
    read_style: FileReadStyle,
    iter_ctor: Ctor,
) -> (
    timely::dataflow::Stream<G, (Out, Option<i64>)>,
    Option<SourceToken>,
)
where
    G: Scope<Timestamp = Timestamp>,
    I: IntoIterator<Item = Result<Out, Err>> + Send + 'static,
    Ctor: FnOnce(Box<dyn Read + Send>) -> Result<I, Err> + Send + 'static,
    Err: Display + Send + 'static,
    Out: Send + Clone + 'static,
{
    const HEARTBEAT: Duration = Duration::from_secs(1); // Update the capability every second if there are no new changes.
    const MAX_RECORDS_PER_INVOCATION: usize = 1024;

    let (id, region, timestamp_histories, timestamp_tx, consistency) = source_config.into_parts();

    let read_file = read_style != FileReadStyle::None;
    let ts = if read_file {
        let prev = timestamp_histories
            .borrow_mut()
            .insert(id.clone(), HashMap::new());
        assert!(prev.is_none());
        timestamp_tx.as_ref().borrow_mut().push((
            id,
            Some((
                ExternalSourceConnector::File(FileSourceConnector {
                    path: path.clone(),
                    tail: read_style == FileReadStyle::TailFollowFd,
                }),
                consistency,
            )),
        ));
        Some(timestamp_tx)
    } else {
        None
    };

    // Buffer placeholder for buffering messages for which we did not have a timestamp
    let mut buffer: Option<Out> = None;
    // Index of the last offset that we have already processed (and assigned a timestamp to)
    let mut last_processed_offset: i64 = 0;
    // Index of the current message's offset
    let mut current_msg_offset: i64 = 0;
    // Records closed timestamps. It corresponds to the smallest timestamp that is still
    // open
    let mut last_closed_ts: u64 = 0;

    let (stream, capability) = source(id, ts, region, &name, move |info| {
        let activator = region.activator_for(&info.address[..]);
        let (tx, rx) = std::sync::mpsc::sync_channel(MAX_RECORDS_PER_INVOCATION);
        if read_file {
            let activator = Arc::new(Mutex::new(region.sync_activator_for(&info.address[..])));
            std::thread::spawn(|| read_file_task(path, tx, Some(activator), read_style, iter_ctor));
        }
        move |cap, output| {
            // If nothing else causes us to wake up, do so after a specified amount of time.
            let mut next_activation_duration = HEARTBEAT;
            // Number of records read for this particular activation
            let mut records_read = 0;

            if read_file {
                // Check if the capability can be downgraded (this is independent of whether
                // there are new messages that can be processed) as timestamps can become
                // closed in the absence of messages
                downgrade_capability(
                    &id,
                    cap,
                    &mut last_processed_offset,
                    &mut last_closed_ts,
                    &timestamp_histories,
                );

                // Check if there was a message buffered. If yes, use this message. Else,
                // attempt to process the next message
                if buffer.is_none() {
                    match rx.try_recv() {
                        Ok(record) => {
                            records_read += 1;
                            current_msg_offset += 1;
                            buffer = Some(record);
                        }
                        Err(TryRecvError::Empty) => {
                            buffer = None;
                        }
                        Err(TryRecvError::Disconnected) => {
                            return SourceStatus::Done;
                        }
                    }
                }

                while let Some(message) = buffer.take() {
                    let ts = find_matching_timestamp(&id, current_msg_offset, &timestamp_histories);
                    match ts {
                        None => {
                            // We have not yet decided on a timestamp for this message,
                            // we need to buffer the message
                            buffer = Some(message);
                            activator.activate();
                            return SourceStatus::Alive;
                        }
                        Some(_) => {
                            last_processed_offset = current_msg_offset;
                            output
                                .session(&cap)
                                .give((message, Some(last_processed_offset)));

                            downgrade_capability(
                                &id,
                                cap,
                                &mut last_processed_offset,
                                &mut last_closed_ts,
                                &timestamp_histories,
                            );
                        }
                    }

                    if records_read == MAX_RECORDS_PER_INVOCATION {
                        next_activation_duration = Default::default();
                        break;
                    }

                    buffer = match rx.try_recv() {
                        Ok(record) => {
                            records_read += 1;
                            current_msg_offset += 1;
                            Some(record)
                        }
                        Err(TryRecvError::Empty) => None,
                        Err(TryRecvError::Disconnected) => return SourceStatus::Done,
                    }
                }
            }
            activator.activate_after(next_activation_duration);
            SourceStatus::Alive
        }
    });

    if read_file {
        (stream, Some(capability))
    } else {
        (stream, None)
    }
}
