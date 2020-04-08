// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(not(target_os = "macos"))]
use notify::{RecursiveMode, Watcher};

use expr::SourceInstanceId;
use log::{error, warn};
use timely::dataflow::Scope;
use timely::scheduling::SyncActivator;

use dataflow_types::Timestamp;

use crate::source::util::source;
use crate::source::{SourceStatus, SourceToken};
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
                    if let Ok(_) = self.rx.recv() {
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
    activator: Arc<Mutex<SyncActivator>>,
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
        activator
            .lock()
            .expect("activator lock poisoned")
            .activate()
            .expect("activation failed");
    }
}

fn read_file_task<Ctor, I, Out, Err>(
    path: PathBuf,
    tx: std::sync::mpsc::SyncSender<Out>,
    activator: Arc<Mutex<SyncActivator>>,
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

    match read_style {
        FileReadStyle::None => unreachable!(),
        FileReadStyle::ReadOnce => {
            let iter = match iter_ctor(Box::new(file)) {
                Ok(iter) => iter,
                Err(err) => {
                    error!("Failed to create read-once source: {}", err);
                    return;
                }
            };
            send_records(iter, tx, activator)
        }
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
                let timer = timer::MessageTimer::new(timer_tx);
                timer
                    .schedule_repeating(chrono::Duration::milliseconds(100), ())
                    .ignore();
                (timer_rx, timer)
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

            let iter = match iter_ctor(Box::new(file)) {
                Ok(iter) => iter,
                Err(err) => {
                    error!("Failed to create tailed file source: {}", err);
                    return;
                }
            };
            send_records(iter, tx, activator)
        }
    }
}

pub fn file<G, Ctor, I, Out, Err>(
    id: SourceInstanceId,
    region: &G,
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
    let n2 = name.clone();
    let read_file = read_style != FileReadStyle::None;
    let (stream, capability) = source(id, None, region, &name, move |info| {
        let activator = region.activator_for(&info.address[..]);
        let (tx, rx) = std::sync::mpsc::sync_channel(MAX_RECORDS_PER_INVOCATION);
        if read_file {
            let activator = Arc::new(Mutex::new(region.sync_activator_for(&info.address[..])));
            std::thread::spawn(|| read_file_task(path, tx, activator, read_style, iter_ctor));
        }
        let mut total_records_read = 0;
        move |cap, output| {
            // We need to make sure we always downgrade the capability.
            // Otherwise, the system will be stuck forever waiting for the timestamp
            // associated with the last-read batch of records to close.
            //
            // To do this, we normally downgrade to one millisecond past the current time.
            // However, if we were *already* 1ms past the current time, we don't want to
            // downgrade again, because if we keep repeating that logic,
            // we could get arbitrarily far ahead of the real system time. So, in that
            // special case, don't downgrade, but ask to be woken up again in 1ms
            // so we can downgrade then.
            //
            // If we were even further past the current time than that, then the system
            // clock has gone backwards; this is possible, especially if the user
            // manually changes his or her system clock, but for now just match Kafka behavior by
            // logging an error and shipping data at the capability timestamp.
            //
            // Example flow:
            // * Record read at 8, we ship it and downgrade to 9
            // * Record read at 15, we ship it and downgrade to 16
            // * Record read at 15, we ship it (at 16, since we can't go backwards) and reschedule for 1ms in the future
            // We wake up and see that it is 16. Regardless of whether we have records to read, we will downgrade to 17.
            let cap_time = *cap.time();
            let sys_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("System time seems to be before 1970.")
                .as_millis() as u64;
            // If nothing else causes us to wake up, do so after a specified amount of time.
            let mut next_activation_duration = HEARTBEAT;
            let next_time = if cap_time > sys_time {
                if cap_time != sys_time + 1 {
                    warn!(
                        "{}: fast-forwarding out-of-order Unix timestamp {}ms ({} -> {})",
                        n2,
                        cap_time - sys_time,
                        sys_time,
                        cap_time,
                    );
                }
                next_activation_duration = Duration::from_millis(cap_time - sys_time);
                cap_time
            } else {
                cap.downgrade(&sys_time);
                sys_time + 1
            };

            let mut records_read = 0;

            let mut session = output.session(cap);
            while records_read < MAX_RECORDS_PER_INVOCATION {
                match rx.try_recv() {
                    Ok(record) => {
                        records_read += 1;
                        total_records_read += 1;
                        session.give((record, Some(total_records_read)));
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return SourceStatus::Done,
                }
                // if let Ok(record) = rx.try_next() {
                //     records_read += 1;
                //     total_records_read += 1;
                //     match record {
                //         Some(record) => session.give((record, Some(total_records_read))),
                //         None => return SourceStatus::Done,
                //     }
                // } else {
                //     break;
                // }
            }
            if records_read == MAX_RECORDS_PER_INVOCATION {
                next_activation_duration = Default::default();
            }
            cap.downgrade(&next_time);
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
