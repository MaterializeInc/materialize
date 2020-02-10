// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use expr::SourceInstanceId;
use futures::ready;
use futures::sink::SinkExt;
use futures::stream::{Fuse, Stream, StreamExt};
use log::{error, warn};
use notify::{RecursiveMode, Watcher};
use timely::dataflow::Scope;
use timely::scheduling::SyncActivator;
use tokio::fs::File;
use tokio::io::{self, AsyncRead};
use tokio::task;
use tokio_util::codec::{FramedRead, LinesCodec};

use dataflow_types::Timestamp;

use crate::source::util::source;
use crate::source::{SourceStatus, SourceToken};

#[derive(PartialEq, Eq)]
pub enum FileReadStyle {
    None,
    ReadOnce,
    TailFollowFd,
    // TODO: TailFollowName,
}

/// Wraps a Tokio file, producing a stream that is tailed forever.
///
/// This involves silently swallowing EOFs,
/// and waiting on a Notify handle for more data to be written.
struct ForeverTailedAsyncFile<S> {
    rx: Fuse<S>,
    inner: tokio::fs::File,
    // This field only exists to keep the watcher alive, if we're using a
    // watcher on this platform.
    _w: Option<notify::RecommendedWatcher>,
}

impl<S> ForeverTailedAsyncFile<S>
where
    S: Stream + Unpin,
{
    fn rx_pin(&mut self) -> Pin<&mut Fuse<S>> {
        Pin::new(&mut self.rx)
    }

    fn inner_pin(&mut self) -> Pin<&mut tokio::fs::File> {
        Pin::new(&mut self.inner)
    }
}

impl<S> AsyncRead for ForeverTailedAsyncFile<S>
where
    S: Stream + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<Result<usize, io::Error>> {
        loop {
            // First drain the buffer of pending events from notify.
            while let Poll::Ready(Some(_)) = self.rx_pin().poll_next(cx) {}
            // After draining all the events, try reading the file. If we
            // run out of data, sleep until `notify` wakes us up again.
            match ready!(self.inner_pin().poll_read(cx, buf))? {
                0 => {
                    if ready!(self.rx_pin().poll_next(cx)).is_some() {
                        // Notify thinks there might be new data. Go around
                        // the loop again to check.
                    } else {
                        error!("notify hung up while tailing file");
                        return Poll::Ready(Ok(0));
                    }
                }
                n => return Poll::Ready(Ok(n)),
            }
        }
    }
}

async fn send_lines<R>(
    reader: R,
    mut tx: futures::channel::mpsc::Sender<String>,
    activator: Arc<Mutex<SyncActivator>>,
) where
    R: AsyncRead + Unpin,
{
    let mut lines = FramedRead::new(reader, LinesCodec::new());
    while let Some(line) = lines.next().await {
        let line = match line {
            Ok(line) => line,
            Err(err) => {
                error!("file source: error while reading file: {}", err);
                return;
            }
        };
        if tx.send(line).await.is_err() {
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

async fn read_file_task(
    path: PathBuf,
    tx: futures::channel::mpsc::Sender<String>,
    activator: Arc<Mutex<SyncActivator>>,
    read_style: FileReadStyle,
) {
    let file = match File::open(&path).await {
        Ok(file) => file,
        Err(err) => {
            error!("file source: unable to open file: {}", err);
            return;
        }
    };
    match read_style {
        FileReadStyle::None => unreachable!(),
        FileReadStyle::ReadOnce => send_lines(file, tx, activator).await,
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
            #[cfg(target = "macos")]
            let (stream, watcher) = {
                let interval = tokio::time::interval(Duration::from_millis(100));
                (interval, None)
            };

            #[cfg(not(target = "macos"))]
            let (stream, watcher) = {
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
                let (async_tx, async_rx) = futures::channel::mpsc::unbounded();
                task::spawn_blocking(move || {
                    for msg in notice_rx {
                        if async_tx.unbounded_send(msg).is_err() {
                            break;
                        }
                    }
                });
                (async_rx, Some(w))
            };

            let file = ForeverTailedAsyncFile {
                rx: stream.fuse(),
                inner: file,
                _w: watcher,
            };
            send_lines(file, tx, activator).await
        }
    }
}

pub fn file<G>(
    id: SourceInstanceId,
    region: &G,
    name: String,
    path: PathBuf,
    executor: &tokio::runtime::Handle,
    read_style: FileReadStyle,
) -> (
    timely::dataflow::Stream<G, (Vec<u8>, Option<i64>)>,
    Option<SourceToken>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    const HEARTBEAT: Duration = Duration::from_secs(1); // Update the capability every second if there are no new changes.
    const MAX_LINES_PER_INVOCATION: usize = 1024;
    let n2 = name.clone();
    let read_file = read_style != FileReadStyle::None;
    let (stream, capability) = source(id, None, region, &name, move |info| {
        let activator = region.activator_for(&info.address[..]);
        let (tx, mut rx) = futures::channel::mpsc::channel(MAX_LINES_PER_INVOCATION);
        if read_file {
            let activator = Arc::new(Mutex::new(region.sync_activator_for(&info.address[..])));
            executor.spawn(read_file_task(path, tx, activator, read_style));
        }
        let mut total_lines_read = 0;
        move |cap, output| {
            // We need to make sure we always downgrade the capability.
            // Otherwise, the system will be stuck forever waiting for the timestamp
            // associated with the last-read batch of lines to close.
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
            // * Line read at 8, we ship it and downgrade to 9
            // * Line read at 15, we ship it and downgrade to 16
            // * Line read at 15, we ship it (at 16, since we can't go backwards) and reschedule for 1ms in the future
            // We wake up and see that it is 16. Regardless of whether we have lines to read, we will downgrade to 17.
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

            let mut lines_read = 0;

            let mut session = output.session(cap);
            while lines_read < MAX_LINES_PER_INVOCATION {
                if let Ok(line) = rx.try_next() {
                    lines_read += 1;
                    total_lines_read += 1;
                    match line {
                        Some(line) => session.give((line.into_bytes(), Some(total_lines_read))),
                        None => return SourceStatus::Done,
                    }
                } else {
                    break;
                }
            }
            if lines_read == MAX_LINES_PER_INVOCATION {
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
