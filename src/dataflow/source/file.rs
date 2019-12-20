// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::ready;
use futures::sink::SinkExt;
use futures::stream::{Fuse, Stream, StreamExt};
use log::error;
use notify::{RawEvent, RecursiveMode, Watcher};
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
struct ForeverTailedAsyncFile {
    rx: Fuse<futures::channel::mpsc::UnboundedReceiver<RawEvent>>,
    inner: tokio::fs::File,
    // this field only exists to keep the watcher alive
    _w: notify::RecommendedWatcher,
}

impl ForeverTailedAsyncFile {
    fn check_notify_event(event: RawEvent) -> Result<(), io::Error> {
        match event {
            RawEvent { op: Ok(_), .. } => Ok(()),
            RawEvent {
                op: Err(notify::Error::Io(err)),
                ..
            } => Err(err),
            RawEvent { op: Err(err), .. } => Err(io::Error::new(io::ErrorKind::Other, err)),
        }
    }

    fn rx_pin(&mut self) -> Pin<&mut Fuse<futures::channel::mpsc::UnboundedReceiver<RawEvent>>> {
        Pin::new(&mut self.rx)
    }

    fn inner_pin(&mut self) -> Pin<&mut tokio::fs::File> {
        Pin::new(&mut self.inner)
    }
}

impl AsyncRead for ForeverTailedAsyncFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<Result<usize, io::Error>> {
        loop {
            // First drain the buffer of pending events from notify.
            while let Poll::Ready(Some(event)) = self.rx_pin().poll_next(cx) {
                Self::check_notify_event(event)?;
            }
            // After draining all the events, try reading the file. If we
            // run out of data, sleep until `notify` wakes us up again.
            match ready!(self.inner_pin().poll_read(cx, buf))? {
                0 => match ready!(self.rx_pin().poll_next(cx)) {
                    Some(event) => Self::check_notify_event(event)?,
                    None => {
                        error!("notify hung up while tailing file");
                        return Poll::Ready(Ok(0));
                    }
                },
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
        tx.send(line).await.expect("csv line receiver hung up");
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
            let (notice_tx, notice_rx) = std::sync::mpsc::channel();
            let mut w = match notify::raw_watcher(notice_tx) {
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
            let file = ForeverTailedAsyncFile {
                rx: async_rx.fuse(),
                inner: file,
                _w: w,
            };
            send_lines(file, tx, activator).await
        }
    }
}

pub fn file<G>(
    region: &G,
    name: String,
    path: PathBuf,
    executor: &tokio::runtime::Handle,
    read_style: FileReadStyle,
) -> (timely::dataflow::Stream<G, Vec<u8>>, Option<SourceToken>)
where
    G: Scope<Timestamp = Timestamp>,
{
    const MAX_LINES_PER_INVOCATION: usize = 1024;
    let n2 = name.clone();
    let read_file = read_style != FileReadStyle::None;
    let (stream, capability) = source(region, &name, move |info| {
        let activator = region.activator_for(&info.address[..]);
        let (tx, mut rx) = futures::channel::mpsc::channel(MAX_LINES_PER_INVOCATION);
        if read_file {
            let activator = Arc::new(Mutex::new(region.sync_activator_for(&info.address[..])));
            executor.spawn(read_file_task(path, tx, activator, read_style));
        }
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
            let next_time = if cap_time > sys_time {
                if cap_time != sys_time + 1 {
                    error!(
                        "{}: fast-forwarding out-of-order Unix timestamp {}ms ({} -> {})",
                        n2,
                        cap_time - sys_time,
                        sys_time,
                        cap_time,
                    );
                }
                activator.activate_after(Duration::from_millis(cap_time - sys_time));
                cap_time
            } else {
                cap.downgrade(&sys_time);
                sys_time + 1
            };

            let mut lines_read = 0;

            let mut session = output.session(cap);
            while lines_read < MAX_LINES_PER_INVOCATION {
                if let Ok(line) = rx.try_next() {
                    match line {
                        Some(line) => session.give(line.into_bytes()),
                        None => return SourceStatus::Done,
                    }
                    lines_read += 1;
                } else {
                    break;
                }
            }
            if lines_read == MAX_LINES_PER_INVOCATION {
                activator.activate();
            }
            cap.downgrade(&next_time);
            SourceStatus::Alive
        }
    });

    if read_file {
        (stream, Some(capability))
    } else {
        (stream, None)
    }
}
