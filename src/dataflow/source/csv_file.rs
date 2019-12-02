// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use differential_dataflow::Hashable;
use futures::ready;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use log::error;
use notify::{RawEvent, RecursiveMode, Watcher};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;
use timely::dataflow::Scope;
use timely::scheduling::SyncActivator;
use tokio::fs::File;
use tokio::io::{self, AsyncRead};
use tokio::task;
use tokio_util::codec::{FramedRead, LinesCodec};

use dataflow_types::{Diff, Timestamp};
use repr::{Datum, Row};

use crate::source::util::source;
use crate::source::{SharedCapability, SourceStatus};

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
    rx: std::sync::mpsc::Receiver<RawEvent>,
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
}

impl AsyncRead for ForeverTailedAsyncFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<Result<usize, io::Error>> {
        loop {
            // First drain the buffer of pending events from notify.
            while let Ok(event) = self.rx.try_recv() {
                Self::check_notify_event(event)?;
            }
            // After draining all the events, try reading the file. If we
            // run out of data, sleep until `notify` wakes us up again.
            match ready!(Pin::new(&mut self.inner).poll_read(cx, buf))? {
                0 => match task::block_in_place(|| self.rx.recv()) {
                    Ok(event) => Self::check_notify_event(event)?,
                    Err(_) => {
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
    mut tx: futures::channel::mpsc::UnboundedSender<String>,
    activator: Arc<Mutex<SyncActivator>>,
) where
    R: AsyncRead + Unpin,
{
    let mut lines = FramedRead::new(reader, LinesCodec::new());
    while let Some(line) = lines.next().await {
        let line = match line {
            Ok(line) => line,
            Err(err) => {
                error!("csv source: error while reading file: {}", err);
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
    tx: futures::channel::mpsc::UnboundedSender<String>,
    activator: Arc<Mutex<SyncActivator>>,
    read_style: FileReadStyle,
) {
    let file = match File::open(&path).await {
        Ok(file) => file,
        Err(err) => {
            error!("csv source: unable to open file: {}", err);
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
                    error!("csv source: failed to create notify watcher: {}", err);
                    return;
                }
            };
            if let Err(err) = w.watch(&path, RecursiveMode::NonRecursive) {
                error!("csv source: failed to add watch: {}", err);
                return;
            }
            let file = ForeverTailedAsyncFile {
                rx: notice_rx,
                inner: file,
                _w: w,
            };
            send_lines(file, tx, activator).await
        }
    }
}

pub fn csv<G>(
    region: &G,
    name: String,
    path: PathBuf,
    n_cols: usize,
    executor: &tokio::runtime::Handle,
    read_style: FileReadStyle,
) -> (
    timely::dataflow::Stream<G, (Row, Timestamp, Diff)>,
    Option<SharedCapability>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    let n2 = name.clone();
    let read_file = read_style != FileReadStyle::None;
    let (stream, capability) = source(region, &name, move |info| {
        let (tx, mut rx) = futures::channel::mpsc::unbounded();
        if read_file {
            let activator = Arc::new(Mutex::new(region.sync_activator_for(&info.address[..])));
            executor.spawn(read_file_task(path, tx, activator, read_style));
        }
        move |cap, output| {
            while let Ok(line) = rx.try_next() {
                match line {
                    Some(line) => {
                        let ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("System time seems to be before 1970.")
                            .as_millis() as u64;
                        if ms >= *cap.time() {
                            cap.downgrade(&ms)
                        } else {
                            let cur = *cap.time();
                            error!(
                                "{}: fast-forwarding out-of-order Unix timestamp {}ms ({} -> {})",
                                n2,
                                cur - ms,
                                ms,
                                cur,
                            );
                        };
                        output.session(cap).give(line);
                    }
                    None => return SourceStatus::Done,
                }
            }
            SourceStatus::ScheduleAgain
        }
    });
    let stream = stream.unary(
        Exchange::new(|x: &String| x.hashed()),
        "CvsDecode",
        |_, _| {
            move |input, output| {
                input.for_each(|cap, lines| {
                    let mut session = output.session(&cap);
                    // TODO: There is extra work going on here:
                    // LinesCodec is already splitting our input into lines,
                    // but the CsvReader *itself* searches for line breaks.
                    // This is mainly an aesthetic/performance-golfing
                    // issue as I doubt it will ever be a bottleneck.
                    for line in &*lines {
                        let mut csv_reader = csv::ReaderBuilder::new()
                            .has_headers(false)
                            .from_reader(line.as_bytes());
                        for result in csv_reader.records() {
                            let record = result.unwrap();
                            if record.len() != n_cols {
                                error!(
                                    "CSV error: expected {} columns, got {}. Ignoring row.",
                                    n_cols,
                                    record.len()
                                );
                                continue;
                            }
                            session.give((
                                Row::pack(record.iter().map(|s| Datum::String(s))),
                                *cap.time(),
                                1,
                            ));
                        }
                    }
                });
            }
        },
    );

    if read_file {
        (stream, Some(capability))
    } else {
        (stream, None)
    }
}
