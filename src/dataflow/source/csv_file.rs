// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::source::util::source;
use crate::source::SharedCapability;
use dataflow_types::{Diff, Timestamp};
use differential_dataflow::Hashable;
use futures::future::poll_fn;
use futures::{Async, Future, Stream};
use notify::{RawEvent, RecursiveMode, Watcher};
use repr::{Datum, Row};
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;
use timely::dataflow::Scope;
use timely::scheduling::SyncActivator;
use tokio::codec::{FramedRead, LinesCodec};
use tokio::io::{AsyncRead, Error};
use tokio_threadpool::blocking;

enum FileReaderMessage {
    Line(String),
    Eof,
}

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
    rx: Receiver<RawEvent>,
    inner: tokio::fs::File,
    // this field only exists to keep the watcher alive
    _w: notify::RecommendedWatcher,
}

impl std::io::Read for ForeverTailedAsyncFile {
    // First drain the buffer of pending events from notify.
    //
    // After draining all the events, try reading the file. If we run out of data,
    // sleep until `notify` wakes us up again.
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        loop {
            loop {
                match self.rx.try_recv() {
                    Ok(RawEvent { op: Ok(_), .. }) => continue,
                    Err(TryRecvError::Empty) => break,
                    Ok(RawEvent {
                        op: Err(notify::Error::Io(err)),
                        ..
                    }) => return Err(err),
                    Ok(RawEvent { op: Err(err), .. }) => {
                        return Err(std::io::Error::new(std::io::ErrorKind::Other, err))
                    }
                    Err(TryRecvError::Disconnected) => {
                        log::error!("Notify hung up while tailing file");
                        return Ok(0);
                    }
                }
            }
            let result = self.inner.read(buf);
            if let Ok(0) = result {
                // EOF from the underlying file. Wait for more data.
                let mut fut = poll_fn(|| {
                    blocking(|| self.rx.recv()).map_err(|_| panic!("Tokio threadpool shut down"))
                });
                match fut.poll() {
                    Ok(Async::NotReady) => break Err(std::io::ErrorKind::WouldBlock.into()),
                    Ok(Async::Ready(ev)) => match ev {
                        Ok(_) => continue,
                        Err(_) => {
                            log::error!("Notify hung up while tailing file");
                            break Ok(0);
                        }
                    },
                    Err(err) => break Err(err),
                };
            } else {
                break result;
            }
        }
    }
}

fn send_lines<T, S>(
    f: T,
    tx: Sender<FileReaderMessage>,
    activator: Arc<Mutex<SyncActivator>>,
) -> impl Future<Item = (), Error = ()>
where
    T: Future<Item = S, Error = std::io::Error>,
    S: AsyncRead,
{
    f.map(|f| FramedRead::new(f, LinesCodec::new()))
        .and_then(move |lines| {
            lines
                .map(|line| FileReaderMessage::Line(line))
                .chain(futures::stream::once(Ok(FileReaderMessage::Eof)))
                .for_each(move |msg| {
                    tx.send(msg)
                        .expect("Internal error - CSV line receiver hung up.");
                    activator
                        .lock()
                        .expect("Internal error (csv) - lock poisoned.")
                        .activate()
                        .unwrap();
                    Ok(())
                })
        })
        .map_err(|e| eprintln!("Error reading file: {}", e))
}

impl AsyncRead for ForeverTailedAsyncFile {}

fn read_file_task(
    path: PathBuf,
    tx: Sender<FileReaderMessage>,
    activator: Arc<Mutex<SyncActivator>>,
    read_style: FileReadStyle,
) -> Box<dyn Future<Item = (), Error = ()> + Send> {
    match read_style {
        FileReadStyle::None => unreachable!(),
        FileReadStyle::ReadOnce => {
            let f = tokio::fs::File::open(path);
            Box::new(send_lines(f, tx, activator))
        }
        FileReadStyle::TailFollowFd => {
            let (notice_tx, notice_rx) = std::sync::mpsc::channel();
            let mut w = match notify::raw_watcher(notice_tx) {
                Ok(w) => w,
                Err(e) => {
                    log::error!("Failed to create notify watcher: {:?}", e);
                    return Box::new(futures::future::ok(()));
                }
            };
            if let Err(e) = w.watch(&path, RecursiveMode::NonRecursive) {
                log::error!("Failed to add watch: {:?}", e);
                return Box::new(futures::future::ok(()));
            }
            let f = tokio::fs::File::open(path).map(|f| ForeverTailedAsyncFile {
                rx: notice_rx,
                inner: f,
                _w: w,
            });
            Box::new(send_lines(f, tx, activator))
        }
    }
}

pub fn csv<G, E>(
    region: &G,
    name: String,
    path: PathBuf,
    n_cols: usize,
    mut executor: E,
    read_style: FileReadStyle,
) -> (
    timely::dataflow::Stream<G, (Row, Timestamp, Diff)>,
    Option<SharedCapability>,
)
where
    G: Scope<Timestamp = Timestamp>,
    E: tokio::executor::Executor,
{
    let n2 = name.clone();
    let read_file = read_style != FileReadStyle::None;
    let (stream, capability) = source(region, &name, move |info| {
        let (tx, rx) = std::sync::mpsc::channel::<FileReaderMessage>();
        if read_file {
            let activator = Arc::new(Mutex::new(region.sync_activator_for(&info.address[..])));
            let task = read_file_task(path, tx, activator, read_style);
            executor.spawn(task).unwrap();
        } else {
            tx.send(FileReaderMessage::Eof)
                .expect("Internal error - CSV line receiver hung up.");
        }
        move |cap, output| {
            while let Ok(msg) = rx.try_recv() {
                match msg {
                    FileReaderMessage::Line(s) => {
                        let ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("System time seems to be before 1970.")
                            .as_millis() as u64;
                        if ms >= *cap.time() {
                            cap.downgrade(&ms)
                        } else {
                            let cur = *cap.time();
                            log::error!(
                                "{}: fast-forwarding out-of-order Unix timestamp {}ms ({} -> {})",
                                n2,
                                cur - ms,
                                ms,
                                cur,
                            );
                        };
                        output.session(cap).give(s)
                    }
                    FileReaderMessage::Eof => cap.downgrade(&std::u64::MAX),
                }
            }
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
                                log::error!(
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
