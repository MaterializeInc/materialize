// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use dataflow_types::{Diff, Timestamp};
use differential_dataflow::Hashable;
use futures::{Future, Stream};
use repr::{Datum, Row};
use std::borrow::Cow;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;
use timely::dataflow::Scope;
use tokio::codec::{FramedRead, LinesCodec};

enum FileReaderMessage {
    Line(String),
    Eof,
}

pub fn csv<G, E>(
    region: &G,
    name: String,
    path: PathBuf,
    n_cols: usize,
    mut executor: E,
    read_file: bool,
) -> timely::dataflow::Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
    E: tokio::executor::Executor,
{
    let stream = timely::dataflow::operators::generic::source(region, &name, move |cap, info| {
        let (tx, rx) = std::sync::mpsc::channel::<FileReaderMessage>();
        if read_file {
            let activator = Arc::new(Mutex::new(region.sync_activator_for(&info.address[..])));
            let task = tokio::fs::File::open(path)
                .map(|f| FramedRead::new(f, LinesCodec::new()))
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
                .map_err(|e| eprintln!("Error reading file: {}", e));
            executor.spawn(Box::new(task)).unwrap();
        } else {
            tx.send(FileReaderMessage::Eof)
                .expect("Internal error - CSV line receiver hung up.");
        }
        let mut cap = Some(cap);
        move |output| {
            while let Ok(msg) = rx.try_recv() {
                match msg {
                    FileReaderMessage::Line(s) => output.session(cap.as_ref().unwrap()).give(s),
                    FileReaderMessage::Eof => cap = None,
                }
            }
        }
    });
    stream.unary(
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
                                Row::pack(record.iter().map(|s| Datum::String(Cow::from(s)))),
                                *cap.time(),
                                1,
                            ));
                        }
                    }
                });
            }
        },
    )
}
