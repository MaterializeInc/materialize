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
    EOF,
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
            let ac2 = activator.clone();
            let tx2 = tx.clone();
            let task = tokio::fs::File::open(path)
                .map(|f| FramedRead::new(f, LinesCodec::new()))
                .and_then(move |lines| {
                    let tx2 = tx2.clone();
                    lines.for_each(move |line| {
                        tx2.send(FileReaderMessage::Line(line))
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
                .and_then(move |_| {
                    tx.send(FileReaderMessage::EOF).unwrap();
                    ac2.lock()
                        .expect("Internal error (csv) - lock poisoned.")
                        .activate()
                        .unwrap();
                    Ok(())
                });
            executor.spawn(Box::new(task)).unwrap();
        } else {
            tx.send(FileReaderMessage::EOF)
                .expect("Internal error - CSV line receiver hung up.");
        }
        let mut cap = Some(cap);
        move |output| {
            while let Ok(msg) = rx.try_recv() {
                match msg {
                    FileReaderMessage::Line(s) => output.session(cap.as_ref().unwrap()).give(s),
                    // The below should be:
                    // FileReaderMessage::EOF => cap = None
                    // but that causes peeks to hang forever,
                    // presumably due to other bugs.
                    FileReaderMessage::EOF => cap.as_mut().unwrap().downgrade(&std::u64::MAX),
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
                    // but the CvsReader *itself* searches for line breaks.
                    // This is mainly an aesthetic/performance-golfing
                    // issue as I doubt it will ever be a bottleneck.
                    for line in &*lines {
                        let mut csv_reader = csv::ReaderBuilder::new()
                            .has_headers(false)
                            .from_reader(stringreader::StringReader::new(line));
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
