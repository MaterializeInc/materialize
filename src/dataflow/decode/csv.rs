use dataflow_types::{Diff, Timestamp};
use differential_dataflow::Hashable;
use log::error;
use repr::{Datum, Row};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};

pub fn csv<G>(stream: &Stream<G, Vec<u8>>, n_cols: usize) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    stream.unary(
        Exchange::new(|x: &Vec<u8>| x.hashed()),
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
                            .from_reader(line.as_slice());
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
    )
}
