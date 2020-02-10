// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dataflow_types::{Diff, Timestamp};
use differential_dataflow::Hashable;
use log::warn;
use regex::Regex;
use repr::{Datum, Row};
use std::cmp::max;
use std::iter;
use std::str;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};

pub fn regex<G>(
    stream: &Stream<G, (Vec<u8>, Option<i64>)>,
    regex: Regex,
    name: &str,
) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    let name = String::from(name);
    stream.unary(
        Exchange::new(|x: &(Vec<u8>, _)| x.0.hashed()),
        "RegexDecode",
        |_, _| {
            move |input, output| {
                input.for_each(|cap, lines| {
                    let mut session = output.session(&cap);
                    for (line, line_no) in &*lines {
                        let line = match str::from_utf8(&line) {
                            Ok(line) => line,
                            _ => {
                                let line_len = max(line.len(), 1024);
                                warn!(
                                    "Line {}{} from source {} cannot be decoded as utf8. Discarding line.",
                                    if line_len == line.len() { "" } else {"starting with: "},
                                    String::from_utf8_lossy(&line[0..line_len]),
                                    name
                                );
                                continue;
                            }
                        };
                        let captures = match regex.captures(line) {
                            Some(captures) => captures,
                            None => continue,
                        };
                        session.give((
                            Row::pack(captures.iter().skip(1).map(
                                |m| Datum::from( m.map(
                                    |m| m.as_str())
                                )
                            ).chain(iter::once(line_no.map(Datum::Int64).into()))),
                            *cap.time(),
                            1,
                        ));
                    }
                });
            }
        },
    )
}
