// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::min;
use std::iter;
use std::str;

use log::warn;
use regex::Regex;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};

use repr::{Datum, Diff, Row, Timestamp};

use crate::source::SourceOutput;

pub fn regex<G>(
    stream: &Stream<G, SourceOutput<Vec<u8>, Vec<u8>>>,
    regex: Regex,
    name: &str,
) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    let name = String::from(name);
    let pact = SourceOutput::<Vec<u8>, Vec<u8>>::position_value_contract();
    let mut row_packer = repr::RowPacker::new();
    stream.unary(pact, "RegexDecode", |_cap, _op_info| {
        move |input, output| {
            input.for_each(|cap, lines| {
                let mut session = output.session(&cap);
                for SourceOutput {
                    key: _,
                    value: line,
                    position: line_no,
                    upstream_time_millis: _,
                } in &*lines
                {
                    let line = match str::from_utf8(&line) {
                        Ok(line) => line,
                        Err(_) => {
                            let line_len = min(line.len(), 1024);
                            warn!(
                                "Line {}{} from source {} cannot be decoded as utf8. \
                                Discarding line.",
                                if line_len == line.len() {
                                    ""
                                } else {
                                    "starting with: "
                                },
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

                    // Skip the 0th capture, which is the entire match, so that
                    // we only output the actual capture groups.
                    let datums = captures
                        .iter()
                        .skip(1)
                        .map(|c| Datum::from(c.map(|c| c.as_str())))
                        .chain(iter::once(Datum::from(*line_no)));

                    session.give((row_packer.pack(datums), *cap.time(), 1));
                }
            });
        }
    })
}
