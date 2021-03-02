// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::iter;
use std::str;

use differential_dataflow::{AsCollection, Collection};
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
) -> Collection<G, Row, Diff>
where
    G: Scope<Timestamp = Timestamp>,
{
    let name = String::from(name);
    let pact = SourceOutput::<Vec<u8>, Vec<u8>>::position_value_contract();
    let mut row_packer = repr::RowPacker::new();
    stream
        .unary(pact, "RegexDecode", |_cap, _op_info| {
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
                                let line_no = match line_no {
                                    Some(line_no) => line_no.to_string(),
                                    None => "unknown".into(),
                                };
                                let line_prefix = String::from_utf8_lossy(&line)
                                    .chars()
                                    .take(64)
                                    .collect::<String>();
                                warn!(
                                    "dropping line with invalid UTF-8 \
                                (source: {}, line number: {}, line prefix: {:?})",
                                    name, line_no, line_prefix,
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
        .as_collection()
}
