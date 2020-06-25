// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs::OpenOptions;

use differential_dataflow::hashable::Hashable;
use log::error;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::{Scope, Stream};

use dataflow_types::{AvroOcfSinkConnector, Diff, Timestamp};
use expr::GlobalId;
use interchange::avro::{DiffPair, Encoder};
use repr::{RelationDesc, Row};

pub fn avro_ocf<G>(
    stream: &Stream<G, (Row, Timestamp, Diff)>,
    id: GlobalId,
    connector: AvroOcfSinkConnector,
    desc: RelationDesc,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let encoder = Encoder::new(desc);
    let schema = encoder.writer_schema();
    let sink_hash = id.hashed();

    let res = OpenOptions::new().append(true).open(&connector.path);
    let mut avro_writer = match res {
        Ok(f) => Some(avro::Writer::new(schema.clone(), f)),
        Err(e) => {
            error!("creating avro ocf file writer for sink failed: {}", e);
            None
        }
    };

    stream.sink(
        Exchange::new(move |_| sink_hash),
        &format!("avro-ocf-{}", id),
        move |input| {
            if avro_writer.is_none() {
                return;
            }

            let avro_writer = avro_writer.as_mut().expect("avro writer known to exist");

            input.for_each(|_, rows| {
                for (row, time, diff) in rows.iter() {
                    let should_emit = if connector.strict {
                        connector.frontier.less_than(&time)
                    } else {
                        connector.frontier.less_equal(&time)
                    };
                    if !should_emit {
                        continue;
                    }

                    let diff_pair = if *diff < 0 {
                        DiffPair {
                            before: Some(row),
                            after: None,
                        }
                    } else {
                        DiffPair {
                            before: None,
                            after: Some(row),
                        }
                    };
                    let value = encoder.diff_pair_to_avro(diff_pair);
                    for _ in 0..diff.abs() {
                        let res = avro_writer.append(value.clone());

                        match res {
                            Ok(_) => (),
                            Err(e) => error!("appending to avro ocf failed: {}", e),
                        }
                    }
                }

                let res = avro_writer.flush();
                match res {
                    Ok(_) => (),
                    Err(e) => error!("flushing bytes to avro ocf failed: {}", e),
                }
            })
        },
    )
}
