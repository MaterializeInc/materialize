// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs::OpenOptions;

use differential_dataflow::Collection;

use itertools::repeat_n;
use log::error;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::Scope;

use dataflow_types::AvroOcfSinkConnector;
use expr::GlobalId;
use interchange::avro::{encode_datums_as_avro, Encoder};
use mz_avro::{self};
use repr::{RelationDesc, Row, Timestamp};

pub fn avro_ocf<G>(
    collection: Collection<G, (Option<Row>, Option<Row>)>,
    id: GlobalId,
    connector: AvroOcfSinkConnector,
    desc: RelationDesc,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let collection = collection.map(|(k, v)| {
        assert!(k.is_none(), "Avro OCF sinks must not have keys");
        let v = v.expect("Avro OCF sinks must have values");
        v
    });
    let (schema, columns) = {
        let encoder = Encoder::new(None, desc, false);
        let schema = encoder.value_writer_schema().clone();
        let columns = encoder.value_columns().to_vec();
        (schema, columns)
    };

    let res = OpenOptions::new().append(true).open(&connector.path);
    let mut avro_writer = match res {
        Ok(f) => Some(mz_avro::Writer::new(schema, f)),
        Err(e) => {
            error!("creating avro ocf file writer for sink failed: {}", e);
            None
        }
    };

    let mut vector = vec![];

    collection
        .inner
        .sink(Pipeline, &format!("avro-ocf-{}", id), move |input| {
            let avro_writer = match avro_writer.as_mut() {
                Some(avro_writer) => avro_writer,
                None => return,
            };
            input.for_each(|_, rows| {
                rows.swap(&mut vector);

                for (v, _time, diff) in vector.drain(..) {
                    let value = encode_datums_as_avro(v.iter(), &columns);
                    assert!(diff > 0, "can't sink negative multiplicities");
                    for value in repeat_n(value, diff as usize) {
                        if let Err(e) = avro_writer.append(value) {
                            error!("appending to avro ocf failed: {}", e)
                        };
                    }
                }
                let res = avro_writer.flush();
                match res {
                    Ok(_) => (),
                    Err(e) => error!("flushing bytes to avro ocf failed: {}", e),
                }
            })
        })
}
