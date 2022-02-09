// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::fs::OpenOptions;
use std::rc::Rc;

use differential_dataflow::{Collection, Hashable};

use itertools::repeat_n;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::Scope;
use tracing::error;

use mz_dataflow_types::sinks::{AvroOcfSinkConnector, SinkDesc};
use mz_expr::GlobalId;
use mz_interchange::avro::{encode_datums_as_avro, AvroSchemaGenerator};
use mz_repr::{Diff, RelationDesc, Row, Timestamp};

use crate::render::sinks::SinkRender;

impl<G> SinkRender<G> for AvroOcfSinkConnector
where
    G: Scope<Timestamp = Timestamp>,
{
    fn uses_keys(&self) -> bool {
        false
    }

    fn get_key_indices(&self) -> Option<&[usize]> {
        None
    }

    fn get_relation_key_indices(&self) -> Option<&[usize]> {
        None
    }

    fn render_continuous_sink(
        &self,
        _compute_state: &mut crate::render::ComputeState,
        _sink: &SinkDesc,
        sink_id: GlobalId,
        sinked_collection: Collection<G, (Option<Row>, Option<Row>), Diff>,
    ) -> Option<Rc<dyn Any>>
    where
        G: Scope<Timestamp = Timestamp>,
    {
        avro_ocf(
            sinked_collection,
            sink_id,
            self.clone(),
            self.value_desc.clone(),
        );

        // no sink token
        None
    }
}

fn avro_ocf<G>(
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
        let schema_generator = AvroSchemaGenerator::new(None, None, None, desc, false);
        let schema = schema_generator.value_writer_schema().clone();
        let columns = schema_generator.value_columns().to_vec();
        (schema, columns)
    };

    let mut vector = vec![];
    let mut avro_writer = None;

    // We want exactly one worker to write to the single output file
    let hashed_id = id.hashed();

    collection.inner.sink(
        Exchange::new(move |_| hashed_id),
        &format!("avro-ocf-{}", id),
        move |input| {
            input.for_each(|_, rows| {
                rows.swap(&mut vector);

                let mut fallible = || -> Result<(), String> {
                    let avro_writer = match avro_writer.as_mut() {
                        Some(v) => v,
                        None => {
                            let file = OpenOptions::new()
                                .append(true)
                                .open(&connector.path)
                                .map_err(|e| {
                                    format!("creating avro ocf file writer for sink failed: {}", e)
                                })?;
                            avro_writer.get_or_insert(mz_avro::Writer::new(schema.clone(), file))
                        }
                    };

                    for (v, _time, diff) in vector.drain(..) {
                        let value = encode_datums_as_avro(v.iter(), &columns);
                        assert!(diff > 0, "can't sink negative multiplicities");
                        for value in repeat_n(value, diff as usize) {
                            avro_writer
                                .append(value)
                                .map_err(|e| format!("appending to avro ocf failed: {}", e))?;
                        }
                    }
                    avro_writer
                        .flush()
                        .map_err(|e| format!("flushing bytes to avro ocf failed: {}", e))?;
                    Ok(())
                };

                if let Err(e) = fallible() {
                    error!("{}", e);
                }
            })
        },
    )
}
