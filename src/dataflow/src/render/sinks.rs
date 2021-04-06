// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to the creation of dataflow sinks.

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use differential_dataflow::hashable::Hashable;
use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
use differential_dataflow::operators::Consolidate;
use differential_dataflow::AsCollection;
use timely::dataflow::operators::exchange::Exchange;
use timely::dataflow::operators::Map;
use timely::dataflow::scopes::Child;
use timely::dataflow::Scope;

use dataflow_types::*;
use expr::{GlobalId, MirRelationExpr};
use interchange::envelopes::{combine_at_timestamp, dbz_format, upsert_format};
use ore::cast::CastFrom;
use repr::adt::decimal::Significand;
use repr::{Datum, Row, Timestamp};

use crate::render::context::Context;
use crate::render::RenderState;
use crate::sink;

impl<'g, G> Context<Child<'g, G, G::Timestamp>, MirRelationExpr, Row, Timestamp>
where
    G: Scope<Timestamp = Timestamp>,
{
    /// Export the sink described by `sink` from the rendering context.
    pub(crate) fn export_sink(
        &mut self,
        render_state: &mut RenderState,
        import_ids: HashSet<GlobalId>,
        sink_id: GlobalId,
        sink: &SinkDesc,
    ) {
        // put together tokens that belong to the export
        let mut needed_source_tokens = Vec::new();
        let mut needed_additional_tokens = Vec::new();
        let mut needed_sink_tokens = Vec::new();
        for import_id in import_ids {
            if let Some(addls) = self.additional_tokens.get(&import_id) {
                needed_additional_tokens.extend_from_slice(addls);
            }
            if let Some(source_token) = self.source_tokens.get(&import_id) {
                needed_source_tokens.push(source_token.clone());
            }
        }
        let (collection, _err_collection) = self
            .collection(&MirRelationExpr::global_get(
                sink.from,
                sink.from_desc.typ().clone(),
            ))
            .expect("Sink source collection not loaded");

        // Some connectors support keys - extract them.
        let key_indices = sink
            .connector
            .get_key_indices()
            .map(|key_indices| key_indices.to_vec());
        let keyed = collection.map(move |row| {
            let key = key_indices.as_ref().map(|key_indices| {
                // TODO[perf] (btv) - is there a way to avoid unpacking and repacking every row and cloning the datums?
                // Does it matter?
                let datums = row.unpack();
                Row::pack(key_indices.iter().map(|&idx| datums[idx].clone()))
            });
            (key, row)
        });

        // Each partition needs to be handled by its own worker, so that we can write messages in order.
        // For now, we only support single-partition sinks.
        let keyed = keyed
            .inner
            .exchange(move |_| sink_id.hashed())
            .as_collection();

        // Apply the envelope.
        // * "Debezium" consolidates the stream, sorts it by time, and produces DiffPairs from it.
        //   It then renders those as Avro.
        // * Upsert" does the same, except at the last step, it renders the diff pair in upsert format.
        //   (As part of doing so, it asserts that there are not multiple conflicting values at the same timestamp)
        // * "Tail" writes some metadata.
        let collection = match sink.envelope {
            SinkEnvelope::Debezium => {
                let combined = combine_at_timestamp(keyed.arrange_by_key().stream);
                // This has to be an `Rc<RefCell<...>>` because the inner closure (passed to `Iterator::map`) references it, and it might outlive the outer closure.
                let rp = Rc::new(RefCell::new(Row::default()));
                let collection = combined.flat_map(move |(mut k, v)| {
                    let max_idx = v.len() - 1;
                    let rp = rp.clone();
                    v.into_iter().enumerate().map(move |(idx, dp)| {
                        let k = if idx == max_idx { k.take() } else { k.clone() };
                        (k, Some(dbz_format(&mut *rp.borrow_mut(), dp)))
                    })
                });
                collection
            }
            SinkEnvelope::Upsert => {
                let combined = combine_at_timestamp(keyed.arrange_by_key().stream);

                let collection = combined.map(|(k, v)| {
                    let v = upsert_format(v);
                    (k, v)
                });
                collection
            }
            SinkEnvelope::Tail { emit_progress } => keyed
                .consolidate()
                .inner
                .map({
                    let mut rp = Row::default();
                    move |((k, v), time, diff)| {
                        rp.push(Datum::Decimal(Significand::new(i128::from(time))));
                        if emit_progress {
                            rp.push(Datum::False);
                        }
                        rp.push(Datum::Int64(i64::cast_from(diff)));
                        rp.extend_by_row(&v);
                        let v = rp.finish_and_reuse();
                        ((k, Some(v)), time, 1)
                    }
                })
                .as_collection(),
        };

        // Some sinks require that the timestamp be appended to the end of the value.
        let append_timestamp = match &sink.connector {
            SinkConnector::Kafka(c) => c.consistency.is_some(),
            SinkConnector::Tail(_) => false,
            SinkConnector::AvroOcf(_) => false,
        };
        let collection = if append_timestamp {
            collection
                .inner
                .map(|((k, v), t, diff)| {
                    let v = v.map(|mut v| {
                        let t = t.to_string();
                        v.push_list_with(|rp| {
                            rp.push(Datum::String(&t));
                        });
                        v
                    });
                    ((k, v), t, diff)
                })
                .as_collection()
        } else {
            collection
        };

        // TODO(benesch): errors should stream out through the sink,
        // if we figure out a protocol for that.

        match sink.connector.clone() {
            SinkConnector::Kafka(c) => {
                let token = sink::kafka(
                    collection,
                    sink_id,
                    c,
                    sink.key_desc.clone(),
                    sink.value_desc.clone(),
                    sink.as_of.clone(),
                );
                needed_sink_tokens.push(token);
            }
            SinkConnector::Tail(c) => {
                let batches = collection
                    .map(move |(k, v)| {
                        assert!(k.is_none(), "tail does not support keys");
                        let v = v.expect("tail must have values");
                        (sink_id, v)
                    })
                    .arrange_by_key()
                    .stream;
                sink::tail(batches, sink_id, c, sink.as_of.clone());
            }
            SinkConnector::AvroOcf(c) => {
                sink::avro_ocf(collection, sink_id, c, sink.value_desc.clone());
            }
        };

        let tokens = Rc::new((
            needed_sink_tokens,
            needed_source_tokens,
            needed_additional_tokens,
        ));
        render_state
            .dataflow_tokens
            .insert(sink_id, Box::new(tokens));
    }
}
