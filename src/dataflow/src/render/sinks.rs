// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to the creation of dataflow sinks.

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
use differential_dataflow::{AsCollection, Collection, Hashable};
use timely::dataflow::operators::Map;
use timely::dataflow::scopes::Child;
use timely::dataflow::Scope;
use timely::progress::Antichain;

use dataflow_types::*;
use expr::GlobalId;
use interchange::envelopes::{combine_at_timestamp, dbz_format, upsert_format};
use ore::cast::CastFrom;
use repr::{Datum, Diff, Row, Timestamp};

use crate::render::context::Context;
use crate::render::{RelevantTokens, RenderState};
use crate::sink;

impl<'g, G> Context<Child<'g, G, G::Timestamp>, Row, Timestamp>
where
    G: Scope<Timestamp = Timestamp>,
{
    /// Export the sink described by `sink` from the rendering context.
    pub(crate) fn export_sink(
        &mut self,
        render_state: &mut RenderState,
        tokens: &mut RelevantTokens,
        import_ids: HashSet<GlobalId>,
        sink_id: GlobalId,
        sink: &SinkDesc,
    ) {
        // put together tokens that belong to the export
        let mut needed_source_tokens = Vec::new();
        let mut needed_additional_tokens = Vec::new();
        let mut needed_sink_tokens = Vec::new();
        for import_id in import_ids {
            if let Some(addls) = tokens.additional_tokens.get(&import_id) {
                needed_additional_tokens.extend_from_slice(addls);
            }
            if let Some(source_token) = tokens.source_tokens.get(&import_id) {
                needed_source_tokens.push(source_token.clone());
            }
        }

        let (collection, _err_collection) = self
            .lookup_id(expr::Id::Global(sink.from))
            .expect("Sink source collection not loaded")
            .as_collection();

        let collection = apply_sink_envelope(sink, collection);

        // TODO(benesch): errors should stream out through the sink,
        // if we figure out a protocol for that.

        let sink_token = match sink.connector.clone() {
            SinkConnector::Kafka(connector) => {
                render_kafka_sink(render_state, sink, sink_id, connector, collection)
            }
            SinkConnector::Tail(connector) => {
                render_tail_sink(render_state, sink, sink_id, connector, collection)
            }
            SinkConnector::AvroOcf(connector) => {
                render_avro_ocf_sink(render_state, sink, sink_id, connector, collection)
            }
        };

        if let Some(sink_token) = sink_token {
            needed_sink_tokens.push(sink_token);
        }

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

fn apply_sink_envelope<'a, G>(
    sink: &SinkDesc,
    collection: Collection<Child<'a, G, G::Timestamp>, Row, Diff>,
) -> Collection<Child<'a, G, G::Timestamp>, (Option<Row>, Option<Row>), Diff>
where
    G: Scope<Timestamp = Timestamp>,
{
    // Some connectors support keys - extract them.
    let keyed = match sink.connector.clone() {
        SinkConnector::Kafka(_) => {
            let user_key_indices = sink
                .connector
                .get_key_indices()
                .map(|key_indices| key_indices.to_vec());

            let relation_key_indices = sink
                .connector
                .get_relation_key_indices()
                .map(|key_indices| key_indices.to_vec());

            // We have three cases here, in descending priority:
            //
            // 1. if there is a user-specified key, use that to consolidate and
            //  distribute work
            // 2. if the sinked relation has a known primary key, use that to
            //  consolidate and distribute work but don't write to the sink
            // 3. if none of the above, use the whole row as key to
            //  consolidate and distribute work but don't write to the sink

            let keyed = if user_key_indices.is_some() {
                let key_indices = user_key_indices.expect("known to exist");
                collection.map(move |row| {
                    // TODO[perf] (btv) - is there a way to avoid unpacking and repacking every row and cloning the datums?
                    // Does it matter?
                    let datums = row.unpack();
                    let key = Row::pack(key_indices.iter().map(|&idx| datums[idx].clone()));
                    (Some(key), row)
                })
            } else if relation_key_indices.is_some() {
                let relation_key_indices = relation_key_indices.expect("known to exist");
                collection.map(move |row| {
                    // TODO[perf] (btv) - is there a way to avoid unpacking and repacking every row and cloning the datums?
                    // Does it matter?
                    let datums = row.unpack();
                    let key =
                        Row::pack(relation_key_indices.iter().map(|&idx| datums[idx].clone()));
                    (Some(key), row)
                })
            } else {
                collection.map(|row| {
                    (
                        Some(Row::pack(Some(Datum::Int64(row.hashed() as i64)))),
                        row,
                    )
                })
            };
            keyed
        }
        SinkConnector::Tail(_) | SinkConnector::AvroOcf(_) => collection.map(|row| (None, row)),
    };

    // Apply the envelope.
    // * "Debezium" consolidates the stream, sorts it by time, and produces DiffPairs from it.
    //   It then renders those as Avro.
    // * Upsert" does the same, except at the last step, it renders the diff pair in upsert format.
    //   (As part of doing so, it asserts that there are not multiple conflicting values at the same timestamp)
    // * "Tail" writes some metadata.
    let collection = match sink.envelope {
        Some(SinkEnvelope::Debezium) => {
            let combined = combine_at_timestamp(keyed.arrange_by_key().stream);

            // if there is no user-specified key, remove the synthetic
            // distribution key again
            let user_key_indices = sink.connector.get_key_indices();
            let combined = if user_key_indices.is_some() {
                combined
            } else {
                combined.map(|(_key, value)| (None, value))
            };

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
        Some(SinkEnvelope::Upsert) => {
            let combined = combine_at_timestamp(keyed.arrange_by_key().stream);

            let collection = combined.map(|(k, v)| {
                let v = upsert_format(v);
                (k, v)
            });
            collection
        }
        // No envelope, this can only happen for TAIL sinks, which work
        // on vanilla rows.
        None => keyed.map(|(key, value)| (key, Some(value))),
    };

    collection
}

fn render_kafka_sink<G>(
    render_state: &mut RenderState,
    sink: &SinkDesc,
    sink_id: GlobalId,
    connector: KafkaSinkConnector,
    sinked_collection: Collection<Child<G, G::Timestamp>, (Option<Row>, Option<Row>), Diff>,
) -> Option<Box<dyn Any>>
where
    G: Scope<Timestamp = Timestamp>,
{
    // consistent/exactly-once Kafka sinks need the timestamp in the row
    let sinked_collection = if connector.consistency.is_some() {
        sinked_collection
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
        sinked_collection
    };

    // Extract handles to the relevant source timestamp histories the sink
    // needs to hear from before it can write data out to Kafka.
    let mut source_ts_histories = Vec::new();

    for id in &connector.transitive_source_dependencies {
        if let Some(history) = render_state.ts_histories.get(id) {
            let mut history_bindings = history.clone();
            // We don't want these to block compaction
            // ever.
            history_bindings.set_compaction_frontier(Antichain::new().borrow());
            source_ts_histories.push(history_bindings);
        }
    }

    // TODO: this is a brittle way to indicate the worker that will write to the sink
    // because it relies on us continuing to hash on the sink_id, with the same hash
    // function, and for the Exchange pact to continue to distribute by modulo number
    // of workers.
    let peers = sinked_collection.inner.scope().peers();
    let worker_index = sinked_collection.inner.scope().index();
    let active_write_worker = (usize::cast_from(sink_id.hashed()) % peers) == worker_index;
    let shared_frontier = Rc::new(RefCell::new(Antichain::from_elem(0)));

    let token = sink::kafka(
        sinked_collection,
        sink_id,
        connector,
        sink.key_desc.clone(),
        sink.value_desc.clone(),
        sink.as_of.clone(),
        source_ts_histories,
        shared_frontier.clone(),
    );

    if active_write_worker {
        render_state
            .sink_write_frontiers
            .insert(sink_id, shared_frontier);
    }

    Some(token)
}

fn render_tail_sink<G>(
    _render_state: &mut RenderState,
    sink: &SinkDesc,
    sink_id: GlobalId,
    connector: TailSinkConnector,
    sinked_collection: Collection<Child<G, G::Timestamp>, (Option<Row>, Option<Row>), Diff>,
) -> Option<Box<dyn Any>>
where
    G: Scope<Timestamp = Timestamp>,
{
    sink::tail(sinked_collection, sink_id, connector, sink.as_of.clone());

    // no sink token
    None
}

fn render_avro_ocf_sink<G>(
    _render_state: &mut RenderState,
    sink: &SinkDesc,
    sink_id: GlobalId,
    connector: AvroOcfSinkConnector,
    sinked_collection: Collection<Child<G, G::Timestamp>, (Option<Row>, Option<Row>), Diff>,
) -> Option<Box<dyn Any>>
where
    G: Scope<Timestamp = Timestamp>,
{
    sink::avro_ocf(
        sinked_collection,
        sink_id,
        connector,
        sink.value_desc.clone(),
    );

    // no sink token
    None
}
