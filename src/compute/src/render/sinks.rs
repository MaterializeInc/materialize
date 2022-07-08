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
use std::collections::BTreeSet;
use std::rc::Rc;

use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
use differential_dataflow::{Collection, Hashable};
use timely::dataflow::Scope;

use mz_expr::{permutation_for_arrangement, MapFilterProject};
use mz_interchange::envelopes::{combine_at_timestamp, dbz_format, upsert_format};
use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};
use mz_storage::controller::CollectionMetadata;
use mz_storage::types::errors::DataflowError;
use mz_storage::types::sinks::{SinkConnection, SinkDesc, SinkEnvelope};

use crate::compute_state::SinkToken;
use crate::render::context::Context;

impl<G> Context<G, Row, Timestamp>
where
    G: Scope<Timestamp = Timestamp>,
{
    /// Export the sink described by `sink` from the rendering context.
    pub(crate) fn export_sink(
        &mut self,
        compute_state: &mut crate::compute_state::ComputeState,
        tokens: &mut std::collections::BTreeMap<GlobalId, Rc<dyn std::any::Any>>,
        import_ids: BTreeSet<GlobalId>,
        sink_id: GlobalId,
        sink: &SinkDesc<CollectionMetadata>,
    ) {
        let sink_render = get_sink_render_for(&sink.connection);

        // put together tokens that belong to the export
        let mut needed_tokens = Vec::new();
        for import_id in import_ids {
            if let Some(token) = tokens.get(&import_id) {
                needed_tokens.push(Rc::clone(&token))
            }
        }

        // TODO[btv] - We should determine the key and permutation to use during planning,
        // rather than at runtime.
        //
        // This is basically an inlined version of the old `as_collection`.
        let bundle = self
            .lookup_id(mz_expr::Id::Global(sink.from))
            .expect("Sink source collection not loaded");
        let (ok_collection, err_collection) = if let Some(collection) = &bundle.collection {
            collection.clone()
        } else {
            let (key, _arrangement) = bundle
                .arranged
                .iter()
                .next()
                .expect("Invariant violated: at least one collection must be present.");
            let unthinned_arity = sink.from_desc.arity();
            let (permutation, thinning) = permutation_for_arrangement(&key, unthinned_arity);
            let mut mfp = MapFilterProject::new(unthinned_arity);
            mfp.permute(permutation, thinning.len() + key.len());
            bundle.as_collection_core(mfp, Some((key.clone(), None)))
        };

        // TODO(teskje): Remove envelope-wrapping once the Kafka sink has been
        // moved to STORAGE.
        let ok_collection = apply_sink_envelope(sink, &sink_render, ok_collection);

        let sink_token = sink_render.render_continuous_sink(
            compute_state,
            sink,
            sink_id,
            ok_collection,
            err_collection,
        );

        if let Some(sink_token) = sink_token {
            needed_tokens.push(sink_token);
        }

        compute_state.sink_tokens.insert(
            sink_id,
            SinkToken {
                token: Box::new(needed_tokens),
                is_tail: matches!(sink.connection, SinkConnection::Tail(_)),
            },
        );
    }
}

#[allow(clippy::borrowed_box)]
fn apply_sink_envelope<G>(
    sink: &SinkDesc<CollectionMetadata>,
    sink_render: &Box<dyn SinkRender<G>>,
    collection: Collection<G, Row, Diff>,
) -> Collection<G, (Option<Row>, Option<Row>), Diff>
where
    G: Scope<Timestamp = Timestamp>,
{
    // Some connections support keys - extract them.
    let keyed = if sink_render.uses_keys() {
        let user_key_indices = sink_render
            .get_key_indices()
            .map(|key_indices| key_indices.to_vec());

        let relation_key_indices = sink_render
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

        let keyed = if let Some(key_indices) = user_key_indices {
            let mut datum_vec = mz_repr::DatumVec::new();
            collection.map(move |row| {
                // TODO[perf] (btv) - is there a way to avoid unpacking and repacking every row and cloning the datums?
                // Does it matter?
                let key = {
                    let datums = datum_vec.borrow_with(&row);
                    Row::pack(key_indices.iter().map(|&idx| datums[idx].clone()))
                };
                (Some(key), row)
            })
        } else if let Some(relation_key_indices) = relation_key_indices {
            let mut datum_vec = mz_repr::DatumVec::new();
            collection.map(move |row| {
                // TODO[perf] (btv) - is there a way to avoid unpacking and repacking every row and cloning the datums?
                // Does it matter?
                let key = {
                    let datums = datum_vec.borrow_with(&row);
                    Row::pack(relation_key_indices.iter().map(|&idx| datums[idx].clone()))
                };
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
    } else {
        collection.map(|row| (None, row))
    };

    // Apply the envelope.
    // * "Debezium" consolidates the stream, sorts it by time, and produces DiffPairs from it.
    //   It then renders those as Avro.
    // * Upsert" does the same, except at the last step, it renders the diff pair in upsert format.
    //   (As part of doing so, it asserts that there are not multiple conflicting values at the same timestamp)
    let collection = match sink.envelope {
        Some(SinkEnvelope::Debezium) => {
            let combined = combine_at_timestamp(keyed.arrange_by_key().stream);

            // if there is no user-specified key, remove the synthetic
            // distribution key again
            let user_key_indices = sink_render.get_key_indices();
            let combined = if user_key_indices.is_some() {
                combined
            } else {
                combined.map(|(_key, value)| (None, value))
            };

            // This has to be an `Rc<RefCell<...>>` because the inner closure (passed to `Iterator::map`) references it, and it might outlive the outer closure.
            let row_buf = Rc::new(RefCell::new(Row::default()));
            let collection = combined.flat_map(move |(mut k, v)| {
                let max_idx = v.len() - 1;
                let row_buf = Rc::clone(&row_buf);
                v.into_iter().enumerate().map(move |(idx, dp)| {
                    let k = if idx == max_idx { k.take() } else { k.clone() };
                    let mut row_buf = row_buf.borrow_mut();
                    dbz_format(&mut row_buf.packer(), dp);
                    (k, Some(row_buf.clone()))
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
        None => keyed.map(|(key, value)| (key, Some(value))),
    };

    collection
}

/// A type that can be rendered as a dataflow sink.
pub(crate) trait SinkRender<G>
where
    G: Scope<Timestamp = Timestamp>,
{
    /// TODO
    fn uses_keys(&self) -> bool;
    /// TODO
    fn get_key_indices(&self) -> Option<&[usize]>;
    /// TODO
    fn get_relation_key_indices(&self) -> Option<&[usize]>;
    /// TODO
    fn render_continuous_sink(
        &self,
        compute_state: &mut crate::compute_state::ComputeState,
        sink: &SinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        sinked_collection: Collection<G, (Option<Row>, Option<Row>), Diff>,
        err_collection: Collection<G, DataflowError, Diff>,
    ) -> Option<Rc<dyn Any>>
    where
        G: Scope<Timestamp = Timestamp>;
}

fn get_sink_render_for<G>(connection: &SinkConnection<CollectionMetadata>) -> Box<dyn SinkRender<G>>
where
    G: Scope<Timestamp = Timestamp>,
{
    match connection {
        SinkConnection::Kafka(connection) => Box::new(connection.clone()),
        SinkConnection::Tail(connection) => Box::new(connection.clone()),
        SinkConnection::Persist(connection) => Box::new(connection.clone()),
    }
}
