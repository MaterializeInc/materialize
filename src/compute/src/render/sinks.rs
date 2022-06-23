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
use std::collections::BTreeSet;
use std::rc::Rc;

use differential_dataflow::Collection;
use timely::dataflow::Scope;

use mz_dataflow_types::sinks::*;
use mz_expr::{permutation_for_arrangement, MapFilterProject};
use mz_repr::{Diff, GlobalId, Row, Timestamp};

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
        sink: &SinkDesc,
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
        let collection = if let Some((collection, _err_collection)) = &bundle.collection {
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
            let (collection, _err_collection) =
                bundle.as_collection_core(mfp, Some((key.clone(), None)));
            collection
        };

        // TODO(benesch): errors should stream out through the sink,
        // if we figure out a protocol for that.

        let sink_token =
            sink_render.render_continuous_sink(compute_state, sink, sink_id, collection);

        if let Some(sink_token) = sink_token {
            needed_tokens.push(sink_token);
        }

        compute_state
            .dataflow_tokens
            .insert(sink_id, Box::new(needed_tokens));
    }
}

/// A type that can be rendered as a dataflow sink.
pub(crate) trait SinkRender<G>
where
    G: Scope<Timestamp = Timestamp>,
{
    /// TODO
    fn render_continuous_sink(
        &self,
        compute_state: &mut crate::compute_state::ComputeState,
        sink: &SinkDesc,
        sink_id: GlobalId,
        sinked_collection: Collection<G, Row, Diff>,
    ) -> Option<Rc<dyn Any>>
    where
        G: Scope<Timestamp = Timestamp>;
}

fn get_sink_render_for<G>(connection: &SinkConnection) -> Box<dyn SinkRender<G>>
where
    G: Scope<Timestamp = Timestamp>,
{
    match connection {
        SinkConnection::Tail(connection) => Box::new(connection.clone()),
        SinkConnection::Persist(connection) => Box::new(connection.clone()),
        SinkConnection::Kafka(_) => unimplemented!(),
    }
}
