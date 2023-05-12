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
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use differential_dataflow::Collection;
use mz_compute_client::types::sinks::{ComputeSinkConnection, ComputeSinkDesc};
use mz_expr::{permutation_for_arrangement, MapFilterProject};
use mz_repr::{Diff, GlobalId, Row};
use mz_storage_client::controller::CollectionMetadata;
use mz_storage_client::types::errors::DataflowError;
use mz_timely_util::probe;
use timely::dataflow::{scopes::Child, Scope};
use timely::progress::Antichain;

use crate::compute_state::SinkToken;
use crate::render::{context::Context, RenderTimestamp};

impl<'g, G, T> Context<Child<'g, G, T>, Row>
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    T: RenderTimestamp,
{
    /// Export the sink described by `sink` from the rendering context.
    pub(crate) fn export_sink(
        &mut self,
        compute_state: &mut crate::compute_state::ComputeState,
        tokens: &mut BTreeMap<GlobalId, Rc<dyn std::any::Any>>,
        dependency_ids: BTreeSet<GlobalId>,
        sink_id: GlobalId,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        probes: Vec<probe::Handle<mz_repr::Timestamp>>,
    ) {
        // put together tokens that belong to the export
        let mut needed_tokens = Vec::new();
        for dep_id in dependency_ids {
            if let Some(token) = tokens.get(&dep_id) {
                needed_tokens.push(Rc::clone(token))
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
            let (permutation, thinning) = permutation_for_arrangement(key, unthinned_arity);
            let mut mfp = MapFilterProject::new(unthinned_arity);
            mfp.permute(permutation, thinning.len() + key.len());
            bundle.as_collection_core(mfp, Some((key.clone(), None)), self.until.clone())
        };

        let ok_collection = ok_collection.leave();
        let err_collection = err_collection.leave();

        let region_name = match sink.connection {
            ComputeSinkConnection::Subscribe(_) => format!("SubscribeSink({:?})", sink_id),
            ComputeSinkConnection::Persist(_) => format!("PersistSink({:?})", sink_id),
        };
        self.scope
            .parent
            .clone()
            .region_named(&region_name, |inner| {
                let sink_render = get_sink_render_for::<_>(&sink.connection);

                let sink_token = sink_render.render_continuous_sink(
                    compute_state,
                    sink,
                    sink_id,
                    self.as_of_frontier.clone(),
                    ok_collection.enter_region(inner),
                    err_collection.enter_region(inner),
                    probes,
                );

                if let Some(sink_token) = sink_token {
                    needed_tokens.push(sink_token);
                }

                compute_state
                    .sink_tokens
                    .insert(sink_id, SinkToken::new(Box::new(needed_tokens)));
            });
    }
}

/// A type that can be rendered as a dataflow sink.
pub(crate) trait SinkRender<G>
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
{
    fn render_continuous_sink(
        &self,
        compute_state: &mut crate::compute_state::ComputeState,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        as_of: Antichain<mz_repr::Timestamp>,
        sinked_collection: Collection<G, Row, Diff>,
        err_collection: Collection<G, DataflowError, Diff>,
        probes: Vec<probe::Handle<mz_repr::Timestamp>>,
    ) -> Option<Rc<dyn Any>>
    where
        G: Scope<Timestamp = mz_repr::Timestamp>;
}

fn get_sink_render_for<G>(
    connection: &ComputeSinkConnection<CollectionMetadata>,
) -> Box<dyn SinkRender<G>>
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
{
    match connection {
        ComputeSinkConnection::Subscribe(connection) => Box::new(connection.clone()),
        ComputeSinkConnection::Persist(connection) => Box::new(connection.clone()),
    }
}
