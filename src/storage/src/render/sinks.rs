// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to the creation of dataflow sinks.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::operators::arrange::Arrange;
use differential_dataflow::trace::implementations::ord_neu::ColValSpine;
use differential_dataflow::{AsCollection, Collection, Hashable};
use mz_interchange::envelopes::{combine_at_timestamp, dbz_format};
use mz_persist_client::operators::shard_source::SnapshotMode;
use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};
use mz_storage_operators::persist_source;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sinks::{
    MetadataFilled, SinkEnvelope, StorageSinkConnection, StorageSinkDesc,
};
use mz_timely_util::builder_async::PressOnDropButton;
use timely::dataflow::operators::Leave;
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, Stream};
use tracing::warn;

use crate::healthcheck::HealthStatusMessage;
use crate::storage_state::StorageState;

/// _Renders_ complete _differential_ [`Collection`]s
/// that represent the sink and its errors as requested
/// by the original `CREATE SINK` statement.
pub(crate) fn render_sink<'g, G: Scope<Timestamp = ()>>(
    scope: &mut Child<'g, G, mz_repr::Timestamp>,
    storage_state: &mut StorageState,
    sink_id: GlobalId,
    sink: &StorageSinkDesc<MetadataFilled, mz_repr::Timestamp>,
) -> (Stream<G, HealthStatusMessage>, Vec<PressOnDropButton>) {
    let sink_render = get_sink_render_for(&sink.connection);

    let mut tokens = vec![];

    let snapshot_mode = if sink.with_snapshot {
        SnapshotMode::Include
    } else {
        SnapshotMode::Exclude
    };
    let (ok_collection, err_collection, persist_tokens) = persist_source::persist_source(
        scope,
        sink.from,
        Arc::clone(&storage_state.persist_clients),
        sink.from_storage_metadata.clone(),
        Some(sink.as_of.clone()),
        snapshot_mode,
        timely::progress::Antichain::new(),
        None,
        None,
    );
    tokens.extend(persist_tokens);

    let ok_collection =
        apply_sink_envelope(sink_id, sink, &sink_render, ok_collection.as_collection());

    let (health, sink_tokens) = sink_render.render_continuous_sink(
        storage_state,
        sink,
        sink_id,
        ok_collection,
        err_collection.as_collection(),
    );

    tokens.extend(sink_tokens);

    (health.leave(), tokens)
}

#[allow(clippy::borrowed_box)]
fn apply_sink_envelope<G>(
    sink_id: GlobalId,
    sink: &StorageSinkDesc<MetadataFilled, mz_repr::Timestamp>,
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
            collection.map(|row| (Some(Row::pack(Some(Datum::UInt64(row.hashed())))), row))
        };
        keyed
    } else {
        collection.map(|row| (None, row))
    };

    fn warn_on_dups<T>(v: &[T], sink_id: GlobalId, from_id: GlobalId) {
        if v.len() > 1 {
            warn!(
                sink_id =? sink_id,
                from_id =? from_id,
                "primary key error: expected at most one update per key and timestamp \
                This can happen when the configured sink key is not a primary key of \
                the sinked relation."
            )
        }
    }

    // Apply the envelope.
    // * "Debezium" consolidates the stream, sorts it by time, and produces DiffPairs from it.
    //   It then renders those as Avro.
    // * Upsert" does the same, except at the last step, it renders the diff pair in upsert format.
    //   (As part of doing so, it asserts that there are not multiple conflicting values at the same timestamp)
    let collection = match sink.envelope {
        SinkEnvelope::Debezium => {
            // Allow access to `arrange_named` because we cannot access Mz's wrapper from here.
            // TODO(#17413): Revisit with cluster unification.
            #[allow(clippy::disallowed_methods)]
            let combined = combine_at_timestamp(
                keyed.arrange_named::<ColValSpine<_, _, _, _>>("Arrange Debezium"),
            );

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
            let from_id = sink.from;
            let collection = combined.flat_map(move |(mut k, v)| {
                warn_on_dups(&v, sink_id, from_id);
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
        SinkEnvelope::Upsert => {
            // Allow access to `arrange_named` because we cannot access Mz's wrapper from here.
            // TODO(#17413): Revisit with cluster unification.
            #[allow(clippy::disallowed_methods)]
            let combined = combine_at_timestamp(
                keyed.arrange_named::<ColValSpine<_, _, _, _>>("Arrange Upsert"),
            );

            let from_id = sink.from;
            let collection = combined.flat_map(move |(mut k, v)| {
                warn_on_dups(&v, sink_id, from_id);
                let max_idx = v.len() - 1;
                v.into_iter().enumerate().map(move |(idx, dp)| {
                    let k = if idx == max_idx { k.take() } else { k.clone() };
                    (k, dp.after)
                })
            });
            collection
        }
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
        storage_state: &mut StorageState,
        sink: &StorageSinkDesc<MetadataFilled, Timestamp>,
        sink_id: GlobalId,
        sinked_collection: Collection<G, (Option<Row>, Option<Row>), Diff>,
        err_collection: Collection<G, DataflowError, Diff>,
    ) -> (Stream<G, HealthStatusMessage>, Vec<PressOnDropButton>)
    where
        G: Scope<Timestamp = Timestamp>;
}

fn get_sink_render_for<G>(connection: &StorageSinkConnection) -> Box<dyn SinkRender<G>>
where
    G: Scope<Timestamp = Timestamp>,
{
    match connection {
        StorageSinkConnection::Kafka(connection) => Box::new(connection.clone()),
    }
}
