// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::cell::RefCell;
use std::convert::Infallible;
use std::ops::DerefMut;
use std::rc::Rc;

use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::Collection;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};
use timely::progress::timestamp::Timestamp as TimelyTimestamp;
use timely::progress::Antichain;
use timely::PartialOrder;

use mz_compute_client::protocol::response::{SubscribeBatch, SubscribeResponse};
use mz_compute_client::types::sinks::{ComputeSinkDesc, SinkAsOf, SubscribeSinkConnection};
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_client::controller::CollectionMetadata;
use mz_storage_client::types::errors::DataflowError;
use mz_timely_util::probe::{self, ProbeNotify};

use crate::render::sinks::SinkRender;

impl<G> SinkRender<G> for SubscribeSinkConnection
where
    G: Scope<Timestamp = Timestamp>,
{
    fn render_continuous_sink(
        &self,
        _scope: &mut G,
        compute_state: &mut crate::compute_state::ComputeState,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        sinked_collection: Collection<G, Row, Diff>,
        err_collection: Collection<G, DataflowError, Diff>,
        probes: Vec<probe::Handle<Timestamp>>,
    ) -> Option<Rc<dyn Any>>
    where
        G: Scope<Timestamp = Timestamp>,
    {
        // An encapsulation of the Subscribe response protocol.
        // Used to send rows and progress messages,
        // and alert if the dataflow was dropped before completing.
        let subscribe_protocol_handle = Rc::new(RefCell::new(Some(SubscribeProtocol {
            sink_id,
            sink_as_of: sink.as_of.frontier.clone(),
            subscribe_response_buffer: Some(Rc::clone(&compute_state.subscribe_response_buffer)),
            prev_upper: Antichain::from_elem(Timestamp::minimum()),
            poison: None,
        })));
        let subscribe_protocol_weak = Rc::downgrade(&subscribe_protocol_handle);

        subscribe(
            sinked_collection,
            err_collection,
            sink_id,
            sink.as_of.clone(),
            sink.up_to.clone(),
            subscribe_protocol_handle,
            probes,
        );

        // Inform the coordinator that we have been dropped,
        // and destroy the subscribe protocol so the sink operator
        // can't send spurious messages while shutting down.
        Some(Rc::new(scopeguard::guard((), move |_| {
            if let Some(subscribe_protocol_handle) = subscribe_protocol_weak.upgrade() {
                std::mem::drop(subscribe_protocol_handle.borrow_mut().take())
            }
        })))
    }
}

fn subscribe<G>(
    sinked_collection: Collection<G, Row, Diff>,
    err_collection: Collection<G, DataflowError, Diff>,
    sink_id: GlobalId,
    as_of: SinkAsOf,
    up_to: Antichain<G::Timestamp>,
    subscribe_protocol_handle: Rc<RefCell<Option<SubscribeProtocol>>>,
    probes: Vec<probe::Handle<Timestamp>>,
) where
    G: Scope<Timestamp = Timestamp>,
{
    // Let the subscribe sink emit a progress stream, so we can attach the flow control probes.
    // The `Infallible` type signals that this steam never transports data updates.
    // TODO: Replace `Infallible` with `!` once the latter is stabilized.
    let progress_stream: Stream<G, Infallible>;

    let mut rows_to_emit = Vec::new();
    let mut errors_to_emit = Vec::new();
    let mut finished = false;
    let mut ok_buf = Default::default();
    let mut err_buf = Default::default();
    progress_stream = sinked_collection.inner.binary_frontier(
        &err_collection.inner,
        Pipeline,
        Pipeline,
        &format!("subscribe-{}", sink_id),
        move |_cap, _info| {
            move |ok_input, err_input, _output| {
                if finished {
                    // Drain the inputs, to avoid the operator being constantly rescheduled
                    ok_input.for_each(|_, _| {});
                    err_input.for_each(|_, _| {});
                    return;
                }

                let mut frontier = ok_input.frontier().frontier().to_owned();
                frontier.extend(err_input.frontier().frontier().iter().copied());

                let should_emit = |time: &Timestamp| {
                    let beyond_as_of = if as_of.strict {
                        as_of.frontier.less_than(time)
                    } else {
                        as_of.frontier.less_equal(time)
                    };
                    let before_up_to = !up_to.less_equal(time);
                    beyond_as_of && before_up_to
                };

                ok_input.for_each(|_, data| {
                    data.swap(&mut ok_buf);
                    for (row, time, diff) in ok_buf.drain(..) {
                        if should_emit(&time) {
                            rows_to_emit.push((time, row, diff));
                        }
                    }
                });
                err_input.for_each(|_, data| {
                    data.swap(&mut err_buf);
                    for (error, time, diff) in err_buf.drain(..) {
                        if should_emit(&time) {
                            errors_to_emit.push((time, error, diff));
                        }
                    }
                });

                if let Some(subscribe_protocol) = subscribe_protocol_handle.borrow_mut().deref_mut()
                {
                    subscribe_protocol.send_batch(
                        frontier.clone(),
                        &mut rows_to_emit,
                        &mut errors_to_emit,
                    );
                }

                if PartialOrder::less_equal(&up_to, &frontier) {
                    finished = true;
                    // We are done; indicate this by sending a batch at the
                    // empty frontier.
                    if let Some(subscribe_protocol) =
                        subscribe_protocol_handle.borrow_mut().deref_mut()
                    {
                        subscribe_protocol.send_batch(
                            Antichain::default(),
                            &mut Vec::new(),
                            &mut Vec::new(),
                        );
                    }
                }
            }
        },
    );

    progress_stream.probe_notify_with(probes);
}

/// A type that guides the transmission of rows back to the coordinator.
///
/// A protocol instance may `send` rows indefinitely in response to `send_batch` calls.
/// A `send_batch` call advancing the upper to the empty frontier is used to indicate the end of
/// a stream. If the stream is not advanced to the empty frontier, the `Drop` implementation sends
/// an indication that the protocol has finished without completion.
struct SubscribeProtocol {
    pub sink_id: GlobalId,
    pub sink_as_of: Antichain<Timestamp>,
    pub subscribe_response_buffer: Option<Rc<RefCell<Vec<(GlobalId, SubscribeResponse)>>>>,
    pub prev_upper: Antichain<Timestamp>,
    /// The error poisoning this subscribe, if any.
    ///
    /// As soon as a subscribe has encountered an error, it is poisoned: It will only return the
    /// same error in subsequent batches, until it is dropped. The subscribe protocol currently
    /// does not support retracting errors (#17781).
    pub poison: Option<String>,
}

impl SubscribeProtocol {
    /// Attempt to send a batch of rows with the given `upper`.
    ///
    /// This method filters the updates to send based on the provided `upper`. Updates are only
    /// sent when their times are before `upper`. If `upper` has not advanced far enough, no batch
    /// will be sent. `rows` and `errors` that have been sent are drained from their respective
    /// vectors, only entries that have not been sent remain after the call returns. The caller is
    /// expected to re-submit these entries, potentially along with new ones, at a later `upper`.
    ///
    /// The subscribe protocol only supports reporting a single error. Because of this, it will
    /// only actually send the first error received in a `SubscribeResponse`. Subsequent errors are
    /// dropped. To simplify life for the caller, this method still maintains the illusion that
    /// `errors` are handled the same way as `rows`.
    fn send_batch(
        &mut self,
        upper: Antichain<Timestamp>,
        rows: &mut Vec<(Timestamp, Row, Diff)>,
        errors: &mut Vec<(Timestamp, DataflowError, Diff)>,
    ) {
        // Only send a batch if both conditions hold:
        //  a) `upper` has reached or passed the sink's `as_of` frontier.
        //  b) `upper` is different from when we last sent a batch.
        if !PartialOrder::less_equal(&self.sink_as_of, &upper) || upper == self.prev_upper {
            return;
        }

        consolidate_updates(rows);
        consolidate_updates(errors);

        let (keep_rows, ship_rows) = rows.drain(..).partition(|u| upper.less_equal(&u.0));
        let (keep_errors, ship_errors) = errors.drain(..).partition(|u| upper.less_equal(&u.0));
        *rows = keep_rows;
        *errors = keep_errors;

        let updates = match (&self.poison, ship_errors.first()) {
            (Some(error), _) => {
                // The subscribe is poisoned; keep sending the same error.
                Err(error.clone())
            }
            (None, Some((_, error, _))) => {
                // The subscribe encountered its first error; poison it.
                let error = error.to_string();
                self.poison = Some(error.clone());
                Err(error)
            }
            (None, None) => {
                // No error encountered so for; ship the rows we have!
                Ok(ship_rows)
            }
        };

        let buffer = self
            .subscribe_response_buffer
            .as_mut()
            .expect("The subscribe response buffer is only cleared on drop.");

        buffer.borrow_mut().push((
            self.sink_id,
            SubscribeResponse::Batch(SubscribeBatch {
                lower: self.prev_upper.clone(),
                upper: upper.clone(),
                updates,
            }),
        ));

        let input_exhausted = upper.is_empty();
        self.prev_upper = upper;
        if input_exhausted {
            // The dataflow's input has been exhausted; clear the channel,
            // to avoid sending `SubscribeResponse::DroppedAt`.
            self.subscribe_response_buffer = None;
        }
    }
}

impl Drop for SubscribeProtocol {
    fn drop(&mut self) {
        if let Some(buffer) = self.subscribe_response_buffer.take() {
            buffer.borrow_mut().push((
                self.sink_id,
                SubscribeResponse::DroppedAt(self.prev_upper.clone()),
            ));
        }
    }
}
