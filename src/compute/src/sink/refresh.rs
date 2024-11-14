// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::consolidation::ConsolidatingContainerBuilder;
use differential_dataflow::{AsCollection, Collection, Data};
use mz_ore::soft_panic_or_log;
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{Diff, Timestamp};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::Scope;
use timely::progress::Antichain;

/// This is for REFRESH options on materialized views. It adds an operator that rounds up the
/// timestamps of data and frontiers to the time of the next refresh. See
/// `doc/developer/design/20231027_refresh_every_mv.md`.
///
/// Note that this currently only works with 1-dim timestamps. (This is not an issue for WMR,
/// because iteration numbers should disappear by the time the data gets to the Persist sink.)
pub(crate) fn apply_refresh<G, D>(
    coll: Collection<G, D, Diff>,
    refresh_schedule: RefreshSchedule,
) -> Collection<G, D, Diff>
where
    G: Scope<Timestamp = Timestamp>,
    D: Data,
{
    // We need to disconnect the reachability graph and manage capabilities manually, because we'd
    // like to round up frontiers as well as data: as soon as our input frontier passes a refresh
    // time, we'll round it up to the next refresh time.
    let mut builder = OperatorBuilder::new("apply_refresh".to_string(), coll.scope());
    let (mut output_buf, output_stream) = builder.new_output::<ConsolidatingContainerBuilder<_>>();
    let mut input = builder.new_input_connection(&coll.inner, Pipeline, vec![Antichain::new()]);
    builder.build(move |capabilities| {
        // This capability directly controls this operator's output frontier (because we have
        // disconnected the input above). We wrap it in an Option so we can drop it to advance to
        // the empty output frontier when the last refresh is done. (We must be careful that we only
        // ever emit output updates at times that are at or beyond this capability.)
        let mut capability = capabilities.into_iter().next(); // (We have 1 one input.)
        move |frontiers| {
            let mut output_handle_core = output_buf.activate();
            input.for_each(|input_cap, data| {
                // Note that we can't use `input_cap` to get an output session because we might have
                // advanced our output frontier already beyond the frontier of this capability.

                // `capability` will be None if we are past the last refresh. We have made sure to
                // not receive any data that is after the last refresh by setting the `until` of the
                // dataflow to the last refresh.
                let Some(capability) = capability.as_mut() else {
                    soft_panic_or_log!(
                        "should have a capability if we received data. input_cap: {:?}, frontier: {:?}",
                        input_cap.time(),
                        frontiers[0].frontier()
                    );
                    return;
                };
                let mut output_buf = output_handle_core.session_with_builder(&capability);

                let mut cached_ts: Option<Timestamp> = None;
                let mut cached_rounded_up_data_ts = None;
                for (d, ts, r) in data.drain(..) {
                    let rounded_up_data_ts = {
                        // We cache the rounded up timestamp for the last seen timestamp,
                        // because the rounding up has a non-negligible cost. Caching for
                        // just the 1 last timestamp helps already, because in some common
                        // cases, we'll be seeing a lot of identical timestamps, e.g.,
                        // during a rehydration, or when we have much more than 1000 records
                        // during a single second.
                        if cached_ts != Some(ts) {
                            cached_ts = Some(ts);
                            cached_rounded_up_data_ts = refresh_schedule.round_up_timestamp(ts);
                        }
                        cached_rounded_up_data_ts
                    };
                    match rounded_up_data_ts {
                        Some(rounded_up_ts) => {
                            output_buf.give((d, rounded_up_ts, r));
                        }
                        None => {
                            // This record is after the last refresh, which is not possible because
                            // we set the dataflow `until` to the last refresh.
                            soft_panic_or_log!("Received data after the last refresh");
                        }
                    }
                }
            });

            // Round up the frontier.
            // Note that `round_up_timestamp` is monotonic. This is needed to ensure that the
            // timestamp (t) of any received data that has a larger timestamp than the original
            // frontier (f) will get rounded up to a time that is at least at the rounded up
            // frontier. In other words, monotonicity ensures that
            // when `t >= f` then `round_up_timestamp(t) >= round_up_timestamp(f)`.
            match frontiers[0].frontier().as_option() { // (We have only 1 input, so only 1 frontier.)
                Some(ts) => {
                    match refresh_schedule.round_up_timestamp(*ts) {
                        Some(rounded_up_ts) => {
                            capability
                                .as_mut()
                                .expect("capability must exist if frontier is <= last refresh")
                                .downgrade(&rounded_up_ts);
                        }
                        None => {
                            // We are past the last refresh. Drop the capability to signal that we
                            // are done.
                            capability = None;
                            // We can only get here if we see the frontier advancing to a time after the last refresh,
                            // but not empty. This is ok, because even though we set the `until` to the last refresh,
                            // frontier advancements might still happen past the `until`.
                        }
                    }
                }
                None => {
                    capability = None;
                }
            }
        }
    });

    output_stream.as_collection()
}
