// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::rc::Rc;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::pushers::{Tee, TeeCore};
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::{OperatorInfo, OutputHandle, OutputWrapper};
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::scheduling::ActivateOnDrop;
use timely::Data;

use mz_ore::collections::CollectionExt;
use mz_repr::Timestamp;
use mz_timely_util::builder_async::{AsyncInputHandle, OperatorBuilder as AsyncOperatorBuilder};

use crate::source::types::{AsyncSourceToken, SourceToken};

/// Constructs a source named `name` in `scope` whose lifetime is controlled
/// both internally and externally.
///
/// The logic for the source is supplied by `construct`, which must return a
/// `tick` function that satisfies `L`. This function will be called
/// periodically while the source is alive and supplied with a capability to
/// produce data and the output handle into which data should be given. The
/// `tick` function is responsible for periodically downgrading this capability
/// whenever it can see that a timestamp is "closed", according to whatever
/// logic makes sense for the source.
///
/// If `tick` realizes it will never produce data again, it should indicate that
/// fact by downgrading the given `CapabilitySet` to the empty frontier before
/// returning. This will guarantee that `tick` is never called again.
///
/// It is `tick`'s responsibility to inform Timely of its desire to be scheduled
/// again by chatting with a [`timely::scheduling::activate::Activator`].
/// Holding on to capabilities using the `CapabilitySet` does not alone cause
/// the source to be scheduled again; it merely keeps the source alive.
///
/// The lifetime of the source is also controlled by the returned
/// [`SourceToken`]. When the last clone of the `SourceToken` is dropped, the
/// `tick` function will no longer be called, and the capability will eventually
/// be dropped.
///
/// When the source token is dropped, the timestamping_flag is set to false
/// to terminate any spawned threads in the source operator
pub fn source<G, D, B, L>(scope: &G, name: String, construct: B) -> (Stream<G, D>, SourceToken)
where
    G: Scope<Timestamp = Timestamp>,
    D: Data,
    B: FnOnce(OperatorInfo) -> L,
    L: FnMut(
            &mut CapabilitySet<Timestamp>,
            &mut OutputHandle<G::Timestamp, D, Tee<G::Timestamp, D>>,
        ) -> ()
        + 'static,
{
    let mut token = None;

    let mut builder = OperatorBuilder::new(name, scope.clone());
    let operator_info = builder.operator_info();

    let (mut data_output, data_stream) = builder.new_output();
    builder.set_notify(false);

    builder.build(|capabilities| {
        let cap_set = CapabilitySet::from_elem(capabilities.into_element());

        let drop_activator = Rc::new(ActivateOnDrop::new(
            (),
            Rc::new(operator_info.address.clone()),
            scope.activations(),
        ));
        let drop_activator_weak = Rc::downgrade(&drop_activator);

        // Export a token to the outside word that will keep this source alive.
        token = Some(SourceToken {
            _activator: drop_activator,
        });

        let tick = construct(operator_info);
        let mut cap_and_tick = Some((cap_set, tick));

        move |_| {
            // Drop all capabilities if `token` is dropped.
            if drop_activator_weak.upgrade().is_none() {
                // Drop the tick closure, too, in case dropping anything it owns
                // (such as a MutexGuard) is important.
                //
                // TODO: This assumes that `tick` is "cancel safe" (in async
                // lingo). Perhaps we want a more graceful shutdown protocol
                // instead/in addition.
                cap_and_tick = None;
            }
            if let Some((cap, tick)) = &mut cap_and_tick {
                // We still have our capability, so the source is still alive.
                // Delegate to the inner source.
                tick(cap, &mut data_output.activate());
                if cap.is_empty() {
                    // The inner source is finished. Drop our capability.
                    cap_and_tick = None;
                }
            }
        }
    });

    // `build()` promises to call the provided closure before returning,
    // so we are guaranteed that `token` is non-None.
    (data_stream, token.unwrap())
}

/// Effectively the same as `source`, but the core logic expects a
/// never-ending future, not a tick closure. Additionally, this
/// operator also contains an input, primarily to inspect the
/// frontier of an upstream operator. This input
/// does not participate in progress tracking.
///
/// Note that this means the input and capabilities are communicated
/// to the future by value, not by &mut reference.
///
/// Returns an `AsyncSourceToken`, which, upon drop, will cause the
/// shutdown of the operator.
pub fn async_source<G, D, B, L>(
    scope: &G,
    name: String,
    input: &Stream<G, ()>,
    construct: B,
) -> (Stream<G, D>, AsyncSourceToken)
where
    G: Scope<Timestamp = Timestamp>,
    D: Data,
    B: FnOnce(
        OperatorInfo,
        CapabilitySet<Timestamp>,
        AsyncInputHandle<
            Timestamp,
            Vec<()>,
            <Pipeline as timely::dataflow::channels::pact::ParallelizationContractCore<
                G::Timestamp,
                Vec<()>,
            >>::Puller,
        >,
        OutputWrapper<G::Timestamp, Vec<D>, TeeCore<G::Timestamp, Vec<D>>>,
    ) -> L,
    L: Future + 'static,
{
    let mut builder = AsyncOperatorBuilder::new(name, scope.clone());
    let operator_info = builder.operator_info();

    let (data_output, data_stream) = builder.new_output();

    let remap_input = builder.new_input_connection(
        input,
        Pipeline,
        // As documented, the input does not
        // participate in progress tracking.
        vec![Antichain::new()],
    );

    let (tx, mut rx) = tokio::sync::oneshot::channel();
    let token = AsyncSourceToken {
        _drop_closes_the_oneshot: tx,
    };

    builder.build(|capabilities| {
        let cap_set = CapabilitySet::from_elem(capabilities.into_element());

        let tick = construct(operator_info, cap_set, remap_input, data_output);
        async move {
            tokio::pin!(tick);
            tokio::select! {
                biased;
                _ = &mut rx => {},
                _ = &mut tick => {},
            }
        }
    });

    (data_stream, token)
}
