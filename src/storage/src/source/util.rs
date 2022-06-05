// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::rc::Rc;

use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::{OperatorInfo, OutputHandle};
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};
use timely::scheduling::ActivateOnDrop;
use timely::Data;

use mz_ore::collections::CollectionExt;
use mz_repr::Timestamp;

use super::{SourceStatus, SourceToken};

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
/// fact by returning [`SourceStatus::Done`], which will immediately drop the
/// capability and guarantee that `tick` is never called again.
///
/// Otherwise, `tick` should return [`SourceStatus::Alive`]. It is `tick`'s
/// responsibility to inform Timely of its desire to be scheduled again by
/// chatting with a [`timely::scheduling::activate::Activator`]. Returning
/// [`SourceStatus::Alive`] does not alone cause the source to be scheduled
/// again; it merely keeps the capability alive.
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
            &mut Capability<Timestamp>,
            &mut OutputHandle<G::Timestamp, D, Tee<G::Timestamp, D>>,
        ) -> SourceStatus
        + 'static,
{
    let mut token = None;

    let mut builder = OperatorBuilder::new(name, scope.clone());
    let operator_info = builder.operator_info();

    let (mut data_output, data_stream) = builder.new_output();
    builder.set_notify(false);

    builder.build(|capabilities| {
        let mut capability = Some(capabilities.into_element());

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

        let mut tick = construct(operator_info);

        move |_frontier| {
            // Drop all capabilities if `token` is dropped.
            if drop_activator_weak.upgrade().is_none() {
                capability = None;
            }
            if let Some(cap) = &mut capability {
                // We still have our capability, so the source is still alive.
                // Delegate to the inner source.
                if let SourceStatus::Done = tick(cap, &mut data_output.activate()) {
                    // The inner source is finished. Drop our capability.
                    capability = None;
                }
            }
        }
    });

    // `build()` promises to call the provided closure before returning,
    // so we are guaranteed that `token` is non-None.
    (data_stream, token.unwrap())
}
