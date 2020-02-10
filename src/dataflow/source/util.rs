// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::RefCell;
use std::rc::Rc;

use expr::SourceInstanceId;

use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::source as timely_source;
use timely::dataflow::operators::generic::{OperatorInfo, OutputHandle};
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};
use timely::Data;

use crate::server::TimestampChanges;
use dataflow_types::Timestamp;

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
pub fn source<G, D, B, L>(
    id: SourceInstanceId,
    timestamp: Option<TimestampChanges>,
    scope: &G,
    name: &str,
    construct: B,
) -> (Stream<G, D>, SourceToken)
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
    let stream = timely_source(scope, name, |cap, info| {
        let cap = Rc::new(RefCell::new(Some(cap)));

        // Export a token to the outside word that will keep this source alive.
        token = Some(SourceToken {
            id,
            capability: cap.clone(),
            activator: scope.activator_for(&info.address[..]),
            timestamp_drop: timestamp,
        });

        let mut tick = construct(info);
        move |output| {
            let mut cap = cap.borrow_mut();
            if let Some(some_cap) = &mut *cap {
                // We still have our capability, so the source is still alive.
                // Delegate to the inner source.
                if let SourceStatus::Done = tick(some_cap, output) {
                    // The inner source is finished. Drop our capability.
                    *cap = None;
                }
            }
        }
    });

    // `timely_source` promises to call the provided closure before returning,
    // so we are guaranteed that `token` is non-None.
    (stream, token.unwrap())
}
