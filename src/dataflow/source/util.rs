// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::cell::RefCell;
use std::rc::Rc;
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::source as timely_source;
use timely::dataflow::operators::generic::{OperatorInfo, OutputHandle};
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};
use timely::Data;

use super::{SharedCapability, SourceStatus};
use dataflow_types::Timestamp;

pub fn source<G, D, B, L>(scope: &G, name: &str, construct: B) -> (Stream<G, D>, SharedCapability)
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
    let mut cap_out = None;
    let stream = timely_source(scope, name, |cap, info| {
        // Share ownership of the source's capability with the outside world.
        let cap = Rc::new(RefCell::new(Some(cap)));
        cap_out = Some(cap.clone());

        // Hold only a weak reference to the capability. If all strong
        // references to the capability are dropped, we automatically shut down.
        let cap = Rc::downgrade(&cap);
        let mut tick = construct(info);
        move |output| {
            if let Some(cap) = cap.upgrade() {
                let mut cap = cap.borrow_mut();
                if let SourceStatus::Done = tick(cap.as_mut().unwrap(), output) {
                    *cap = None;
                }
            }
        }
    });

    // `timely_source` promises to call the provided closure before returning,
    // so we are guaranteed that `cap_out` is non-None.
    (stream, cap_out.unwrap())
}
