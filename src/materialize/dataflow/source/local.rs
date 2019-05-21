// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::cell::Cell;
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use timely::dataflow::operators::generic::source;
use timely::dataflow::{Scope, Stream};

use crate::clock::{Clock, Timestamp};
use crate::dataflow::types::{Diff, LocalSourceConnector};
use crate::glue::*;
use crate::repr::Datum;

pub type InsertMux = Arc<RwLock<Mux<String, Vec<Datum>>>>;

pub fn local<G>(
    scope: &G,
    name: &str,
    _connector: &LocalSourceConnector,
    done: Rc<Cell<bool>>,
    clock: &Clock,
    mux: &InsertMux,
) -> Stream<G, (Datum, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    if scope.index() != 0 {
        // Only the first worker reads data, since Rust channels are single
        // consumer and can't be shared between threads. The other workers get
        // dummy sources that never produce any data.
        return source(scope, name, move |_cap, _info| move |_output| ());
    }

    let mut receiver = mux.write().unwrap().channel(name.to_string()).unwrap();

    source(scope, name, move |cap, info| {
        let activator = scope.activator_for(&info.address[..]);
        let mut maybe_cap = Some(cap);
        let clock = clock.clone();

        move |output| {
            if done.get() {
                maybe_cap = None;
                return;
            }
            let cap = maybe_cap.as_mut().unwrap();

            // Indicate that we should run again.
            activator.activate();

            let ts = clock.now();

            // Consume all data waiting in the queue
            while let Ok(Some(datums)) = receiver.try_next() {
                let cap = cap.delayed(&ts);
                let mut session = output.session(&cap);
                for datum in datums {
                    session.give((datum, *cap.time(), 1));
                }
            }

            cap.downgrade(&ts);
        }
    })
}
