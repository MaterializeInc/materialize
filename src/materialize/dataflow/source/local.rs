// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use lazy_static::lazy_static;
use std::cell::Cell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::mpsc::channel;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Mutex;
use timely::dataflow::operators::generic::source;
use timely::dataflow::{Scope, Stream};
use uuid::Uuid;

use crate::clock::{Clock, Timestamp};
use crate::dataflow::types::{Diff, LocalConnector};
use crate::repr::Datum;

// TODO(jamii) There doesn't seem to be any way to use #[allow(clippy::type_complexity)] inside lazy_static
type Channels = Mutex<HashMap<Uuid, (Sender<Datum>, Option<Receiver<Datum>>)>>;
// TODO(jamii) Ideally this would be part of the materialized state so we don't have to worry about reinitialization
lazy_static! {
    pub static ref CHANNELS: Channels = Mutex::new(HashMap::new());
}

impl LocalConnector {
    #[allow(clippy::new_without_default)]
    pub fn new() -> LocalConnector {
        LocalConnector { id: Uuid::new_v4() }
    }

    pub fn get_sender(&self) -> Sender<Datum> {
        CHANNELS
            .lock()
            .unwrap()
            .entry(self.id)
            .or_insert_with(|| {
                let (sender, receiver) = channel();
                (sender, Some(receiver))
            })
            .0
            .clone()
    }

    pub fn take_receiver(&self) -> Receiver<Datum> {
        CHANNELS
            .lock()
            .unwrap()
            .entry(self.id)
            .or_insert_with(|| {
                let (sender, receiver) = channel();
                (sender, Some(receiver))
            })
            .1
            .take()
            .expect("Someone already took the receiver?")
    }
}

pub fn local<G>(
    scope: &G,
    name: &str,
    connector: &LocalConnector,
    done: Rc<Cell<bool>>,
    clock: &Clock,
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

    let receiver = connector.take_receiver();

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
            // TODO(jamii) are we allowed to block this thread instead of polling?
            while let Ok(datum) = receiver.try_recv() {
                let cap = cap.delayed(&ts);
                output.session(&cap).give((datum, *cap.time(), 1));
            }

            cap.downgrade(&ts);
        }
    })
}
