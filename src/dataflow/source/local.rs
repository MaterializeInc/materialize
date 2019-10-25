// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use timely::dataflow::{Scope, Stream};

use super::util::source;
use super::SharedCapability;
use dataflow_types::{Diff, LocalInput, LocalSourceConnector, Timestamp};
use ore::mpmc::Mux;
use repr::Row;
use uuid::Uuid;

pub fn local<G>(
    scope: &G,
    name: &str,
    connector: LocalSourceConnector,
    read_input: bool,
    local_input_mux: &mut Mux<Uuid, LocalInput>,
) -> (Stream<G, (Row, Timestamp, Diff)>, Option<SharedCapability>)
where
    G: Scope<Timestamp = Timestamp>,
{
    let (stream, capability) = source(scope, name, move |info| {
        let activator = scope.activator_for(&info.address[..]);
        let mut receiver = if read_input {
            Some(
                local_input_mux
                    .write()
                    .unwrap()
                    .receiver(&connector.uuid)
                    .unwrap(),
            )
        } else {
            None
        };

        move |cap, output| {
            if let Some(receiver) = receiver.as_mut() {
                // Indicate that we should run again.
                activator.activate();

                // TODO(jamii) we should gracefully wind these down once the sender is gone
                while let Ok(Some(local_input)) = receiver.try_next() {
                    match local_input {
                        LocalInput::Updates(updates) => {
                            let mut session = output.session(&cap);
                            for update in updates {
                                assert!(
                                    update.timestamp >= *cap.time(),
                                    "Local input went backwards in time: update.timestamp={}, cap.time={}",
                                    update.timestamp,
                                    cap.time()
                                );
                                session.give((update.row, update.timestamp, update.diff));
                            }
                        }
                        LocalInput::Watermark(timestamp) => {
                            cap.downgrade(&(timestamp));
                        }
                    }
                }
            }
        }
    });

    if read_input {
        (stream, Some(capability))
    } else {
        (stream, None)
    }
}
