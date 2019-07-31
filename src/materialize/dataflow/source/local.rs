// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use timely::dataflow::{Scope, Stream};

use super::util::source;
use super::SharedCapability;
use crate::dataflow::types::{Diff, LocalSourceConnector, Timestamp};
use crate::glue::{LocalInput, LocalInputMux};
use crate::repr::Datum;

pub fn local<G>(
    scope: &G,
    name: &str,
    connector: &LocalSourceConnector,
    read_input: bool,
    local_input_mux: &mut LocalInputMux,
) -> (
    Stream<G, (Vec<Datum>, Timestamp, Diff)>,
    Option<SharedCapability>,
)
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

                // TODO(jamii) if we crash here, might need some graceful shutdown logic
                while let Some(local_input) = receiver.try_next().unwrap() {
                    match local_input {
                        LocalInput::Updates(updates) => {
                            let mut session = output.session(&cap);
                            for update in updates {
                                assert!(
                                    update.timestamp >= *cap.time(),
                                    "Local input went backwards in time"
                                );
                                session.give((update.row, update.timestamp, update.diff));
                            }
                        }
                        LocalInput::Watermark(timestamp) => {
                            cap.downgrade(&timestamp);
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
