// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Traits and types for capturing and replaying collections of data.
use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;

use differential_dataflow::Collection;
use timely::dataflow::Scope;

use mz_dataflow_types::DataflowError;
use mz_dataflow_types::SourceInstanceKey;
use mz_repr::{Diff, Row};

/// A type that can capture a specific source.
pub trait StorageCapture {
    /// Captures the source and binds to `id`.
    fn capture<G: Scope<Timestamp = mz_repr::Timestamp>>(
        &mut self,
        id: SourceInstanceKey,
        ok: Collection<G, Row, Diff>,
        err: Collection<G, DataflowError, Diff>,
        token: Rc<dyn Any>,
        name: &str,
        dataflow_id: uuid::Uuid,
    );
}

impl<SC: StorageCapture> StorageCapture for Rc<RefCell<SC>> {
    fn capture<G: Scope<Timestamp = mz_repr::Timestamp>>(
        &mut self,
        id: SourceInstanceKey,
        ok: Collection<G, Row, Diff>,
        err: Collection<G, DataflowError, Diff>,
        token: Rc<dyn Any>,
        name: &str,
        dataflow_id: uuid::Uuid,
    ) {
        self.borrow_mut()
            .capture(id, ok, err, token, name, dataflow_id)
    }
}

impl<CR: ComputeReplay> ComputeReplay for Rc<RefCell<CR>> {
    fn replay<G: Scope<Timestamp = mz_repr::Timestamp>>(
        &mut self,
        id: SourceInstanceKey,
        scope: &mut G,
        name: &str,
        dataflow_id: uuid::Uuid,
    ) -> (
        Collection<G, Row, Diff>,
        Collection<G, DataflowError, Diff>,
        Rc<dyn Any>,
    ) {
        self.borrow_mut().replay(id, scope, name, dataflow_id)
    }
}

/// A type that can replay specific sources
pub trait ComputeReplay {
    /// Replays the source bound to `id`.
    ///
    /// `None` is returned if the source does not exist, either because
    /// it was not there to begin, or has already been replayed. Either
    /// case is likely an error.
    fn replay<G: Scope<Timestamp = mz_repr::Timestamp>>(
        &mut self,
        id: SourceInstanceKey,
        scope: &mut G,
        name: &str,
        dataflow_id: uuid::Uuid,
    ) -> (
        Collection<G, Row, Diff>,
        Collection<G, DataflowError, Diff>,
        Rc<dyn Any>,
    );
}

/// A boundary implementation that panics on use.
pub struct DummyBoundary;

impl ComputeReplay for DummyBoundary {
    fn replay<G: Scope<Timestamp = mz_repr::Timestamp>>(
        &mut self,
        _id: SourceInstanceKey,
        _scope: &mut G,
        _name: &str,
        _dataflow_id: uuid::Uuid,
    ) -> (
        Collection<G, Row, Diff>,
        Collection<G, DataflowError, Diff>,
        Rc<dyn Any>,
    ) {
        panic!("DummyBoundary cannot replay")
    }
}

impl StorageCapture for DummyBoundary {
    fn capture<G: Scope<Timestamp = mz_repr::Timestamp>>(
        &mut self,
        _id: SourceInstanceKey,
        _ok: Collection<G, Row, Diff>,
        _err: Collection<G, DataflowError, Diff>,
        _token: Rc<dyn Any>,
        _name: &str,
        _dataflow_id: uuid::Uuid,
    ) {
        panic!("DummyBoundary cannot capture")
    }
}

pub use event_link::EventLinkBoundary;

/// A simple boundary that uses activated event linked lists.
mod event_link {

    use std::any::Any;
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::rc::Rc;
    use std::time::Duration;

    use differential_dataflow::AsCollection;
    use differential_dataflow::Collection;
    use timely::dataflow::operators::capture::EventLink;
    use timely::dataflow::Scope;

    use mz_dataflow_types::DataflowError;
    use mz_dataflow_types::SourceInstanceKey;
    use mz_repr::{Diff, Row};

    use crate::activator::RcActivator;
    use crate::replay::MzReplay;
    use crate::server::ActivatedEventPusher;

    use super::{ComputeReplay, StorageCapture};

    /// A simple boundary that uses activated event linked lists.
    pub struct EventLinkBoundary {
        /// Source boundaries shared between storage and compute.
        shared: BTreeMap<SourceInstanceKey, SourceBoundary>,
    }

    impl EventLinkBoundary {
        /// Create a new boundary, initializing the state to be empty.
        pub fn new() -> Self {
            Self {
                shared: BTreeMap::new(),
            }
        }
    }

    impl StorageCapture for EventLinkBoundary {
        fn capture<G: Scope<Timestamp = mz_repr::Timestamp>>(
            &mut self,
            id: SourceInstanceKey,
            ok: Collection<G, Row, Diff>,
            err: Collection<G, DataflowError, Diff>,
            token: Rc<dyn Any>,
            name: &str,
            _dataflow_id: uuid::Uuid,
        ) {
            // If the replayer got here before we did ...
            let boundary = if let Some(boundary) = self.shared.remove(&id) {
                *boundary.token.borrow_mut() = Some(token);
                boundary
            } else {
                let boundary = SourceBoundary::with_token(name, token);
                self.shared.insert(id, boundary.clone());
                boundary
            };

            use timely::dataflow::operators::Capture;
            ok.inner.capture_into(boundary.ok);
            err.inner.capture_into(boundary.err);
        }
    }

    impl ComputeReplay for EventLinkBoundary {
        fn replay<G: Scope<Timestamp = mz_repr::Timestamp>>(
            &mut self,
            id: SourceInstanceKey,
            scope: &mut G,
            name: &str,
            dataflow_id: uuid::Uuid,
        ) -> (
            Collection<G, Row, Diff>,
            Collection<G, DataflowError, Diff>,
            Rc<dyn Any>,
        ) {
            let boundary = if let Some(boundary) = self.shared.remove(&id) {
                boundary
            } else {
                let boundary = SourceBoundary::new(name);
                self.shared.insert(id, boundary.clone());
                boundary
            };

            let ok = Some(boundary.ok.inner)
                .mz_replay(
                    scope,
                    &format!("{name}-ok"),
                    Duration::MAX,
                    boundary.ok.activator,
                )
                .as_collection();
            let err = Some(boundary.err.inner)
                .mz_replay(
                    scope,
                    &format!("{name}-err"),
                    Duration::MAX,
                    boundary.err.activator,
                )
                .as_collection();

            (ok, err, boundary.token)
        }
    }

    /// Information about each source that must be communicated between storage and compute layers.
    #[derive(Clone)]
    pub struct SourceBoundary {
        /// Captured `row` updates representing a differential collection.
        pub ok: ActivatedEventPusher<
            Rc<EventLink<mz_repr::Timestamp, (Row, mz_repr::Timestamp, mz_repr::Diff)>>,
        >,
        /// Captured error updates representing a differential collection.
        pub err: ActivatedEventPusher<
            Rc<EventLink<mz_repr::Timestamp, (DataflowError, mz_repr::Timestamp, mz_repr::Diff)>>,
        >,
        /// A token shell that should be dropped to terminate the source.
        ///
        /// The shell can be populated after the fact, so that one can build a dataflow from this
        /// and only subsequently be informed how to communicate the drop.
        pub token: Rc<RefCell<Option<Rc<dyn std::any::Any>>>>,
    }

    impl SourceBoundary {
        /// Create a new boundary, from a name and a token.
        fn with_token(name: &str, token: Rc<dyn Any>) -> Self {
            let result = Self::new(name);
            *result.token.borrow_mut() = Some(token);
            result
        }

        /// Create a new boundary, from a name and a token.
        fn new(name: &str) -> Self {
            let ok_activator = RcActivator::new(format!("{name}-ok"), 1);
            let err_activator = RcActivator::new(format!("{name}-err"), 1);
            let ok_handle = ActivatedEventPusher::new(Rc::new(EventLink::new()), ok_activator);
            let err_handle = ActivatedEventPusher::new(Rc::new(EventLink::new()), err_activator);
            SourceBoundary {
                ok: ActivatedEventPusher::<_>::clone(&ok_handle),
                err: ActivatedEventPusher::<_>::clone(&err_handle),
                token: Rc::new(RefCell::new(None)),
            }
        }
    }
}
