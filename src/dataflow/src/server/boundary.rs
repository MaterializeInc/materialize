// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

/// Traits and types for capturing and replaying collections of data.
use std::any::Any;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Duration;

use differential_dataflow::AsCollection;
use differential_dataflow::Collection;
use timely::dataflow::operators::capture::EventLink;
use timely::dataflow::Scope;

use mz_dataflow_types::DataflowError;
use mz_expr::GlobalId;
use mz_repr::{Diff, Row};

use crate::activator::RcActivator;
use crate::replay::MzReplay;
use crate::server::ActivatedEventPusher;
use crate::server::SourceBoundary;

/// A type that can capture a specific source.
pub trait StorageCapture {
    /// Captures the source and binds to `id`.
    fn capture<G: Scope<Timestamp = mz_repr::Timestamp>>(
        &mut self,
        id: GlobalId,
        ok: Collection<G, Row, Diff>,
        err: Collection<G, DataflowError, Diff>,
        token: Rc<dyn Any>,
        name: &str,
    );
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
        id: GlobalId,
        scope: &mut G,
        name: &str,
    ) -> Option<(
        Collection<G, Row, Diff>,
        Collection<G, DataflowError, Diff>,
        Rc<dyn Any>,
    )>;
}

/// A simple boundary that uses activated event linked lists.
pub struct EventLinkBoundary {
    sources: BTreeMap<GlobalId, crate::server::SourceBoundary>,
}

impl EventLinkBoundary {
    pub fn new() -> Self {
        Self {
            sources: BTreeMap::new(),
        }
    }
}

impl StorageCapture for EventLinkBoundary {
    fn capture<G: Scope<Timestamp = mz_repr::Timestamp>>(
        &mut self,
        id: GlobalId,
        ok: Collection<G, Row, Diff>,
        err: Collection<G, DataflowError, Diff>,
        token: Rc<dyn Any>,
        name: &str,
    ) {
        let ok_activator = RcActivator::new(format!("{name}-ok"), 1);
        let err_activator = RcActivator::new(format!("{name}-err"), 1);

        let ok_handle = ActivatedEventPusher::new(Rc::new(EventLink::new()), ok_activator);
        let err_handle = ActivatedEventPusher::new(Rc::new(EventLink::new()), err_activator);

        self.sources.insert(
            id,
            SourceBoundary {
                ok: ActivatedEventPusher::<_>::clone(&ok_handle),
                err: ActivatedEventPusher::<_>::clone(&err_handle),
                token,
            },
        );

        use timely::dataflow::operators::Capture;
        ok.inner.capture_into(ok_handle);
        err.inner.capture_into(err_handle);
    }
}

impl ComputeReplay for EventLinkBoundary {
    fn replay<G: Scope<Timestamp = mz_repr::Timestamp>>(
        &mut self,
        id: GlobalId,
        scope: &mut G,
        name: &str,
    ) -> Option<(
        Collection<G, Row, Diff>,
        Collection<G, DataflowError, Diff>,
        Rc<dyn Any>,
    )> {
        self.sources.remove(&id).map(|source| {
            let ok = Some(source.ok.inner)
                .mz_replay(
                    scope,
                    &format!("{name}-ok"),
                    Duration::MAX,
                    source.ok.activator,
                )
                .as_collection();
            let err = Some(source.err.inner)
                .mz_replay(
                    scope,
                    &format!("{name}-err"),
                    Duration::MAX,
                    source.err.activator,
                )
                .as_collection();
            (ok, err, source.token)
        })
    }
}
