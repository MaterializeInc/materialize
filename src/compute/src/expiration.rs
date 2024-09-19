// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helper functions to detect expired frontiers.

use differential_dataflow::{AsCollection, Collection};
use timely::dataflow::operators::InspectCore;
use timely::dataflow::{Scope, StreamCore};
use timely::progress::frontier::AntichainRef;
use timely::Container;

/// Panics if the frontier of a [`StreamCore`] exceeds a given `expiration` time.
pub fn expire_stream_at<G, D>(
    stream: &StreamCore<G, D>,
    expiration: G::Timestamp,
) -> StreamCore<G, D>
where
    G: Scope,
    G::Timestamp: timely::progress::Timestamp,
    D: Container,
{
    stream.inspect_container(move |data_or_frontier| {
        if let Err(frontier) = data_or_frontier {
            assert!(
                frontier.is_empty() || AntichainRef::new(frontier).less_than(&expiration),
                "frontier {frontier:?} has exceeded expiration {expiration:?}!",
            );
        }
    })
}

/// Wrapper around [`expire_stream_at`] for a [`Collection`].
pub fn expire_collection_at<G, D, R>(
    collection: &Collection<G, D, R>,
    expiration: G::Timestamp,
) -> Collection<G, D, R>
where
    G: Scope,
    G::Timestamp: timely::progress::Timestamp,
    D: Clone + 'static,
    R: Clone + 'static,
{
    expire_stream_at(&collection.inner, expiration).as_collection()
}
