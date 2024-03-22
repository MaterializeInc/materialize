// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use derivative::Derivative;
use itertools::Itertools;
use mz_repr::TimestampManipulation;
use serde::Serialize;
use timely::progress::frontier::AntichainRef;
use timely::progress::{Antichain, Timestamp};

/// Compaction policies for collections maintained by `Controller`.
///
/// NOTE(benesch): this might want to live somewhere besides the storage crate,
/// because it is fundamental to both storage and compute.
#[derive(Clone, Derivative, Serialize)]
#[derivative(Debug)]
pub enum ReadPolicy<T> {
    /// No-one has yet requested a `ReadPolicy` from us, which means that we can
    /// still change the implied_capability/the collection since if we need
    /// to.
    NoPolicy { initial_since: Antichain<T> },
    /// Maintain the collection as valid from this frontier onward.
    ValidFrom(Antichain<T>),
    /// Maintain the collection as valid from a function of the write frontier.
    ///
    /// This function will only be re-evaluated when the write frontier changes.
    /// If the intended behavior is to change in response to external signals,
    /// consider using the `ValidFrom` variant to manually pilot compaction.
    ///
    /// The `Arc` makes the function cloneable.
    LagWriteFrontier(
        #[derivative(Debug = "ignore")]
        #[serde(skip)]
        Arc<dyn Fn(AntichainRef<T>) -> Antichain<T> + Send + Sync>,
    ),
    /// Allows one to express multiple read policies, taking the least of
    /// the resulting frontiers.
    Multiple(Vec<ReadPolicy<T>>),
}

impl<T> ReadPolicy<T>
where
    T: Timestamp + TimestampManipulation,
{
    /// Creates a read policy that lags the write frontier "by one".
    pub fn step_back() -> Self {
        Self::LagWriteFrontier(Arc::new(move |upper| {
            if upper.is_empty() {
                Antichain::from_elem(Timestamp::minimum())
            } else {
                let stepped_back = upper
                    .to_owned()
                    .into_iter()
                    .map(|time| {
                        if time == T::minimum() {
                            time
                        } else {
                            time.step_back().unwrap()
                        }
                    })
                    .collect_vec();
                stepped_back.into()
            }
        }))
    }
}

impl ReadPolicy<mz_repr::Timestamp> {
    /// Creates a read policy that lags the write frontier by the indicated amount, rounded down to (at most) the specified value.
    /// The rounding down is done to reduce the number of changes the capability undergoes.
    pub fn lag_writes_by(lag: mz_repr::Timestamp, max_granularity: mz_repr::Timestamp) -> Self {
        Self::LagWriteFrontier(Arc::new(move |upper| {
            if upper.is_empty() {
                Antichain::from_elem(Timestamp::minimum())
            } else {
                // Subtract the lag from the time, and then round down to a multiple of `granularity` to cut chatter.
                let mut time = upper[0];
                if lag != mz_repr::Timestamp::default() {
                    time = time.saturating_sub(lag);
                    // It makes little sense to refuse to compact if the user genuinely
                    // sets a smaller compaction window than the default, so honor it here.
                    let granularity = std::cmp::min(lag, max_granularity);
                    time = time.saturating_sub(time % granularity);
                }
                Antichain::from_elem(time)
            }
        }))
    }
}

impl<T: Timestamp> ReadPolicy<T> {
    pub fn frontier(&self, write_frontier: AntichainRef<T>) -> Antichain<T> {
        match self {
            ReadPolicy::NoPolicy { initial_since } => initial_since.clone(),
            ReadPolicy::ValidFrom(frontier) => frontier.clone(),
            ReadPolicy::LagWriteFrontier(logic) => logic(write_frontier),
            ReadPolicy::Multiple(policies) => {
                let mut frontier = Antichain::new();
                for policy in policies.iter() {
                    for time in policy.frontier(write_frontier).iter() {
                        frontier.insert(time.clone());
                    }
                }
                frontier
            }
        }
    }
}
