// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared compute readiness, peek summaries and introspection derivation.
//!
//! Maintained execution belongs to catalog-following replicas. These helpers
//! describe observations without owning instances, replicas or read capabilities.

use mz_expr::row::RowCollection;
use mz_ore::cast::CastFrom;
use mz_repr::{Timestamp, frontier_within_lag};
use serde::{Deserialize, Serialize};
use timely::progress::Antichain;

use crate::protocol::response::PeekResponse;

pub mod error;
mod introspection;
pub use introspection::refresh_introspection;

/// A collection's hydration and catch-up status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectionReadiness {
    /// Hydrated and within the requested lag allowance.
    Ready,
    /// Hydrated, but behind the requested frontier.
    Lagging {
        /// The gap in timestamp ticks, or `None` when awaiting completion.
        /// Ticks are milliseconds only on the epoch-milliseconds timeline.
        lag: Option<u64>,
    },
    /// Not yet hydrated.
    Unhydrated,
}

impl CollectionReadiness {
    /// Classifies progress against an optional reference and lag allowance.
    ///
    /// The caller selects comparable frontiers: graceful replacement uses
    /// per-replica output frontiers, whereas 0dt uses collection write frontiers.
    /// No lag requirement means hydration alone. An empty reference requires
    /// an empty frontier, since completion cannot be matched by finite progress.
    pub fn classify(
        hydrated: bool,
        frontier: &Antichain<Timestamp>,
        lag_requirement: Option<(&Antichain<Timestamp>, Timestamp)>,
    ) -> Self {
        if !hydrated {
            Self::Unhydrated
        } else if lag_requirement
            .is_some_and(|(reference, lag)| !frontier_within_lag(frontier, reference, lag))
        {
            let reference = lag_requirement.expect("lag requirement was checked").0;
            let lag =
                frontier
                    .as_option()
                    .zip(reference.as_option())
                    .map(|(frontier, reference)| {
                        u64::from(*reference).saturating_sub(u64::from(*frontier))
                    });
            Self::Lagging { lag }
        } else {
            Self::Ready
        }
    }
}

/// Notification and summary of a received and forwarded [`crate::protocol::response::ComputeResponse::PeekResponse`].
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum PeekNotification {
    /// Returned rows of a successful peek.
    Success {
        /// Number of rows in the returned peek result.
        rows: u64,
        /// Size of the returned peek result in bytes.
        result_size: u64,
    },
    /// Error of an unsuccessful peek, including the reason for the error.
    Error(String),
    /// The peek was canceled.
    Canceled,
}

impl PeekNotification {
    /// Construct a new [`PeekNotification`] from a [`PeekResponse`]. The `offset` and `limit`
    /// parameters are used to calculate the number of rows in the peek result.
    pub fn new(peek_response: &PeekResponse, offset: usize, limit: Option<usize>) -> Self {
        match peek_response {
            PeekResponse::Rows(rows) => {
                let num_rows = u64::cast_from(RowCollection::offset_limit(
                    rows.iter().map(|r| r.count()).sum(),
                    offset,
                    limit,
                ));
                let result_size = u64::cast_from(rows.iter().map(|r| r.byte_len()).sum::<usize>());

                tracing::trace!(?num_rows, ?result_size, "inline peek result");

                Self::Success {
                    rows: num_rows,
                    result_size,
                }
            }
            PeekResponse::Stashed(stashed_response) => {
                let rows = stashed_response.num_rows(offset, limit);
                let result_size = stashed_response.size_bytes();

                tracing::trace!(?rows, ?result_size, "stashed peek result");

                Self::Success {
                    rows: u64::cast_from(rows),
                    result_size: u64::cast_from(result_size),
                }
            }
            PeekResponse::Error(err) => Self::Error(err.to_string()),
            PeekResponse::Canceled => Self::Canceled,
        }
    }
}
