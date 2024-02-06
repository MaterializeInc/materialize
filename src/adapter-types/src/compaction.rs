// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::TryFromIntError;
use std::time::Duration;

use mz_repr::Timestamp;
use mz_storage_types::read_policy::ReadPolicy;
use serde::Serialize;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};

/// `DEFAULT_LOGICAL_COMPACTION_WINDOW`, in milliseconds.
/// The default is set to a second to track the default timestamp frequency for sources.
const DEFAULT_LOGICAL_COMPACTION_WINDOW_MILLIS: u64 = 1000;

/// `DEFAULT_LOGICAL_COMPACTION_WINDOW` as an `EpochMillis` timestamp
const DEFAULT_LOGICAL_COMPACTION_WINDOW_TS: Timestamp =
    Timestamp::new(DEFAULT_LOGICAL_COMPACTION_WINDOW_MILLIS);

/// The value to round all `since` frontiers to.
/// We pick 1s somewhat arbitrarily, but matching historical practice.
// TODO[btv] If we want to further reduce capability chatter, we can implement the design in
// `20230322_metrics_since_granularity.md`, making it configurable.
pub const SINCE_GRANULARITY: mz_repr::Timestamp = mz_repr::Timestamp::new(1000);

// A common type (that is usable by the sql crate and also can implement various methods on types in
// storage) to express compaction windows.
#[derive(Clone, Default, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum CompactionWindow {
    /// Unspecified by the user, use a system-provided default.
    #[default]
    Default,
    /// Disable compaction.
    DisableCompaction,
    /// Create a compaction window for a specified duration.
    Duration(Timestamp),
}

impl CompactionWindow {
    pub fn lag_from(&self, from: Timestamp) -> Timestamp {
        let lag = match self {
            CompactionWindow::Default => DEFAULT_LOGICAL_COMPACTION_WINDOW_TS,
            CompactionWindow::DisableCompaction => return Timestamp::minimum(),
            CompactionWindow::Duration(d) => *d,
        };
        from.saturating_sub(lag)
    }
}

impl From<CompactionWindow> for ReadPolicy<Timestamp> {
    fn from(value: CompactionWindow) -> Self {
        let time = match value {
            CompactionWindow::Default => DEFAULT_LOGICAL_COMPACTION_WINDOW_TS,
            CompactionWindow::Duration(time) => time,
            CompactionWindow::DisableCompaction => {
                return ReadPolicy::ValidFrom(Antichain::from_elem(Timestamp::minimum()))
            }
        };
        ReadPolicy::lag_writes_by(time, SINCE_GRANULARITY)
    }
}

impl From<CompactionWindow> for ReadCapability<Timestamp> {
    fn from(value: CompactionWindow) -> Self {
        let policy: ReadPolicy<Timestamp> = value.into();
        policy.into()
    }
}

impl TryFrom<Duration> for CompactionWindow {
    type Error = TryFromIntError;

    fn try_from(value: Duration) -> Result<Self, Self::Error> {
        Ok(Self::Duration(value.try_into()?))
    }
}

/// Information about the read capability requirements of a collection.
///
/// This type tracks both a default policy, as well as various holds that may
/// be expressed, as by transactions to ensure collections remain readable.
#[derive(Debug)]
pub struct ReadCapability<T = mz_repr::Timestamp>
where
    T: timely::progress::Timestamp,
{
    /// The default read policy for the collection when no holds are present.
    pub base_policy: ReadPolicy<T>,
    /// Holds expressed by transactions, that should prevent compaction.
    pub holds: MutableAntichain<T>,
}

impl<T: timely::progress::Timestamp> From<ReadPolicy<T>> for ReadCapability<T> {
    fn from(base_policy: ReadPolicy<T>) -> Self {
        Self {
            base_policy,
            holds: MutableAntichain::new(),
        }
    }
}

impl<T: timely::progress::Timestamp> ReadCapability<T> {
    /// Acquires the effective read policy, reflecting both the base policy and any holds.
    pub fn policy(&self) -> ReadPolicy<T> {
        // TODO: This could be "optimized" when `self.holds.frontier` is empty.
        ReadPolicy::Multiple(vec![
            ReadPolicy::ValidFrom(self.holds.frontier().to_owned()),
            self.base_policy.clone(),
        ])
    }
}
