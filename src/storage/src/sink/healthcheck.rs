// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Healthchecks for sinks
use std::fmt::Display;

/// Identify the state a worker for a given source can be at a point in time
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, PartialOrd, Ord)]
pub enum SinkStatus {
    /// Intended to be the state while the `clusterd` process is initializing itself
    /// Pushed by the Healthchecker on creation.
    Starting,
    /// State indicating the sink is running fine. Pushed automatically as long
    /// as rows are being consumed.
    Running,
    /// Represents a stall in the export process that might get resolved.
    /// Existing data is still available and queryable.
    Stalled {
        /// Error string used to populate the `error` column in the `mz_sink_status_history` table.
        error: String,
        /// Optional hint string which if present, will be added to the `details` column in
        /// the `mz_sink_status_history` table.
        hint: Option<String>,
    },
    // Managed by the storage controller.
    // Dropped,
}

impl crate::healthcheck::HealthStatus for SinkStatus {
    fn name(&self) -> &'static str {
        self.name()
    }
    fn error(&self) -> Option<&str> {
        self.error()
    }
    fn hint(&self) -> Option<&str> {
        self.hint()
    }
    fn should_halt(&self) -> bool {
        matches!(self, Self::Stalled { .. })
    }
    fn can_transition_from(&self, other: Option<&Self>) -> bool {
        Self::can_transition(other, self)
    }
    fn starting() -> Self {
        Self::Starting
    }
}

impl SinkStatus {
    fn name(&self) -> &'static str {
        match self {
            SinkStatus::Starting => "starting",
            SinkStatus::Running => "running",
            SinkStatus::Stalled { .. } => "stalled",
        }
    }

    fn error(&self) -> Option<&str> {
        match self {
            SinkStatus::Stalled { error, .. } => Some(&*error),
            SinkStatus::Starting => None,
            SinkStatus::Running => None,
        }
    }

    fn hint(&self) -> Option<&str> {
        match self {
            SinkStatus::Stalled { error: _, hint } => hint.as_deref(),
            SinkStatus::Starting => None,
            SinkStatus::Running => None,
        }
    }

    fn can_transition(old_status: Option<&SinkStatus>, new_status: &SinkStatus) -> bool {
        match old_status {
            None => true,
            // All other states can transition freely to any other state
            Some(
                old @ SinkStatus::Starting
                | old @ SinkStatus::Running
                | old @ SinkStatus::Stalled { .. },
            ) => old != new_status,
        }
    }
}

impl Display for SinkStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stalled() -> SinkStatus {
        SinkStatus::Stalled {
            error: "".into(),
            hint: None,
        }
    }

    #[mz_ore::test]
    fn test_can_transition() {
        let test_cases = [
            // Allowed transitions
            (
                Some(SinkStatus::Starting),
                vec![SinkStatus::Running, stalled()],
                true,
            ),
            (
                Some(SinkStatus::Running),
                vec![SinkStatus::Starting, stalled()],
                true,
            ),
            (
                Some(stalled()),
                vec![SinkStatus::Starting, SinkStatus::Running],
                true,
            ),
            (
                None,
                vec![SinkStatus::Starting, SinkStatus::Running, stalled()],
                true,
            ),
            // Forbidden transitions
            (
                Some(SinkStatus::Starting),
                vec![SinkStatus::Starting],
                false,
            ),
            (Some(SinkStatus::Running), vec![SinkStatus::Running], false),
            (Some(stalled()), vec![stalled()], false),
        ];

        for test_case in test_cases {
            run_test(test_case)
        }

        fn run_test(test_case: (Option<SinkStatus>, Vec<SinkStatus>, bool)) {
            let (from_status, to_status, allowed) = test_case;
            for status in to_status {
                assert_eq!(
                    allowed,
                    SinkStatus::can_transition(from_status.as_ref(), &status),
                    "Bad can_transition: {from_status:?} -> {status:?}; expected allowed: {allowed:?}"
                );
            }
        }
    }
}
