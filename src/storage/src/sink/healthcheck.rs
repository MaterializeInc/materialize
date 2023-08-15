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
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SinkStatus {
    /// Initial state of a Sink during initialization.
    Setup,
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
    /// Represents a irrecoverable failure in the pipeline. Data from this collection
    /// is not queryable any longer. The only valid transition from Failed is Dropped.
    Failed {
        /// Error string used to populate the `error` column in the `mz_sink_status_history` table.
        error: String,
        /// Optional hint string which if present, will be added to the `details` column in
        /// the `mz_sink_status_history` table.
        hint: Option<String>,
    },
    /// Represents a sink that was dropped.
    Dropped,
}

impl SinkStatus {
    /// Name of the update
    pub fn name(&self) -> &'static str {
        match self {
            SinkStatus::Setup => "setup",
            SinkStatus::Starting => "starting",
            SinkStatus::Running => "running",
            SinkStatus::Stalled { .. } => "stalled",
            SinkStatus::Failed { .. } => "failed",
            SinkStatus::Dropped => "dropped",
        }
    }

    /// Source error, if any
    pub fn error(&self) -> Option<&str> {
        match self {
            SinkStatus::Stalled { error, .. } => Some(&*error),
            SinkStatus::Failed { error, .. } => Some(&*error),
            SinkStatus::Setup => None,
            SinkStatus::Starting => None,
            SinkStatus::Running => None,
            SinkStatus::Dropped => None,
        }
    }

    /// Extra info about the status for error statuses
    pub fn hint(&self) -> Option<&str> {
        match self {
            SinkStatus::Stalled { error: _, hint } => hint.as_deref(),
            SinkStatus::Failed { error: _, hint } => hint.as_deref(),
            SinkStatus::Setup => None,
            SinkStatus::Starting => None,
            SinkStatus::Running => None,
            SinkStatus::Dropped => None,
        }
    }

    /// Whether or not this sink status can transition to the specified new status
    pub fn can_transition(old_status: Option<&SinkStatus>, new_status: &SinkStatus) -> bool {
        match old_status {
            None => true,
            // Failed can only transition to Dropped
            Some(SinkStatus::Failed { .. }) => matches!(new_status, SinkStatus::Dropped),
            // Dropped is a terminal state
            Some(SinkStatus::Dropped) => false,
            // All other states can transition freely to any other state
            Some(
                old @ SinkStatus::Setup
                | old @ SinkStatus::Starting
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

    fn failed() -> SinkStatus {
        SinkStatus::Failed {
            error: "".into(),
            hint: None,
        }
    }

    #[mz_ore::test]
    fn test_can_transition() {
        let test_cases = [
            // Allowed transitions
            (
                Some(SinkStatus::Setup),
                vec![
                    SinkStatus::Starting,
                    SinkStatus::Running,
                    stalled(),
                    failed(),
                    SinkStatus::Dropped,
                ],
                true,
            ),
            (
                Some(SinkStatus::Starting),
                vec![
                    SinkStatus::Setup,
                    SinkStatus::Running,
                    stalled(),
                    failed(),
                    SinkStatus::Dropped,
                ],
                true,
            ),
            (
                Some(SinkStatus::Running),
                vec![
                    SinkStatus::Setup,
                    SinkStatus::Starting,
                    stalled(),
                    failed(),
                    SinkStatus::Dropped,
                ],
                true,
            ),
            (
                Some(stalled()),
                vec![
                    SinkStatus::Setup,
                    SinkStatus::Starting,
                    SinkStatus::Running,
                    failed(),
                    SinkStatus::Dropped,
                ],
                true,
            ),
            (Some(failed()), vec![SinkStatus::Dropped], true),
            (
                None,
                vec![
                    SinkStatus::Setup,
                    SinkStatus::Starting,
                    SinkStatus::Running,
                    stalled(),
                    failed(),
                    SinkStatus::Dropped,
                ],
                true,
            ),
            // Forbidden transitions
            (Some(SinkStatus::Setup), vec![SinkStatus::Setup], false),
            (
                Some(SinkStatus::Starting),
                vec![SinkStatus::Starting],
                false,
            ),
            (Some(SinkStatus::Running), vec![SinkStatus::Running], false),
            (Some(stalled()), vec![stalled()], false),
            (
                Some(failed()),
                vec![
                    SinkStatus::Setup,
                    SinkStatus::Starting,
                    SinkStatus::Running,
                    stalled(),
                    failed(),
                ],
                false,
            ),
            (
                Some(SinkStatus::Dropped),
                vec![
                    SinkStatus::Setup,
                    SinkStatus::Starting,
                    SinkStatus::Running,
                    stalled(),
                    failed(),
                    SinkStatus::Dropped,
                ],
                false,
            ),
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
