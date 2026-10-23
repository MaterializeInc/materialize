// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Classifying the errors Materialize returns during a subscription.
//!
//! Several conditions share a SQLSTATE: a dataflow error, a result over
//! `max_result_size`, and a genuine internal error are all `XX000`, because an
//! error raised inside a running subscribe reaches the client as
//! `AdapterError::Unstructured` (`src/adapter/src/active_compute_sink.rs`).
//! History loss and a result over `max_result_size` can only be told apart by
//! message, and all message matching lives in [`classify`], pinned by the
//! conformance vectors.
//!
//! The rules match Materialize v26.39 and later. v26.39 is the first release
//! that retires a subscribe whose client fell behind (#37905).

use serde::{Deserialize, Serialize};

use crate::identity::IdentityChange;

/// A class of failure, which decides how a consumer recovers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ErrorKind {
    /// The connection failed or the session ended. Reconnect and resume.
    ///
    /// Every failure without a server error is transient, including one in
    /// TLS or name resolution, so a consumer that retries transient errors
    /// reports a misconfiguration only after its retries run out.
    Transient,
    /// The server refused the session's credentials, which happens when they
    /// expire and when they are wrong. Reconnect once with fresh ones, and
    /// treat a second refusal as fatal.
    CredentialsExpired,
    /// An operator canceled the statement, or a statement timeout fired.
    Canceled,
    /// The server retired the subscribe because the client fell behind.
    FellBehind,
    /// The updates between two progress rows exceeded `max_result_size`.
    /// Retrying repeats the failure.
    ResultTooLarge,
    /// The resume point is older than the object's readable history.
    HistoryLost,
    /// The subscribed object, or the cluster it ran on, was dropped.
    ObjectDropped,
    /// The subscribed object's output changed shape.
    SchemaMismatch,
    /// The dataflow raised an error. It repeats until the data or the object
    /// changes, so retrying cannot fix it.
    StreamPoisoned,
    /// An index on the subscribing cluster serves the object, so its history
    /// follows the index's retention, one second by default, instead of the
    /// object's.
    IndexedTarget,
    /// Anything else, including authentication and SQL errors.
    Fatal,
}

/// When an error arrived, relative to the cursor's first progress row.
///
/// A driver that drops a `FETCH`'s rows when the same `FETCH` ends in an error
/// reports a dataflow error from the first `FETCH` as `Starting`. It then
/// classifies as [`ErrorKind::Fatal`] instead of
/// [`ErrorKind::StreamPoisoned`], and both stop the subscription.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// Before the first progress row: `DECLARE` or the first `FETCH`, which
    /// plans and checks the statement again.
    Starting,
    /// After the first progress row.
    Streaming,
}

/// The classification of an error, with any check that must run before it is
/// final.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "action", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Classification {
    /// The error is of this kind.
    Kind {
        /// The kind.
        kind: ErrorKind,
    },
    /// The error can come from the subscribed object changing. Resolve its
    /// name again, compare the result with the stored fingerprint, and
    /// resolve the check with the change found.
    CheckObject {
        /// The kind if the object is unchanged.
        otherwise: ErrorKind,
    },
    /// History was lost, unless an index on the subscribing cluster now serves
    /// the object, which rejects the `AS OF` the same way. List the indexes
    /// to resolve the check.
    CheckIndex,
}

/// Classifies an error by its SQLSTATE, its message, and when it arrived.
///
/// `sqlstate` is `None` for a failure with no server error, such as a closed
/// connection.
pub fn classify(sqlstate: Option<&str>, message: &str, phase: Phase) -> Classification {
    use Classification::{CheckIndex, CheckObject, Kind};
    use ErrorKind::*;

    let Some(sqlstate) = sqlstate else {
        return Kind { kind: Transient };
    };
    match sqlstate {
        // balancerd refuses connections with 08004 while `environmentd`
        // restarts. 25P03 ends a session left idle in its transaction for
        // `idle_in_transaction_session_timeout`.
        "08000" | "08001" | "08003" | "08004" | "08006" | "25P03" => Kind { kind: Transient },
        // 28P01 is a wrong password, from balancerd and pgwire.
        "28000" | "28P01" => Kind {
            kind: CredentialsExpired,
        },
        "57014" => Kind { kind: Canceled },
        // The adapter's own result-size error is also 53200, but a running
        // subscribe reports its size error as XX000, below.
        "53200" => Kind { kind: FellBehind },
        "22000" if message.contains("could not find a valid timestamp for the query") => CheckIndex,
        // "total result exceeds max size of" from `PendingSubscribe::stash` in
        // `src/compute-client/src/service.rs`, and "result exceeds max size
        // of" from a replica (`SubscribeBatch::to_error_if_exceeds`).
        "XX000" if message.contains("result exceeds max size of") => Kind {
            kind: ResultTooLarge,
        },
        "42704" => CheckObject {
            otherwise: ObjectDropped,
        },
        // A cluster scaled to no replicas, as during maintenance, refuses a
        // new subscribe until a replica returns (`NoClusterReplicasAvailable`
        // in `src/adapter/src/error.rs`).
        "0A000" if message.contains("has no replicas available to service request") => {
            Kind { kind: Transient }
        }
        // A dropped id fails with XX000, and changed columns fail at the
        // first FETCH with this 0A000 (`ChangedPlan`). Other 0A000 errors are
        // not about the object.
        "0A000"
            if phase == Phase::Starting
                && message.contains("cached plan must not change result type") =>
        {
            CheckObject {
                otherwise: SchemaMismatch,
            }
        }
        "XX000" => match phase {
            Phase::Starting => CheckObject { otherwise: Fatal },
            Phase::Streaming => Kind {
                kind: StreamPoisoned,
            },
        },
        _ => Kind { kind: Fatal },
    }
}

/// What a [`Classification::CheckIndex`] comes to, given the indexes on the
/// subscribing cluster that serve the object.
pub fn resolve_index_check<S: AsRef<str>>(indexes: &[S]) -> ErrorKind {
    if indexes.is_empty() {
        ErrorKind::HistoryLost
    } else {
        ErrorKind::IndexedTarget
    }
}

/// What a [`Classification::CheckObject`] comes to, given how the object its
/// name resolves to differs from the stored fingerprint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ObjectOutcome {
    /// The error stands as this kind.
    Error {
        /// The kind.
        kind: ErrorKind,
    },
    /// See [`IdentityChange::Replaced`].
    Replaced,
    /// See [`IdentityChange::Swapped`]. The sink stops, or takes a new
    /// snapshot of the new object if it follows the name.
    Swapped,
}

/// Resolves a [`Classification::CheckObject`] with the identity change found.
pub fn resolve_object_check(otherwise: ErrorKind, change: IdentityChange) -> ObjectOutcome {
    match change {
        IdentityChange::Unchanged => ObjectOutcome::Error { kind: otherwise },
        IdentityChange::Dropped => ObjectOutcome::Error {
            kind: ErrorKind::ObjectDropped,
        },
        IdentityChange::SchemaChanged => ObjectOutcome::Error {
            kind: ErrorKind::SchemaMismatch,
        },
        IdentityChange::Replaced => ObjectOutcome::Replaced,
        IdentityChange::Swapped => ObjectOutcome::Swapped,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kind(sqlstate: Option<&str>, message: &str, phase: Phase) -> Option<ErrorKind> {
        match classify(sqlstate, message, phase) {
            Classification::Kind { kind } => Some(kind),
            _ => None,
        }
    }

    #[test]
    fn connection_failures_are_transient() {
        for sqlstate in [
            None,
            Some("08000"),
            Some("08004"),
            Some("08006"),
            Some("25P03"),
        ] {
            assert_eq!(
                kind(sqlstate, "", Phase::Streaming),
                Some(ErrorKind::Transient),
                "{sqlstate:?}"
            );
        }
        assert_eq!(
            kind(Some("08P01"), "", Phase::Streaming),
            Some(ErrorKind::Fatal)
        );
    }

    #[test]
    fn history_loss_needs_an_index_check() {
        let message = "could not find a valid timestamp for the query";
        for phase in [Phase::Starting, Phase::Streaming] {
            assert_eq!(
                classify(Some("22000"), message, phase),
                Classification::CheckIndex
            );
        }
        assert_eq!(
            kind(
                Some("22000"),
                "subscription lower bound (`AS OF`) is greater than its upper bound (`UP TO`)",
                Phase::Starting
            ),
            Some(ErrorKind::Fatal)
        );
    }

    #[test]
    fn xx000_depends_on_message_and_phase() {
        for too_large in [
            "total result exceeds max size of 1073.7 MB",
            "result exceeds max size of 1073.7 MB",
        ] {
            for phase in [Phase::Starting, Phase::Streaming] {
                assert_eq!(
                    kind(Some("XX000"), too_large, phase),
                    Some(ErrorKind::ResultTooLarge)
                );
            }
        }
        assert_eq!(
            kind(Some("XX000"), "division by zero", Phase::Streaming),
            Some(ErrorKind::StreamPoisoned)
        );
        assert_eq!(
            classify(Some("XX000"), "invalid id u5", Phase::Starting),
            Classification::CheckObject {
                otherwise: ErrorKind::Fatal
            }
        );
    }

    #[test]
    fn a_changed_plan_is_a_schema_change_only_while_starting() {
        assert_eq!(
            classify(
                Some("0A000"),
                "cached plan must not change result type",
                Phase::Starting
            ),
            Classification::CheckObject {
                otherwise: ErrorKind::SchemaMismatch
            }
        );
        assert_eq!(
            kind(Some("0A000"), "", Phase::Streaming),
            Some(ErrorKind::Fatal)
        );
        assert_eq!(
            kind(
                Some("0A000"),
                "CLUSTER \"c\" has no replicas available to service request",
                Phase::Starting
            ),
            Some(ErrorKind::Transient)
        );
    }

    #[test]
    fn an_object_check_resolves_by_identity_change() {
        let resolve = |change| resolve_object_check(ErrorKind::Fatal, change);
        assert_eq!(
            resolve(IdentityChange::Unchanged),
            ObjectOutcome::Error {
                kind: ErrorKind::Fatal
            }
        );
        assert_eq!(
            resolve(IdentityChange::Dropped),
            ObjectOutcome::Error {
                kind: ErrorKind::ObjectDropped
            }
        );
        assert_eq!(
            resolve(IdentityChange::SchemaChanged),
            ObjectOutcome::Error {
                kind: ErrorKind::SchemaMismatch
            }
        );
        assert_eq!(resolve(IdentityChange::Replaced), ObjectOutcome::Replaced);
        assert_eq!(resolve(IdentityChange::Swapped), ObjectOutcome::Swapped);
    }

    #[test]
    fn json_form() {
        assert_eq!(
            serde_json::to_value(Classification::CheckObject {
                otherwise: ErrorKind::ObjectDropped
            })
            .unwrap(),
            serde_json::json!({"action": "check_object", "otherwise": "object_dropped"})
        );
    }
}
