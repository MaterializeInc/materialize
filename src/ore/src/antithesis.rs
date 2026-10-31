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

//! Integration with the Antithesis testing platform.
//!
//! Every function here is a no-op unless the `antithesis` feature is enabled.
//! Only the Antithesis build flavor enables it, so production binaries do not
//! link the SDK. With the feature on, process panics and logged soft
//! assertions are reported to Antithesis as failed properties, which turns the
//! existing assertion corpus into test properties without per-site changes.

use std::cell::Cell;

#[cfg(all(feature = "antithesis", target_os = "linux"))]
use antithesis_instrumentation as _;

/// Re-export of the SDK for the assertion macros below.
#[doc(hidden)]
#[cfg(feature = "antithesis")]
pub use antithesis_sdk as sdk;

std::thread_local! {
    static PANIC_EXPECTED: Cell<bool> = const { Cell::new(false) };
}

#[doc(hidden)]
#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! __antithesis_assert {
    ($assert:ident, $($arg:expr),+) => {
        $crate::antithesis::sdk::$assert!($($arg),+)
    };
}

#[doc(hidden)]
#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! __antithesis_assert {
    ($assert:ident, $($arg:expr),+) => {{
        let _ = ($($arg),+);
    }};
}

#[doc(hidden)]
#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! __antithesis_observe {
    ($cond:expr, $message:expr, $details:expr) => {{
        let observed = $cond;
        $crate::antithesis::observed(observed, $message, $details);
        $crate::antithesis::sdk::guidance_helper!(
            $crate::antithesis::sdk::assert::guidance::GuidanceType::Boolean,
            ::std::concat!("workload coverage: ", $message),
            true,
            $crate::antithesis::sdk::serde_json::json!({"observed": observed})
        );
    }};
}

#[doc(hidden)]
#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! __antithesis_observe {
    ($cond:expr, $message:expr, $details:expr) => {{
        let _ = ($cond, $message, $details);
    }};
}

/// Asserts that `$cond` holds whenever this site is evaluated.
///
/// Without the `antithesis` feature this only evaluates its arguments.
/// `$message` must be a string literal unique to the call site: Antithesis
/// catalogs assertions by message at build time. `$details` is a reference to
/// a `serde::Serialize` value.
#[macro_export]
macro_rules! antithesis_always {
    ($cond:expr, $message:expr, $details:expr) => {
        $crate::__antithesis_assert!(assert_always_or_unreachable, $cond, $message, $details)
    };
}

/// Records coverage and guides exploration toward `$cond` being true. See
/// [`antithesis_always`] for the argument contract.
#[macro_export]
macro_rules! antithesis_sometimes {
    ($cond:expr, $message:expr, $details:expr) => {
        $crate::__antithesis_observe!($cond, $message, $details)
    };
}

/// Records that this coverage milestone was reached. See
/// [`antithesis_always`] for the argument contract.
#[macro_export]
macro_rules! antithesis_reachable {
    ($message:expr, $details:expr) => {
        $crate::__antithesis_observe!(true, $message, $details)
    };
}

/// Asserts that this point is never reached. See [`antithesis_always`] for
/// the argument contract.
#[macro_export]
macro_rules! antithesis_unreachable {
    ($message:expr, $details:expr) => {
        $crate::__antithesis_assert!(assert_unreachable, $message, $details)
    };
}

/// Marks the next panic on this thread as an outcome the system produces by
/// design, so it is reported as reached rather than as a failure.
///
/// Call immediately before panicking on a path whose cause is outside the
/// process's control, such as giving up after an expired lease. Every other
/// panic is reported as a failed property.
pub fn expect_panic() {
    PANIC_EXPECTED.with(|expected| expected.set(true));
}

/// Initializes the SDK and registers product assertions and exploration guidance.
pub fn init() {
    #[cfg(feature = "antithesis")]
    antithesis_sdk::antithesis_init();
}

/// Records a coverage observation without defining a product failure condition.
#[doc(hidden)]
#[cfg(feature = "antithesis")]
pub fn observed<S: serde::Serialize + ?Sized>(condition: bool, message: &str, details: &S) {
    match antithesis_sdk::serde_json::to_value(details) {
        Ok(details) => antithesis_sdk::lifecycle::send_event(
            "workload_coverage",
            &antithesis_sdk::serde_json::json!({
                "message": message,
                "observed": condition,
                "details": details,
            }),
        ),
        Err(error) => antithesis_sdk::lifecycle::send_event(
            "workload_harness_error",
            &antithesis_sdk::serde_json::json!({
                "valid": false,
                "component": "native coverage",
                "message": "coverage details could not be serialized",
                "details": {"target": message, "error": error.to_string()},
            }),
        ),
    }
}

/// Reports a panic that is about to take down the process.
pub fn panicked(location: &str, message: &str) {
    let expected = PANIC_EXPECTED.with(|expected| expected.replace(false));
    #[cfg(feature = "antithesis")]
    {
        #[derive(serde::Serialize)]
        struct Details<'a> {
            location: &'a str,
            message: &'a str,
        }
        let details = Details { location, message };
        if expected {
            crate::antithesis_reachable!("Materialize process panicked by design", &details);
        } else {
            antithesis_sdk::assert_unreachable!("Materialize process panicked", &details);
        }
    }
    #[cfg(not(feature = "antithesis"))]
    let _ = (location, message, expected);
}

/// A `halt!` the system produces by design: its cause is outside the halting
/// process, and a restart is the intended recovery.
#[derive(Debug)]
pub struct DesignedHalt {
    /// Substrings that must all occur in the halt message.
    pub matches: &'static [&'static str],
    /// Why a correct system halts with this message.
    pub reason: &'static str,
}

/// The reviewed list of designed halts, matched against the formatted halt
/// message. A halt that matches no entry is reported as a failed property, so
/// a new `halt!` site is a finding until it is reviewed and added here.
///
/// An entry must name a cause outside the process. A designed halt that
/// repeats in a loop is still a bug, which the workload's crash-loop check
/// catches from pod restart history.
pub const DESIGNED_HALTS: &[DesignedHalt] = &[
    DesignedHalt {
        matches: &["fenced by envd @"],
        reason: "a newer environmentd (a restart or a promoted generation) took over \
                 the critical since handles of the storage collections",
    },
    DesignedHalt {
        matches: &["fenced by another process @"],
        reason: "a newer environmentd took over the txn-wal shard",
    },
    DesignedHalt {
        matches: &["fenced out old deployment; rebooting as leader"],
        reason: "0dt promotion: the read-only generation fenced the leader and reboots \
                 to run as leader",
    },
    DesignedHalt {
        matches: &["rebooting in read-only mode"],
        reason: "0dt catch-up: the leader ran DDL the read-only generation must reboot \
                 to pick up",
    },
    DesignedHalt {
        matches: &["after the reader has expired"],
        reason: "a persist listen outlived its reader lease, which takes a stall longer \
                 than the lease duration",
    },
    DesignedHalt {
        matches: &["has been expired out of state"],
        reason: "a fetch found its blob deleted and refreshed state shows the leasing \
                 reader expired, the halting twin of the designed fetch panic",
    },
    DesignedHalt {
        matches: &[
            "cannot serve requested as_of",
            "Since(Antichain { elements: [] })",
        ],
        reason: "a dataflow imported a collection whose shard was finalized because \
                 the collection was dropped concurrently",
    },
    DesignedHalt {
        matches: &["timely communication error:"],
        reason: "a peer process of the same replica died, and replica processes share \
                 fate; the peer reports its own cause",
    },
    DesignedHalt {
        matches: &["code at version", "cannot read data with version"],
        reason: "during an upgrade an older generation reads persist state that the \
                 newer version already wrote",
    },
    DesignedHalt {
        matches: &["in orchestrator >= next ID"],
        reason: "a competing environmentd of the same generation created replicas",
    },
    DesignedHalt {
        matches: &["new instance configuration not compatible"],
        reason: "a reconnecting controller sent an instance configuration this replica \
                 cannot adopt in place, and a fresh process is the designed way to adopt it",
    },
    DesignedHalt {
        matches: &["txns-shard write reached attempt limit"],
        reason: "another writer kept moving the txns shard upper, which happens when a \
                 promoted generation writes before this process observes its fencing",
    },
];

/// Halts that indicate a filed bug. Each gets its own property so a new
/// finding is not hidden behind a known one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnownHaltBug {
    /// A dataflow import's as_of is behind a live collection's since
    /// (PER-49, CPU-228, CPU-203).
    AsOfNotReadable,
    /// A fetch found its blob deleted while the leasing reader is still in
    /// state (PER-62).
    MissingBlobLiveLease,
    /// Bootstrap found a dependency with an empty since under a dependent
    /// that can still be written (SQL-603).
    EmptyDependencySince,
}

/// How a halt message is classified for Antithesis.
#[derive(Debug, PartialEq, Eq)]
pub enum HaltClass {
    /// Matches a [`DESIGNED_HALTS`] entry, carrying its reason.
    Designed(&'static str),
    /// Indicates a filed bug.
    KnownBug(KnownHaltBug),
    /// Matches neither list.
    Unexpected,
}

/// Classifies a formatted `halt!` message. [`DESIGNED_HALTS`] is checked
/// first, so a designed special case of a known-bug message wins.
pub fn classify_halt(message: &str) -> HaltClass {
    for designed in DESIGNED_HALTS {
        if designed.matches.iter().all(|m| message.contains(m)) {
            return HaltClass::Designed(designed.reason);
        }
    }
    if message.contains("cannot serve requested as_of") {
        HaltClass::KnownBug(KnownHaltBug::AsOfNotReadable)
    } else if message.contains("still present in state") {
        HaltClass::KnownBug(KnownHaltBug::MissingBlobLiveLease)
    } else if message.contains("dependency since frontier is empty while dependent upper") {
        HaltClass::KnownBug(KnownHaltBug::EmptyDependencySince)
    } else {
        HaltClass::Unexpected
    }
}

/// Reports a `halt!` that is about to exit the process, classified by
/// [`classify_halt`]. Called by `halt!` after its per-site event.
pub fn halted(location: &str, message: &str) {
    #[cfg(feature = "antithesis")]
    {
        #[derive(serde::Serialize)]
        struct Details<'a> {
            location: &'a str,
            message: &'a str,
            designed_reason: Option<&'a str>,
        }
        let class = classify_halt(message);
        let designed_reason = match class {
            HaltClass::Designed(reason) => Some(reason),
            _ => None,
        };
        let details = Details {
            location,
            message,
            designed_reason,
        };
        match class {
            HaltClass::Designed(_) => {
                crate::antithesis_reachable!("Materialize process halted by design", &details)
            }
            HaltClass::KnownBug(KnownHaltBug::AsOfNotReadable) => {
                antithesis_sdk::assert_unreachable!(
                    "Materialize process halted because a live collection cannot serve a dataflow's as_of (known: PER-49, CPU-228, CPU-203)",
                    &details
                )
            }
            HaltClass::KnownBug(KnownHaltBug::MissingBlobLiveLease) => {
                antithesis_sdk::assert_unreachable!(
                    "Materialize process halted on a missing blob despite a live reader lease (known: PER-62)",
                    &details
                )
            }
            HaltClass::KnownBug(KnownHaltBug::EmptyDependencySince) => {
                antithesis_sdk::assert_unreachable!(
                    "Materialize process halted on an empty dependency since under a writable dependent (known: SQL-603)",
                    &details
                )
            }
            HaltClass::Unexpected => antithesis_sdk::assert_unreachable!(
                "Materialize process halted for an unexpected reason",
                &details
            ),
        }
    }
    #[cfg(not(feature = "antithesis"))]
    let _ = (location, message);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One message per `halt!` site, formatted as the site formats it.
    #[mz_ore::test]
    fn halt_classification() {
        let designed = [
            "fenced by envd @ Some(Epoch(3)). ours = 2",
            "fenced by envd @ Ok(Some(PersistEpoch(Some(4)))). ours = Some(3)",
            "fenced by another process @ [1, 2]. ours = [0, 1]",
            "fenced out old deployment; rebooting as leader",
            "there have been DDL that we need to react to; rebooting in read-only mode. new ids: [u7]",
            "Listen on s123 received a batch Description { .. } advanced past the listen frontier \
             Antichain { elements: [5] } after the reader has expired. This can happen in exceptional cases",
            "unhandled error in compute_import: batch fetcher could not fetch batch part k: \
             reader r1 has been expired out of state; the process likely failed to heartbeat it",
            "unhandled error in compute_import: compute_import u5: s1 cannot serve requested as_of \
             Antichain { elements: [7] }: Since(Antichain { elements: [] })",
            "timely communication error: reading data: connection reset",
            "code at version 26.45.1 cannot read data with version 26.46.0",
            "found replica ID (u4) in orchestrator >= next ID (u4)",
            "new instance configuration not compatible with existing instance configuration:\na\nvs\nb",
            "txns-shard write reached attempt limit 8 after 8 conflicts, rebuilding",
        ];
        for message in designed {
            assert!(
                matches!(classify_halt(message), HaltClass::Designed(_)),
                "{message}"
            );
        }
        let known = [
            (
                "unhandled error in compute_import: compute_import u5: s1 cannot serve requested \
                 as_of Antichain { elements: [7] }: Since(Antichain { elements: [9] })",
                KnownHaltBug::AsOfNotReadable,
            ),
            (
                "unhandled error in compute_import: batch fetcher could not fetch batch part k: \
                 reader r1 is still present in state (Some(..)); a missing blob despite a live \
                 lease indicates a GC bug",
                KnownHaltBug::MissingBlobLiveLease,
            ),
            (
                "dependency since frontier is empty while dependent upper is not empty \
                 (dependent id=u3, write_frontier=[5], dependency_read_holds=[]), this indicates \
                 concurrent deletion of a collection",
                KnownHaltBug::EmptyDependencySince,
            ),
        ];
        for (message, bug) in known {
            assert_eq!(
                classify_halt(message),
                HaltClass::KnownBug(bug),
                "{message}"
            );
        }
        let unexpected = [
            "group committer terminated before a table registration could be submitted",
            "group committer terminated with a table forget outstanding",
            "unhandled error in compute_import: batch fetcher could not fetch batch part k: \
             reader r1: could not refresh state within 30s to diagnose the lease; partitioned \
             from consensus?",
            "a halt site nobody reviewed",
        ];
        for message in unexpected {
            assert_eq!(classify_halt(message), HaltClass::Unexpected, "{message}");
        }
    }
}
