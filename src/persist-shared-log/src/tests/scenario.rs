// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared vocabulary for simulation testing and (future) Stateright model checking.
//!
//! Defines the operations, observations, and sequential specification (oracle)
//! for the persist-backed shared log. The oracle is an independent reimplementation
//! of the learner's [`StateMachine`](crate::persist_log::learner) semantics — if
//! both agree, we have confidence; if they diverge, we have a bug.
//!
//! The [`SharedLogOracle`] implements Stateright's [`SequentialSpec`] trait, so it
//! can serve as the linearizability reference for both DST (via
//! [`LinearizabilityTester`]) and a future Stateright model.
//!
//! [`SequentialSpec`]: stateright::semantics::SequentialSpec
//! [`LinearizabilityTester`]: stateright::semantics::LinearizabilityTester

use std::collections::BTreeMap;

use stateright::semantics::SequentialSpec;

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

/// An operation against the shared log.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum SharedLogOp {
    Cas {
        shard: String,
        expected: Option<u64>,
        seqno: u64,
        data: Vec<u8>,
    },
    Head {
        shard: String,
    },
    Scan {
        shard: String,
        from: u64,
        limit: u64,
    },
    Truncate {
        shard: String,
        seqno: u64,
    },
}

// ---------------------------------------------------------------------------
// Observations
// ---------------------------------------------------------------------------

/// The result of an operation.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SharedLogObservation {
    Cas { committed: bool },
    Head { data: Option<VersionedData> },
    Scan { data: Vec<VersionedData> },
    Truncate(Result<u64, String>),
}

/// A versioned data entry.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct VersionedData {
    pub seqno: u64,
    pub data: Vec<u8>,
}

// ---------------------------------------------------------------------------
// System events (not linearized)
// ---------------------------------------------------------------------------

/// System-level events that are recorded in the trace but don't participate
/// in linearizability checking.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SystemEvent {
    CrashAndRecover,
    RetractionSweep,
}

// ---------------------------------------------------------------------------
// Oracle (sequential specification)
// ---------------------------------------------------------------------------

/// Sequential specification / oracle for the shared log.
///
/// Independent reimplementation of the learner's `StateMachine` semantics.
/// Operates on scenario types, not protobuf. Deliberately separate from the
/// production code so that a bug in one surfaces as a mismatch.
#[derive(Clone, Debug, Default, Hash, PartialEq, Eq)]
pub struct SharedLogOracle {
    shards: BTreeMap<String, Vec<VersionedData>>,
}

impl SharedLogOracle {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply an operation, mutating state and returning the observation.
    pub fn apply(&mut self, op: &SharedLogOp) -> SharedLogObservation {
        match op {
            SharedLogOp::Cas {
                shard,
                expected,
                seqno,
                data,
            } => {
                let current_seqno = self
                    .shards
                    .get(shard.as_str())
                    .and_then(|entries| entries.last())
                    .map(|e| e.seqno);

                let committed = current_seqno == *expected;

                if committed {
                    self.shards
                        .entry(shard.clone())
                        .or_default()
                        .push(VersionedData {
                            seqno: *seqno,
                            data: data.clone(),
                        });
                }

                SharedLogObservation::Cas { committed }
            }
            SharedLogOp::Head { shard } => {
                let data = self
                    .shards
                    .get(shard.as_str())
                    .and_then(|entries| entries.last())
                    .map(|e| VersionedData {
                        seqno: e.seqno,
                        data: e.data.clone(),
                    });
                SharedLogObservation::Head { data }
            }
            SharedLogOp::Scan { shard, from, limit } => {
                let data = if let Some(entries) = self.shards.get(shard.as_str()) {
                    let from_idx = entries.partition_point(|e| e.seqno < *from);
                    let lim = usize::try_from(*limit).unwrap_or(usize::MAX);
                    let slice = &entries[from_idx..];
                    let slice = &slice[..usize::min(lim, slice.len())];
                    slice
                        .iter()
                        .map(|e| VersionedData {
                            seqno: e.seqno,
                            data: e.data.clone(),
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                SharedLogObservation::Scan { data }
            }
            SharedLogOp::Truncate { shard, seqno } => {
                let entries = match self.shards.get(shard.as_str()) {
                    Some(entries) if !entries.is_empty() => entries,
                    _ => {
                        return SharedLogObservation::Truncate(Err(format!(
                            "no data at key: {}",
                            shard
                        )));
                    }
                };

                let head_seqno = entries.last().unwrap().seqno;
                if *seqno > head_seqno {
                    return SharedLogObservation::Truncate(Err(format!(
                        "upper bound too high for truncate: {}",
                        seqno
                    )));
                }

                let keep_from = entries.partition_point(|e| e.seqno < *seqno);
                let deleted = u64::try_from(keep_from).expect("deleted count fits u64");

                // Apply the mutation.
                let entries = self.shards.get_mut(shard.as_str()).unwrap();
                entries.drain(..keep_from);

                SharedLogObservation::Truncate(Ok(deleted))
            }
        }
    }

    /// Return the current head seqno for a shard, if any.
    pub fn head_seqno(&self, shard: &str) -> Option<u64> {
        self.shards
            .get(shard)
            .and_then(|entries| entries.last())
            .map(|e| e.seqno)
    }

    /// Return all shard names that have data.
    pub fn shard_names(&self) -> Vec<String> {
        self.shards.keys().cloned().collect()
    }
}

// Defined separately so the `SequentialSpec` import can find the main `apply`.
impl SequentialSpec for SharedLogOracle {
    type Op = SharedLogOp;
    type Ret = SharedLogObservation;

    fn invoke(&mut self, op: &Self::Op) -> Self::Ret {
        self.apply(op)
    }
}

// ---------------------------------------------------------------------------
// Display helpers
// ---------------------------------------------------------------------------

impl std::fmt::Display for SharedLogOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SharedLogOp::Cas {
                shard,
                expected,
                seqno,
                data,
            } => write!(
                f,
                "Cas({}, expected={:?}, seqno={}, {}B)",
                shard,
                expected,
                seqno,
                data.len()
            ),
            SharedLogOp::Head { shard } => write!(f, "Head({})", shard),
            SharedLogOp::Scan { shard, from, limit } => {
                write!(f, "Scan({}, from={}, limit={})", shard, from, limit)
            }
            SharedLogOp::Truncate { shard, seqno } => {
                write!(f, "Truncate({}, seqno={})", shard, seqno)
            }
        }
    }
}

impl std::fmt::Display for SharedLogObservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SharedLogObservation::Cas { committed } => write!(f, "committed={}", committed),
            SharedLogObservation::Head { data: None } => write!(f, "None"),
            SharedLogObservation::Head {
                data: Some(VersionedData { seqno, data }),
            } => write!(f, "seqno={}, {}B", seqno, data.len()),
            SharedLogObservation::Scan { data } => {
                write!(f, "[{} entries]", data.len())
            }
            SharedLogObservation::Truncate(Ok(deleted)) => write!(f, "deleted={}", deleted),
            SharedLogObservation::Truncate(Err(e)) => write!(f, "err={}", e),
        }
    }
}

impl std::fmt::Display for SystemEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SystemEvent::CrashAndRecover => write!(f, "CrashAndRecover"),
            SystemEvent::RetractionSweep => write!(f, "RetractionSweep"),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oracle_cas_commit_and_reject() {
        let mut oracle = SharedLogOracle::new();

        // First CAS: expected=None, should commit.
        let obs = oracle.apply(&SharedLogOp::Cas {
            shard: "s0".into(),
            expected: None,
            seqno: 1,
            data: vec![1, 2, 3],
        });
        assert_eq!(obs, SharedLogObservation::Cas { committed: true });

        // Second CAS with wrong expected: should reject.
        let obs = oracle.apply(&SharedLogOp::Cas {
            shard: "s0".into(),
            expected: None, // wrong — head is now 1
            seqno: 2,
            data: vec![4, 5, 6],
        });
        assert_eq!(obs, SharedLogObservation::Cas { committed: false });

        // Second CAS with correct expected: should commit.
        let obs = oracle.apply(&SharedLogOp::Cas {
            shard: "s0".into(),
            expected: Some(1),
            seqno: 2,
            data: vec![4, 5, 6],
        });
        assert_eq!(obs, SharedLogObservation::Cas { committed: true });
    }

    #[test]
    fn oracle_head() {
        let mut oracle = SharedLogOracle::new();

        // Head on empty shard.
        let obs = oracle.apply(&SharedLogOp::Head {
            shard: "s0".into(),
        });
        assert_eq!(obs, SharedLogObservation::Head { data: None });

        // Commit something.
        oracle.apply(&SharedLogOp::Cas {
            shard: "s0".into(),
            expected: None,
            seqno: 1,
            data: vec![10, 20],
        });

        // Head should return it.
        let obs = oracle.apply(&SharedLogOp::Head {
            shard: "s0".into(),
        });
        assert_eq!(
            obs,
            SharedLogObservation::Head {
                data: Some(VersionedData {
                    seqno: 1,
                    data: vec![10, 20],
                })
            }
        );
    }

    #[test]
    fn oracle_scan() {
        let mut oracle = SharedLogOracle::new();

        // Commit a few entries.
        for i in 1..=5 {
            oracle.apply(&SharedLogOp::Cas {
                shard: "s0".into(),
                expected: if i == 1 { None } else { Some(i - 1) },
                seqno: i,
                data: vec![i as u8],
            });
        }

        // Scan from seqno 3.
        let obs = oracle.apply(&SharedLogOp::Scan {
            shard: "s0".into(),
            from: 3,
            limit: 100,
        });
        match obs {
            SharedLogObservation::Scan { data } => {
                assert_eq!(data.len(), 3); // seqno 3, 4, 5
                assert_eq!(data[0].seqno, 3);
                assert_eq!(data[2].seqno, 5);
            }
            other => panic!("expected Scan, got {:?}", other),
        }

        // Scan with limit.
        let obs = oracle.apply(&SharedLogOp::Scan {
            shard: "s0".into(),
            from: 1,
            limit: 2,
        });
        match obs {
            SharedLogObservation::Scan { data } => {
                assert_eq!(data.len(), 2);
                assert_eq!(data[0].seqno, 1);
                assert_eq!(data[1].seqno, 2);
            }
            other => panic!("expected Scan, got {:?}", other),
        }
    }

    #[test]
    fn oracle_truncate() {
        let mut oracle = SharedLogOracle::new();

        // Truncate on empty shard → error.
        let obs = oracle.apply(&SharedLogOp::Truncate {
            shard: "s0".into(),
            seqno: 1,
        });
        assert!(matches!(obs, SharedLogObservation::Truncate(Err(_))));

        // Commit entries 1..=5.
        for i in 1..=5 {
            oracle.apply(&SharedLogOp::Cas {
                shard: "s0".into(),
                expected: if i == 1 { None } else { Some(i - 1) },
                seqno: i,
                data: vec![i as u8],
            });
        }

        // Truncate below seqno 3 → deletes entries 1, 2.
        let obs = oracle.apply(&SharedLogOp::Truncate {
            shard: "s0".into(),
            seqno: 3,
        });
        assert_eq!(obs, SharedLogObservation::Truncate(Ok(2)));

        // Verify: scan should return entries 3, 4, 5.
        let obs = oracle.apply(&SharedLogOp::Scan {
            shard: "s0".into(),
            from: 0,
            limit: 100,
        });
        match obs {
            SharedLogObservation::Scan { data } => {
                assert_eq!(data.len(), 3);
                assert_eq!(data[0].seqno, 3);
            }
            other => panic!("expected Scan, got {:?}", other),
        }

        // Truncate above head → error.
        let obs = oracle.apply(&SharedLogOp::Truncate {
            shard: "s0".into(),
            seqno: 10,
        });
        assert!(matches!(obs, SharedLogObservation::Truncate(Err(_))));
    }

    #[test]
    fn oracle_shard_independence() {
        let mut oracle = SharedLogOracle::new();

        // Commit to two different shards.
        oracle.apply(&SharedLogOp::Cas {
            shard: "s0".into(),
            expected: None,
            seqno: 1,
            data: vec![10],
        });
        oracle.apply(&SharedLogOp::Cas {
            shard: "s1".into(),
            expected: None,
            seqno: 1,
            data: vec![20],
        });

        // Heads are independent.
        let h0 = oracle.apply(&SharedLogOp::Head {
            shard: "s0".into(),
        });
        let h1 = oracle.apply(&SharedLogOp::Head {
            shard: "s1".into(),
        });
        assert_ne!(h0, h1);
    }
}
