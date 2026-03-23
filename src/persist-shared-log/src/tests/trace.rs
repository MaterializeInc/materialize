// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Simulation trace: a complete log of everything the simulation does.
//!
//! Records all operations (invoke/return), system events (crash/recovery),
//! and annotations. On assertion failure, the trace is printed to stderr
//! so the full operation history leading to the failure is visible.
//!
//! The trace uses logical step indices (not wall-clock time) so it is
//! deterministic and supports equality comparison for the determinism test.

use std::fmt;

use super::scenario::{SharedLogObservation, SharedLogOp, SystemEvent};

// ---------------------------------------------------------------------------
// Thread identity
// ---------------------------------------------------------------------------

/// Identifies the actor performing an operation.
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum SimThread {
    Client(usize),
}

impl fmt::Display for SimThread {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SimThread::Client(id) => write!(f, "Client({})", id),
        }
    }
}

// ---------------------------------------------------------------------------
// Trace events
// ---------------------------------------------------------------------------

/// A single event in the simulation trace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceEntry {
    /// Monotonically increasing event index.
    pub idx: usize,
    /// Simulation step (operation number within the test).
    pub step: usize,
    pub kind: TraceEventKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TraceEventKind {
    /// An operation was invoked against the real system.
    Invoke { thread: SimThread, op: SharedLogOp },
    /// An operation returned from the real system.
    Return {
        thread: SimThread,
        op: SharedLogOp,
        actual: SharedLogObservation,
        expected: SharedLogObservation,
    },
    /// A system event (not linearized).
    System { event: SystemEvent },
    /// A free-form annotation.
    Note { message: String },
}

// ---------------------------------------------------------------------------
// SimTrace
// ---------------------------------------------------------------------------

/// Accumulates trace events during simulation.
pub struct SimTrace {
    entries: Vec<TraceEntry>,
    seed: u64,
    next_idx: usize,
}

impl SimTrace {
    pub fn new(seed: u64) -> Self {
        SimTrace {
            entries: Vec::new(),
            seed,
            next_idx: 0,
        }
    }

    pub fn record_invoke(&mut self, step: usize, thread: SimThread, op: &SharedLogOp) {
        self.entries.push(TraceEntry {
            idx: self.next_idx,
            step,
            kind: TraceEventKind::Invoke {
                thread,
                op: op.clone(),
            },
        });
        self.next_idx += 1;
    }

    pub fn record_return(
        &mut self,
        step: usize,
        thread: SimThread,
        op: &SharedLogOp,
        actual: &SharedLogObservation,
        expected: &SharedLogObservation,
    ) {
        self.entries.push(TraceEntry {
            idx: self.next_idx,
            step,
            kind: TraceEventKind::Return {
                thread,
                op: op.clone(),
                actual: actual.clone(),
                expected: expected.clone(),
            },
        });
        self.next_idx += 1;
    }

    pub fn record_system(&mut self, step: usize, event: &SystemEvent) {
        self.entries.push(TraceEntry {
            idx: self.next_idx,
            step,
            kind: TraceEventKind::System {
                event: event.clone(),
            },
        });
        self.next_idx += 1;
    }

    pub fn record_note(&mut self, step: usize, message: String) {
        self.entries.push(TraceEntry {
            idx: self.next_idx,
            step,
            kind: TraceEventKind::Note { message },
        });
        self.next_idx += 1;
    }

    /// Return the entries for equality comparison (determinism test).
    pub fn entries(&self) -> &[TraceEntry] {
        &self.entries
    }
}

impl fmt::Display for SimTrace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for entry in &self.entries {
            write!(f, "[seed={} step={}] ", self.seed, entry.step)?;
            match &entry.kind {
                TraceEventKind::Invoke { thread, op } => {
                    writeln!(f, "INVOKE {} {}", thread, op)?;
                }
                TraceEventKind::Return {
                    thread,
                    op,
                    actual,
                    expected,
                } => {
                    let ok = if actual == expected { "OK" } else { "MISMATCH" };
                    writeln!(
                        f,
                        "RETURN {} {} -> {}  (expected: {}) {}",
                        thread, op, actual, expected, ok,
                    )?;
                }
                TraceEventKind::System { event } => {
                    writeln!(f, "SYSTEM {}", event)?;
                }
                TraceEventKind::Note { message } => {
                    writeln!(f, "  NOTE {}", message)?;
                }
            }
        }
        Ok(())
    }
}
