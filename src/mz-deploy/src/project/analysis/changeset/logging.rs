// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Verbose logging helpers for the Datalog fixed-point computation.
//!
//! These functions emit structured progress information when the user
//! enables verbose output, making it easy to trace rule firings and
//! convergence behavior.

use super::base_facts::BaseFacts;
use super::datalog::DirtyState;
use crate::project::ir::object_id::ObjectId;
use crate::verbose;
use owo_colors::{OwoColorize, Stream, Style};
use std::collections::BTreeSet;

/// Emits an initial summary of inputs before rule evaluation starts.
pub(super) fn log_datalog_start(changed_stmts: &BTreeSet<ObjectId>, base_facts: &BaseFacts) {
    let header_style = Style::new().cyan().bold();
    verbose!(
        "{} {}",
        "▶".if_supports_color(Stream::Stderr, |t| t.cyan()),
        "Starting fixed-point computation..."
            .if_supports_color(Stream::Stderr, |t| header_style.style(t))
    );
    verbose!(
        "  ├─ Initial changed statements: [{}]",
        changed_stmts
            .iter()
            .map(|o| o
                .to_string()
                .if_supports_color(Stream::Stderr, |t| t.cyan())
                .to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    verbose!(
        "  └─ Known sinks: [{}]",
        base_facts
            .is_sink
            .iter()
            .map(|o| o
                .to_string()
                .if_supports_color(Stream::Stderr, |t| t.yellow())
                .to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
}

/// Emits per-iteration progress for dirty set growth.
pub(super) fn log_iteration(iteration: usize, state: &DirtyState) {
    let header_style = Style::new().cyan().bold();
    verbose!(
        "\n{} {} (stmts={}, clusters={}, schemas={})",
        "▶".if_supports_color(Stream::Stderr, |t| t.cyan()),
        format!("Iteration {}", iteration)
            .if_supports_color(Stream::Stderr, |t| header_style.style(t)),
        state
            .dirty_stmts
            .len()
            .to_string()
            .if_supports_color(Stream::Stderr, |t| t.bold()),
        state
            .dirty_clusters
            .len()
            .to_string()
            .if_supports_color(Stream::Stderr, |t| t.bold()),
        state
            .dirty_schemas
            .len()
            .to_string()
            .if_supports_color(Stream::Stderr, |t| t.bold())
    );
}

/// Emits final dirty object/cluster/schema sets after convergence.
pub(super) fn log_final_results(state: &DirtyState) {
    let header_style = Style::new().cyan().bold();
    verbose!(
        "{} {}",
        "▶".if_supports_color(Stream::Stderr, |t| t.cyan()),
        "Final Results".if_supports_color(Stream::Stderr, |t| header_style.style(t))
    );
    verbose!(
        "  ├─ Dirty statements ({}): [{}]",
        state
            .dirty_stmts
            .len()
            .to_string()
            .if_supports_color(Stream::Stderr, |t| t.bold()),
        state
            .dirty_stmts
            .iter()
            .map(|o| o
                .to_string()
                .if_supports_color(Stream::Stderr, |t| t.cyan())
                .to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    verbose!(
        "  ├─ Dirty clusters ({}): [{}]",
        state
            .dirty_clusters
            .len()
            .to_string()
            .if_supports_color(Stream::Stderr, |t| t.bold()),
        state
            .dirty_clusters
            .iter()
            .map(|c| c
                .if_supports_color(Stream::Stderr, |t| t.magenta())
                .to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    verbose!(
        "  └─ Dirty schemas ({}): [{}]",
        state
            .dirty_schemas
            .len()
            .to_string()
            .if_supports_color(Stream::Stderr, |t| t.bold()),
        state
            .dirty_schemas
            .iter()
            .map(|sq| format!("{}.{}", sq.database, sq.schema)
                .if_supports_color(Stream::Stderr, |t| t.blue())
                .to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
}
