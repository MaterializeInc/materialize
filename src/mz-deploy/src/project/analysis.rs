// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Analyses derived from compiled project state.
//!
//! This subsystem owns computations performed over the compiled project or its
//! dependency graph, including:
//!
//! - deployment snapshots
//! - dirty propagation and incremental deployment planning
//! - dependency extraction and topological traversal
//! - graph-wide deployment validations

pub(crate) mod changeset;
pub(crate) mod deployment_snapshot;
pub(crate) mod deps;
pub(crate) mod graph_validation;
pub(crate) mod topology;
