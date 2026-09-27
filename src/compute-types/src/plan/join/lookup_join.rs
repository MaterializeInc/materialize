// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Lookup join execution planning.
//!
//! A lookup join streams the updates of one input, the source relation, and probes
//! arrangements of the other inputs, the lookup relations. Only the positive updates of the
//! source relation produce output. Changes to the lookup relations never do. The output is
//! therefore not the join of the inputs as collections, and a lookup join is only correct where
//! its consumer reads each source update's results at that update's time and ignores the rest.
//!
//! The plan has the shape of a single delta join path whose source is read as a raw collection.

use serde::{Deserialize, Serialize};

use crate::plan::join::JoinClosure;
use crate::plan::join::delta_join::DeltaStagePlan;

/// A lookup join is implemented by a sequence of lookups driven by the source relation.
///
/// A positive update `(s, t, d)` of the source relation is joined with the lookup relations as
/// they are at `t`, including their updates at `t`, and the results are emitted at `t`.
/// Non-positive updates of the source relation produce no output, and updates of the lookup
/// relations never do.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct LookupJoinPlan {
    /// The relation whose updates drive the lookups, read as a raw collection.
    pub source_relation: usize,
    /// An initial closure to apply to the raw rows of the source relation before any stages.
    pub initial_closure: JoinClosure,
    /// A *sequence* of stages to apply one after the other.
    pub stage_plans: Vec<DeltaStagePlan>,
    /// A concluding closure to apply after the last stage.
    ///
    /// Values of `None` indicate the identity closure.
    pub final_closure: Option<JoinClosure>,
}
