// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Hosts [`CrossJoin`].

use std::collections::BTreeSet;
use std::fmt;

use mz_repr::GlobalId;
use mz_repr::explain::ExprHumanizer;

use crate::notice::{ActionKind, OptimizerNoticeApi};

/// A plan involves a cross join.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct CrossJoin;

impl OptimizerNoticeApi for CrossJoin {
    fn dependencies(&self) -> BTreeSet<GlobalId> {
        BTreeSet::new()
    }

    fn fmt_message(
        &self,
        f: &mut fmt::Formatter<'_>,
        _humanizer: &dyn ExprHumanizer,
        _redacted: bool,
    ) -> fmt::Result {
        write!(
            f,
            "Cross join. \
            The join will be completely skewed to one worker thread, \
            which can lead to performance problems if an input relation to a cross join is large."
        )
    }

    fn fmt_hint(
        &self,
        f: &mut fmt::Formatter<'_>,
        _humanizer: &dyn ExprHumanizer,
        _redacted: bool,
    ) -> fmt::Result {
        write!(
            f,
            "If you encounter slow queries or slow hydrations where only one CPU core is being \
            utilized for extended periods of time, try to eliminate the cross join by changing \
            your query."
        )
        //////////////// todo: what else to suggest?
        // - hints for how to find in EXPLAIN where the cross join is
        // - point to https://materialize.com/docs/transform-data/dataflow-troubleshooting/#is-work-distributed-equally-across-workers
        // - maybe use EXPLAIN ANALYZE to see whether the join inputs are big?
        // - make a list of common things that can lead to cross joins:
        //   - obvious stuff, like typing out CROSS JOIN, or not giving a join condition at all (neither in ON or WHERE)
        //   - OR in join conditions (see https://github.com/MaterializeInc/database-issues/issues/5841)
        //   - inequality joins
        //   - (one side of the equality involving both join inputs)
        // Maybe put all this in a dedicated docs page, and just link to that here.
    }

    fn fmt_action(
        &self,
        _f: &mut fmt::Formatter<'_>,
        _humanizer: &dyn ExprHumanizer,
        _redacted: bool,
    ) -> fmt::Result {
        Ok(())
    }

    fn action_kind(&self, _humanizer: &dyn ExprHumanizer) -> ActionKind {
        ActionKind::None
    }
}
