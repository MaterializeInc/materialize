// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Hosts [`IndexKeyEmpty`].

use std::collections::BTreeSet;
use std::fmt;

use mz_repr::explain::ExprHumanizer;
use mz_repr::GlobalId;

use crate::notice::{ActionKind, OptimizerNoticeApi};

/// An index with an empty key is maximally skewed (all of the data goes to a single worker),
/// and is almost never really useful. It's slightly useful for a cross join, because a cross
/// join also has an empty key, so we avoid rearranging the input. However, this is still
/// not very useful, because
///  - Rearranging the input shouldn't take too much memory, because if a cross join has a big
///    input, then we have a serious problem anyway.
///  - Even with the arrangement already there, the cross join will read every input record, so
///    the orders of magnitude performance improvements that can happen with other joins when an
///    input arrangement exists can't happen with a cross join.
/// Also note that skew is hard to debug, so it's good to avoid this problem in the first place.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct IndexKeyEmpty;

impl OptimizerNoticeApi for IndexKeyEmpty {
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
            "Empty index key. \
            The index will be completely skewed to one worker thread, \
            which can lead to performance problems."
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
            "CREATE DEFAULT INDEX is almost always better than an index with an empty key. \
            (Except for cross joins with big inputs, which are better to avoid anyway.)"
        )
    }

    fn fmt_action(
        &self,
        f: &mut fmt::Formatter<'_>,
        _humanizer: &dyn ExprHumanizer,
        _redacted: bool,
    ) -> fmt::Result {
        write!(
            f,
            "Drop the enclosing index and re-create it using `CREATE DEFAULT INDEX ON` instead."
        )
    }

    fn action_kind(&self, _humanizer: &dyn ExprHumanizer) -> ActionKind {
        ActionKind::PlainText
    }
}
