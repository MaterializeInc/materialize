// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Which dataflows a compute runtime renders.

use mz_compute_types::dataflows::DataflowClass;

/// The dataflow classes a runtime renders.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Placement {
    /// The runtime renders every class, as the only runtime of its process.
    All,
    /// The runtime renders maintained dataflows, logging included.
    Maintained,
    /// The runtime renders one-shot reads.
    OneShotRead,
}

impl Placement {
    /// Whether the runtime renders dataflows of `class`.
    pub(crate) fn renders(self, class: DataflowClass) -> bool {
        match (self, class) {
            (Placement::All, _) => true,
            (Placement::Maintained, DataflowClass::Maintained) => true,
            (Placement::OneShotRead, DataflowClass::OneShotRead) => true,
            (Placement::Maintained, DataflowClass::OneShotRead)
            | (Placement::OneShotRead, DataflowClass::Maintained) => false,
        }
    }
}
