# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Type-level crate dependency analysis behind `bin/crate-cuts`.

The stages communicate through plain data, so each can be tested alone:

* `extract` reads `cargo metadata` and a rust-analyzer SCIP index into
  `facts.Facts`, the packages and the module reference graph.
* `model` holds the current assignment of modules to crates and the crate
  dependency edges, derives transitive closures and module needs, and
  applies splits.
* `search` ranks splits and edge cuts and plans splits greedily.
* `report` renders the results.
"""
