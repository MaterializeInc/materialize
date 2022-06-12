# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize import ROOT, cargo


def environmentd_rust_version() -> str:
    rust_version = cargo.Workspace(ROOT).crates["mz-environmentd"].rust_version
    assert (
        rust_version is not None
    ), "environmentd crate missing rust version configuration"
    return rust_version
