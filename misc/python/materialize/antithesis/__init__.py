# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Workload library for testing Materialize under Antithesis.

The modules here run inside the workload pod of `test/antithesis`, but they do
not depend on the Antithesis runtime: outside Antithesis the SDK falls back to
local randomness and no-op (or file-logged, via `ANTITHESIS_SDK_LOCAL_OUTPUT`)
assertions, so the same drivers run under a local kind cluster.

Assertions are called directly from `antithesis.assertions` with literal
messages at each call site. The Antithesis cataloger only finds assertions in
that form, and an assertion it has not cataloged cannot be reported as never
reached.
"""
