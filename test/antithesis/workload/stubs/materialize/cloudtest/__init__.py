# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Stub of `materialize.cloudtest` for the Antithesis workload image.

The real package orchestrates kubernetes-side cloudtest scenarios and pulls in
the full Kubernetes Python client, the Materialize Helm chart, and a wide
slice of `materialize.mzcompose.*`. None of that runs in the Antithesis
sandbox; we ship only the symbols `materialize.checks.executors` imports at
load time (just `MaterializeApplication` from
`cloudtest.app.materialize_application`).
"""
