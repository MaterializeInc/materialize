# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Layered-into-place stub for the `materialize.checks` package.

The Antithesis platform-checks workload image needs the real `checks.py`,
`actions.py`, `common.py`, `features.py`, and `all_checks/**` from
`misc/python/materialize/checks/`.  Those are layered on top of this stub
by the workload Dockerfile's `COPY materialize/ ...` step.  The stub
exists so that the package directory has an `__init__.py` even when
mzbuild's pre-image:copy is selecting individual files (the real
`__init__.py` is included in that copy too and will overwrite this one
in the final image).

`executors.py` is deliberately NOT in the real-package copy list — the
real module pulls in `materialize.cloudtest.app.materialize_application`,
`materialize.mzcompose.composition`, etc.  We ship a tiny stub at
`stubs/materialize/checks/executors.py` instead, layered in before the
real-package copy.
"""
