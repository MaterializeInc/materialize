# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Stub of `materialize.mzcompose.services.mysql`. See package __init__.py.

Only the `DEFAULT_ROOT_PASSWORD` class attribute is read at runtime — the
constant must match the real `MySql` service so the parallel-workload's
`CREATE SECRET mypass AS ...` matches the actual MySQL container password
provisioned by the Antithesis topology.
"""

from __future__ import annotations


class MySql:
    DEFAULT_ROOT_PASSWORD = "p@ssw0rd"
