# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Stub of `materialize.mzcompose.services.sql_server`. See package __init__.py.

Constants kept in sync with the real `SqlServer` service so any SQL emitted
referring to them stays well-formed. The Antithesis topology doesn't actually
include a sql-server container — driver code avoids
`CreateSqlServerSourceAction` and overrides the connection setup in
`Database.create` accordingly.
"""

from __future__ import annotations


class SqlServer:
    DEFAULT_USER = "SA"
    DEFAULT_SA_PASSWORD = "RPSsql12345"
