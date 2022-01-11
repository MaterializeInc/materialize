# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from pg8000.dbapi import Connection  # type: ignore


class FitnessFunction:
    def __init__(self, conn: Connection):
        self._conn = conn
        self._cur = conn.cursor()

    def fitness(self, query: str) -> float:
        assert False
