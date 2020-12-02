# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import sqlalchemy
import unittest


class SmokeTest(unittest.TestCase):
    def test_sqlalchemy(self):
        engine = sqlalchemy.engine.create_engine(
            "postgresql://materialized:6875/materialize"
        )
        results = [[c1, c2] for c1, c2 in engine.execute("VALUES (1, 2), (3, 4)")]
        self.assertEqual(results, [[1, 2], [3, 4]])
