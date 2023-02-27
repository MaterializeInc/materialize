# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent
from typing import List

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check
from materialize.util import MzVersion


class UUID(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE TABLE uuid_table (f1 UUID, f2 UUID, f3 STRING);
            > INSERT INTO uuid_table VALUES (uuid_generate_v5('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'bar'), 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12', 'baz');
            """
                if self.base_version >= MzVersion(0, 46, 0)
                else ""
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE MATERIALIZED VIEW uuid_view1 AS SELECT
                    f1,
                    uuid_generate_v5(f2, f3) as f2,
                    uuid_generate_v5('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13', 'foobar') as f3
                  FROM uuid_table;
                > INSERT INTO uuid_table VALUES (uuid_generate_v5(NULL, 'foo'), NULL, 'foo');
            """
                if self.base_version >= MzVersion(0, 46, 0)
                else "",
                """
                > CREATE MATERIALIZED VIEW uuid_view2 AS SELECT
                    f1,
                    uuid_generate_v5(f2, f3) as f2,
                    uuid_generate_v5('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13', 'foobar') as f3
                  FROM uuid_table;
                > INSERT INTO uuid_table VALUES (uuid_generate_v5('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', NULL), 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', NULL);
            """
                if self.base_version >= MzVersion(0, 46, 0)
                else "",
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > SELECT * FROM uuid_view1;
            0b259031-4bae-587c-870a-2f641fe621fe 9ab9ba37-d48e-5c94-bb56-2ccc3129f361 64feaf9e-0633-5eba-9bca-b5f1afd6b084
            <null> <null> 64feaf9e-0633-5eba-9bca-b5f1afd6b084
            <null> <null> 64feaf9e-0633-5eba-9bca-b5f1afd6b084

            > SELECT * FROM uuid_view2;
            0b259031-4bae-587c-870a-2f641fe621fe 9ab9ba37-d48e-5c94-bb56-2ccc3129f361 64feaf9e-0633-5eba-9bca-b5f1afd6b084
            <null> <null> 64feaf9e-0633-5eba-9bca-b5f1afd6b084
            <null> <null> 64feaf9e-0633-5eba-9bca-b5f1afd6b084
            """
                if self.base_version >= MzVersion(0, 46, 0)
                else ""
            )
        )
