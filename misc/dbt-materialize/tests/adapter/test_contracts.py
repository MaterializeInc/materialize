# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

# NOTE(benesch): these tests are backported, with modifications, from
# dbt-core#8887.

# TODO(benesch): consider removing these tests when v1.8 ships with these tests
# as part of core.

my_timestamp_model_sql = """
select
  '2023-01-01T00:00:00'::timestamp as ts
"""

my_mz_timestamp_model_sql = """
select
  '1672531200000'::mz_timestamp as ts
"""

model_schema_timestamp_yml = """
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: ts
        data_type: timestamp
"""

model_schema_mz_timestamp_yml = """
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: ts
        data_type: mz_timestamp
"""


class TestModelContractUnrecognizedTypeCode1:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_mz_timestamp_model_sql,
            "schema.yml": model_schema_mz_timestamp_yml,
        }

    def test_nonstandard_data_type(self, project):
        run_dbt(["run"], expect_pass=True)


class TestModelContractUnrecognizedTypeCodeActualMismatch:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_mz_timestamp_model_sql,
            "schema.yml": model_schema_timestamp_yml,
        }

    def test_nonstandard_data_type(self, project):
        expected_msg = "custom type unknown to dbt (OID 16552) | DATETIME      | data type mismatch"
        _, logs = run_dbt_and_capture(["run"], expect_pass=False)
        assert expected_msg in logs


class TestModelContractUnrecognizedTypeCodeExpectedMismatch:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_timestamp_model_sql,
            "schema.yml": model_schema_mz_timestamp_yml,
        }

    def test_nonstandard_data_type(self, project):
        expected_msg = "DATETIME        | custom type unknown to dbt (OID 16552) | data type mismatch"
        _, logs = run_dbt_and_capture(["run"], expect_pass=False)
        print(logs)
        assert expected_msg in logs
