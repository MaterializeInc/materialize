# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the
# root of this repository, or online at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest

from dbt.tests.adapter.simple_seed.test_seed import SeedConfigBase
from dbt.tests.adapter.simple_seed.test_seed_type_override import BaseSimpleSeedColumnOverride
from dbt.tests.util import run_dbt

_SCHEMA_YML = """
version: 2
seeds:
- name: seed_enabled
  columns:
  - name: birthday
    tests:
    - column_type:
        type: text
  - name: seed_id
    tests:
    - column_type:
        type: integer

- name: seed_tricky
  columns:
  - name: seed_id
    tests:
    - column_type:
        type: integer
  - name: seed_id_str
    tests:
    - column_type:
        type: text
  - name: a_bool
    tests:
    - column_type:
        type: boolean
  - name: looks_like_a_bool
    tests:
    - column_type:
        type: text
  - name: a_date
    tests:
    - column_type:
        type: timestamp
  - name: looks_like_a_date
    tests:
    - column_type:
        type: text
  - name: relative
    tests:
    - column_type:
        type: text
  - name: weekday
    tests:
    - column_type:
        type: text
""".lstrip()


class TestSimpleBigSeedBatched(SeedConfigBase):
    @pytest.fixture(scope="class")
    def seeds(self):
        seed_data = ["seed_id"]
        seed_data.extend([str(i) for i in range(20_000)])
        return {"big_batched_seed.csv": "\n".join(seed_data)}

    def test_big_batched_seed(self, project):
        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 1


class TestSimpleSeedColumnOverride(BaseSimpleSeedColumnOverride):
    @pytest.fixture(scope="class")
    def schema(self):
        return "simple_seed"

    @pytest.fixture(scope="class")
    def models(self):
        return {"models-materialize.yml": _SCHEMA_YML}

    @staticmethod
    def seed_enabled_types():
        return {
            "birthday": "text",
            "seed_id": "integer",
        }
    
    def test_materialize_simple_seed_with_column_override_materialize(self, project):
        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 2
        test_results = run_dbt(["test"])
        assert len(test_results) == 10