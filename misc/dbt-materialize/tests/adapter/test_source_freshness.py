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
from dbt.cli.main import dbtRunner

freshness_via_metadata_schema_yml = """version: 2
sources:
  - name: test_source
    loader: custom
    freshness:
      warn_after: {count: 10, period: hour}
      error_after: {count: 1, period: day}
    schema: my_schema
    quoting:
      identifier: True
    tables:
      - name: test_table
        identifier: source
"""


class TestMetadataFreshnessFails:
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": freshness_via_metadata_schema_yml}

    # We mark TableLastModifiedMetadata as not implemented, so trying to use metadata-based source freshness checks should result in a parse-time warning.
    def test_metadata_freshness_fails(self, project):
        got_warning = False

        def warning_probe(e):
            nonlocal got_warning
            if e.info.name == "FreshnessConfigProblem" and e.info.level == "warn":
                got_warning = True

        runner = dbtRunner(callbacks=[warning_probe])
        runner.invoke(["parse"])

        assert got_warning
