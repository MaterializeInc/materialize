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
from dbt.tests.util import check_relations_equal, run_dbt

models__override_cluster_sql = """
{{ config(materialize_cluster='not_default', materialized='materializedview') }}
select 1
"""

models__invalid_cluster_sql = """
{{ config(materialize_cluster='not_exist', materialized='materializedview') }}
select 1
"""

project_config_models__override_cluster_sql = """
{{ config(materialized='materializedview') }}
select 1
"""

catalog_sql = """
select
    c.name as cluster
from mz_materialized_views v
join mz_clusters c on v.cluster_id = c.id and v.name = 'override_cluster'
"""


@pytest.mark.skip_profile("materialize_binary")
class TestModelCluster:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "override_cluster.sql": models__override_cluster_sql,
            "invalid_cluster.sql": models__invalid_cluster_sql,
        }

    def test_materialize_override_ok(self, project):
        project.run_sql("CREATE CLUSTER not_default REPLICAS (r1 (SIZE '1'))")
        run_dbt(["run", "--models", "override_cluster"])

        results = project.run_sql(catalog_sql, fetch="one")
        assert results[0] == "not_default"

    def test_materialize_override_noexist(self, project):
        run_dbt(["run", "--models", "invalid_cluster"], expect_pass=False)


@pytest.mark.skip_profile("materialize_binary")
class TestConfigCluster:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "override_cluster.sql": project_config_models__override_cluster_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+materialize_cluster": "not_default",
                },
            },
        }

    def test_materialize_override_ok(self, project):
        run_dbt(["run", "--models", "override_cluster"])

        results = project.run_sql(catalog_sql, fetch="one")
        assert results[0] == "not_default"
