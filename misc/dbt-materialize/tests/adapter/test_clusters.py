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
{{ config(cluster='not_default', materialized='materializedview') }}
select 1 as col_1
"""

models__override_cluster_and_index_sql = """
{{ config(
    cluster='not_default',
    materialized='materializedview',
    indexes=[{'columns': ['col_1'], 'name':'c_i_col_1_idx'}]
) }}
select 1 as col_1
"""

models__override_index_cluster_sql = """
{{ config(
    materialized='materializedview',
    indexes=[{'columns': ['col_1'], 'cluster': 'not_default', 'name':'i_col_1_idx'}]
) }}
select 1 as col_1
"""

models__invalid_cluster_sql = """
{{ config(cluster='not_exist', materialized='materializedview') }}
select 1 as col_1
"""

project_override_cluster_sql = """
{{ config(materialized='materializedview') }}
select 1 as col_1
"""

models_actual_clusters = """
select
    mv.name as materialized_view_name,
    c_mv.name as cluster_name,
    i.name as index_name,
    c_i.name as index_cluster_name
from mz_materialized_views mv
join mz_clusters c_mv on mv.cluster_id = c_mv.id
left join mz_indexes i on mv.id = i.on_id
left join mz_clusters c_i on i.cluster_id = c_i.id
where mv.id like 'u%'
"""

models_expected_clusters = """
materialized_view_name,cluster_name,index_name,index_cluster_name
expected_clusters,default,,
override_cluster,not_default,,
override_index_cluster,default,i_col_1_idx,not_default
override_cluster_and_index,not_default,c_i_col_1_idx,not_default
""".lstrip()

project_actual_clusters = """
select
c.name as cluster
from mz_materialized_views v
join mz_clusters c on v.cluster_id = c.id and v.name = 'override_cluster'
"""


@pytest.mark.skip_profile("materialize_binary")
class TestModelCluster:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected_clusters.csv": models_expected_clusters,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "override_cluster.sql": models__override_cluster_sql,
            "override_cluster_and_index.sql": models__override_cluster_and_index_sql,
            "override_index_cluster.sql": models__override_index_cluster_sql,
            "invalid_cluster.sql": models__invalid_cluster_sql,
            "actual_clusters.sql": models_actual_clusters,
        }

    def test_materialize_override_ok(self, project):

        results = run_dbt(["seed"])
        assert len(results) == 1

        project.run_sql("CREATE CLUSTER not_default REPLICAS (r1 (SIZE '1'))")
        run_dbt(["run", "--exclude", "invalid_cluster"])

        check_relations_equal(project.adapter, ["actual_clusters", "expected_clusters"])

    def test_materialize_override_noexist(self, project):
        run_dbt(["run", "--models", "invalid_cluster"], expect_pass=False)


@pytest.mark.skip_profile("materialize_binary")
class TestProjectConfigCluster:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "override_cluster.sql": project_override_cluster_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+cluster": "not_default",
                },
            },
        }

    def test_materialize_override_ok(self, project):

        run_dbt(["run", "--models", "override_cluster"])

        results = project.run_sql(project_actual_clusters, fetch="one")
        assert results[0] == "not_default"
