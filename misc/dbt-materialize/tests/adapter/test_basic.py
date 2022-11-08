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
from dbt.tests.adapter.basic.expected_catalog import (
    base_expected_catalog,
    expected_references_catalog,
    no_stats,
)
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_docs_generate import (
    BaseDocsGenerate,
    BaseDocsGenReferences,
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.util import (
    check_relation_types,
    check_relations_equal,
    check_result_nodes_by_name,
    relation_from_name,
    run_dbt,
)
from fixtures import expected_base_relation_types


class TestSimpleMaterializationsMaterialize(BaseSimpleMaterializations):
    @pytest.fixture(autouse=True)
    def _pass_profile_value(self, profile):
        self._profile = profile

    # Custom base test that removes the incremental portion and overrides the expected relations

    def test_base(self, project):

        # seed command
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1

        # run command
        results = run_dbt()
        # run result length
        assert len(results) == 3

        expected = expected_base_relation_types[self._profile]

        # names exist in result nodes
        check_result_nodes_by_name(results, ["view_model", "table_model", "swappable"])

        check_relation_types(project.adapter, expected)

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation}", fetch="one"
        )
        assert result[0] == 10

        # relations_equal
        check_relations_equal(
            project.adapter, ["base", "view_model", "table_model", "swappable"]
        )

        # check relations in catalog
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 4
        assert len(catalog.sources) == 1

        # run_dbt changing materialized_var to view
        results = run_dbt(
            ["run", "-m", "swappable", "--vars", "materialized_var: view"]
        )
        assert len(results) == 1

        # check relation types, swappable is view
        expected["swappable"] = "view"
        check_relation_types(project.adapter, expected)


class TestSingularTestsMaterialize(BaseSingularTests):
    pass


class TestSingularTestsEphemeralMaterialize(BaseSingularTestsEphemeral):
    pass


class TestEmptyMaterialize(BaseEmpty):
    pass


class TestEphemeral(BaseEphemeral):
    pass


@pytest.mark.skip(reason="dbt-materialize does not support incremental models")
class TestIncrementalMaterialize(BaseIncremental):
    pass


class TestGenericTestsMaterialize(BaseGenericTests):
    pass


@pytest.mark.skip(reason="dbt-materialize does not support snapshots")
class TestSnapshotCheckColsMaterialize(BaseSnapshotCheckCols):
    pass


@pytest.mark.skip(reason="dbt-materialize does not support snapshots")
class TestSnapshotTimestampMaterialize(BaseSnapshotTimestamp):
    pass


# This additional model sql was needed to ensure we use the materialize__create_view_as sql.
class TestBaseAdapterMethodMaterialize(BaseAdapterMethod):
    models__model_sql = """
{% set upstream = ref('upstream') %}
{% if execute %}
    {# don't ever do any of this #}
    {%- do adapter.drop_schema(upstream) -%}
    {% set existing = adapter.get_relation(upstream.database, upstream.schema, upstream.identifier) %}
    {% if existing is not none %}
        {% do exceptions.raise_compiler_error('expected ' ~ ' to not exist, but it did') %}
    {% endif %}
    {%- do adapter.create_schema(upstream) -%}
    {% set sql = materialize__create_view_as(upstream, 'select 2 as id') %}
    {% do run_query(sql) %}
{% endif %}
select * from {{ upstream }}
"""
    pass


# We skip doc tests for binary versions of Materialize because the `materialize__get_catalog`
# macro that is used to generate the docs must stay consistent with the latest Materialize release.
# The `type` of a materialized view in changed to kebab-case in v0.29.
# Users running earlier versions of Materialize will see seeds referenced as 'view'
# types instead of 'materializedview' types in the generated docs.
@pytest.mark.skip_profile("materialize_binary")
class TestDocsGenerateMaterialize(BaseDocsGenerate):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        return base_expected_catalog(
            project,
            role=None,
            id_type="integer",
            text_type="text",
            time_type="timestamp",
            view_type="view",
            table_type="materializedview",
            model_stats=no_stats(),
        )


@pytest.mark.skip_profile("materialize_binary")
class TestDocsGenReferencesMaterialize(BaseDocsGenReferences):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        return expected_references_catalog(
            project,
            role=None,
            id_type="integer",
            text_type="text",
            time_type="timestamp",
            bigint_type="bigint",
            view_type="view",
            table_type="materializedview",
            model_stats=no_stats(),
        )
