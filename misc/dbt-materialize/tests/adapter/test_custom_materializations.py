import pytest

from dbt.tests.util import (
    run_dbt,
    check_relations_equal,
)

from fixtures import (
    test_materialized_view,
    test_materialized_view_index,
    test_view_index,
    test_source,
    test_index,
    test_source_index,
    test_sink,
    actual_indexes,
    expected_indexes
)


class TestCustomMaterializations:

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "custom_materializations"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected_indexes.csv": expected_indexes,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_materialized_view.sql": test_materialized_view,
            "test_materialized_view_index.sql": test_materialized_view_index,
            "test_view_index.sql": test_view_index,
            "test_source.sql": test_source,
            "test_index.sql": test_index,
            "test_source_index.sql": test_source_index,
            "test_sink.sql": test_sink,
            "actual_indexes.sql": actual_indexes,
        }


    def test_custom_materializations(self, project):
        """
        Seed, then run, then test.
        """
        # seed seeds
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1
        # run models
        results = run_dbt(["run"])
        # run result length
        assert len(results) == 8

        # relations_equal
        check_relations_equal(project.adapter, ["test_materialized_view", "test_view_index"])

        check_relations_equal(project.adapter, ["actual_indexes", "expected_indexes"])
