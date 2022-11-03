import pytest
from dbt.tests.adapter.ephemeral.test_ephemeral import BaseEphemeralMulti
from dbt.tests.util import check_relations_equal, run_dbt


class TestEphemeralMultiMaterialize(BaseEphemeralMulti):
    def test_ephemeral_multi_materialize(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 3
        check_relations_equal(
            project.adapter,
            ["seed", "dependent", "double_dependent", "super_dependent"],
        )
