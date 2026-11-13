# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from types import SimpleNamespace

import pytest

from materialize.data_ingest.query_error import QueryError
from materialize.parallel_workload.action import Action
from materialize.parallel_workload.settings import Complexity, Scenario
from materialize.parallel_workload.worker import Worker


def ignored_errors(scenario, complexity):
    executor = SimpleNamespace(
        statement_timeout_set=False,
        db=SimpleNamespace(
            scenario=scenario, complexity=complexity, retiring_generations=frozenset()
        ),
    )
    return Action(random.Random(0), None).errors_to_ignore(executor)


@pytest.mark.parametrize("complexity", [Complexity.Read, Complexity.DML])
def test_native_deployments_require_full_writers_and_current_catalogs(complexity):
    ignored = ignored_errors(Scenario.ZeroDowntimeDeploy, complexity)
    for error in (
        "cannot write in read-only mode",
        "500: internal storage failure! ReadOnly",
        "unknown catalog item u42",
        "unknown schema s1",
        "unknown database d1",
        "invalid database d1",
        "unknown network policy p1",
    ):
        assert not any(pattern in error for pattern in ignored), error
    assert "server closed the connection unexpectedly" in ignored


def test_native_deployment_still_allows_concurrent_ddl():
    ignored = ignored_errors(Scenario.ZeroDowntimeDeploy, Complexity.DDL)
    assert "unknown schema" in ignored
    assert "was concurrently dropped" in ignored
    assert "cannot write in read-only mode" not in ignored


@pytest.mark.parametrize("scenario", [Scenario.Kill, Scenario.BackupRestore])
def test_restart_and_restore_keep_catalog_loss_exceptions(scenario):
    ignored = ignored_errors(scenario, Complexity.DML)
    assert "unknown schema" in ignored
    assert "unknown database" in ignored
    assert "server closed the connection unexpectedly" in ignored


@pytest.mark.parametrize("scenario", [Scenario.ZeroDowntimeDeploy, Scenario.Kill])
@pytest.mark.parametrize("requested", [frozenset(), frozenset({3}), frozenset({4})])
def test_retirement_rejection_requires_explicit_target(scenario, requested):
    class Rejected(Action):
        def run(self, exe):
            raise QueryError(
                "catalog deployment generation 3 has been retired", "SELECT 1"
            )

    action = Rejected(random.Random(0), None)
    worker = Worker(random.Random(0), [action], [1], 0, True, False, None)
    executor = SimpleNamespace(
        statement_timeout_set=False,
        reconnect_next=False,
        rollback_next=False,
        db=SimpleNamespace(
            scenario=scenario, complexity=Complexity.DML, retiring_generations=requested
        ),
    )
    worker.exe = executor
    if scenario == Scenario.ZeroDowntimeDeploy and 3 in requested:
        worker.run_action(action)
        assert executor.reconnect_next
        assert not executor.rollback_next
        assert worker.occurred_exception is None
    else:
        with pytest.raises(QueryError, match="generation 3 has been retired"):
            worker.run_action(action)
        assert not executor.reconnect_next
