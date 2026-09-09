# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from contextlib import nullcontext
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.test_result import FailedTestExecutionError
from materialize.workload_replay import executor


def stats(
    creation: float = 100, cpu: float = 100, error: str | None = None
) -> dict[str, Any]:
    return {
        "object_creation": creation,
        "queries": {"errors": {error: ["SELECT 1"]} if error else {}},
        "docker": [(0, {"materialized": {"cpu_percent": cpu, "mem_percent": 10}})],
    }


@pytest.mark.parametrize(
    "runs,compare_against,failure",
    [
        pytest.param([stats(), stats(120)], "reference", None, id="threshold-passes"),
        pytest.param(
            [stats(), stats(150), stats(), stats(120)],
            "reference",
            None,
            id="transient-regression",
        ),
        pytest.param(
            [stats(), stats(150), stats(), stats(130)],
            "reference",
            "regressed",
            id="persistent-same-metric",
        ),
        pytest.param(
            [stats(), stats(150), stats(), stats(cpu=150)],
            "reference",
            None,
            id="different-metric-does-not-confirm",
        ),
        pytest.param(
            [stats(50), stats(), stats(), stats()],
            "reference",
            None,
            id="retry-baseline-too",
        ),
        pytest.param(
            [stats(), stats(150, error="unexpected query failure")],
            "reference",
            "new errors",
            id="query-errors-are-not-retried",
        ),
        pytest.param(
            [stats(), stats(150), stats(), stats(error="unexpected query failure")],
            "reference",
            "new errors",
            id="confirmation-query-error-is-fatal",
        ),
        pytest.param([stats()], None, None, id="no-reference"),
    ],
)
def test_benchmark_paired_confirmation(
    runs: list[dict[str, Any]], compare_against: str | None, failure: str | None
) -> None:
    c = MagicMock(spec=Composition)
    c.query_mz_version.return_value = "test-version"
    with (
        patch.object(executor, "test", side_effect=runs) as replay,
        patch.object(executor, "resolve_tag", return_value="reference"),
        patch.object(executor, "Materialized") as materialized,
        patch.object(executor, "print_workload_stats"),
        patch.object(executor, "plot_docker_stats_compare") as plots,
    ):
        with (
            pytest.raises(FailedTestExecutionError) if failure else nullcontext()
        ) as exc:
            executor.benchmark(
                c=c,
                file=Path("workload.json"),
                workload={},
                compare_against=compare_against,
                factor_initial_data=1,
                factor_ingestions=1,
                factor_queries=1,
                runtime=1,
                verbose=False,
                seed="test-seed",
                early_initial_data=False,
                max_concurrent_queries=1,
            )
        if failure:
            assert exc is not None
            assert any(failure in error.message for error in exc.value.errors)
            if failure == "regressed":
                details = exc.value.errors[0].details
                assert details is not None
                assert "Attempt 1\n" in details and "Attempt 2\n" in details
        assert replay.call_count == len(runs)
        assert c.rm_volumes.call_count == len(runs)
        plot_files = [call.kwargs["file"] for call in plots.call_args_list]
        assert len(set(plot_files)) == len(runs) // 2
        assert [
            call.kwargs["image"] is None for call in materialized.call_args_list
        ] == ([False, True] * (len(runs) // 2) if compare_against else [True])


def test_benchmark_requires_requested_reference() -> None:
    with (
        patch.object(executor, "resolve_tag", return_value=None),
        patch.object(executor, "print_workload_stats"),
        patch.object(executor, "test") as replay,
    ):
        with pytest.raises(ValueError, match="Could not resolve reference"):
            executor.benchmark(
                c=MagicMock(spec=Composition),
                file=Path("workload.json"),
                workload={},
                compare_against="common-ancestor",
                factor_initial_data=1,
                factor_ingestions=1,
                factor_queries=1,
                runtime=1,
                verbose=False,
                seed="test-seed",
                early_initial_data=False,
                max_concurrent_queries=1,
            )
        replay.assert_not_called()
