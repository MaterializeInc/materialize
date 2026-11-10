# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from contextlib import nullcontext
from unittest.mock import Mock, call

import pytest

from materialize import MZ_ROOT, mzbuild
from materialize.checks import mzcompose_actions
from materialize.checks.actions import Initialize, Manipulate, Validate
from materialize.checks.executors import MzcomposeExecutor
from materialize.checks.features import Features
from materialize.checks.mzcompose_actions import StartMz
from materialize.checks.scenarios_zero_downtime import (
    ZeroDowntimeBumpedVersion,
    ZeroDowntimeRestartEntireMz,
    wait_ready_and_promote,
)
from materialize.mz_version import MzVersion
from materialize.mzcompose import DEFAULT_MZ_ENVIRONMENT_ID
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import DeploymentStatus
from materialize.ui import CommandFailureCausedUIError, UIError


def composition():
    c = Mock(compose={"services": {f"mz_{i}": {} for i in range(1, 5)}})
    c.override.return_value = nullcontext()
    c.query_mz_version.return_value = str(MzVersion.parse_cargo())
    return c


@pytest.mark.parametrize("retirement_fails", [False, True])
def test_native_platform_promotion_retires_before_cleanup(retirement_fails):
    c = composition()
    e = MzcomposeExecutor(c)
    if retirement_fails:
        c.retire_mz_deployment.side_effect = UIError("retirement failed")
    for generation in range(3):
        predecessor, survivor = f"mz_{generation + 1}", f"mz_{generation + 2}"
        c.reset_mock()
        with (
            pytest.raises(UIError, match="retirement failed")
            if retirement_fails
            else nullcontext()
        ):
            for action in wait_ready_and_promote(
                survivor, predecessor, previous_generation=generation
            ):
                action.execute(e)
        expected = [
            call.await_mz_deployment_status(
                DeploymentStatus.READY_TO_PROMOTE, survivor, timeout=None
            ),
            call.promote_mz(survivor, retire_mz_service=None),
            call.await_mz_deployment_status(DeploymentStatus.IS_LEADER, survivor),
            call.query_mz_version(service=survivor),
            call.retire_mz_deployment(
                survivor, generation, environment_id=DEFAULT_MZ_ENVIRONMENT_ID
            ),
        ]
        if not retirement_fails:
            expected.append(call.kill_fenced_mz(predecessor))
        assert c.method_calls == expected


def test_historical_platform_promotion_keeps_legacy_cleanup():
    c = composition()
    for action in wait_ready_and_promote("mz_2", "mz_1"):
        action.execute(MzcomposeExecutor(c))
    c.promote_mz.assert_called_once_with("mz_2", retire_mz_service="mz_1")
    c.retire_mz_deployment.assert_not_called()
    c.kill.assert_not_called()


@pytest.mark.parametrize("state", ["stopped", "exit_race", "still_running"])
def test_native_platform_cleanup_tolerates_retirement_exit(monkeypatch, state):
    c = Composition(
        mzbuild.Repository(MZ_ROOT), "platform-checks", resolve_image_specs=False
    )
    retire, wait = Mock(), Mock()
    invoke = Mock()
    if state != "stopped":
        invoke.side_effect = CommandFailureCausedUIError("kill failed", ["kill"])
    monkeypatch.setattr(c, "retire_mz_deployment", retire)
    monkeypatch.setattr(c, "invoke", invoke)
    monkeypatch.setattr(c, "wait", wait)
    monkeypatch.setattr(c, "is_running", Mock(return_value=state == "still_running"))
    with (
        pytest.raises(CommandFailureCausedUIError, match="kill failed")
        if state == "still_running"
        else nullcontext()
    ):
        mzcompose_actions.RetireMzDeployment("mz_2", 0, retired_service="mz_1").execute(
            MzcomposeExecutor(c)
        )
    retire.assert_called_once_with("mz_2", 0, environment_id=DEFAULT_MZ_ENVIRONMENT_ID)
    invoke.assert_called_once_with("kill", "-sSIGKILL", "mz_1")
    if state == "still_running":
        wait.assert_not_called()
    else:
        # The real cleanup must not require SIGKILL's exit code: retirement may
        # already have stopped the predecessor successfully.
        wait.assert_called_once_with("mz_1")


@pytest.mark.parametrize(
    "scenario_type,deployment_count",
    [(ZeroDowntimeRestartEntireMz, 4), (ZeroDowntimeBumpedVersion, 3)],
)
def test_native_platform_preserves_workload_and_shared_relay(
    monkeypatch, scenario_type, deployment_count
):
    c = composition()
    e = MzcomposeExecutor(c)
    scenario = scenario_type([], e, Features([]))
    actions = scenario.actions()
    phases = [
        (
            type(action),
            action.mz_service,
            action.phase if isinstance(action, Manipulate) else None,
        )
        for action in actions
        if isinstance(action, (Initialize, Manipulate, Validate))
    ]
    assert phases == [
        (Initialize, "mz_1", None),
        (Manipulate, "mz_1", 0),
        (Manipulate, "mz_2", 1),
        (Validate, f"mz_{deployment_count - 1}", None),
        (Validate, f"mz_{deployment_count}", None),
    ]
    monkeypatch.setattr(mzcompose_actions, "setup_default_ssh_test_connection", Mock())
    starts = [action for action in actions if isinstance(action, StartMz)]
    assert len(starts) == deployment_count
    for generation, action in enumerate(starts):
        action.execute(e)
        mz = c.override.call_args.args[0]
        assert mz.name == f"mz_{generation + 1}"
        assert f"--deploy-generation={generation}" in mz.config["command"]
        assert "pubsub" in mz.config["depends_on"]
        assert "--persist-pubsub-external" in mz.config["command"]
        assert "--persist-pubsub-url=http://pubsub:6879" in mz.config["command"]
    assert [
        (action.mz_service, action.deploy_generation)
        for action in actions
        if isinstance(action, mzcompose_actions.RetireMzDeployment)
    ] == [
        (f"mz_{generation + 2}", generation)
        for generation in range(deployment_count - 1)
    ]


@pytest.mark.parametrize("tag", [None, MzVersion.parse_mz("v26.45.0")])
def test_platform_other_startups_keep_relay_lifetime(tag):
    c = composition()
    e = MzcomposeExecutor(c)
    scenario = ZeroDowntimeRestartEntireMz([], e, Features([]))
    StartMz(scenario, tag=tag, deploy_generation=1).execute(e)
    config = c.override.call_args.args[0].config
    assert "pubsub" not in config["depends_on"]
    assert "--persist-pubsub-external" not in config["command"]
    assert "--persist-pubsub-url=http://pubsub:6879" not in config["command"]
